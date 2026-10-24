#!/usr/bin/env python3
"""Phase 3 chaos & stress harness: disconnect storms and latency injection.

Drives the real paper session (``PaperSession`` -> C++ ``paper_engine``)
through an unstable feed and verifies that the engine survives, that the
heartbeat dead-man's switch trips, and that the ``KillSwitch`` hooks cancel and
flatten on shutdown.

Modes
-----
``disconnect_storm``
    Blocks the feed for ``--disconnect-s`` every ``--interval`` seconds. Frames
    inside an outage never reach the engine, so the book misses updates and the
    heartbeat goes silent until the "socket" returns; the C++ gap detector
    flags the missed sequence ids when the feed carries ``pu``. Reconnects bump
    the connection epoch.
``latency_injection``
    Delays ``--latency-probability`` of frames by ``uniform(min, max)`` ms. The
    book keeps updating, but every so often a frame arrives 200-1000 ms late --
    what a congested gateway looks like -- and the dead-man's switch has to
    decide whether the feed is still alive.

Markers
-------
``!!`` disconnect   ``++`` reconnect   ``~~`` latency spike   ``..`` shutdown
``!!!`` heartbeat timeout / kill-switch trip (CRITICAL)

Examples
--------
    python3 tests/chaos_network_test.py --mode disconnect_storm \\
        --interval 2 --disconnect-s 1 --duration 8
    python3 tests/chaos_network_test.py --mode latency_injection \\
        --latency-min-ms 200 --latency-max-ms 1000 --duration 8 --seed 7

Exit codes
----------
``0`` PASS, ``2`` FAIL (see the checks table), ``130`` interrupted.
"""
from __future__ import annotations

import argparse
import json
import logging
import signal
import sys
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

# Make the repository root importable when this file is run directly.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from python_analytics.bridge import PaperSession, feed_archive  # noqa: E402
from python_analytics.chaos import (  # noqa: E402
    ChaosConfig,
    ChaosMode,
    ChaosNetworkInjector,
    HeartbeatWatchdog,
)
from python_analytics.costs import DEFAULT_MAKER_BPS, DEFAULT_TAKER_BPS  # noqa: E402
from python_analytics.native import hft_native  # noqa: E402
from python_analytics.native.factory import build_kill_switch  # noqa: E402
from python_analytics.utils import configure_logging, load_settings  # noqa: E402

logger = logging.getLogger("zephyr.chaos.cli")

DEFAULT_SOURCE = Path(__file__).resolve().parents[1] / "data" / "sample_events.ndjson"

RESTING_CLIENT_ID = 10_000


@dataclass
class Counters:
    """Harness-side telemetry that the injector does not own."""

    batches: int = 0
    depth: int = 0
    accepted: int = 0
    blocked: int = 0
    rejected: int = 0
    passes: int = 0
    gate_errors: int = 0
    cancelled: float = 0.0
    flattened: float = 0.0
    resting_accepted: Optional[bool] = None
    resting_client_id: int = RESTING_CLIENT_ID
    next_report: float = 0.0
    # Trips observed before the harness trips the switch to shut down: the exit
    # is deliberate, so it must not colour the chaos verdict.
    trips_before_shutdown: int = 0


@dataclass
class Check:
    """One post-run invariant."""

    name: str
    ok: bool
    detail: str = ""

    def render(self) -> str:
        return f"  [{'PASS' if self.ok else 'FAIL'}] {self.name:<34} {self.detail}"


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--mode", required=True,
                        choices=[mode.value for mode in ChaosMode],
                        help="chaos profile to inject")
    parser.add_argument("--source", default=str(DEFAULT_SOURCE),
                        help="NDJSON archive replayed through the paper engine")
    parser.add_argument("--duration", type=float, default=8.0,
                        help="wall-clock duration in seconds (0 = single pass)")
    parser.add_argument("--interval", type=float, default=2.0,
                        help="seconds between scheduled outages (disconnect_storm)")
    parser.add_argument("--disconnect-s", type=float, default=1.0,
                        help="length of each outage in seconds")
    parser.add_argument("--latency-min-ms", type=float, default=200.0,
                        help="minimum injected lag (latency_injection)")
    parser.add_argument("--latency-max-ms", type=float, default=1000.0,
                        help="maximum injected lag (latency_injection)")
    parser.add_argument("--latency-probability", type=float, default=0.15,
                        help="fraction of frames delayed (latency_injection)")
    parser.add_argument("--seed", type=int, default=None,
                        help="RNG seed so a latency run is reproducible")
    parser.add_argument("--replay-delay", type=float, default=0.01,
                        help="seconds between frames (feed pacing)")
    parser.add_argument("--heartbeat-timeout-ms", type=float, default=500.0,
                        help="feed silence that trips the kill switch (0 disables)")
    parser.add_argument("--recovery-delay", type=float, default=0.5,
                        help="remediation window before the halt clears")
    parser.add_argument("--no-auto-recover", action="store_true",
                        help="leave the switch TRIGGERED instead of resetting it")
    parser.add_argument("--no-kill-switch", action="store_true",
                        help="run the chaos without the dead-man's switch")
    parser.add_argument("--order-quantity", default="0.05")
    parser.add_argument("--maker-bps", type=float, default=DEFAULT_MAKER_BPS,
                        help="maker fee in fractional basis points "
                             "(default: 2.0 = retail VIP 0; negative = rebate)")
    parser.add_argument("--taker-bps", type=float, default=DEFAULT_TAKER_BPS,
                        help="taker fee in fractional basis points "
                             "(default: 5.0 = retail VIP 0)")
    parser.add_argument("--initial-cash", type=float, default=100_000.0)
    parser.add_argument("--tick-size", default=None)
    parser.add_argument("--center", default=None)
    parser.add_argument("--batch-size", type=int, default=32,
                        help="lines per C++ bulk-ingest batch")
    parser.add_argument("--report-every", type=float, default=2.0,
                        help="seconds between live telemetry lines")
    parser.add_argument("--json-out", default=None,
                        help="optional path for the machine-readable report")
    parser.add_argument("--log-level", default="INFO")
    return parser.parse_args(argv)


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def install_signal_handlers(
    stop: threading.Event,
    injector: Optional[ChaosNetworkInjector],
    on_signal: Optional[Any] = None,
) -> Dict[int, Any]:
    """Routes SIGINT/SIGTERM to the stop flag; returns the previous handlers.

    The injector is told first so it aborts a `sleep` slice immediately, then
    the loop flag stops the feed at the next frame boundary. Nothing raises out
    of the handler, so the C++ engine is never unwound mid-call.
    """
    previous: Dict[int, Any] = {}

    def handler(signum: int, _frame: Any) -> None:
        logger.warning(".. SHUTDOWN  signal=%s", signal.Signals(signum).name)
        if on_signal is not None:
            on_signal(signum)
        if injector is not None:
            injector.request_stop()
        stop.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            previous[sig] = signal.getsignal(sig)
            signal.signal(sig, handler)
        except (ValueError, OSError):  # non-main thread (tests) or platform limit
            logger.debug("cannot install a handler for signal %s", sig)
    return previous


def restore_signal_handlers(previous: Dict[int, Any]) -> None:
    for sig, handler in previous.items():
        try:
            signal.signal(sig, handler)
        except (ValueError, OSError):
            pass


def seed_resting_order(session: PaperSession, quantity: str,
                       client_id: int = RESTING_CLIENT_ID) -> Optional[bool]:
    """Leaves one post-only bid eight ticks below the touch.

    It gives the cancel hook something real to cancel: cancelling an empty
    order book proves nothing. Makers re-quote continuously, so the harness
    re-seeds it whenever the previous one was filled or cancelled.
    """
    bbo = session.engine.bbo()
    if not bbo.is_valid():
        return None
    tick = session.engine.tick_size()
    request = hft_native.OrderRequest()
    request.client_id = client_id
    request.side = hft_native.Side.BUY
    request.type = hft_native.OrderType.POST_ONLY
    request.price = hft_native.Price.from_raw(bbo.bid_price.raw() - 8 * tick.raw())
    request.quantity = hft_native.Quantity.from_string(quantity)
    ack = session.engine.submit(request)
    return bool(ack.accepted())


def render_telemetry(elapsed: float, counters: Counters, session: PaperSession,
                     switch: Optional[Any]) -> None:
    """One compact status line, printed on a fixed cadence."""
    metrics = session.metrics()
    state = session.order_state() or "n/a"
    print(
        f"  t=+{elapsed:6.2f}s  frames={counters.depth:>6}  "
        f"acc={counters.accepted:>5}  blocked={counters.blocked:>4}  "
        f"rej={counters.rejected:>3}  gaps={metrics['gap_events']:>3}  "
        f"state={state:<10}  equity={metrics['equity']:>13,.2f}"
    )


def build_checks(
    args: argparse.Namespace,
    config: ChaosConfig,
    injector: ChaosNetworkInjector,
    watchdog_stats: Optional[Dict[str, Any]],
    switch: Optional[Any],
    counters: Counters,
    engine_metrics: Dict[str, float],
    state_after_chaos: str,
    expect_trip: bool,
) -> List[Check]:
    """The invariants a chaos run has to satisfy to be called a pass."""
    chaos = injector.stats
    checks: List[Check] = []

    checks.append(Check(
        "feed survived",
        chaos.frames_forwarded > 0 and engine_metrics["depth_events"] > 0,
        f"{chaos.frames_forwarded} forwarded, "
        f"{engine_metrics['depth_events']} depth events ingested",
    ))
    checks.append(Check(
        "injector stayed healthy",
        counters.gate_errors == 0,
        "no gate exception"
        if counters.gate_errors == 0
        else f"{counters.gate_errors} gate exception(s), frames failed open",
    ))

    if config.mode is ChaosMode.DISCONNECT_STORM:
        checks.append(Check(
            "disconnect storm executed",
            chaos.outages >= 1 and chaos.reconnects == chaos.outages,
            f"{chaos.outages} outages / {chaos.reconnects} reconnects, "
            f"epoch={injector.epoch}",
        ))
    else:
        checks.append(Check(
            "latency spikes injected",
            chaos.latency_events >= 1,
            f"{chaos.latency_events} spikes, mean={chaos.mean_latency_ms:.0f}ms, "
            f"max={chaos.max_latency_ms:.0f}ms",
        ))

    if switch is None:
        checks.append(Check("dead-man's switch", True, "disabled (--no-kill-switch)"))
    elif watchdog_stats is None:
        checks.append(Check("dead-man's switch", True, "disabled (timeout 0)"))
    elif expect_trip:
        trips = counters.trips_before_shutdown
        timeouts = int(watchdog_stats["timeouts"])
        checks.append(Check(
            "dead-man's switch tripped",
            trips >= 1 and timeouts >= 1,
            f"trips={trips}, watchdog timeouts={timeouts}, "
            f"limit={args.heartbeat_timeout_ms:.0f}ms",
        ))
        checks.append(Check(
            "order path stayed alive",
            counters.accepted + counters.rejected + counters.blocked > 0,
            f"accepted={counters.accepted}, blocked={counters.blocked}, "
            f"rejected={counters.rejected}",
        ))
        if not args.no_auto_recover:
            checks.append(Check(
                "recovered to NORMAL",
                state_after_chaos == "NORMAL",
                f"state={state_after_chaos}, "
                f"{int(watchdog_stats['recoveries'])} recovery cycle(s)",
            ))
    else:
        checks.append(Check(
            "dead-man's switch idle",
            counters.trips_before_shutdown == 0,
            "no trip during the run "
            f"(trips={counters.trips_before_shutdown}, "
            f"limit={args.heartbeat_timeout_ms:.0f}ms)",
        ))

    checks.append(Check(
        "engine left flat and clean",
        engine_metrics["open_orders"] == 0
        and abs(engine_metrics["net_quantity"]) < 1e-9,
        f"open_orders={engine_metrics['open_orders']}, "
        f"net={engine_metrics['net_quantity']:+.8f}",
    ))
    return checks


def wait_for_state(switch: Any, target: Any, timeout: float,
                   heartbeat: Optional[Any] = None) -> str:
    """Polls the switch until it reaches ``target`` (or the timeout expires).

    ``heartbeat`` is beaten on every poll: automatic recovery only clears the
    halt once a *fresh* frame has arrived, so a caller that has stopped feeding
    must keep the heartbeat alive for the recovery to complete.
    """
    deadline = time.monotonic() + max(0.0, timeout)
    while time.monotonic() < deadline:
        if switch.get_state() == target:
            break
        if heartbeat is not None:
            heartbeat.beat()
        time.sleep(0.02)
    return str(switch.get_state().name)


# --------------------------------------------------------------------------- #
# Runner
# --------------------------------------------------------------------------- #
def run(args: argparse.Namespace) -> int:
    settings = load_settings()
    tick_size = args.tick_size or settings.engine.tick_size
    center = args.center or settings.engine.center
    source = Path(args.source).expanduser()
    if not source.exists():
        logger.error("feed source not found: %s", source)
        return 2

    config = ChaosConfig(
        mode=ChaosMode(args.mode),
        interval_s=args.interval,
        duration_s=args.duration,
        disconnect_s=args.disconnect_s,
        latency_min_ms=args.latency_min_ms,
        latency_max_ms=args.latency_max_ms,
        latency_probability=args.latency_probability,
        seed=args.seed,
    )

    cadence_ms = args.replay_delay * 1000.0
    if 0 < args.heartbeat_timeout_ms <= cadence_ms * 2:
        logger.warning(
            "heartbeat timeout %.0fms is close to the %.0fms frame cadence; "
            "healthy jitter may trip the switch",
            args.heartbeat_timeout_ms, cadence_ms,
        )
    if (config.mode is ChaosMode.DISCONNECT_STORM and 0 < args.duration <= args.interval):
        # Fail fast: a storm cannot start before the first interval elapses.
        logger.error(
            "duration %.1fs is not longer than one chaos interval (%.1fs): no "
            "outage can occur. Use --interval < %.1f or a longer --duration.",
            args.duration, args.interval, args.duration,
        )
        return 2

    switch = None if args.no_kill_switch else build_kill_switch(settings)
    session = PaperSession.create(
        tick_size=tick_size,
        center=center,
        maker_bps=args.maker_bps,
        taker_bps=args.taker_bps,
        initial_cash=args.initial_cash,
        kill_switch=switch,
    )
    if switch is not None:
        switch.set_trip_hook(
            lambda state, reason: logger.critical(
                "!!! KILL SWITCH  state=%s  reason=%s", state, reason))

    monitor = None
    if switch is not None and args.heartbeat_timeout_ms > 0:
        heartbeat_config = hft_native.HeartbeatConfig()
        heartbeat_config.timeout_ns = int(args.heartbeat_timeout_ms * 1_000_000)
        monitor = hft_native.HeartbeatMonitor(heartbeat_config)
        monitor.arm(switch)
        # Warm start: the expiry window begins now, not at construction.
        monitor.beat()

    injector = ChaosNetworkInjector(config, kill_switch=switch)
    watchdog = None
    if monitor is not None:
        watchdog = HeartbeatWatchdog(
            monitor,
            kill_switch=switch,
            auto_recover=not args.no_auto_recover,
            recovery_delay_s=args.recovery_delay,
        )

    stop = threading.Event()
    run_state: Dict[str, Any] = {"interrupted": False}
    counters = Counters(next_report=time.monotonic() + args.report_every)
    started = time.monotonic()

    def gate(sequence: int) -> bool:
        # Pace the replay so the schedule runs in wall-clock time, then let the
        # injector decide whether this frame survives.
        if args.replay_delay > 0:
            time.sleep(args.replay_delay)
        forward = injector.admit(sequence)
        if forward and monitor is not None:
            # A delivered frame is the heartbeat: an outage or a lag spike is
            # exactly the absence of this call.
            monitor.beat()
        return forward

    def time_up() -> bool:
        """Stop condition polled by the bridge at every frame boundary."""
        if stop.is_set():
            return True
        return args.duration > 0 and (time.monotonic() - started) >= args.duration

    def on_batch(batch: Dict[str, int]) -> None:
        counters.batches += 1
        counters.depth += int(batch["depth"])
        # Safe point for kill-switch remediation: this thread owns the engine.
        outcome = session.service_remediation()
        if outcome:
            counters.cancelled += outcome.get("cancelled", 0.0)
            counters.flattened += outcome.get("flattened", 0.0)
        if counters.resting_accepted is None and counters.depth > 0:
            counters.resting_accepted = seed_resting_order(session, args.order_quantity)
        elif (counters.batches % 8 == 0
              and session.order_state() in (None, "NORMAL")
              and session.metrics()["open_orders"] == 0):
            # A maker re-quotes when its order is gone -- but never while the
            # switch is halting, so seeding cannot bypass a halt.
            counters.resting_client_id += 1
            counters.resting_accepted = seed_resting_order(
                session, args.order_quantity, counters.resting_client_id)
        if stop.is_set():
            return
        # Two buys per sell keeps a small long on the book: a maker carries
        # inventory, and the shutdown flatten then has something to close.
        side = (hft_native.Side.SELL if counters.batches % 3 == 0
                else hft_native.Side.BUY)
        result = session.market(side, args.order_quantity)
        if result.get("blocked"):
            counters.blocked += 1
        elif result["accepted"]:
            counters.accepted += 1
        else:
            counters.rejected += 1
        now = time.monotonic()
        if now >= counters.next_report:
            counters.next_report = now + max(0.2, args.report_every)
            render_telemetry(now - started, counters, session, switch)

    previous = install_signal_handlers(
        stop, injector, lambda _signum: run_state.__setitem__("interrupted", True))
    if watchdog is not None:
        watchdog.start()

    logger.info("run start: mode=%s source=%s duration=%.1fs heartbeat=%s",
                config.mode.value, source.name, args.duration,
                f"{args.heartbeat_timeout_ms:.0f}ms" if monitor else "off")

    elapsed = 0.0
    expect_trip = False
    state_after_chaos = "n/a"
    shutdown: Dict[str, float] = {"cancelled": 0.0, "flattened": 0.0}
    try:
        while not time_up():
            totals = feed_archive(
                session.engine,
                source,
                batch_size=args.batch_size,
                on_batch=on_batch,
                frame_gate=gate,
                should_stop=time_up,
            )
            counters.passes += 1
            counters.gate_errors += int(totals["gate_errors"])
            if args.duration <= 0 or totals["stopped"]:
                break

        elapsed = time.monotonic() - started
        # The observed silence is the injected disruption plus one frame of
        # pacing, so the expectation has to include that margin.
        expect_trip = bool(
            monitor is not None
            and config.expected_silence_ms + cadence_ms > args.heartbeat_timeout_ms
        )
        if switch is not None:
            if expect_trip and not args.no_auto_recover:
                state_after_chaos = wait_for_state(
                    switch, hft_native.SystemState.NORMAL,
                    args.recovery_delay + 3.0,
                    heartbeat=monitor,
                )
            else:
                state_after_chaos = str(switch.get_state().name)
    except KeyboardInterrupt:  # pragma: no cover - the handler normally swallows it
        logger.warning(".. SHUTDOWN  keyboard interrupt")
        stop.set()
        injector.request_stop()
        elapsed = time.monotonic() - started
        if switch is not None:
            state_after_chaos = str(switch.get_state().name)
    finally:
        stop.set()
        injector.request_stop()
        if watchdog is not None:
            watchdog.stop()
            watchdog.join(2.0)
        if switch is not None:
            # Drain anything a previous trip staged, then halt for the exit:
            # cancel resting orders and flatten the residual position through
            # the registered KillSwitch hooks (on this thread).
            session.service_remediation()
            logger.warning(".. SHUTDOWN  state=%s -> cancel_all + flatten",
                           switch.get_state().name)
            # The exit trip below is deliberate; keep it out of the verdict.
            counters.trips_before_shutdown = int(switch.get_stats().trip_count)
            switch.trip("shutdown: chaos harness exit")
            switch.begin_recovery()
            outcome = session.service_remediation()
            if outcome:
                shutdown = {
                    "cancelled": float(outcome.get("cancelled", 0.0)),
                    "flattened": float(outcome.get("flattened", 0.0)),
                }
            # A graceful exit ends in a defined state; with --no-auto-recover the
            # halt is deliberately left in place for an operator.
            if not args.no_auto_recover:
                switch.reset()
        else:
            # No switch installed, but a clean exit must still leave no live
            # orders behind.
            logger.warning(".. SHUTDOWN  no kill switch -> cancel_all + flatten")
            shutdown = {
                "cancelled": float(session.cancel_all()),
                "flattened": float(session.flatten()["flattened"]),
            }
        restore_signal_handlers(previous)

    engine_metrics = session.metrics()
    watchdog_stats = watchdog.stats() if watchdog is not None else None
    checks = build_checks(args, config, injector, watchdog_stats, switch, counters,
                          engine_metrics, state_after_chaos, expect_trip)
    passed = all(check.ok for check in checks)

    print_report(args, config, injector, switch, watchdog_stats, counters,
                 engine_metrics, checks, shutdown, state_after_chaos, elapsed,
                 interrupted=bool(run_state["interrupted"]))

    if args.json_out:
        report = {
            "mode": config.mode.value,
            "source": str(source),
            "elapsed_s": round(elapsed, 3),
            "args": vars(args),
            "chaos": injector.snapshot(),
            "heartbeat": watchdog_stats,
            "kill_switch": None if switch is None else {
                "state_after_chaos": state_after_chaos,
                "state": str(switch.get_state().name),
                "trip_count": switch.get_stats().trip_count,
                "trip_count_before_shutdown": counters.trips_before_shutdown,
                "blocked_orders": switch.get_stats().blocked_orders,
                "reset_count": switch.get_stats().reset_count,
                "last_trip_reason": switch.get_stats().last_trip_reason,
            },
            "engine": engine_metrics,
            "orders": {
                "accepted": counters.accepted,
                "blocked": counters.blocked,
                "rejected": counters.rejected,
                "resting_seeded": counters.resting_accepted,
            },
            "shutdown": shutdown,
            "checks": [
                {"name": check.name, "ok": check.ok, "detail": check.detail}
                for check in checks
            ],
            "passed": passed,
        }
        path = Path(args.json_out).expanduser()
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(report, indent=2), encoding="utf-8")
        logger.info("report written: %s", path)

    if run_state["interrupted"]:
        # The partial report above still shows what was halted and mitigated.
        return 130
    return 0 if passed else 2


def print_report(
    args: argparse.Namespace,
    config: ChaosConfig,
    injector: ChaosNetworkInjector,
    switch: Optional[Any],
    watchdog_stats: Optional[Dict[str, Any]],
    counters: Counters,
    engine_metrics: Dict[str, float],
    checks: List[Check],
    shutdown: Dict[str, float],
    state_after_chaos: str,
    elapsed: float,
    interrupted: bool = False,
) -> None:
    """Prints the post-run evidence table and the PASS/FAIL verdict."""
    width = 78
    print("=" * width)
    print("  CHAOS STRESS HARNESS  (Phase 3.1 / 3.2)")
    print("=" * width)
    print(f"  scenario        : {config.mode.value}")
    print(f"  duration        : {elapsed:.2f}s "
          f"({counters.passes} archive pass(es))")
    print("  chaos           :")
    for line in injector.summary().splitlines():
        print(f"    {line}")
    if watchdog_stats is not None:
        print(f"  heartbeat       : limit={args.heartbeat_timeout_ms:.0f}ms  "
              f"beats={watchdog_stats['monitor_beats']}  "
              f"timeouts={watchdog_stats['monitor_timeouts']}  "
              f"max_silence={watchdog_stats['monitor_max_silence_ms']:.0f}ms  "
              f"recoveries={watchdog_stats['recoveries']}")
    else:
        print("  heartbeat       : disabled")
    if switch is not None:
        stats = switch.get_stats()
        print(f"  kill switch     : state={state_after_chaos}  "
              f"trips={stats.trip_count} "
              f"(pre-exit {counters.trips_before_shutdown})  "
              f"blocked={stats.blocked_orders}  "
              f"resets={stats.reset_count}")
        print(f"  last halt       : {stats.last_trip_reason or '-'}")
    else:
        print("  kill switch     : disabled")
    print(f"  engine          : depth={engine_metrics['depth_events']}  "
          f"gaps={engine_metrics['gap_events']}  "
          f"fills={engine_metrics['fills']}  "
          f"equity={engine_metrics['equity']:,.2f}  "
          f"net={engine_metrics['net_quantity']:+.8f}")
    print(f"  orders          : accepted={counters.accepted}  "
          f"blocked={counters.blocked}  rejected={counters.rejected}  "
          f"resting_seeded={counters.resting_accepted}")
    print(f"  remediation     : during run cancelled={counters.cancelled:.0f}  "
          f"flattened={counters.flattened:.8f}")
    print(f"  shutdown        : exit cancelled={shutdown['cancelled']:.0f}  "
          f"flattened={shutdown['flattened']:.8f}  "
          f"open_orders={engine_metrics['open_orders']}")
    print("-" * width)
    print("  CHECKS")
    for check in checks:
        print(check.render())
    print("=" * width)
    if interrupted:
        verdict = "INTERRUPTED (partial run, exit 130)"
    else:
        verdict = "PASS" if all(check.ok for check in checks) else "FAIL"
    print(f"  RESULT: {verdict}")
    print("=" * width)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    configure_logging(args.log_level)
    try:
        return run(args)
    except ValueError as exc:
        logger.error("invalid chaos configuration: %s", exc)
        return 2
    except KeyboardInterrupt:  # pragma: no cover - handler covers the normal path
        print("\nInterrupted by user; shutting down cleanly.")
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
