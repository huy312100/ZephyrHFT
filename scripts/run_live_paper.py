#!/usr/bin/env python3
"""Phase 2 live-forward paper trading: Phase 1 NDJSON -> C++ paper engine.

Streams a Phase 1 archive (``.ndjson.gz`` or ``.ndjson``) through the C++
``paper_engine`` and submits alternating taker orders so slippage, fees, PnL
and per-frame latency are all observable in real time.

Phase 3.1/3.2 adds an optional chaos layer (``--chaos-mode``) plus the native
dead-man's switch: a feed that goes silent for ``--heartbeat-timeout-ms`` trips
the ``KillSwitch``, orders stop leaving the process, and the registered hooks
cancel resting orders and flatten the position. SIGINT/SIGTERM halts the same
way, so Ctrl+C never leaves a live order behind.
"""
from __future__ import annotations

import argparse
import signal
import sys
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from python_analytics.bridge import PaperSession, feed_archive  # noqa: E402
from python_analytics.chaos import (  # noqa: E402
    ChaosConfig,
    ChaosMode,
    ChaosNetworkInjector,
    HeartbeatWatchdog,
)
from python_analytics.native import hft_native  # noqa: E402
from python_analytics.native.factory import build_kill_switch  # noqa: E402
from python_analytics.utils import configure_logging, load_settings  # noqa: E402

DEFAULT_NDJSON = Path(__file__).resolve().parents[1] / "data" / "sample_events.ndjson"


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--source", default=str(DEFAULT_NDJSON),
                        help="Phase 1 archive (.ndjson.gz or .ndjson)")
    parser.add_argument("--event-limit", type=int, default=0,
                        help="stop after N depth events (0 = whole file)")
    parser.add_argument("--batch-size", type=int, default=64,
                        help="lines per C++ bulk-ingest batch")
    parser.add_argument("--tick-size", type=str, default=None)
    parser.add_argument("--center", type=str, default=None)
    parser.add_argument("--maker-fee-bps", type=float, default=0.0,
                        help="maker fee in basis points; negative = rebate, "
                             "fractions allowed (default: 0.0)")
    parser.add_argument("--taker-fee-bps", type=float, default=5.0,
                        help="taker fee in basis points; fractions allowed "
                             "(default: 5.0 = 0.05%%, 0.5 = 0.005%%)")
    parser.add_argument("--initial-cash", type=float, default=0.0)
    parser.add_argument("--order-quantity", type=str, default="0.05")
    parser.add_argument("--report-every", type=int, default=5,
                        help="print telemetry every N batches")
    parser.add_argument("--log-level", default="INFO")

    chaos = parser.add_argument_group("chaos & resilience (Phase 3.1 / 3.2)")
    chaos.add_argument("--chaos-mode", default="none",
                       choices=["none"] + [mode.value for mode in ChaosMode],
                       help="inject network chaos into the ingest loop")
    chaos.add_argument("--chaos-interval", type=float, default=2.0,
                       help="seconds between outages (disconnect_storm)")
    chaos.add_argument("--chaos-duration", type=float, default=0.0,
                       help="seconds of chaos (0 = whole run)")
    chaos.add_argument("--chaos-disconnect-s", type=float, default=1.0,
                       help="outage length in seconds (disconnect_storm)")
    chaos.add_argument("--latency-min-ms", type=float, default=200.0)
    chaos.add_argument("--latency-max-ms", type=float, default=1000.0)
    chaos.add_argument("--latency-probability", type=float, default=0.15)
    chaos.add_argument("--chaos-seed", type=int, default=None)
    chaos.add_argument("--frame-delay", type=float, default=0.0,
                       help="seconds between frames (0 = as fast as possible)")
    chaos.add_argument("--heartbeat-timeout-ms", type=float, default=500.0,
                       help="feed silence that trips the kill switch (0 disables)")
    chaos.add_argument("--recovery-delay", type=float, default=0.5,
                       help="remediation window before the halt clears")
    chaos.add_argument("--no-auto-recover", action="store_true",
                       help="leave the switch TRIGGERED (operator must reset)")
    chaos.add_argument("--no-kill-switch", action="store_true",
                       help="disable the kill switch and the dead-man's switch")
    return parser.parse_args(argv)


def render(metrics: dict, frames: int, elapsed: float) -> None:
    rate = frames / elapsed if elapsed > 0 else 0.0
    print(
        f"frames={frames:>6}  rate={rate:6.1f}/s  "
        f"us/frame(p50={metrics['p50_ns'] / 1000:7.2f} "
        f"p99={metrics['p99_ns'] / 1000:7.2f})  "
        f"cash={metrics['cash']:>12,.2f}  equity={metrics['equity']:>12,.2f}  "
        f"net={metrics['net_quantity']:+.4f}  "
        f"slippage={metrics['slippage_total']:,.4f}  gaps={metrics['gap_events']}"
    )


def render_summary(metrics: dict, switch: Optional[Any],
                   injector: Optional[ChaosNetworkInjector],
                   watchdog: Optional[HeartbeatWatchdog],
                   remediation: Dict[str, float]) -> None:
    print("=" * 78)
    print("  fills            :", metrics["fills"])
    print("  filled qty       :", metrics["filled_quantity"])
    print("  traded notional  :", metrics["traded_notional"])
    print("  slippage total   :", metrics["slippage_total"])
    print("  fees paid        :", metrics["fees_paid"])
    print("  realized pnl     :", metrics["realized_pnl"])
    print("  unrealized pnl   :", metrics["unrealized_pnl"])
    print("  equity           :", metrics["equity"])
    print("  net position     :", metrics["net_quantity"])
    print("  open orders      :", metrics["open_orders"])
    print("  latency p99      :", f"{metrics['p99_ns'] / 1000:.2f} us")
    if injector is not None:
        print("-" * 78)
        for line in injector.summary().splitlines():
            print("  " + line)
    if watchdog is not None:
        stats = watchdog.stats()
        print("  heartbeat        :",
              f"beats={stats['monitor_beats']} "
              f"timeouts={stats['monitor_timeouts']} "
              f"recoveries={stats['recoveries']} "
              f"max_silence={stats['monitor_max_silence_ms']:.0f}ms")
    if switch is not None:
        stats = switch.get_stats()
        print("  kill switch      :",
              f"state={switch.get_state().name} trips={stats.trip_count} "
              f"blocked={stats.blocked_orders} resets={stats.reset_count}")
        print("  last halt        :", stats.last_trip_reason or "-")
    print("  remediation      :",
          f"cancelled={remediation.get('cancelled', 0):.0f} "
          f"flattened={remediation.get('flattened', 0.0):.8f}")
    print("=" * 78)


def install_signal_handlers(stop: threading.Event,
                            injector: Optional[ChaosNetworkInjector]) -> Dict[int, Any]:
    """SIGINT/SIGTERM -> stop flag, so the loop ends at a frame boundary."""
    previous: Dict[int, Any] = {}

    def handler(signum: int, _frame: Any) -> None:
        print(f"\n  signal {signal.Signals(signum).name}: shutting down cleanly")
        if injector is not None:
            injector.request_stop()
        stop.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            previous[sig] = signal.getsignal(sig)
            signal.signal(sig, handler)
        except (ValueError, OSError):  # non-main thread or exotic platform
            pass
    return previous


def run(args: argparse.Namespace) -> int:
    settings = load_settings()
    tick_size = args.tick_size or settings.engine.tick_size
    center = args.center or settings.engine.center

    switch = None if args.no_kill_switch else build_kill_switch(settings)
    session = PaperSession.create(
        tick_size=tick_size,
        center=center,
        maker_bps=args.maker_fee_bps,
        taker_bps=args.taker_fee_bps,
        initial_cash=args.initial_cash,
        kill_switch=switch,
    )
    if switch is not None:
        switch.set_trip_hook(
            lambda state, reason: print(f"  [KILL SWITCH] {state}: {reason}"))

    monitor = None
    if switch is not None and args.heartbeat_timeout_ms > 0:
        heartbeat_config = hft_native.HeartbeatConfig()
        heartbeat_config.timeout_ns = int(args.heartbeat_timeout_ms * 1_000_000)
        monitor = hft_native.HeartbeatMonitor(heartbeat_config)
        monitor.arm(switch)
        monitor.beat()

    injector = None
    if args.chaos_mode != "none":
        config = ChaosConfig(
            mode=ChaosMode(args.chaos_mode),
            interval_s=args.chaos_interval,
            duration_s=args.chaos_duration,
            disconnect_s=args.chaos_disconnect_s,
            latency_min_ms=args.latency_min_ms,
            latency_max_ms=args.latency_max_ms,
            latency_probability=args.latency_probability,
            seed=args.chaos_seed,
        )
        # The gate below owns the beat, so the monitor stays alive even when no
        # chaos is configured (a silent feed is a halt in either case).
        injector = ChaosNetworkInjector(config, kill_switch=switch)

    watchdog = None
    if monitor is not None:
        watchdog = HeartbeatWatchdog(
            monitor,
            kill_switch=switch,
            auto_recover=not args.no_auto_recover,
            recovery_delay_s=args.recovery_delay,
        )

    state: Dict[str, Any] = {
        "frames": 0,
        "batches": 0,
        "buy_next": True,
        "started": time.monotonic(),
        "blocked": 0,
        "remediation": {"cancelled": 0.0, "flattened": 0.0},
    }
    stop = threading.Event()

    def on_batch(batch: Dict[str, int]) -> None:
        state["batches"] += 1
        state["frames"] += batch["depth"]
        outcome = session.service_remediation()
        if outcome:
            for key in ("cancelled", "flattened"):
                state["remediation"][key] += outcome.get(key, 0.0)
        if stop.is_set():
            return
        side = hft_native.Side.BUY if state["buy_next"] else hft_native.Side.SELL
        state["buy_next"] = not state["buy_next"]
        result = session.market(side, args.order_quantity)
        if result.get("blocked"):
            state["blocked"] += 1
        elif not result["accepted"]:
            print(f"  reject: {result['reason']}", file=sys.stderr)
        if state["batches"] % args.report_every == 0:
            render(session.metrics(), state["frames"],
                   time.monotonic() - state["started"])
        if args.event_limit and state["frames"] >= args.event_limit:
            raise StopIteration

    def frame_gate(sequence: int) -> bool:
        if args.frame_delay > 0:
            time.sleep(args.frame_delay)
        forward = injector.admit(sequence) if injector is not None else True
        if forward and monitor is not None:
            # One beat per delivered frame: silence is what the watchdog sees
            # during an outage or a lag spike, not a timer of its own.
            monitor.beat()
        return forward

    def should_stop() -> bool:
        return stop.is_set()

    previous = install_signal_handlers(stop, injector)
    if watchdog is not None:
        watchdog.start()
    try:
        try:
            feed_archive(session.engine, Path(args.source),
                         batch_size=args.batch_size, on_batch=on_batch,
                         frame_gate=frame_gate, should_stop=should_stop)
        except StopIteration:
            pass
        except KeyboardInterrupt:
            print("\n  Interrupted: halting cleanly")
    finally:
        stop.set()
        if injector is not None:
            injector.request_stop()
        if watchdog is not None:
            watchdog.stop()
            watchdog.join(2.0)
        # Graceful exit: trip the kill switch so the registered hooks cancel
        # resting orders and flatten the position; then run them on this thread.
        if switch is not None:
            session.service_remediation()
            switch.trip("shutdown: live paper exit")
            switch.begin_recovery()
            outcome = session.service_remediation()
            if outcome:
                for key in ("cancelled", "flattened"):
                    state["remediation"][key] += outcome.get(key, 0.0)
            # Leave the process in a defined state; the halt stays in the audit
            # trail either way.
            if not args.no_auto_recover:
                switch.reset()
        else:
            # No switch installed: still leave no live orders behind.
            state["remediation"]["cancelled"] += float(session.cancel_all())
            state["remediation"]["flattened"] += float(session.flatten()["flattened"])
        for sig, handler in previous.items():
            try:
                signal.signal(sig, handler)
            except (ValueError, OSError):
                pass

    metrics = session.metrics()
    render(metrics, state["frames"], time.monotonic() - state["started"])
    render_summary(metrics, switch, injector, watchdog, state["remediation"])
    return 0


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    configure_logging(args.log_level)
    try:
        return run(args)
    except ValueError as exc:
        # Invalid chaos schedule (e.g. disconnect_s >= interval_s).
        print(f"invalid configuration: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
