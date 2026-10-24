"""Phase 3.1 / 3.2 chaos injection and dead-man's switch tests.

Two layers are covered:

* ``python_analytics.chaos`` — the frame-gate schedule (deterministic, driven by
  a fake clock) and the native ``HeartbeatMonitor`` driven through
  ``HeartbeatWatchdog``;
* the bridge seam — ``feed_archive(frame_gate=..., should_stop=...)`` and
  ``PaperSession`` order gating / staged cancel + flatten hooks.

The CLI in ``tests/chaos_network_test.py`` is exercised end-to-end so the
harness that a human runs is the same code path the suite validates.
"""
from __future__ import annotations

import importlib.util
import sys
import time
from pathlib import Path
from typing import List, Optional

import pytest

from python_analytics.bridge import PaperSession, feed_archive
from python_analytics.chaos import (
    ChaosConfig,
    ChaosEventType,
    ChaosMode,
    ChaosNetworkInjector,
    HeartbeatWatchdog,
)
from python_analytics.native import hft_native

ROOT = Path(__file__).resolve().parents[2]

DEPTH1 = (
    '{"e":"depthUpdate","E":1000,"U":1,"u":2,"pu":0,'
    '"b":[["30000.00","10"],["29999.50","4"]],'
    '"a":[["30000.50","2"],["30001.00","3"]]}'
)


class FakeClock:
    """Monotonic clock whose only source of movement is ``sleep``/``tick``."""

    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.now += max(0.0, seconds)

    def tick(self, seconds: float) -> None:
        self.now += seconds


class CountingHeartbeat:
    def __init__(self) -> None:
        self.beats = 0

    def beat(self) -> None:
        self.beats += 1


def _injector(config: ChaosConfig, clock: Optional[FakeClock] = None,
              **kwargs: object) -> ChaosNetworkInjector:
    clock = clock or FakeClock()
    return ChaosNetworkInjector(config, clock=clock, sleep=clock.sleep, **kwargs)


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
def test_config_defaults_and_mode_coercion() -> None:
    config = ChaosConfig()
    assert config.mode is ChaosMode.DISCONNECT_STORM
    assert config.interval_s == pytest.approx(2.0)
    assert config.disconnect_s == pytest.approx(1.0)
    assert config.expected_silence_ms == pytest.approx(1000.0)

    latency = ChaosConfig(mode="latency_injection", latency_max_ms=900.0)
    assert latency.mode is ChaosMode.LATENCY_INJECTION
    assert latency.expected_silence_ms == pytest.approx(900.0)


@pytest.mark.parametrize(
    "overrides, message",
    [
        ({"interval_s": 0.0}, "interval_s"),
        ({"disconnect_s": 0.0}, "disconnect_s"),
        ({"interval_s": 1.0, "disconnect_s": 1.0}, "shorter than interval_s"),
        ({"latency_min_ms": 0.0}, "latency window"),
        ({"latency_min_ms": 500.0, "latency_max_ms": 100.0}, "latency window"),
        ({"latency_probability": 1.5}, "latency_probability"),
        ({"duration_s": -1.0}, "duration_s"),
    ],
)
def test_config_rejects_invalid_schedules(overrides: dict, message: str) -> None:
    with pytest.raises(ValueError, match=message):
        ChaosConfig(**overrides)


def test_latency_mode_ignores_the_disconnect_ordering() -> None:
    # Only the storm needs disconnect_s < interval_s; latency mode never sleeps
    # for an outage, so an unrelated default must not fail validation.
    config = ChaosConfig(mode="latency_injection", interval_s=0.5, disconnect_s=1.0)
    assert config.mode is ChaosMode.LATENCY_INJECTION


# --------------------------------------------------------------------------- #
# Injector schedule
# --------------------------------------------------------------------------- #
def test_disconnect_storm_blocks_the_feed_and_reconnects() -> None:
    clock = FakeClock()
    heartbeat = CountingHeartbeat()
    injector = _injector(
        ChaosConfig(mode="disconnect_storm", interval_s=0.1, disconnect_s=0.05),
        clock=clock,
        heartbeat=heartbeat,
    )

    forwarded: List[bool] = []
    for sequence in range(40):
        clock.tick(0.01)
        forwarded.append(injector.admit(sequence))

    stats = injector.stats
    assert forwarded[0] is True  # the run always starts on a healthy feed
    # How many outages fit in the run depends on the frame cadence plus the
    # outage sleeps themselves; the invariants below are what must hold.
    assert stats.outages >= 3
    assert stats.reconnects == stats.outages
    assert stats.frames_dropped == stats.outages
    assert stats.frames_seen == 40
    assert stats.frames_forwarded == 40 - stats.frames_dropped
    assert sum(forwarded) == stats.frames_forwarded
    assert injector.epoch == stats.outages
    # The outage is the longest silence the injector can produce.
    assert stats.max_heartbeat_gap_s >= 0.05
    assert heartbeat.beats == stats.frames_forwarded


def test_disconnect_storm_emits_paired_events() -> None:
    clock = FakeClock()
    injector = _injector(
        ChaosConfig(mode="disconnect_storm", interval_s=0.1, disconnect_s=0.05),
        clock=clock,
    )
    for sequence in range(30):
        clock.tick(0.02)
        injector.admit(sequence)

    kinds = [event.kind for event in injector.events]
    assert kinds, "a storm must record events"
    assert kinds[0] is ChaosEventType.DISCONNECT
    for disconnect, reconnect in zip(kinds[0::2], kinds[1::2]):
        assert disconnect is ChaosEventType.DISCONNECT
        assert reconnect is ChaosEventType.RECONNECT
    assert injector.events[0].as_dict()["kind"] == "disconnect"


def test_latency_injection_is_seeded_bounded_and_counted() -> None:
    config = ChaosConfig(
        mode="latency_injection",
        latency_min_ms=200.0,
        latency_max_ms=1000.0,
        latency_probability=1.0,
        seed=7,
    )
    first = _injector(config)
    second = _injector(config)
    delays_one = [first._draw_latency_ms() for _ in range(20)]  # noqa: SLF001
    delays_two = [second._draw_latency_ms() for _ in range(20)]  # noqa: SLF001
    assert delays_one == delays_two, "the seed must make a run reproducible"
    assert all(200.0 <= delay <= 1000.0 for delay in delays_one)

    clock = FakeClock()
    injector = _injector(config, clock=clock)
    for sequence in range(5):
        clock.tick(0.01)
        assert injector.admit(sequence) is True  # latency delays, never drops
    stats = injector.stats
    assert stats.latency_events == 5
    assert stats.frames_dropped == 0
    assert 200.0 <= stats.max_latency_ms <= 1000.0
    assert stats.latency_injected_ms >= stats.max_latency_ms
    assert stats.mean_latency_ms == pytest.approx(
        stats.latency_injected_ms / stats.latency_events)


def test_latency_probability_zero_is_a_passthrough() -> None:
    clock = FakeClock()
    injector = _injector(
        ChaosConfig(mode="latency_injection", latency_probability=0.0),
        clock=clock,
    )
    for sequence in range(10):
        clock.tick(0.01)
        assert injector.admit(sequence) is True
    assert injector.stats.latency_events == 0
    assert injector.stats.frames_forwarded == 10


def test_chaos_window_expires_into_a_clean_tail() -> None:
    clock = FakeClock()
    injector = _injector(
        ChaosConfig(mode="disconnect_storm", interval_s=0.1, disconnect_s=0.05,
                    duration_s=0.15),
        clock=clock,
    )
    for sequence in range(40):
        clock.tick(0.01)
        injector.admit(sequence)

    assert injector.stats.outages >= 1
    # Everything after the window is forwarded and adds no new outage.
    outages_at_end = injector.stats.outages
    for sequence in range(40, 60):
        clock.tick(0.01)
        assert injector.admit(sequence) is True
    assert injector.stats.outages == outages_at_end


def test_request_stop_stops_forwarding() -> None:
    clock = FakeClock()
    injector = _injector(ChaosConfig(), clock=clock)
    assert injector.admit(1) is True
    injector.request_stop()
    assert injector.stopped is True
    assert injector.admit(2) is False
    assert injector.events[-1].kind is ChaosEventType.STOP
    snapshot = injector.snapshot()
    assert snapshot["stopped"] is True
    assert snapshot["mode"] == "disconnect_storm"
    assert "max feed gap" in injector.summary()


# --------------------------------------------------------------------------- #
# Dead-man's switch
# --------------------------------------------------------------------------- #
def _armed_monitor(timeout_ms: float = 500.0):
    config = hft_native.HeartbeatConfig()
    config.timeout_ns = int(timeout_ms * 1_000_000)
    monitor = hft_native.HeartbeatMonitor(config)
    switch = hft_native.KillSwitch()
    monitor.arm(switch)
    return monitor, switch


def test_heartbeat_monitor_contract() -> None:
    monitor, switch = _armed_monitor()
    assert monitor.armed() is True
    assert monitor.timeout_ns() == 500_000_000
    assert str(switch.get_state().name) == "NORMAL"

    monitor.beat(1_000_000_000)
    stats = monitor.stats(1_100_000_000)
    assert stats.beats == 1
    assert stats.timeouts == 0
    assert stats.silence_ns == 100_000_000
    assert stats.expired is False
    assert stats.state == hft_native.SystemState.NORMAL

    assert monitor.poll(1_400_000_000) is False
    assert monitor.poll(1_600_000_000) is True
    assert switch.get_state() == hft_native.SystemState.TRIGGERED
    assert switch.get_stats().last_trip_reason.startswith("heartbeat_timeout")
    # One trip per expiry window, no matter how often the watchdog polls.
    assert monitor.poll(1_700_000_000) is False
    assert switch.get_stats().trip_count == 1

    monitor.reset(2_000_000_000)
    stats = monitor.stats(2_050_000_000)
    assert stats.beats == 1, "telemetry survives reset"
    assert stats.timeouts == 1
    assert stats.expired is False


def test_watchdog_trips_then_recovers() -> None:
    monitor, switch = _armed_monitor(timeout_ms=500.0)
    monitor.beat(1_000_000_000)
    watchdog = HeartbeatWatchdog(
        monitor, kill_switch=switch, auto_recover=True, recovery_delay_s=0.0,
        interval_s=0.01,
    )

    assert watchdog.poll_once(1_400_000_000) is False
    assert switch.get_state() == hft_native.SystemState.NORMAL

    assert watchdog.poll_once(1_600_000_000) is True
    # The watchdog immediately moves the switch into the remediation window.
    assert switch.get_state() == hft_native.SystemState.RECOVERING
    assert switch.get_stats().trip_count == 1
    assert watchdog.stats()["timeouts"] == 1

    assert watchdog.poll_once(1_650_000_000) is False  # still latched

    watchdog.recover()
    assert switch.get_state() == hft_native.SystemState.NORMAL
    assert switch.get_stats().reset_count == 1
    assert watchdog.stats()["recoveries"] == 1
    assert monitor.stats(1_650_000_000).timeouts == 1


def test_watchdog_manual_recovery_clears_the_halt() -> None:
    monitor, switch = _armed_monitor(timeout_ms=100.0)
    monitor.beat(1_000_000_000)
    watchdog = HeartbeatWatchdog(monitor, kill_switch=switch, auto_recover=False)
    assert watchdog.poll_once(1_200_000_000) is True
    assert switch.get_state() == hft_native.SystemState.RECOVERING
    # auto_recover=False leaves the halt in place until an operator clears it.
    assert watchdog.stats()["recoveries"] == 0
    watchdog.recover()
    assert switch.get_state() == hft_native.SystemState.NORMAL


def test_watchdog_waits_for_a_beat_before_recovering() -> None:
    monitor, switch = _armed_monitor(timeout_ms=100.0)
    monitor.beat(1_000_000_000)
    watchdog = HeartbeatWatchdog(monitor, kill_switch=switch, auto_recover=True,
                                 interval_s=0.01, recovery_delay_s=0.0,
                                 recovery_timeout_s=0.05)
    assert watchdog.poll_once(1_200_000_000) is True
    assert switch.get_state() == hft_native.SystemState.RECOVERING
    # No frame after the trip: the halt must stay in place.
    assert watchdog.await_beat() is False
    assert switch.get_state() == hft_native.SystemState.RECOVERING
    assert watchdog.stats()["recoveries"] == 0

    # A frame proves the feed is back; now the switch may be cleared.
    monitor.beat()
    assert watchdog.await_beat(timeout_s=1.0) is True
    watchdog.recover()
    assert switch.get_state() == hft_native.SystemState.NORMAL


def test_watchdog_thread_supervises_and_recovers() -> None:
    monitor, switch = _armed_monitor(timeout_ms=80.0)
    monitor.beat()
    watchdog = HeartbeatWatchdog(
        monitor, kill_switch=switch, auto_recover=True,
        interval_s=0.01, recovery_delay_s=0.02, recovery_timeout_s=3.0,
    ).start()
    try:
        # Silence first: the watchdog must trip on its own.
        deadline = time.monotonic() + 3.0
        while switch.get_stats().trip_count == 0 and time.monotonic() < deadline:
            time.sleep(0.01)
        assert switch.get_stats().trip_count >= 1

        # Then the feed returns; recovery needs a fresh beat to clear the halt.
        deadline = time.monotonic() + 3.0
        while switch.get_stats().reset_count == 0 and time.monotonic() < deadline:
            monitor.beat()
            time.sleep(0.01)
        assert watchdog.running is True
        stats = watchdog.stats()
        assert stats["timeouts"] >= 1
        assert stats["recoveries"] >= 1
        assert switch.get_state() == hft_native.SystemState.NORMAL
    finally:
        watchdog.stop()
        watchdog.join(2.0)
    assert watchdog.running is False


def test_watchdog_rejects_invalid_intervals() -> None:
    monitor, switch = _armed_monitor()
    with pytest.raises(ValueError, match="interval_s"):
        HeartbeatWatchdog(monitor, kill_switch=switch, interval_s=0.0)
    with pytest.raises(ValueError, match="recovery_delay_s"):
        HeartbeatWatchdog(monitor, kill_switch=switch, recovery_delay_s=-1.0)
    with pytest.raises(ValueError, match="recovery_timeout_s"):
        HeartbeatWatchdog(monitor, kill_switch=switch, recovery_timeout_s=0.0)


# --------------------------------------------------------------------------- #
# Bridge seam
# --------------------------------------------------------------------------- #
def test_feed_archive_gate_drops_frames(tmp_path: Path) -> None:
    archive = tmp_path / "sample.ndjson"
    archive.write_text("\n".join([DEPTH1] * 8) + "\n", encoding="utf-8")
    session = PaperSession.create("0.5", "30000")

    seen: List[int] = []

    def drop_odd(sequence: int) -> bool:
        seen.append(sequence)
        return sequence % 2 == 0

    totals = feed_archive(session.engine, archive, batch_size=3, frame_gate=drop_odd)
    assert seen == list(range(1, 9))
    assert totals["lines"] == 4
    assert totals["dropped"] == 4
    assert totals["gate_errors"] == 0
    assert totals["stopped"] == 0
    assert session.metrics()["depth_events"] == 4


def test_feed_archive_fails_open_when_a_gate_raises(tmp_path: Path) -> None:
    archive = tmp_path / "sample.ndjson"
    archive.write_text("\n".join([DEPTH1] * 4) + "\n", encoding="utf-8")
    session = PaperSession.create("0.5", "30000")

    def flaky(sequence: int) -> bool:
        if sequence == 2:
            raise RuntimeError("injector bug")
        return True

    totals = feed_archive(session.engine, archive, batch_size=2, frame_gate=flaky)
    # The frame is forwarded, the failure is counted, and the feed survives.
    assert totals["gate_errors"] == 1
    assert totals["lines"] == 4
    assert session.metrics()["depth_events"] == 4


def test_feed_archive_stops_at_a_frame_boundary(tmp_path: Path) -> None:
    archive = tmp_path / "sample.ndjson"
    archive.write_text("\n".join([DEPTH1] * 20) + "\n", encoding="utf-8")
    session = PaperSession.create("0.5", "30000")

    budget = {"frames": 0}

    def should_stop() -> bool:
        return budget["frames"] >= 5

    def gate(sequence: int) -> bool:
        budget["frames"] += 1
        return True

    totals = feed_archive(session.engine, archive, batch_size=1,
                          frame_gate=gate, should_stop=should_stop)
    assert totals["stopped"] == 1
    assert totals["lines"] == 5
    assert session.metrics()["depth_events"] == 5


# --------------------------------------------------------------------------- #
# PaperSession + KillSwitch hooks
# --------------------------------------------------------------------------- #
def _session_with_switch(**kwargs: object) -> tuple:
    switch = hft_native.KillSwitch()
    session = PaperSession.create("0.5", "30000", taker_bps=5.0,
                                  initial_cash=100_000.0, kill_switch=switch,
                                  **kwargs)
    session.feed([DEPTH1])
    return session, switch


def test_tripped_switch_blocks_orders_without_touching_the_engine() -> None:
    session, switch = _session_with_switch()
    assert session.market(hft_native.Side.BUY, "1")["accepted"] is True
    submitted = session.metrics()["orders_submitted"]

    switch.trip("chaos: disconnect")
    blocked = session.market(hft_native.Side.BUY, "1")
    assert blocked["accepted"] is False
    assert blocked["blocked"] is True
    assert blocked["reason"] == "kill_switch:TRIGGERED"
    assert session.metrics()["orders_submitted"] == submitted
    assert switch.get_stats().blocked_orders == 1

    switch.reset()
    resumed = session.market(hft_native.Side.BUY, "1")
    assert resumed["accepted"] is True
    assert resumed["blocked"] is False


def test_recovery_state_also_blocks_orders() -> None:
    session, switch = _session_with_switch()
    switch.trip("chaos: latency")
    assert switch.begin_recovery() is True
    blocked = session.market(hft_native.Side.SELL, "1")
    assert blocked["accepted"] is False
    assert blocked["reason"] == "kill_switch:RECOVERING"


def test_kill_switch_hooks_cancel_and_flatten_on_the_engine_thread() -> None:
    session, switch = _session_with_switch()
    assert session.market(hft_native.Side.BUY, "2")["accepted"] is True
    assert session.metrics()["net_quantity"] == pytest.approx(2.0)

    # A resting maker quote the cancel hook has to remove.
    bbo = session.engine.bbo()
    tick = session.engine.tick_size()
    request = hft_native.OrderRequest()
    request.client_id = 777
    request.side = hft_native.Side.BUY
    request.type = hft_native.OrderType.POST_ONLY
    request.price = hft_native.Price.from_raw(bbo.bid_price.raw() - 2 * tick.raw())
    request.quantity = hft_native.Quantity.from_string("1")
    assert session.engine.submit(request).accepted()
    assert session.metrics()["open_orders"] == 1

    switch.trip("chaos: dead feed")
    # The hooks only stage; nothing touches the engine from the trip path.
    assert session.remediation_pending is True
    assert session.metrics()["open_orders"] == 1

    outcome = session.service_remediation()
    assert outcome == {"cancelled": 1.0, "flattened": 2.0}
    metrics = session.metrics()
    assert metrics["open_orders"] == 0
    assert metrics["net_quantity"] == pytest.approx(0.0)
    assert session.remediation_pending is False

    # Nothing staged -> no-op.
    assert session.service_remediation() == {}


def test_flatten_is_a_noop_when_already_flat() -> None:
    session, switch = _session_with_switch()
    switch.trip("chaos: flat")
    outcome = session.service_remediation()
    assert outcome["flattened"] == 0.0
    assert session.metrics()["net_quantity"] == pytest.approx(0.0)


# --------------------------------------------------------------------------- #
# CLI harness
# --------------------------------------------------------------------------- #
def _load_cli():
    path = ROOT / "tests" / "chaos_network_test.py"
    spec = importlib.util.spec_from_file_location("chaos_network_cli", path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module  # dataclasses need a registered module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def chaos_cli():
    return _load_cli()


def test_cli_parses_both_modes(chaos_cli) -> None:
    args = chaos_cli.parse_args(["--mode", "disconnect_storm"])
    assert args.mode == "disconnect_storm"
    assert args.heartbeat_timeout_ms == pytest.approx(500.0)
    args = chaos_cli.parse_args(["--mode", "latency_injection", "--seed", "3"])
    assert args.seed == 3
    with pytest.raises(SystemExit):
        chaos_cli.parse_args(["--mode", "partition"])


def test_cli_disconnect_storm_passes(chaos_cli, capsys) -> None:
    code = chaos_cli.main([
        "--mode", "disconnect_storm",
        "--duration", "1.2",
        "--interval", "0.4",
        "--disconnect-s", "0.2",
        "--heartbeat-timeout-ms", "100",
        "--recovery-delay", "0.05",
        "--replay-delay", "0.002",
        "--report-every", "10",
        "--log-level", "CRITICAL",
    ])
    output = capsys.readouterr().out
    assert code == 0
    assert "RESULT: PASS" in output
    assert "dead-man's switch tripped" in output
    assert "disconnect storm executed" in output


def test_cli_latency_injection_passes(chaos_cli, capsys) -> None:
    code = chaos_cli.main([
        "--mode", "latency_injection",
        "--duration", "1.0",
        "--latency-min-ms", "200",
        "--latency-max-ms", "600",
        "--latency-probability", "0.25",
        "--seed", "11",
        "--heartbeat-timeout-ms", "150",
        "--recovery-delay", "0.05",
        "--replay-delay", "0.002",
        "--report-every", "10",
        "--log-level", "CRITICAL",
    ])
    output = capsys.readouterr().out
    assert code == 0
    assert "RESULT: PASS" in output
    assert "latency spikes injected" in output


def test_cli_reports_a_missing_source(chaos_cli, tmp_path: Path) -> None:
    code = chaos_cli.main([
        "--mode", "disconnect_storm",
        "--source", str(tmp_path / "missing.ndjson"),
        "--log-level", "CRITICAL",
    ])
    assert code == 2


def test_cli_rejects_a_run_shorter_than_the_chaos_interval(chaos_cli) -> None:
    # A storm cannot start before the first interval, so this run would have
    # zero outages: fail fast instead of reporting an empty PASS/FAIL.
    code = chaos_cli.main([
        "--mode", "disconnect_storm",
        "--duration", "1.0",
        "--interval", "2.0",
        "--log-level", "CRITICAL",
    ])
    assert code == 2
