"""Tests for the native emergency kill switch (``hft_native.KillSwitch``).

The switch is the last line of defence: once tripped it must refuse every
order, stay tripped under concurrent trips, and only release order flow after
an explicit ``reset()``.
"""
from __future__ import annotations

import threading

import pytest

from python_analytics.native import hft_native
from python_analytics.native.factory import build_kill_switch, kill_switch_config
from python_analytics.utils import load_settings
from python_analytics.utils.config import (
    ConfigError,
    KillSwitchSettings,
    as_dict,
    load_settings as load_settings_from,
)


def _switch(**overrides: object) -> "hft_native.KillSwitch":
    config = hft_native.KillSwitchConfig()
    for name, value in overrides.items():
        setattr(config, name, value)
    return hft_native.KillSwitch(config)


def test_defaults_are_normal() -> None:
    switch = hft_native.KillSwitch()
    assert switch.get_state() == hft_native.SystemState.NORMAL
    assert not switch.is_tripped()
    assert switch.check_order_allowed()

    config = switch.config()
    assert config.enabled is True
    assert config.max_drawdown_limit == 0.0
    assert config.max_consecutive_rejects == 0

    stats = switch.get_stats()
    assert stats.trip_count == 0
    assert stats.last_trip_timestamp_ns == 0
    assert stats.last_trip_reason == ""
    assert stats.state == hft_native.SystemState.NORMAL
    assert stats.blocked_orders == 0
    assert stats.reset_count == 0


def test_system_state_contract() -> None:
    # pybind11 enums are value-comparable but not identity-stable across the
    # C++/Python boundary, so the suite always compares with ``==``.
    members = hft_native.SystemState.__members__
    assert list(members) == ["NORMAL", "TRIGGERED", "RECOVERING"]
    assert int(hft_native.SystemState.NORMAL) == 0
    assert int(hft_native.SystemState.TRIGGERED) == 1
    assert int(hft_native.SystemState.RECOVERING) == 2
    assert hft_native.SystemState.TRIGGERED.name == "TRIGGERED"
    assert hft_native.system_state_name(hft_native.SystemState.TRIGGERED) == "TRIGGERED"


def test_manual_trip_blocks_orders() -> None:
    switch = hft_native.KillSwitch()
    switch.trip("ops: stop trading")

    assert switch.is_tripped()
    assert switch.get_state() == hft_native.SystemState.TRIGGERED
    assert not switch.check_order_allowed()

    stats = switch.get_stats()
    assert stats.trip_count == 1
    assert stats.last_trip_reason == "ops: stop trading"
    assert stats.last_trip_timestamp_ns > 0
    assert stats.state == hft_native.SystemState.TRIGGERED
    assert stats.blocked_orders == 1


def test_trip_records_every_event() -> None:
    switch = hft_native.KillSwitch()
    switch.trip("first")
    first_ts = switch.get_stats().last_trip_timestamp_ns

    switch.trip("second")
    stats = switch.get_stats()
    assert stats.trip_count == 2
    assert stats.last_trip_reason == "second"
    assert stats.last_trip_timestamp_ns >= first_ts
    assert switch.get_state() == hft_native.SystemState.TRIGGERED


def test_trip_without_reason_uses_placeholder() -> None:
    switch = hft_native.KillSwitch()
    switch.trip("")
    assert switch.get_stats().last_trip_reason == "unspecified"


def test_reset_restores_order_flow() -> None:
    switch = hft_native.KillSwitch()
    switch.trip("drawdown breach")
    assert not switch.check_order_allowed()

    switch.reset()
    assert switch.get_state() == hft_native.SystemState.NORMAL
    assert not switch.is_tripped()
    assert switch.check_order_allowed()

    stats = switch.get_stats()
    assert stats.reset_count == 1
    # Post-mortem telemetry survives the reset; the blocked counter is
    # cumulative for the process lifetime.
    assert stats.trip_count == 1
    assert stats.last_trip_reason == "drawdown breach"
    assert stats.blocked_orders == 1
    assert stats.state == hft_native.SystemState.NORMAL


def test_reset_on_healthy_switch_is_a_no_op() -> None:
    switch = hft_native.KillSwitch()
    switch.reset()
    switch.reset()
    stats = switch.get_stats()
    assert stats.reset_count == 2
    assert stats.trip_count == 0
    assert switch.check_order_allowed()


def test_recovery_window_still_blocks_orders() -> None:
    switch = hft_native.KillSwitch()
    assert switch.begin_recovery() is False  # nothing to recover from

    switch.trip("manual")
    assert switch.begin_recovery() is True
    assert switch.get_state() == hft_native.SystemState.RECOVERING
    assert switch.is_tripped()
    assert not switch.check_order_allowed()
    assert switch.begin_recovery() is False  # already recovering

    switch.reset()
    assert switch.check_order_allowed()


# ---------------------------------------------------------------------------
# Automatic guards
# ---------------------------------------------------------------------------
def test_drawdown_guard_trips_at_limit() -> None:
    switch = _switch(max_drawdown_limit=1_000.0)

    assert switch.check_drawdown(999.99) is False
    assert switch.check_order_allowed()
    assert switch.get_stats().trip_count == 0

    assert switch.check_drawdown(1_000.0) is True
    assert switch.get_state() == hft_native.SystemState.TRIGGERED
    assert not switch.check_order_allowed()
    reason = switch.get_stats().last_trip_reason
    assert reason.startswith("drawdown=1000.00>=limit=1000.00")

    # Already tripped: the guard reports no *new* transition.
    assert switch.check_drawdown(5_000.0) is False
    assert switch.get_stats().trip_count == 1


def test_drawdown_guard_fails_closed_on_nan() -> None:
    switch = _switch(max_drawdown_limit=10.0)
    assert switch.check_drawdown(float("nan")) is True
    assert switch.is_tripped()


def test_drawdown_guard_disabled_when_limit_is_zero() -> None:
    switch = _switch(max_drawdown_limit=0.0)
    assert switch.check_drawdown(1e12) is False
    assert not switch.is_tripped()


def test_consecutive_rejects_trip_on_threshold() -> None:
    switch = _switch(max_consecutive_rejects=3)

    assert switch.record_order_rejected() is False
    assert switch.record_order_rejected() is False
    assert switch.check_order_allowed()
    assert switch.get_stats().consecutive_rejects == 2

    assert switch.record_order_rejected() is True
    assert switch.get_state() == hft_native.SystemState.TRIGGERED
    assert switch.get_stats().last_trip_reason.startswith("consecutive_rejects=3")
    # A trip clears the streak so a post-reset session starts clean.
    assert switch.get_stats().consecutive_rejects == 0


def test_accepted_order_clears_the_reject_streak() -> None:
    switch = _switch(max_consecutive_rejects=2)
    assert switch.record_order_rejected() is False
    switch.record_order_accepted()
    assert switch.get_stats().consecutive_rejects == 0
    assert switch.record_order_rejected() is False
    assert not switch.is_tripped()


def test_reject_guard_disabled_when_threshold_is_zero() -> None:
    switch = _switch(max_consecutive_rejects=0)
    for _ in range(100):
        assert switch.record_order_rejected() is False
    assert not switch.is_tripped()


def test_disabled_switch_still_honours_a_manual_trip() -> None:
    switch = _switch(enabled=False, max_drawdown_limit=1.0, max_consecutive_rejects=1)
    assert switch.enabled is False

    # Automation is inert...
    assert switch.check_drawdown(1e9) is False
    assert switch.record_order_rejected() is False
    assert not switch.is_tripped()

    # ...but an operator must always be able to stop the process.
    switch.trip("operator override")
    assert switch.get_state() == hft_native.SystemState.TRIGGERED
    assert not switch.check_order_allowed()


def test_set_config_and_enabled_property() -> None:
    switch = hft_native.KillSwitch()
    config = hft_native.KillSwitchConfig()
    config.max_drawdown_limit = 50.0
    config.max_consecutive_rejects = 2
    switch.set_config(config)
    assert switch.config().max_drawdown_limit == 50.0
    assert switch.config().max_consecutive_rejects == 2

    switch.enabled = False
    assert switch.config().enabled is False
    assert switch.check_drawdown(100.0) is False

    switch.enabled = True
    assert switch.check_drawdown(100.0) is True


# ---------------------------------------------------------------------------
# Hooks
# ---------------------------------------------------------------------------
def test_hooks_run_in_cancel_flatten_log_order() -> None:
    switch = hft_native.KillSwitch()
    events: list[object] = []
    switch.set_cancel_all_hook(lambda: events.append("cancel"))
    switch.set_flatten_hook(lambda: events.append("flatten"))
    switch.set_trip_hook(lambda state, reason: events.append((state, reason)))

    switch.trip("halt")

    assert events == ["cancel", "flatten", ("TRIGGERED", "halt")]
    assert switch.get_state() == hft_native.SystemState.TRIGGERED


def test_hook_failures_do_not_unwind_the_trip() -> None:
    switch = hft_native.KillSwitch()

    def boom(*_args: object) -> None:
        raise RuntimeError("hook exploded")

    switch.set_cancel_all_hook(boom)
    switch.set_flatten_hook(boom)
    switch.set_trip_hook(boom)

    switch.trip("halt")  # must not raise
    assert switch.get_state() == hft_native.SystemState.TRIGGERED
    assert switch.get_stats().trip_count == 1


def test_clear_hooks_stops_dispatch() -> None:
    switch = hft_native.KillSwitch()
    calls: list[str] = []
    switch.set_cancel_all_hook(lambda: calls.append("cancel"))
    switch.clear_hooks()
    switch.trip("halt")
    assert calls == []
    assert switch.is_tripped()


# ---------------------------------------------------------------------------
# Concurrency and snapshot semantics
# ---------------------------------------------------------------------------
def test_concurrent_trips_stay_consistent() -> None:
    switch = hft_native.KillSwitch()
    barrier = threading.Barrier(8)
    errors: list[BaseException] = []

    def worker() -> None:
        try:
            barrier.wait(timeout=5.0)
            for _ in range(25):
                switch.trip("concurrent halt")
                assert switch.is_tripped()
        except BaseException as exc:  # pragma: no cover - failure path
            errors.append(exc)

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=30.0)
        assert not thread.is_alive()

    assert errors == []
    stats = switch.get_stats()
    # Every trip is counted exactly once: one atomic RMW per call, no torn
    # state and no lost updates across 8 threads x 25 trips.
    assert stats.trip_count == 200
    assert stats.state == hft_native.SystemState.TRIGGERED
    assert stats.last_trip_reason == "concurrent halt"
    assert not switch.check_order_allowed()

    switch.reset()
    assert switch.check_order_allowed()


def test_stats_are_a_snapshot() -> None:
    switch = hft_native.KillSwitch()
    before = switch.get_stats()
    switch.trip("halt")
    after = switch.get_stats()
    assert before.trip_count == 0
    assert before.last_trip_reason == ""
    assert after.trip_count == 1
    assert after is not before


# ---------------------------------------------------------------------------
# Configuration plumbing
# ---------------------------------------------------------------------------
def test_settings_expose_the_kill_switch_block() -> None:
    settings = load_settings()
    assert settings.kill_switch == KillSwitchSettings()
    assert settings.kill_switch.enabled is True
    assert settings.kill_switch.max_drawdown_limit == "25000"
    assert settings.kill_switch.max_consecutive_rejects == 10
    assert as_dict(settings)["kill_switch"] == {
        "enabled": True,
        "max_drawdown_limit": "25000",
        "max_consecutive_rejects": 10,
    }


def test_factory_builds_an_armed_switch() -> None:
    settings = load_settings()
    config = kill_switch_config(settings)
    assert config.enabled is True
    assert config.max_drawdown_limit == pytest.approx(25_000.0)
    assert config.max_consecutive_rejects == 10

    switch = build_kill_switch(settings)
    assert switch.config().max_drawdown_limit == pytest.approx(25_000.0)
    assert switch.check_drawdown(24_999.0) is False
    assert switch.check_drawdown(25_000.0) is True
    assert switch.get_state() == hft_native.SystemState.TRIGGERED


def test_kill_switch_settings_can_be_built_directly() -> None:
    block = KillSwitchSettings(enabled=False, max_drawdown_limit="0",
                               max_consecutive_rejects=0)
    switch = build_kill_switch(block)
    config = switch.config()
    assert config.enabled is False
    assert config.max_drawdown_limit == 0.0
    assert config.max_consecutive_rejects == 0
    assert switch.check_drawdown(1e9) is False


def test_negative_drawdown_limit_is_rejected(tmp_path) -> None:
    path = tmp_path / "settings.yaml"
    path.write_text('kill_switch:\n  max_drawdown_limit: "-1"\n', encoding="utf-8")
    with pytest.raises(ConfigError, match="kill_switch.max_drawdown_limit"):
        load_settings_from(path)


def test_negative_reject_threshold_is_rejected(tmp_path) -> None:
    path = tmp_path / "settings.yaml"
    path.write_text("kill_switch:\n  max_consecutive_rejects: -5\n", encoding="utf-8")
    with pytest.raises(ConfigError, match="kill_switch.max_consecutive_rejects"):
        load_settings_from(path)


def test_zero_thresholds_disable_the_automatic_guards(tmp_path) -> None:
    path = tmp_path / "settings.yaml"
    path.write_text(
        'kill_switch:\n  enabled: true\n  max_drawdown_limit: "0"\n'
        "  max_consecutive_rejects: 0\n",
        encoding="utf-8",
    )
    settings = load_settings_from(path)
    assert settings.kill_switch == KillSwitchSettings(
        enabled=True, max_drawdown_limit="0", max_consecutive_rejects=0
    )
    switch = build_kill_switch(settings)
    assert switch.check_drawdown(1e9) is False
    assert switch.record_order_rejected() is False
    assert not switch.is_tripped()


# ---------------------------------------------------------------------------
# Repr / ergonomics
# ---------------------------------------------------------------------------
def test_reprs_are_readable() -> None:
    switch = hft_native.KillSwitch()
    assert repr(switch) == "KillSwitch(state=NORMAL)"
    switch.trip("halt")
    assert repr(switch) == "KillSwitch(state=TRIGGERED)"
    assert repr(switch.config()).startswith("KillSwitchConfig(enabled=True")
    stats_repr = repr(switch.get_stats())
    assert "trip_count=1" in stats_repr
    assert "halt" in stats_repr
