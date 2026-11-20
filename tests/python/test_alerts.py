"""Tests for the alerting system."""
from __future__ import annotations

from datetime import datetime, timezone

from python_analytics.live.alerts import (
    Alert,
    AlertRouter,
    AlertSeverity,
    LogSink,
    alpha_gate_rejected,
    funding_drain_warning,
    kill_switch_reset,
    kill_switch_tripped,
    reconciliation_unresolved,
    router,
)


class CaptureSink:
    """Test sink that captures alerts in memory."""

    def __init__(self) -> None:
        self.alerts: list = []

    def send(self, alert) -> None:
        self.alerts.append(alert)


def test_alert_router_sends_to_sinks() -> None:
    sink = CaptureSink()
    router = AlertRouter([sink])

    alert = Alert(
        ts=datetime.now(timezone.utc),
        severity=AlertSeverity.CRITICAL,
        source="test",
        code="TEST_CODE",
        message="test message",
    )
    router.send(alert)

    assert len(sink.alerts) == 1
    assert sink.alerts[0].code == "TEST_CODE"
    assert sink.alerts[0].severity == AlertSeverity.CRITICAL


def test_default_router_sends_to_log_sink() -> None:
    assert isinstance(router._default, LogSink)


def test_kill_switch_tripped_alert() -> None:
    sink = CaptureSink()
    r = AlertRouter([sink])
    r.add_sink = lambda s: None  # keep default + our sink? No, just test via factory funcs
    # Use module-level router with replaced sink
    import python_analytics.live.alerts as alerts_module

    original = alerts_module.router._sinks
    alerts_module.router._sinks = [sink]
    try:
        kill_switch_tripped("drawdown limit", "BTCUSDT")
        assert len(sink.alerts) == 1
        assert sink.alerts[0].code == "KILL_SWITCH_TRIPPED"
        assert "BTCUSDT" in sink.alerts[0].message
        assert sink.alerts[0].severity == AlertSeverity.CRITICAL
    finally:
        alerts_module.router._sinks = original


def test_kill_switch_reset_alert() -> None:
    sink = CaptureSink()
    import python_analytics.live.alerts as alerts_module

    original = alerts_module.router._sinks
    alerts_module.router._sinks = [sink]
    try:
        kill_switch_reset("BTCUSDT")
        assert len(sink.alerts) == 1
        assert sink.alerts[0].code == "KILL_SWITCH_RESET"
        assert sink.alerts[0].severity == AlertSeverity.INFO
    finally:
        alerts_module.router._sinks = original


def test_alpha_gate_rejected_alert() -> None:
    sink = CaptureSink()
    import python_analytics.live.alerts as alerts_module

    original = alerts_module.router._sinks
    alerts_module.router._sinks = [sink]
    try:
        alpha_gate_rejected("stale report")
        assert len(sink.alerts) == 1
        assert sink.alerts[0].code == "ALPHA_GATE_REJECTED"
        assert sink.alerts[0].severity == AlertSeverity.CRITICAL
    finally:
        alerts_module.router._sinks = original


def test_reconciliation_unresolved_alert() -> None:
    sink = CaptureSink()
    import python_analytics.live.alerts as alerts_module

    original = alerts_module.router._sinks
    alerts_module.router._sinks = [sink]
    try:
        reconciliation_unresolved(["order-1", "order-2"])
        assert len(sink.alerts) == 1
        assert sink.alerts[0].code == "UNRESOLVED_ORDERS"
        assert "order-1" in sink.alerts[0].message
        assert sink.alerts[0].severity == AlertSeverity.WARNING
    finally:
        alerts_module.router._sinks = original


def test_funding_drain_warning_alert() -> None:
    sink = CaptureSink()
    import python_analytics.live.alerts as alerts_module

    original = alerts_module.router._sinks
    alerts_module.router._sinks = [sink]
    try:
        funding_drain_warning("BTCUSDT", -12.50, "24h")
        assert len(sink.alerts) == 1
        assert sink.alerts[0].code == "FUNDING_DRAIN"
        assert "-12.50" in sink.alerts[0].message
        assert sink.alerts[0].severity == AlertSeverity.WARNING
    finally:
        alerts_module.router._sinks = original