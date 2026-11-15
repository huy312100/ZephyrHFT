"""Alerting for live trading sessions.

Provides a simple, pluggable alerting interface. The default implementation
logs to stderr; subclasses can send to email, Slack, PagerDuty, etc.

Alerts are intentionally coarse-grained: kill-switch trips, alpha gate
rejections, reconciliation failures, and funding-drain warnings. Each alert
carries a severity and a free-form message.
"""
from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Optional


class AlertSeverity(Enum):
    INFO = "INFO"
    WARNING = "WARNING"
    CRITICAL = "CRITICAL"


@dataclass(frozen=True)
class Alert:
    """One alert emitted by the live system."""

    ts: datetime
    severity: AlertSeverity
    source: str  # module/component that raised it
    code: str  # short machine-readable code, e.g. "KILL_SWITCH_TRIPPED"
    message: str  # human-readable description


class AlertSink(ABC):
    """Destination for alerts. Subclass to integrate with a notification
    service (Slack, PagerDuty, email, etc.)."""

    @abstractmethod
    def send(self, alert: Alert) -> None:
        """Deliver ``alert`` to the sink."""


class LogSink(AlertSink):
    """Default sink: writes alerts to Python logging."""

    def __init__(self, logger_name: str = "zephyr.alerts") -> None:
        self._logger = logging.getLogger(logger_name)

    def send(self, alert: Alert) -> None:
        record = {
            "ts": alert.ts.isoformat(),
            "severity": alert.severity.value,
            "source": alert.source,
            "code": alert.code,
            "message": alert.message,
        }
        if alert.severity == AlertSeverity.CRITICAL:
            self._logger.critical("ALERT %s: %s", alert.code, alert.message,
                                  extra=record)
        elif alert.severity == AlertSeverity.WARNING:
            self._logger.warning("ALERT %s: %s", alert.code, alert.message,
                                 extra=record)
        else:
            self._logger.info("ALERT %s: %s", alert.code, alert.message,
                              extra=record)


class AlertRouter:
    """Routes alerts to one or more sinks."""

    def __init__(self, sinks: list[AlertSink] | None = None) -> None:
        self._sinks: list[AlertSink] = list(sinks or [])
        #: Default sink if none configured (logs to stderr).
        self._default = LogSink()

    def add_sink(self, sink: AlertSink) -> None:
        self._sinks.append(sink)

    def send(self, alert: Alert) -> None:
        if not self._sinks:
            self._default.send(alert)
        else:
            for sink in self._sinks:
                try:
                    sink.send(alert)
                except Exception:  # pragma: no cover - sink failures are fatal
                    self._default.send(Alert(
                        ts=datetime.now(timezone.utc),
                        severity=AlertSeverity.CRITICAL,
                        source="alerts.router",
                        code="SINK_FAILURE",
                        message=f"alert sink {sink.__class__.__name__} raised",
                    ))

    # ---------------------------------------------------------------- factory
    @classmethod
    def default(cls) -> "AlertRouter":
        """Router with the standard log sink."""
        return cls([LogSink()])


#: Module-level router for convenience. Import and call ``router.send(...)``
#: from any live component.
router = AlertRouter.default()


def kill_switch_tripped(reason: str, symbol: str = "BTCUSDT") -> None:
    router.send(Alert(
        ts=datetime.now(timezone.utc),
        severity=AlertSeverity.CRITICAL,
        source="live.kill_switch",
        code="KILL_SWITCH_TRIPPED",
        message=f"kill switch tripped for {symbol}: {reason}",
    ))


def kill_switch_reset(symbol: str = "BTCUSDT") -> None:
    router.send(Alert(
        ts=datetime.now(timezone.utc),
        severity=AlertSeverity.INFO,
        source="live.kill_switch",
        code="KILL_SWITCH_RESET",
        message=f"kill switch reset for {symbol}",
    ))


def alpha_gate_rejected(reason: str) -> None:
    router.send(Alert(
        ts=datetime.now(timezone.utc),
        severity=AlertSeverity.CRITICAL,
        source="live.alpha_gate",
        code="ALPHA_GATE_REJECTED",
        message=f"real-money arming refused: {reason}",
    ))


def reconciliation_unresolved(order_ids: list[str]) -> None:
    router.send(Alert(
        ts=datetime.now(timezone.utc),
        severity=AlertSeverity.WARNING,
        source="live.reconcile",
        code="UNRESOLVED_ORDERS",
        message=f"{len(order_ids)} order(s) unresolved after reconcile: "
                f"{', '.join(order_ids)}",
    ))


def funding_drain_warning(symbol: str, paid_usdt: float,
                          period: str) -> None:
    router.send(Alert(
        ts=datetime.now(timezone.utc),
        severity=AlertSeverity.WARNING,
        source="live.funding",
        code="FUNDING_DRAIN",
        message=f"{symbol} funding paid {paid_usdt:+.2f} USDT over {period}",
    ))