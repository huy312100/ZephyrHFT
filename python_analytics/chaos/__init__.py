"""Network chaos injection and feed watchdogs (Phase 3.1 / 3.2).

Two failure modes dominate a live crypto feed and both are invisible to
strategy code:

* **silent disconnects** — the socket dies without an error frame, so the
  process keeps quoting against a book that no longer updates;
* **latency spikes** — frames keep arriving, but 900 ms late, so risk limits
  and order timeouts are evaluated against stale prices.

:class:`ChaosNetworkInjector` reproduces both at the frame boundary of the
paper/live ingestion loop, and :class:`HeartbeatWatchdog` supervises the native
``HeartbeatMonitor`` so a silent feed trips the ``KillSwitch`` dead-man's
switch instead of trading on.
"""
from __future__ import annotations

from .network import (
    ChaosConfig,
    ChaosEvent,
    ChaosEventType,
    ChaosMode,
    ChaosNetworkInjector,
    ChaosStats,
)
from .watchdog import HeartbeatWatchdog

__all__ = [
    "ChaosConfig",
    "ChaosEvent",
    "ChaosEventType",
    "ChaosMode",
    "ChaosNetworkInjector",
    "ChaosStats",
    "HeartbeatWatchdog",
]
