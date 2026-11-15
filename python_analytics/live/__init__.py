"""Live execution: durable order state, reconciliation and the arming gate.

``LiveExecutor`` is deliberately strategy-agnostic: it takes the orders a
strategy decided to place and makes sure the process and the venue agree on what
exists. ``require_alpha_go`` is the other half — it decides whether a real-money
session may be armed at all.
"""
from .alerts import (
    Alert,
    AlertRouter,
    AlertSeverity,
    AlertSink,
    LogSink,
    alpha_gate_rejected,
    funding_drain_warning,
    kill_switch_reset,
    kill_switch_tripped,
    reconciliation_unresolved,
    router,
)
from .executor import (
    UNKNOWN_ORDER_CODE,
    VENUE_STATUS,
    AlphaGateError,
    LiveExecutionError,
    LiveExecutor,
    ReconcileReport,
    SubmissionResult,
    alpha_gate_verdict,
    require_alpha_go,
)
from .funding import FundingEvent, FundingLedger
from .store import (
    ACTIVE_STATUSES,
    KNOWN_STATUSES,
    MAX_CLIENT_ORDER_ID,
    TERMINAL_STATUSES,
    LiveStoreError,
    OrderStore,
)
from .supervisor import (
    LiveSupervisor,
    SessionReport,
    StepResult,
    SupervisorConfig,
)

__all__ = [
    "ACTIVE_STATUSES",
    "Alert",
    "AlertRouter",
    "AlertSeverity",
    "AlertSink",
    "FundingEvent",
    "FundingLedger",
    "KNOWN_STATUSES",
    "LogSink",
    "MAX_CLIENT_ORDER_ID",
    "TERMINAL_STATUSES",
    "UNKNOWN_ORDER_CODE",
    "VENUE_STATUS",
    "AlphaGateError",
    "LiveExecutionError",
    "LiveExecutor",
    "LiveStoreError",
    "LiveSupervisor",
    "OrderStore",
    "ReconcileReport",
    "SessionReport",
    "StepResult",
    "SubmissionResult",
    "SupervisorConfig",
    "alpha_gate_verdict",
    "alpha_gate_rejected",
    "funding_drain_warning",
    "kill_switch_reset",
    "kill_switch_tripped",
    "reconciliation_unresolved",
    "require_alpha_go",
    "router",
]
