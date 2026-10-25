"""Live execution: durable order state, reconciliation and the arming gate.

``LiveExecutor`` is deliberately strategy-agnostic: it takes the orders a
strategy decided to place and makes sure the process and the venue agree on what
exists. ``require_alpha_go`` is the other half — it decides whether a real-money
session may be armed at all.
"""
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
from .store import (
    ACTIVE_STATUSES,
    KNOWN_STATUSES,
    MAX_CLIENT_ORDER_ID,
    TERMINAL_STATUSES,
    LiveStoreError,
    OrderStore,
)

__all__ = [
    "ACTIVE_STATUSES",
    "KNOWN_STATUSES",
    "MAX_CLIENT_ORDER_ID",
    "TERMINAL_STATUSES",
    "UNKNOWN_ORDER_CODE",
    "VENUE_STATUS",
    "AlphaGateError",
    "LiveExecutionError",
    "LiveExecutor",
    "LiveStoreError",
    "OrderStore",
    "ReconcileReport",
    "SubmissionResult",
    "alpha_gate_verdict",
    "require_alpha_go",
]
