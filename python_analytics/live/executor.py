"""Live order execution: durable intents, idempotent submits, reconciliation.

The paper engine never has to answer "did that order land?". A live one does,
on every ambiguous failure: a POST that times out may have been accepted, and
retrying it blindly can double a position while giving up on it can leave an
unhedged quote resting. This module makes that ambiguity explicit instead of
hoping:

* the intent is written to :class:`~python_analytics.live.store.OrderStore`
  before the request is sent, keyed by the client order id Binance dedupes on;
* a transport failure is recorded as ``UNKNOWN`` and then *resolved* by querying
  the venue with that same id, so the process learns what actually happened;
* :meth:`LiveExecutor.reconcile` diffs the local book against ``openOrders`` and
  the position, adopting orders it did not place and resolving orders that
  vanished while it was down;
* :meth:`LiveExecutor.panic` is the single kill path: cancel the symbol, flatten
  the position, and leave an audit trail either way.

Nothing here decides *whether* to quote. That is the strategy's job, and
:func:`require_alpha_go` exists because the honest answer today is that the
study is red: arming real money on a red study needs a typed override, not a
comment in a config file.
"""
from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Callable, Dict, List, Mapping, Optional, Tuple

from python_analytics.exchange.binance_futures import (
    BinanceAPIError,
    BinanceError,
    BinanceRateLimitError,
    BinanceTransportError,
    OpenOrder,
    OrderValidationError,
    Position,
)
from python_analytics.live.store import LiveStoreError, OrderStore

logger = logging.getLogger(__name__)

#: Binance's "order does not exist" code, returned when querying by client id.
UNKNOWN_ORDER_CODE = -2013

#: Venue status strings mapped onto the store's vocabulary.
VENUE_STATUS = {
    "NEW": "SUBMITTED",
    "PARTIALLY_FILLED": "SUBMITTED",
    "FILLED": "FILLED",
    "CANCELED": "CANCELLED",
    "CANCELLED": "CANCELLED",
    "EXPIRED": "EXPIRED",
    "EXPIRED_IN_MATCH": "EXPIRED",
    "REJECTED": "REJECTED",
}


class LiveExecutionError(RuntimeError):
    """Raised when a live execution invariant would be violated."""


class AlphaGateError(LiveExecutionError):
    """Raised when a real-money session is armed on a red or stale study."""


@dataclass(frozen=True)
class SubmissionResult:
    """Outcome of one submit attempt, including the resolved-ambiguity case."""

    client_order_id: str
    status: str
    venue_order_id: Optional[int] = None
    retryable: bool = False
    note: Optional[str] = None

    @property
    def accepted(self) -> bool:
        """True only when the venue is known to hold the order."""

        return self.status in ("SUBMITTED", "ADOPTED")

    def describe(self) -> str:
        venue = f" venueId={self.venue_order_id}" if self.venue_order_id else ""
        retry = " retryable" if self.retryable else ""
        note = f" ({self.note})" if self.note else ""
        return f"{self.client_order_id} {self.status}{venue}{retry}{note}"


@dataclass(frozen=True)
class ReconcileReport:
    """What the venue holds versus what this process thought it held."""

    venue_open: List[OpenOrder] = field(default_factory=list)
    adopted: List[OpenOrder] = field(default_factory=list)
    vanished: List[str] = field(default_factory=list)
    resolved: Dict[str, str] = field(default_factory=dict)
    position: Optional[Position] = None

    @property
    def unresolved(self) -> List[str]:
        """Client ids still in doubt after resolution (``UNKNOWN``/``LOST``)."""

        return [
            cid
            for cid, status in self.resolved.items()
            if status not in ("FILLED", "CANCELLED", "EXPIRED", "REJECTED")
        ]

    @property
    def clean(self) -> bool:
        return not self.adopted and not self.vanished

    def describe(self) -> str:
        position = (
            self.position.describe() if self.position is not None else "flat (no row)"
        )
        adopted = ", ".join(o.client_order_id or "?" for o in self.adopted) or "-"
        resolved = (
            ", ".join(f"{cid}={status}" for cid, status in self.resolved.items()) or "-"
        )
        return (
            f"reconcile: venue_open={len(self.venue_open)}"
            f" adopted={adopted} vanished={resolved} position={position}"
        )


def _decimal(value: Any, label: str) -> Decimal:
    try:
        parsed = Decimal(str(value))
    except Exception as exc:  # noqa: BLE001 - the message is the point
        raise OrderValidationError(f"{label} is not a number: {value!r}") from exc
    if parsed.is_nan() or parsed.is_infinite():
        raise OrderValidationError(f"{label} must be finite: {value!r}")
    return parsed


class LiveExecutor:
    """Owns the order lifecycle for one symbol on one account.

    The client is duck-typed on ``new_order`` / ``query_order`` / ``open_orders``
    / ``positions`` / ``cancel_all_orders`` / ``flatten`` so tests can inject a
    fake without a socket, matching how ``BinanceFuturesClient`` itself takes a
    transport.
    """

    def __init__(
        self,
        client: Any,
        store: OrderStore,
        symbol: str,
        *,
        clock: Callable[[], float] = time.time,
        id_prefix: str = "z",
    ) -> None:
        self._client = client
        self._store = store
        self.symbol = str(symbol or "").upper()
        if not self.symbol:
            raise LiveExecutionError("symbol must not be empty")
        self._clock = clock
        self._id_prefix = str(id_prefix)
        self._sequence = 0

    @property
    def store(self) -> OrderStore:
        return self._store

    def next_client_order_id(self) -> str:
        """A unique, venue-legal id: prefix-symbol-epochMs-sequence."""

        self._sequence += 1
        stamp = int(self._clock() * 1000)
        cid = f"{self._id_prefix}-{self.symbol.lower()}-{stamp}-{self._sequence}"
        if len(cid) > 36:
            raise LiveExecutionError(
                f"generated client order id exceeds 36 characters: {cid!r}"
            )
        return cid

    # ---------------------------------------------------------------- writing
    def submit_post_only(
        self,
        *,
        side: str,
        quantity: Any,
        price: Any,
        client_order_id: Optional[str] = None,
    ) -> SubmissionResult:
        """Places one post-only limit order, resolving an ambiguous failure.

        ``GTX`` makes the venue reject rather than cross, so a quote that would
        have taken liquidity fails loudly instead of paying the taker fee the
        cost model does not assume.
        """

        qty = _decimal(quantity, "quantity")
        px = _decimal(price, "price")
        if qty <= 0 or px <= 0:
            raise OrderValidationError(
                f"quantity and price must be positive: qty={qty} price={px}"
            )
        cid = str(client_order_id or self.next_client_order_id())

        # Durable before the wire: a crash between here and the response leaves
        # a row that reconciliation can chase, which is the whole point.
        self._store.record_intent(
            client_order_id=cid,
            symbol=self.symbol,
            side=side,
            price=px,
            quantity=qty,
        )

        try:
            payload = self._client.new_order(
                symbol=self.symbol,
                side=str(side).upper(),
                order_type="LIMIT",
                quantity=qty,
                price=px,
                client_order_id=cid,
                post_only=True,
            )
        except BinanceRateLimitError as exc:
            note = f"rate limited: {exc}"
            self._store.mark_status(cid, "REJECTED", note=note)
            self._store.record_event("rate_limited", f"{cid}: {exc}")
            return SubmissionResult(cid, "REJECTED", retryable=True, note=note)
        except BinanceTransportError as exc:
            # The request may or may not have reached the matcher. Say so, then
            # ask the venue instead of guessing.
            self._store.mark_status(cid, "UNKNOWN", note=f"transport error: {exc}")
            self._store.record_event("transport_error", f"{cid}: {exc}")
            status = self.resolve(cid)
            return SubmissionResult(
                cid, status, note="resolved after transport error"
            )
        except BinanceAPIError as exc:
            note = f"venue rejected: {exc}"
            self._store.mark_status(cid, "REJECTED", note=note)
            self._store.record_event("order_rejected", f"{cid}: {exc}")
            return SubmissionResult(cid, "REJECTED", note=note)

        venue_order_id = payload.get("orderId") if isinstance(payload, Mapping) else None
        self._store.mark_status(cid, "SUBMITTED", venue_order_id=venue_order_id)
        self._store.record_event("order_submitted", f"{cid}: {px} x {qty}")
        logger.info("submitted %s %s %s @ %s", self.symbol, side, qty, px)
        return SubmissionResult(cid, "SUBMITTED", venue_order_id=venue_order_id)

    def resolve(self, client_order_id: str) -> str:
        """Asks the venue what became of ``client_order_id`` and records it."""

        try:
            payload = self._client.query_order(
                symbol=self.symbol, client_order_id=client_order_id
            )
        except BinanceAPIError as exc:
            if exc.code == UNKNOWN_ORDER_CODE:
                self._store.mark_status(
                    client_order_id, "LOST", note="venue reports no such order"
                )
                self._store.record_event(
                    "order_lost", f"{client_order_id}: venue has no such order"
                )
                return "LOST"
            raise

        raw_status = str(payload.get("status") or "").upper()
        venue_order_id = payload.get("orderId")
        status = VENUE_STATUS.get(raw_status)
        if status is None:
            self._store.mark_status(
                client_order_id,
                "UNKNOWN",
                venue_order_id=venue_order_id,
                note=f"unrecognised venue status {raw_status!r}",
            )
            return "UNKNOWN"
        self._store.mark_status(
            client_order_id,
            status,
            venue_order_id=venue_order_id,
            note=f"resolved from {raw_status}",
        )
        self._store.record_event(
            "order_resolved", f"{client_order_id}: {raw_status} -> {status}"
        )
        return status

    # ------------------------------------------------------------ reconciling
    def reconcile(self) -> ReconcileReport:
        """Diffs the local book against the venue and repairs the local side.

        This is the call that makes a restart safe. It never cancels anything on
        its own: an order the process did not place is adopted (so the next
        ``panic`` knows to cancel it) rather than silently killed, and a local
        order missing from ``openOrders`` is *resolved* by query, because it may
        have filled instead of having been cancelled.
        """

        venue_open = self._client.open_orders(self.symbol)
        venue_ids = {o.client_order_id for o in venue_open if o.client_order_id}
        local_active = self._store.active_orders(self.symbol)
        local_ids = {row["client_order_id"] for row in local_active}

        adopted: List[OpenOrder] = []
        for order in venue_open:
            cid = order.client_order_id
            if cid and cid in local_ids:
                continue
            if not cid:
                self._store.record_event(
                    "venue_order_without_client_id", order.describe()
                )
                adopted.append(order)
                continue
            try:
                if self._store.get(cid) is None:
                    self._store.record_intent(
                        client_order_id=cid,
                        symbol=self.symbol,
                        side=order.side,
                        price=order.price,
                        quantity=order.quantity,
                        note="adopted from venue during reconcile",
                    )
                self._store.mark_status(
                    cid,
                    "ADOPTED",
                    venue_order_id=order.order_id,
                    note="adopted from venue during reconcile",
                )
            except LiveStoreError as exc:
                # A terminal local row while the venue still holds the order is
                # real drift; surface it rather than pretending it away.
                self._store.record_event("adopt_conflict", f"{cid}: {exc}")
            adopted.append(order)

        resolved: Dict[str, str] = {}
        for row in local_active:
            cid = str(row["client_order_id"])
            if cid in venue_ids:
                continue
            try:
                resolved[cid] = self.resolve(cid)
            except BinanceError as exc:
                self._store.record_event("resolve_failed", f"{cid}: {exc}")
                resolved[cid] = "UNKNOWN"

        position = next(iter(self._client.positions(self.symbol)), None)
        report = ReconcileReport(
            venue_open=venue_open,
            adopted=adopted,
            vanished=list(resolved),
            resolved=resolved,
            position=position,
        )
        self._store.record_event("reconciled", report.describe())
        if not report.clean or report.unresolved:
            logger.warning("%s", report.describe())
        return report

    def panic(self, reason: str = "manual") -> List[str]:
        """Cancels the symbol and flattens the position; returns what failed.

        Callers must log the return value and exit non-zero on a non-empty list:
        a kill switch that reports success it did not achieve is worse than no
        kill switch at all.
        """

        errors: List[str] = []
        self._store.record_event("panic", reason)
        logger.error("panic: %s (%s)", reason, self.symbol)

        cancel_ok = True
        try:
            self._client.cancel_all_orders(self.symbol)
        except BinanceError as exc:
            cancel_ok = False
            errors.append(f"cancel_all_orders: {exc}")
            logger.error("panic: cancel_all_orders failed: %s", exc)

        position = None
        try:
            position = next(iter(self._client.positions(self.symbol)), None)
        except BinanceError as exc:
            errors.append(f"positions: {exc}")
            logger.error("panic: position read failed: %s", exc)

        if position is not None and not position.is_flat:
            try:
                self._client.flatten(self.symbol)
                logger.error("panic: flattened %s", position.describe())
            except BinanceError as exc:
                errors.append(f"flatten: {exc}")
                logger.error("panic: flatten failed: %s", exc)

        for row in self._store.active_orders(self.symbol):
            self._store.mark_status(
                str(row["client_order_id"]),
                "CANCELLED" if cancel_ok else "UNKNOWN",
                note=f"panic: {reason}",
            )

        self._store.record_event(
            "panic_complete", f"{reason}: errors={errors or 'none'}"
        )
        return errors


# --------------------------------------------------------------------------- #
# Arming gate
# --------------------------------------------------------------------------- #
def _report_age_hours(
    report: Mapping[str, Any], now: Optional[datetime]
) -> float:
    stamp = report.get("generated_at")
    if not isinstance(stamp, str) or not stamp.strip():
        raise AlphaGateError(
            "readiness report carries no generated_at; freshness cannot be judged"
        )
    try:
        generated = datetime.fromisoformat(stamp.strip().replace("Z", "+00:00"))
    except ValueError as exc:
        raise AlphaGateError(f"unparseable generated_at: {stamp!r}") from exc
    if generated.tzinfo is None:
        generated = generated.replace(tzinfo=timezone.utc)
    reference = now or datetime.now(timezone.utc)
    return (reference - generated).total_seconds() / 3600.0


def alpha_gate_verdict(report: Mapping[str, Any]) -> Tuple[bool, str]:
    """Reads the readiness report the way a human should: is there an edge?

    The gate is deliberately two-part. A passing alpha *phase* is not enough on
    its own — the study can pass every structural check and still print a
    negative net once fees are applied — so the net figure is read explicitly,
    and the report's own headline verdict is cross-checked when present.
    """

    if not isinstance(report, Mapping):
        return False, "readiness report is not a JSON object"
    checks = report.get("checks")
    if not isinstance(checks, list):
        return False, "readiness report has no checks list"
    alpha = [
        c for c in checks if isinstance(c, Mapping) and c.get("phase") == "alpha"
    ]
    if not alpha:
        return False, "readiness report has no alpha phase checks"
    failed = [
        str(c.get("id") or c.get("name") or "?") for c in alpha if not c.get("passed")
    ]
    if failed:
        return False, "alpha checks failed: " + ", ".join(sorted(failed))

    net: Any = None
    evidence = report.get("evidence")
    if isinstance(evidence, Mapping):
        block = evidence.get("alpha")
        if isinstance(block, Mapping):
            net = block.get("net_pnl")
    if net is None:
        return False, "readiness report carries no alpha.net_pnl"
    try:
        value = float(net)
    except (TypeError, ValueError):
        return False, f"alpha.net_pnl is not numeric: {net!r}"
    if value <= 0:
        return (
            False,
            f"alpha.net_pnl is {value:.4f} USDT after costs; a live session needs > 0",
        )
    headline = report.get("alpha_verdict")
    if isinstance(headline, str) and headline.strip().upper() not in ("GO", "PASS"):
        # Evidence says edge, headline says no: a contradictory report is not a
        # licence to trade, it is a bug in whichever side is stale.
        return (
            False,
            f"report headline alpha_verdict is {headline!r} while the checks "
            "pass; refusing a contradictory report",
        )
    return True, f"alpha GO (net_pnl={value:.4f} USDT)"


def require_alpha_go(
    report_path: Any,
    *,
    max_age_hours: float = 168.0,
    override_reason: Optional[str] = None,
    store: Optional[OrderStore] = None,
    now: Optional[datetime] = None,
) -> str:
    """Refuses to arm a real-money session on a red or stale study.

    ``override_reason`` is the escape hatch, and it is intentionally a sentence
    rather than a boolean: it is written to the order store's audit trail, where
    the answer to "why did we trade a negative-expectancy model" has to live.
    """

    path = Path(report_path)
    if not path.is_file():
        raise AlphaGateError(f"readiness report not found: {path}")
    try:
        report = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise AlphaGateError(f"unreadable readiness report {path}: {exc}") from exc

    ok, detail = alpha_gate_verdict(report)
    try:
        age = _report_age_hours(report, now)
    except AlphaGateError as exc:
        age = None
        freshness = str(exc)
    else:
        freshness = f"{age:.1f}h old (limit {max_age_hours:.0f}h)"
    stale = age is None or age > max_age_hours

    if ok and not stale:
        if store is not None:
            store.record_event("alpha_gate", detail)
        return detail

    why = detail if not ok else f"evidence is stale: {freshness}"
    if override_reason:
        message = f"alpha override ({why}): {override_reason}"
        logger.warning("%s", message)
        if store is not None:
            store.record_event("alpha_override", message)
        return message
    if store is not None:
        store.record_event("alpha_gate_blocked", why)
    raise AlphaGateError(why + "; pass an explicit override reason to trade anyway")
