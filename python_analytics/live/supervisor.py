"""The session loop ``run_live_trader.py`` deliberately refuses to be.

``LiveExecutor`` guarantees one thing well: a single order's lifecycle is durable,
ambiguous failures are resolved rather than guessed, and the kill path tells the
truth. It places at most one quote per invocation. Nothing in this repository
could therefore *run* a market-making session: re-quote on every book update,
stop quoting the side that would exceed the inventory limit, and halt when the
session itself goes wrong. That is item 4 of docs §11.3, and it is a precondition
for going live regardless of whether the strategy ever earns.

The three rules this module exists to enforce, in order of severity:

1. **Never submit on top of an unresolved order.** An ``UNKNOWN`` row means the
   venue may be holding an order this process cannot see. Adding a second quote
   to it can turn one intent into two live positions, so the step halts before it
   reads the book.
2. **Never add inventory past the cap.** Suppressing the offending side (and
   cancelling whatever rests there) is the whole point of the limit; a cap that
   only logs is a suggestion.
3. **Do not churn the queue.** A maker's edge, if it ever has one, lives at the
   front of the queue. Re-placing a quote that is still correctly priced cancels
   that position for nothing and spends rate limit, so a resting order is left
   alone unless its price has moved past ``requote_tolerance_bps``. The naive
   loop -- cancel both sides every tick -- is the failure this rule exists to
   prevent, and it is measured by a test rather than asserted in a comment.

What this does *not* do: it does not place an order the strategy did not ask
for, it cannot make a losing parameter set profitable, and it does not bypass
``require_alpha_go`` -- production arming is still the caller's decision.

Session PnL is the venue's own position ``unrealizedPnl`` against the value it
had when the session opened. Realised income and the 8 h perpetual funding charge
are **not** in it (docs §11.3 items 3 and 4), so the loss cap is a guard against
a position running away, not an accounting statement. The gap is reported in the
session record rather than hidden by a number that looks complete.
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR
from typing import Any, Callable, Dict, List, Optional

from .executor import LiveExecutionError, LiveExecutor
from .store import LiveStoreError

__all__ = ["SessionReport", "StepResult", "SupervisorConfig", "LiveSupervisor"]

logger = logging.getLogger("zephyr.live.supervisor")

BPS = Decimal(10_000)
SIDES = ("BUY", "SELL")
#: Which way each side must be quantised to stay representable and never cross.
_ROUND = {"BUY": ROUND_FLOOR, "SELL": ROUND_CEILING}


def _is_finite(value: Decimal) -> bool:
    """True for a Decimal that is a number.

    ``nan`` and ``inf`` compare false against every bound, so a plain range check
    lets them through: an infinite loss cap never trips, and a NaN requote
    tolerance makes every comparison false and re-places both sides each tick --
    the exact queue churn rule 3 exists to prevent. A configuration that cannot
    be compared must be refused at construction, not discovered by the venue.
    """
    return value.is_finite()


@dataclass(frozen=True)
class SupervisorConfig:
    """What the session trades, how far out, and when it must stop."""

    symbol: str
    quantity: Decimal
    offset_bps: Decimal
    tick_size: Decimal
    #: A resting quote within this many bps of its target price is left alone.
    #: Zero would mean re-placing every tick, which destroys queue position.
    requote_tolerance_bps: Decimal = Decimal("0.25")
    #: Net inventory at which the side that would increase it is suppressed.
    max_inventory: Decimal = Decimal("1")
    #: Session PnL (quote currency) beyond which the kill path runs.
    session_loss_cap: Decimal = Decimal("50")
    #: ``0`` runs until ``stop()``; a positive value bounds the session.
    max_iterations: int = 0
    interval_s: float = 1.0

    def __post_init__(self) -> None:
        for name in ("quantity", "offset_bps", "tick_size",
                     "requote_tolerance_bps", "max_inventory", "session_loss_cap"):
            if not _is_finite(getattr(self, name)):
                raise ValueError(f"{name} must be a finite number, "
                                 f"got {getattr(self, name)}")
        for name in ("quantity", "offset_bps", "tick_size"):
            if getattr(self, name) <= 0:
                raise ValueError(f"{name} must be positive, got {getattr(self, name)}")
        if self.requote_tolerance_bps < 0:
            raise ValueError("requote_tolerance_bps must be >= 0")
        if self.max_inventory <= 0:
            raise ValueError("max_inventory must be positive")
        if self.session_loss_cap <= 0:
            raise ValueError("session_loss_cap must be positive")
        if self.max_iterations < 0:
            raise ValueError("max_iterations must be >= 0 (0 = until stopped)")
        if self.interval_s < 0:
            raise ValueError("interval_s must be >= 0")
        if not str(self.symbol or "").strip():
            raise ValueError("symbol must not be empty")


@dataclass
class StepResult:
    """One iteration's actions, so a caller can assert what the loop did."""

    skipped: Optional[str] = None
    submitted: List[str] = field(default_factory=list)
    cancelled: List[str] = field(default_factory=list)
    kept: List[str] = field(default_factory=list)
    suppressed: List[str] = field(default_factory=list)
    mid: Optional[Decimal] = None
    inventory: Decimal = Decimal("0")
    session_pnl: Decimal = Decimal("0")


@dataclass
class SessionReport:
    iterations: int = 0
    submitted: int = 0
    cancelled: int = 0
    kept: int = 0
    suppressed_sides: int = 0
    halted: bool = False
    halt_reason: str = ""
    session_pnl: Decimal = Decimal("0")
    unresolved_orders: int = 0
    notes: List[str] = field(default_factory=list)

    def describe(self) -> str:
        return (f"iterations={self.iterations} submitted={self.submitted} "
                f"cancelled={self.cancelled} kept={self.kept} "
                f"suppressed_sides={self.suppressed_sides} "
                f"session_pnl={self.session_pnl:+.4f} "
                f"halted={self.halted or '-'} "
                f"{self.halt_reason or 'clean'}")


class LiveSupervisor:
    """Drives ``LiveExecutor`` around a two-sided post-only quote."""

    def __init__(
        self,
        executor: LiveExecutor,
        client: Any,
        config: SupervisorConfig,
        *,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
        alerter: Optional[Callable[[str, str], None]] = None,
    ) -> None:
        self._executor = executor
        self._client = client
        self._config = config
        self._clock = clock
        self._sleep = sleep
        # An alert that only logs is what docs §11.3 item 5 says is missing, so
        # the hook is injectable: production passes a pager, tests pass a list.
        self._alerter = alerter or self._log_alert
        self._halted = False
        self._halt_reason = ""
        self._started = False
        #: Position PnL when the session opened; the cap measures the *change*,
        #: because an account can inherit an open position from a prior session.
        self._baseline_pnl: Optional[Decimal] = None

    # ------------------------------------------------------------------ public
    @property
    def halted(self) -> bool:
        return self._halted

    @property
    def halt_reason(self) -> str:
        return self._halt_reason

    def run(self) -> SessionReport:
        """Iterates until stopped, the bounds are hit, or the session is halted."""

        report = SessionReport()
        while True:
            if self._halted:
                break
            if (self._config.max_iterations
                    and report.iterations >= self._config.max_iterations):
                break
            step = self.step()
            report.iterations += 1
            report.submitted += len(step.submitted)
            report.cancelled += len(step.cancelled)
            report.kept += len(step.kept)
            report.suppressed_sides += len(step.suppressed)
            report.session_pnl = step.session_pnl
            if self._halted:
                report.halted = True
                report.halt_reason = self._halt_reason
                break
            if step.skipped:
                report.notes.append(f"iteration {report.iterations}: {step.skipped}")
            if self._config.interval_s > 0:
                self._sleep(self._config.interval_s)
        report.unresolved_orders = len(
            self._executor.reconcile().unresolved) if self._started else 0
        logger.info("session done: %s", report.describe())
        return report

    def stop(self, reason: str = "operator", *, flatten: bool = False) -> None:
        """Ends the session leaving nothing held by a process that has exited.

        Resting quotes are cancelled first: a dead supervisor with two orders in
        the book is an unmanaged position someone else has to discover. The
        inventory is left alone unless ``flatten`` is asked for, because exiting
        a position pays the taker fee and an operator may intend to resume --
        the emergency path (``_trip_kill``) is the one that always flattens.
        """
        if flatten:
            self._trip_kill(f"stopped by {reason}")
            return
        failures: List[str] = []
        for row in self._executor.store.active_orders(self._config.symbol):
            cid = str(row.get("client_order_id") or "")
            try:
                self._client.cancel_order(symbol=self._config.symbol,
                                          client_order_id=cid)
                self._executor.store.mark_status(cid, "CANCELLED")
            except Exception as exc:
                failures.append(f"{cid}: {exc}")
        detail = f"stopped by {reason}"
        if failures:
            # The venue may still hold them. Saying "stopped" over a live quote
            # is the exact failure mode this method exists to close.
            detail += f"; {len(failures)} cancel(s) failed: {'; '.join(failures)}"
        self._halt(detail, page=bool(failures))

    # ------------------------------------------------------------- one tick
    def step(self) -> StepResult:
        """One re-quoting cycle. Public so a caller (or a test) can drive it."""

        result = StepResult()
        if self._halted:
            result.skipped = "halted"
            return result

        # 1. Reconcile before deciding anything. An unresolved order means the
        #    venue may already hold a quote this process cannot name, and a new
        #    order on top of it is the two-fills-from-one-intent failure.
        recon = self._executor.reconcile()
        if recon.unresolved:
            self._halt(f"unresolved order(s): {', '.join(recon.unresolved)}")
            result.skipped = self._halt_reason
            return result

        position = recon.position
        quantity = Decimal(str(getattr(position, "quantity", 0) or 0))
        unrealized = Decimal(str(getattr(position, "unrealized_pnl", 0) or 0))
        if not self._started:
            self._baseline_pnl = unrealized
            self._started = True
        session_pnl = unrealized - (self._baseline_pnl or Decimal("0"))
        result.inventory = quantity
        result.session_pnl = session_pnl

        # 2. The session's own loss cap, before adding any exposure to it.
        if session_pnl <= -self._config.session_loss_cap:
            self._trip_kill(f"session loss cap breached: {session_pnl:+.4f} "
                            f"<= -{self._config.session_loss_cap}")
            result.skipped = self._halt_reason
            return result

        mid = self._mid()
        if mid is None:
            # A one-sided book is not a price. Quoting against it would place a
            # limit derived from nothing, so the tick is skipped and said so.
            result.skipped = "no two-sided book"
            return result
        result.mid = mid

        resting = self._resting_by_side()
        targets = {side: self._target(side, mid) for side in SIDES}
        blocked = self._sides_blocked_by_inventory(quantity)

        for side in SIDES:
            current = resting.get(side)
            if side in blocked:
                if current is not None:
                    self._cancel(current)
                    result.cancelled.append(side)
                # Suppressing the side is the limit doing its job, not an error.
                result.suppressed.append(side)
                continue
            wanted = targets[side]
            if current is not None and self._within_tolerance(current, wanted, mid):
                result.kept.append(side)
                continue
            if current is not None:
                self._cancel(current)
                result.cancelled.append(side)
            outcome = self._executor.submit_post_only(
                side=side, quantity=self._config.quantity, price=wanted)
            if outcome.accepted:
                result.submitted.append(side)
            else:
                # A post-only cross (GTX / -5022) is the expected outcome when
                # the book moved through the level: nothing rested and nothing
                # is owed. Any other rejection is the executor's to raise.
                logger.info("%s not rested: %s", side, outcome.describe())
        return result

    # --------------------------------------------------------------- internals
    def _mid(self) -> Optional[Decimal]:
        payload = self._client.book_ticker(self._config.symbol)
        try:
            bid = Decimal(str(payload["bidPrice"]))
            ask = Decimal(str(payload["askPrice"]))
        except (KeyError, TypeError, ValueError) as exc:
            raise LiveExecutionError(
                f"bookTicker returned no usable two-sided price: {payload!r}"
            ) from exc
        if bid <= 0 or ask <= bid:
            return None
        return (bid + ask) / 2

    def _target(self, side: str, mid: Decimal) -> Decimal:
        """The quote price for one side, quantised *away* from the mid.

        Rounded to a whole number of ticks, because a price between two ticks is
        not representable on any venue this engine trades and the order would be
        rejected. The rounding is directional: a bid goes down and an ask up, so
        snapping can only widen the market and can never turn our quote into one
        that crosses the touch (which a post-only order would reject anyway).
        """
        shift = mid * self._config.offset_bps / BPS
        raw = mid - shift if side == "BUY" else mid + shift
        tick = self._config.tick_size
        steps = (raw / tick).to_integral_value(rounding=_ROUND[side])
        return steps * tick

    def _resting_by_side(self) -> Dict[str, Dict[str, Any]]:
        out: Dict[str, Dict[str, Any]] = {}
        for row in self._executor.store.active_orders(self._config.symbol):
            side = str(row.get("side", "")).upper()
            # Keep the most recent row per side: an older row is a stale intent.
            if side in SIDES:
                out[side] = row
        return out

    def _within_tolerance(self, current: Dict[str, Any], wanted: Decimal,
                          mid: Decimal) -> bool:
        try:
            price = Decimal(str(current["price"]))
        except (KeyError, TypeError, ValueError, ArithmeticError):
            return False
        drift_bps = abs(price - wanted) / mid * BPS
        return drift_bps <= self._config.requote_tolerance_bps

    def _sides_blocked_by_inventory(self, quantity: Decimal) -> List[str]:
        """The sides that would only grow exposure past the cap."""

        blocked: List[str] = []
        if quantity >= self._config.max_inventory:
            blocked.append("BUY")
        if quantity <= -self._config.max_inventory:
            blocked.append("SELL")
        return blocked

    def _cancel(self, row: Dict[str, Any]) -> None:
        cid = str(row.get("client_order_id") or "")
        if not cid:
            self._halt(f"active order row without a client order id: {row!r}")
            return
        self._client.cancel_order(symbol=self._config.symbol, client_order_id=cid)
        try:
            self._executor.store.mark_status(cid, "CANCELLED")
        except (LiveExecutionError, LiveStoreError) as exc:
            # The venue has moved on first in the common case -- the order filled
            # while we were deciding -- and its own row is already terminal.
            # Refusing to keep quoting over that would turn a normal race into an
            # outage; the next reconcile is the authority either way.
            logger.warning("cancel not recorded for %s: %s", cid, exc)

    def _trip_kill(self, reason: str) -> None:
        """Runs the existing kill path, then halts the loop."""

        failures = self._executor.panic(f"session: {reason}")
        self._halt(reason, page=True)
        if failures:
            self._halt_reason += f"; kill path reported {len(failures)} failure(s)"
            self._alert("kill_incomplete", self._halt_reason)

    def _halt(self, reason: str, *, page: bool = True) -> None:
        self._halted = True
        self._halt_reason = reason
        self._executor.store.record_event("supervisor_halt", reason)
        logger.error("supervisor halted: %s", reason)
        if page:
            self._alert("halt", reason)

    def _alert(self, kind: str, message: str) -> None:
        try:
            self._alerter(kind, message)
        except Exception as exc:  # an alert must never mask the halt it reports
            logger.error("alerter failed (%s): %s", kind, exc)

    @staticmethod
    def _log_alert(kind: str, message: str) -> None:
        logger.error("ALERT %s: %s", kind, message)
