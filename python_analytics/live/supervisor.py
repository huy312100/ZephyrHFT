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

from python_analytics.exchange.binance_futures import BinanceError

__all__ = ["SessionReport", "StepResult", "SupervisorConfig", "LiveSupervisor"]

logger = logging.getLogger("zephyr.live.supervisor")

# The venue codes that mean "nothing rested and nothing is owed", which is the
# only rejection a post-only maker loop may treat as normal. ``-5022`` is
# futures-only and self-proving; ``-2010`` is the generic new-order rejection and
# has to be confirmed by its text (same rule ``run_live_trader.py`` applies).
POST_ONLY_CROSS_CODES = (-5022,)
POST_ONLY_GENERIC_CODE = -2010
POST_ONLY_GENERIC_TEXT = "would immediately trigger"
#: Statuses that mean the venue's answer is unknown, so the loop must stop.
AMBIGUOUS_STATUSES = ("UNKNOWN", "LOST")

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
    rejected: List[str] = field(default_factory=list)
    #: Sides whose stale quote could not be cancelled, left resting on purpose
    #: rather than doubled. Reported so a quiet tick is not mistaken for a clean one.
    held_over: List[str] = field(default_factory=list)
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
        self._finished = False
        self._started = False
        #: True once the kill path has run. The caller needs this as a flag, not
        #: as a substring of the reason: running ``panic()`` twice would flatten
        #: a position that is already being closed and report it as a new failure.
        self._killed = False
        #: Client ids this session placed. ``reconcile`` adopts orders it did not
        #: place, and those must never be cancelled by us: the live layer's
        #: documented promise is that a manual order is adopted passively.
        self._session_ids: Dict[str, str] = {}
        #: Cancels that did not land. Distinct from ``halted`` because a stop is
        #: halted either way, and only this says the venue may still hold a quote.
        self.cleanup_failures: List[str] = []
        #: Set by a signal handler; acted upon between steps, never inside one.
        #: Handling a stop in the handler would let the interrupted step carry on
        #: quoting after the operator was told the session had stopped.
        self.stop_requested = False
        #: Position PnL when the session opened; the cap measures the *change*,
        #: because an account can inherit an open position from a prior session.
        self._baseline_pnl: Optional[Decimal] = None

    # ------------------------------------------------------------------ public
    @property
    def halted(self) -> bool:
        """True when something went wrong and the session needs attention."""
        return self._halted

    @property
    def finished(self) -> bool:
        """True after a routine stop that left nothing behind.

        Kept apart from ``halted`` on purpose: a session that ran to its
        iteration limit and cancelled both quotes *finished*, and reporting that
        as an incident would train the operator to ignore the exit code.
        """
        return self._finished

    @property
    def killed(self) -> bool:
        """True when the kill path has already run for this session."""
        return self._killed

    @property
    def halt_reason(self) -> str:
        return self._halt_reason

    def run(self) -> SessionReport:
        """Iterates until stopped, the bounds are hit, or the session is halted."""

        report = SessionReport()
        while True:
            if self._halted or self._finished or self.stop_requested:
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
            if step.skipped == "stop requested":
                report.halted = True
                report.halt_reason = "stop requested"
                break
            if step.skipped:
                report.notes.append(f"iteration {report.iterations}: {step.skipped}")
            if self._config.interval_s > 0:
                self._sleep(self._config.interval_s)
        # The last word on what the venue holds, so a caller can refuse to call
        # this run a success while an order is still out there.
        if self._started:
            try:
                report.unresolved_orders = len(self._executor.reconcile().unresolved)
            except BinanceError as exc:
                report.notes.append(f"final reconcile unreadable: {exc}")
                report.unresolved_orders = -1
        logger.info("session done: %s", report.describe())
        return report

    def stop(self, reason: str = "operator", *, flatten: bool = False) -> None:
        """Ends the session leaving nothing of ours held by a dead process.

        Only ids this session placed are cancelled. An order ``reconcile`` adopted
        belongs to a human or to an earlier run, and the live layer's promise is
        that such an order is never killed by us.

        Inventory is left alone unless ``flatten`` is asked for, because exiting a
        position pays the taker fee and an operator may intend to resume -- the
        emergency path (``_trip_kill``) is the one that always flattens.
        """
        if flatten:
            self._trip_kill(f"stopped by {reason}")
            return
        self._cancel_session_orders()
        if self._halted:
            return                      # an incident during cleanup already halted
        if self.cleanup_failures:
            # Saying "stopped" over a quote the venue still holds is the exact
            # failure this method exists to close.
            self._halt(f"stopped by {reason} but "
                       f"{len(self.cleanup_failures)} cancel(s) failed: "
                       + "; ".join(self.cleanup_failures), page=True,
                       incident=True)
            return
        self._halt(f"stopped by {reason}", page=False, incident=False)

    # ------------------------------------------------------------- one tick
    def step(self) -> StepResult:
        """One re-quoting cycle. Public so a caller (or a test) can drive it."""

        result = StepResult()
        if self._halted:
            result.skipped = "halted"
            return result
        if self._finished or self.stop_requested:
            # A session that was stopped has stopped: quoting after that would
            # put an order in the book from a process the operator already
            # believes is finished.
            result.skipped = "stopped"
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

        resting = self._session_resting_by_side()
        if resting is None:
            result.skipped = self._halt_reason
            return result

        targets = {side: self._target(side, mid) for side in SIDES}
        blocked = self._sides_blocked_by_inventory(quantity)

        for side in SIDES:
            # The handler only sets a flag; acting here means an interrupted
            # session never places a quote after the operator asked it to stop.
            if self.stop_requested:
                result.skipped = "stop requested"
                return result
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
                # A cancel the venue refused means the old quote is probably still
                # live: replacing it now would put two orders on one side.
                if not self._cancel(current):
                    if self._halted:
                        result.skipped = self._halt_reason
                        return result
                    result.held_over.append(side)
                    continue
                result.cancelled.append(side)
            outcome = self._place(side, wanted)
            if outcome is None:
                result.skipped = self._halt_reason
                return result
            if outcome.accepted:
                result.submitted.append(side)
            else:
                result.rejected.append(f"{side}:{outcome.status}")
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

    def _session_resting_by_side(self) -> Optional[Dict[str, Dict[str, Any]]]:
        """This session's active rows, one per side, or None if we must not act.

        Two rules are enforced here rather than assumed:

        * An active order this process did not place is **never** cancelled by us
          -- ``reconcile`` adopts it precisely so a human's manual order survives.
          If it sits on a side we would quote, we cannot tell the venue's state
          from ours, so the session halts instead of trading over it.
        * Two session rows on one side mean an earlier cancel did not land.
          Re-quoting that side would add a third; halting is the honest answer.
        """
        out: Dict[str, Dict[str, Any]] = {}
        foreign: List[str] = []
        for row in self._executor.store.active_orders(self._config.symbol):
            side = str(row.get("side", "")).upper()
            if side not in SIDES:
                continue
            cid = str(row.get("client_order_id") or "")
            if cid not in self._session_ids:
                foreign.append(f"{side}={cid or '?'}")
                continue
            if side in out:
                self._halt(f"two active {side} rows for one session; the venue "
                           "state is not knowable from here")
                return None
            out[side] = row
        if foreign:
            self._halt("refusing to quote over an order this session did not "
                       f"place: {', '.join(foreign)}")
            return None
        return out

    def _place(self, side: str, price: Decimal) -> Optional[Any]:
        """Submits one quote, registering its id *before* the request leaves.

        The id is claimed first because an ambiguous submit may still have
        landed: if we did not record it, the next reconcile would read the live
        order as foreign and this session would halt on its own quote.
        """
        cid = self._executor.next_client_order_id()
        self._session_ids[cid] = side
        outcome = self._executor.submit_post_only(
            side=side, quantity=self._config.quantity, price=price,
            client_order_id=cid)
        if outcome.accepted:
            return outcome
        if outcome.status in AMBIGUOUS_STATUSES:
            # The venue may be holding it. Submitting the other side before that
            # is resolved risks two live orders from one intent, so the tick ends.
            self._halt(f"{side} submit is {outcome.status}: "
                       f"{outcome.describe()}")
            return None
        if self._is_post_only_cross(outcome):
            # Expected when the book moved through the level: nothing rested,
            # nothing is owed, and the next tick re-evaluates the price.
            logger.info("%s not rested (post-only cross): %s", side,
                        outcome.describe())
            return outcome
        # Any other rejection is a filter/notional/pricing mistake: repeating it
        # every tick spends rate limit on a configuration error.
        self._halt(f"{side} rejected: {outcome.describe()}")
        return None

    @staticmethod
    def _is_post_only_cross(outcome: Any) -> bool:
        code = getattr(outcome, "code", None)
        if code in POST_ONLY_CROSS_CODES:
            return True
        if code == POST_ONLY_GENERIC_CODE:
            return POST_ONLY_GENERIC_TEXT in str(getattr(outcome, "note", "")).lower()
        return False

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

    def _cancel(self, row: Dict[str, Any]) -> bool:
        """Cancels one of this session's orders. True when it is safe to replace.

        ``-2011`` ("Unknown order sent") is the ordinary race where the order
        filled while we were deciding, and letting the exception escape would
        abandon the loop with the other side unmanaged. So a refused cancel is
        *resolved*, and the answer decides what happens next:

        * gone (``FILLED`` / ``CANCELLED`` / ``REJECTED``) -- safe to replace;
        * still live -- **not** safe. Placing a second order on that side is the
          double-exposure failure rule 1 exists to prevent, so the caller leaves
          the resting quote alone and retries on the next tick;
        * unreadable -- halt, because the venue's state is unknown.
        """
        cid = str(row.get("client_order_id") or "")
        if not cid:
            self._halt(f"active order row without a client order id: {row!r}")
            return False
        try:
            self._client.cancel_order(symbol=self._config.symbol,
                                      client_order_id=cid)
        except BinanceError as exc:
            try:
                status = self._executor.resolve(cid)
            except BinanceError as resolve_exc:
                self._halt(f"cancel of {cid} failed ({exc}) and the venue "
                           f"answer could not be read: {resolve_exc}")
                return False
            if status in AMBIGUOUS_STATUSES:
                self._halt(f"cancel of {cid} failed and the order is still "
                           f"{status}")
                return False
            logger.warning("cancel of %s refused (%s); resolved as %s, so it "
                           "stays where it is until the next tick", cid, exc,
                           status)
            return False
        self._session_ids.pop(cid, None)
        try:
            self._executor.store.mark_status(cid, "CANCELLED")
        except (LiveExecutionError, LiveStoreError) as exc:
            # The venue has moved on first in the common case -- the order filled
            # while we were deciding -- and its own row is already terminal. The
            # cancel did land at the venue, so replacing it is still correct;
            # the next reconcile settles the local row.
            logger.warning("cancel not recorded for %s: %s", cid, exc)
        return True

    def _cancel_session_orders(self) -> None:
        """Cancels only what this session placed, and records what did not land.

        A refused cancel whose order is still live is a cleanup failure: the
        session is over but the venue holds a quote nobody is managing, which the
        caller must not report as a clean exit.
        """

        for row in self._executor.store.active_orders(self._config.symbol):
            cid = str(row.get("client_order_id") or "")
            if cid not in self._session_ids:
                # An adopted order outlives us by design: someone else owns it.
                continue
            if not self._cancel(row) and not self._halted:
                self.cleanup_failures.append(
                    f"{cid}: still resting after a refused cancel")

    def _trip_kill(self, reason: str) -> None:
        """Runs the existing kill path, then halts the loop."""

        self._killed = True
        failures = self._executor.panic(f"session: {reason}")
        self._halt(reason, page=True)
        if failures:
            self._halt_reason += f"; kill path reported {len(failures)} failure(s)"
            self._alert("kill_incomplete", self._halt_reason)

    def _halt(self, reason: str, *, page: bool = True,
              incident: bool = True) -> None:
        """Records why the loop stopped.

        ``incident=False`` is a routine finish: it stops the loop and says so,
        but it does not make the session look broken to the caller's exit code.
        """
        self._halt_reason = reason
        if incident:
            self._halted = True
        else:
            self._finished = True
        self._executor.store.record_event(
            "supervisor_halt" if incident else "supervisor_stop", reason)
        if incident:
            logger.error("supervisor halted: %s", reason)
            if page:
                self._alert("halt", reason)
        else:
            logger.info("supervisor stopped: %s", reason)

    def _alert(self, kind: str, message: str) -> None:
        try:
            self._alerter(kind, message)
        except Exception as exc:  # an alert must never mask the halt it reports
            logger.error("alerter failed (%s): %s", kind, exc)

    @staticmethod
    def _log_alert(kind: str, message: str) -> None:
        logger.error("ALERT %s: %s", kind, message)
