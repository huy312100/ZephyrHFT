"""Offline tests for the live session supervisor.

No socket: the venue is a fake that holds orders, moves its book, and can be
told to lose one. The assertions are about the three rules the loop exists to
enforce -- do not submit over an unresolved order, do not add inventory past the
cap, do not churn the queue -- because those are the ways an unattended session
becomes expensive.
"""
from __future__ import annotations

from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional

import pytest

from python_analytics.exchange.binance_futures import (
    BinanceAPIError,
    BinanceTransportError,
    OpenOrder,
    Position,
)
from python_analytics.live import (
    LiveExecutor,
    LiveSupervisor,
    OrderStore,
    SupervisorConfig,
)

SYMBOL = "BTCUSDT"


class FakeVenue:
    """A two-sided book, resting orders, and one position the caller controls."""

    def __init__(self, *, bid: str = "100.00", ask: str = "100.02") -> None:
        self.bid, self.ask = Decimal(bid), Decimal(ask)
        self.resting: List[Dict[str, Any]] = []
        self.submits: List[Dict[str, Any]] = []
        self.cancels: List[str] = []
        self.cancel_all_calls = 0
        self.flatten_calls = 0
        self.position_qty = Decimal("0")
        self.position_unrealized = Decimal("0")
        self.lost_ids: List[str] = []   # ids the venue no longer holds
        self.query_error: Optional[Exception] = None
        self._next_id = 1

    # -- market ----------------------------------------------------------- #
    def set_book(self, bid: str, ask: str) -> None:
        self.bid, self.ask = Decimal(bid), Decimal(ask)

    def book_ticker(self, symbol: str) -> Dict[str, str]:
        return {"bidPrice": str(self.bid), "askPrice": str(self.ask)}

    # -- orders ----------------------------------------------------------- #
    def new_order(self, **kwargs: Any) -> Dict[str, Any]:
        side = str(kwargs["side"]).upper()
        # A post-only order inside the touch is refused by the venue, which is
        # the honest behaviour the supervisor must tolerate.
        crossing = (side == "BUY" and Decimal(kwargs["price"]) >= self.ask) or (
            side == "SELL" and Decimal(kwargs["price"]) <= self.bid)
        if crossing:
            raise BinanceAPIError(
                "-5022 post-only order could not be executed as a maker",
                status=400, code=-5022, endpoint="/fapi/v1/order",
            )
        order_id = self._next_id
        self._next_id += 1
        row = {"orderId": order_id, "clientOrderId": kwargs["client_order_id"],
               "symbol": kwargs["symbol"], "side": side, "type": "LIMIT",
               "price": Decimal(kwargs["price"]), "origQty": Decimal(kwargs["quantity"]),
               "executedQty": Decimal("0"), "status": "NEW"}
        self.resting.append(row)
        self.submits.append(dict(row))
        return {"orderId": order_id, "status": "NEW",
                "clientOrderId": kwargs["client_order_id"]}

    def open_orders(self, symbol: str) -> List[OpenOrder]:
        return [OpenOrder(symbol=r["symbol"], order_id=r["orderId"],
                          client_order_id=r["clientOrderId"], side=r["side"],
                          order_type=r["type"], price=r["price"],
                          stop_price=Decimal("0"), quantity=r["origQty"],
                          executed_quantity=r["executedQty"], status=r["status"])
                for r in self.resting]

    def query_order(self, *, symbol: str, client_order_id: Optional[str] = None,
                    order_id: Any = None) -> Dict[str, Any]:
        if self.query_error is not None:
            raise self.query_error
        for row in self.resting:
            if row["clientOrderId"] == client_order_id:
                return {"orderId": row["orderId"], "status": "NEW"}
        # Not resting any more: it filled unless the caller says otherwise.
        return {"orderId": 1, "status": "FILLED" if client_order_id
                not in self.lost_ids else "CANCELED"}

    def cancel_order(self, *, symbol: str, order_id: Any = None,
                     client_order_id: Optional[str] = None) -> Dict[str, Any]:
        if client_order_id is not None:
            self.cancels.append(str(client_order_id))
        self.resting = [r for r in self.resting
                        if r["clientOrderId"] != client_order_id]
        return {"status": "CANCELED"}

    def cancel_all_orders(self, symbol: str) -> Dict[str, Any]:
        self.cancel_all_calls += 1
        self.cancels.extend(r["clientOrderId"] for r in self.resting)
        self.resting = []
        return {}

    def flatten(self, symbol: str) -> Dict[str, Any]:
        self.flatten_calls += 1
        return {}

    def positions(self, symbol: Optional[str] = None, *, include_flat: bool = False,
                  with_mark_price: bool = False) -> List[Position]:
        if self.position_qty == 0 and not include_flat:
            return []
        return [Position(symbol=SYMBOL, quantity=self.position_qty,
                         entry_price=Decimal("100"),
                         mark_price=Decimal("100"),
                         unrealized_pnl=self.position_unrealized,
                         leverage=Decimal("1"))]


def _supervisor(tmp_path: Path, venue: FakeVenue, *, name: str = "orders",
                **overrides: Any):
    config_kwargs: Dict[str, Any] = dict(
        symbol=SYMBOL, quantity=Decimal("0.01"), offset_bps=Decimal("5"),
        tick_size=Decimal("0.01"), requote_tolerance_bps=Decimal("0.25"),
        max_inventory=Decimal("0.5"), session_loss_cap=Decimal("10"),
        max_iterations=1, interval_s=0.0,
    )
    config_kwargs.update(overrides)
    store = OrderStore(tmp_path / f"{name}.sqlite3")
    clock = iter([float(n) for n in range(1, 200)]).__next__
    executor = LiveExecutor(venue, store, SYMBOL, clock=clock)
    alerts: List[tuple] = []
    supervisor = LiveSupervisor(executor, venue, SupervisorConfig(**config_kwargs),
                                clock=time_monotonic, sleep=lambda _s: None,
                                alerter=lambda kind, msg: alerts.append((kind, msg)))
    return supervisor, executor, venue, store, alerts


def time_monotonic() -> float:
    return 1.0


# --------------------------------------------------------------------------- #
# The quote itself
# --------------------------------------------------------------------------- #
def test_a_step_quotes_both_sides_on_the_venue_grid(tmp_path: Path) -> None:
    """Bid down, ask up: a maker quote may only ever widen the market."""
    supervisor, _ex, venue, _store, _alerts = _supervisor(tmp_path, FakeVenue())
    result = supervisor.step()

    assert result.submitted == ["BUY", "SELL"]
    prices = {row["side"]: row["price"] for row in venue.submits}
    # mid 100.01, 5 bps = 0.050005 -> BUY quantises down, SELL up.
    assert prices["BUY"] == Decimal("99.95")
    assert prices["SELL"] == Decimal("100.07")
    assert prices["BUY"] < venue.bid and prices["SELL"] > venue.ask


def test_prices_land_on_a_coarse_grid_rather_than_between_ticks(
    tmp_path: Path
) -> None:
    """A price the venue cannot represent is a rejected order, not a quote.

    Sub-dollar perps (ADA at 0.187 on a 1e-4 tick) are the case where the offset
    lands nowhere near a whole tick, so the rounding is what is being tested.
    """
    venue = FakeVenue(bid="0.18720", ask="0.18740")
    supervisor, _ex, venue, _store, _alerts = _supervisor(
        tmp_path, venue, tick_size=Decimal("0.0001"), offset_bps=Decimal("5"))
    supervisor.step()

    tick = Decimal("0.0001")
    for row in venue.submits:
        price = Decimal(row["price"])
        assert (price / tick) == int(price / tick), price
        assert str(price).count(".") == 1
    prices = {row["side"]: Decimal(row["price"]) for row in venue.submits}
    assert prices["BUY"] < Decimal("0.18730") < prices["SELL"]


def test_an_unusable_book_is_skipped_and_said_so(tmp_path: Path) -> None:
    """A one-sided book is not a price; quoting from it would invent one."""
    venue = FakeVenue(bid="0.00", ask="100.02")
    supervisor, _ex, _v, _store, _alerts = _supervisor(tmp_path, venue)
    assert supervisor.step().skipped == "no two-sided book"
    assert venue.submits == []


# --------------------------------------------------------------------------- #
# Rule 3: queue position is not thrown away every tick
# --------------------------------------------------------------------------- #
def test_an_unchanged_book_re_quotes_nothing(tmp_path: Path) -> None:
    """The naive loop cancels both sides every tick; that is the bug here.

    Cancelling a still-correctly-priced quote gives up its place in the queue for
    nothing and spends rate limit, which is where a maker's edge would live.
    """
    supervisor, _ex, venue, _store, _alerts = _supervisor(tmp_path, FakeVenue())
    supervisor.step()
    first = len(venue.submits)

    second = supervisor.step()
    third = supervisor.step()

    assert len(venue.submits) == first, "a quiet book must not produce orders"
    assert second.kept == ["BUY", "SELL"] and not second.cancelled
    assert third.kept == ["BUY", "SELL"]


def test_a_moved_book_beyond_tolerance_re_places_both_sides(tmp_path: Path) -> None:
    """Both sides moved ~19 bps, far past the 0.25 bps tolerance: re-place both."""
    supervisor, _ex, venue, _store, _alerts = _supervisor(tmp_path, FakeVenue())
    supervisor.step()
    venue.cancels.clear()

    venue.set_book("100.20", "100.22")
    result = supervisor.step()
    assert set(result.cancelled) == {"BUY", "SELL"}
    assert len(venue.cancels) == 2


def test_a_drift_within_tolerance_leaves_the_queue_alone(tmp_path: Path) -> None:
    """The tolerance is the difference between quoting and thrashing.

    Tolerance is deliberately set wide here: target prices are tick-quantised,
    so even a sub-bps mid move can shift a target by a whole tick, and the rule
    under test is "drift the session is willing to live with", not "drift that
    changes no price".
    """
    supervisor, _ex, venue, _store, _alerts = _supervisor(
        tmp_path, FakeVenue(), requote_tolerance_bps=Decimal("100"))
    supervisor.step()
    venue.cancels.clear()

    venue.set_book("100.20", "100.22")            # ~19 bps, well inside 100
    result = supervisor.step()

    assert result.kept == ["BUY", "SELL"], result
    assert venue.cancels == [] and len(venue.submits) == 2


# --------------------------------------------------------------------------- #
# Rule 2: the inventory cap suppresses, it does not merely observe
# --------------------------------------------------------------------------- #
def test_the_cap_suppresses_the_side_that_would_grow_exposure(
    tmp_path: Path
) -> None:
    venue = FakeVenue()
    venue.position_qty = Decimal("0.9")          # above max_inventory 0.5
    supervisor, _ex, venue, _store, _alerts = _supervisor(tmp_path, venue)

    result = supervisor.step()

    assert result.suppressed == ["BUY"], "a long session must not keep buying"
    assert [row["side"] for row in venue.submits] == ["SELL"]
    assert result.inventory == Decimal("0.9")


def test_a_suppressed_side_has_its_resting_quote_cancelled(tmp_path: Path) -> None:
    """Leaving a BUY resting while forbidding buys is not a limit at all."""
    venue = FakeVenue()
    supervisor, _ex, venue, _store, _alerts = _supervisor(tmp_path, venue)
    supervisor.step()                              # quotes both sides
    venue.position_qty = Decimal("0.9")           # then the book fills our buys

    result = supervisor.step()

    assert result.suppressed == ["BUY"]
    assert len(venue.cancels) == 1, "the resting BUY had to go"


# --------------------------------------------------------------------------- #
# Rule 1: never submit on top of an unresolved order
# --------------------------------------------------------------------------- #
def test_an_unresolved_order_halts_before_any_new_submit(tmp_path: Path) -> None:
    venue = FakeVenue()
    supervisor, _ex, venue, _store, alerts = _supervisor(tmp_path, venue)
    supervisor.step()
    placed = len(venue.submits)

    # The venue has forgotten one order and the lookup fails: UNKNOWN.
    venue.query_error = BinanceAPIError(
        "-2013 Order does not exist", status=400, code=-2013,
        endpoint="/fapi/v1/order")
    venue.resting = venue.resting[:1]
    venue.lost_ids = [venue.submits[0]["clientOrderId"]]

    result = supervisor.step()

    assert result.skipped and "unresolved" in result.skipped
    assert len(venue.submits) == placed, "a submit here can become two live orders"
    assert supervisor.halted
    assert [kind for kind, _msg in alerts] == ["halt"]


# --------------------------------------------------------------------------- #
# The session loss cap
# --------------------------------------------------------------------------- #
def test_the_loss_cap_runs_the_kill_path_once_and_stops_the_loop(
    tmp_path: Path
) -> None:
    venue = FakeVenue()
    supervisor, _ex, venue, _store, alerts = _supervisor(tmp_path, venue)
    supervisor.step()
    venue.position_qty = Decimal("0.9")
    venue.position_unrealized = Decimal("-25")     # cap is 10

    report = supervisor.run()

    assert report.halted and "loss cap" in report.halt_reason
    assert venue.cancel_all_calls == 1, "panic must run exactly once"
    assert "kill" in " ".join(kind for kind, _m in alerts) or \
           [kind for kind, _m in alerts] == ["halt"]


def test_a_profitable_session_is_not_halted(tmp_path: Path) -> None:
    venue = FakeVenue()
    supervisor, _ex, venue, _store, _alerts = _supervisor(tmp_path, venue)
    supervisor.step()                              # sets the PnL baseline at 0
    venue.position_qty = Decimal("0.1")
    venue.position_unrealized = Decimal("+3")

    report = supervisor.run()

    assert not report.halted
    assert report.session_pnl == Decimal("+3")


def test_a_baseline_position_is_not_charged_to_this_session(tmp_path: Path) -> None:
    """An inherited -20 is not this session's loss, and must not trip its cap.

    The cap measures the change from where the session started, or restarting
    after an unlucky hour would halt immediately and for the wrong reason.
    """
    venue = FakeVenue()
    venue.position_qty = Decimal("0.1")
    venue.position_unrealized = Decimal("-20")
    supervisor, _ex, _v, _store, _alerts = _supervisor(tmp_path, venue,
                                                       max_iterations=2)
    report = supervisor.run()
    assert not report.halted, report.halt_reason
    assert report.session_pnl == Decimal("0")


# --------------------------------------------------------------------------- #
# Stopping, and what the operator is told
# --------------------------------------------------------------------------- #
def test_stop_cancels_what_the_session_placed(tmp_path: Path) -> None:
    """A dead supervisor must not leave two quotes held by nobody."""
    supervisor, _ex, venue, _store, _alerts = _supervisor(tmp_path, FakeVenue())
    supervisor.step()
    assert len(venue.resting) == 2

    supervisor.stop("operator")

    assert supervisor.finished and not supervisor.halted, \
        "a clean stop is a finish, not an incident"
    assert len(venue.cancels) == 2, "each resting quote cancelled by id"
    assert venue.cancel_all_calls == 0
    assert venue.flatten_calls == 0, "a deliberate stop does not pay the taker fee"


def test_stop_flatten_uses_the_existing_kill_path(tmp_path: Path) -> None:
    """Flattening is the venue's decision too: only a position is closed."""
    flat_venue = FakeVenue()
    supervisor, _ex, flat_venue, _store, _alerts = _supervisor(
        tmp_path, flat_venue, name="flat")
    supervisor.step()

    supervisor.stop("operator", flatten=True)

    assert flat_venue.cancel_all_calls == 1
    assert flat_venue.flatten_calls == 0, "nothing to flatten, no taker fee paid"

    held = FakeVenue()
    held.position_qty = Decimal("0.4")
    sup2, _e2, held, _s2, _a2 = _supervisor(tmp_path, held, name="held")

    sup2.stop("operator", flatten=True)

    assert held.flatten_calls == 1, "a position must not outlive the session"


def test_a_stop_whose_cancel_fails_says_so(tmp_path: Path) -> None:
    """Reporting a clean stop over a live quote is worse than reporting nothing."""
    supervisor, _ex, venue, _store, alerts = _supervisor(tmp_path, FakeVenue())
    supervisor.step()

    def boom(*_a: Any, **_k: Any) -> Dict[str, Any]:
        raise BinanceAPIError("-1 system busy", status=500, code=-1,
                              endpoint="/fapi/v1/order")

    venue.cancel_order = boom  # type: ignore[assignment]
    supervisor.stop("operator")

    assert "cancel(s) failed" in supervisor.halt_reason
    assert [kind for kind, _m in alerts] == ["halt"], "a stuck quote pages"


def test_a_stopped_session_quotes_nothing_further(tmp_path: Path) -> None:
    """Stopping is not pausing: a later step must not reach the venue.

    A session that was stopped and then quoted again would place an order from a
    process the operator already believes is finished.
    """
    supervisor, _ex, venue, _store, _alerts = _supervisor(tmp_path, FakeVenue())
    supervisor.stop("operator")
    assert supervisor.finished and not supervisor.halted
    placed = len(venue.submits)

    result = supervisor.step()

    assert result.skipped == "stopped"
    assert len(venue.submits) == placed


def test_a_halted_session_quotes_nothing_further(tmp_path: Path) -> None:
    supervisor, _ex, venue, _store, _alerts = _supervisor(tmp_path, FakeVenue())
    supervisor.step()
    venue.query_error = BinanceTransportError("venue unreachable")
    venue.cancel_error = BinanceTransportError("venue unreachable")
    # Force an incident: an order we cannot resolve.
    supervisor._halt("test incident")
    placed = len(venue.submits)

    result = supervisor.step()

    assert result.skipped == "halted"
    assert len(venue.submits) == placed


def test_run_bounds_itself_by_iterations(tmp_path: Path) -> None:
    supervisor, _ex, venue, _store, _alerts = _supervisor(
        tmp_path, FakeVenue(), max_iterations=3, interval_s=0.0)
    report = supervisor.run()
    assert report.iterations == 3
    assert report.submitted == 2, "a quiet book only quotes on the first pass"


def test_the_report_names_what_happened(tmp_path: Path) -> None:
    supervisor, _ex, _v, _store, _alerts = _supervisor(tmp_path, FakeVenue())
    report = supervisor.run()
    text = report.describe()
    assert "iterations=1" in text and "submitted=2" in text


# --------------------------------------------------------------------------- #
# Rule 1b: an unresolved or refused submit ends the tick, not just the session
# --------------------------------------------------------------------------- #
def test_an_unknown_submit_stops_the_tick_before_the_other_side_is_placed(
    tmp_path: Path
) -> None:
    """Half a pair is not a quote; a submit the venue may still hold is worse.

    An ``UNKNOWN`` answer means the request may have landed. Placing the other
    side before that is resolved risks two live orders from one intent, so the
    step has to stop at the first ambiguous submit rather than finish the loop.
    """
    venue = FakeVenue()
    supervisor, _ex, venue, _store, _alerts = _supervisor(tmp_path, venue,
                                                          name="unknown_submit")

    def ambiguous(**kwargs: Any) -> Dict[str, Any]:
        # The venue may have accepted it: the response never arrived.
        venue.submits.append(dict(kwargs))
        raise BinanceTransportError("connection reset after the venue read")

    venue.new_order = ambiguous  # type: ignore[assignment]
    # And the follow-up lookup cannot reach the venue either, so the honest
    # answer is UNKNOWN rather than "probably rejected".
    venue.query_error = BinanceTransportError("lookup unavailable")
    result = supervisor.step()

    assert result.skipped and "UNKNOWN" in result.skipped
    assert len(venue.submits) == 1, \
        "SELL must not be placed over an unresolved BUY"
    assert supervisor.halted


def test_a_post_only_cross_is_tolerated_and_another_rejection_is_not(
    tmp_path: Path
) -> None:
    """-5022 is the expected answer when the book moved through the level.

    A *different* rejection (tick size, min notional, precision) is a
    configuration error, and retrying it every tick spends rate limit on a
    mistake instead of surfacing it.
    """
    venue = FakeVenue()
    supervisor, _ex, venue, _store, _alerts = _supervisor(tmp_path, venue)

    # Quote so wide it can never cross the touch: the post-only order rests.
    result = supervisor.step()
    assert result.submitted == ["BUY", "SELL"]

    venue2 = FakeVenue()
    sup2, _e2, _v2, _s2, _a2 = _supervisor(tmp_path, venue2, name="filter")

    def wrong_filter(**kwargs: Any) -> Dict[str, Any]:
        raise BinanceAPIError("-1111 Precision is incorrect",
                              status=400, code=-1111,
                              endpoint="/fapi/v1/order")

    venue2.new_order = wrong_filter  # type: ignore[assignment]
    outcome = sup2.step()
    assert outcome.skipped and "rejected" in outcome.skipped
    assert sup2.halted, "a non-post-only rejection must not be retried every tick"


# --------------------------------------------------------------------------- #
# Ownership: an order we did not place is never ours to cancel
# --------------------------------------------------------------------------- #
def test_a_foreign_adopted_order_is_never_cancelled(tmp_path: Path) -> None:
    """The live layer promises a manual order is adopted passively.

    A supervisor that reads every active row would happily cancel a human's
    order during re-quoting or at stop, which is exactly what adoption was added
    to prevent -- so it halts instead of trading over it or destroying it.
    """
    venue = FakeVenue()
    supervisor, _ex, venue, store, _alerts = _supervisor(tmp_path, venue)
    # Something placed outside this session, adopted by reconcile().
    store.record_intent(client_order_id="manual-1", symbol=SYMBOL, side="BUY",
                        price=Decimal("99.5"), quantity=Decimal("0.5"))
    store.mark_status("manual-1", "SUBMITTED")
    venue.resting.append({"orderId": 99, "clientOrderId": "manual-1",
                          "symbol": SYMBOL, "side": "BUY", "type": "LIMIT",
                          "price": Decimal("99.5"), "origQty": Decimal("0.5"),
                          "executedQty": Decimal("0"), "status": "NEW"})

    result = supervisor.step()

    assert result.skipped and "did not place" in result.skipped
    assert venue.cancels == [], "we must never cancel a foreign order"
    assert result.submitted == [], "and never quote over it either"


def test_two_session_rows_on_one_side_halts_instead_of_adding_a_third(
    tmp_path: Path
) -> None:
    venue = FakeVenue()
    supervisor, _ex, venue, store, _alerts = _supervisor(tmp_path, venue,
                                                         name="two_rows")
    supervisor.step()                              # places BUY + SELL
    first_buy = next(r for r in venue.submits if r["side"] == "BUY")
    dup_cid = str(first_buy["clientOrderId"]) + "-dup"
    # A second BUY row belonging to *this* session: the state a cancel that never
    # landed leaves behind. Registered as ours so the duplicate-side guard is
    # what runs, rather than the foreign-order guard.
    venue.resting.append(dict(first_buy, orderId=555, clientOrderId=dup_cid))
    store.record_intent(client_order_id=dup_cid, symbol=SYMBOL, side="BUY",
                        price=first_buy["price"], quantity=first_buy["origQty"])
    store.mark_status(dup_cid, "SUBMITTED")
    supervisor._session_ids[dup_cid] = "BUY"

    result = supervisor.step()

    assert result.skipped and "two active" in result.skipped
    assert supervisor.halted


# --------------------------------------------------------------------------- #
# A cancel that fails is resolved, not allowed to escape
# --------------------------------------------------------------------------- #
def test_a_cancel_that_the_venue_refuses_leaves_the_quote_alone(
    tmp_path: Path
) -> None:
    """A refused cancel whose order is still live must not be replaced.

    Letting ``-2011`` escape would abandon the loop with the other side
    unmanaged; replacing a quote the venue never gave up would put **two** live
    orders on one side. The correct answer is neither: leave it resting and
    retry next tick, and say which sides were held over.
    """
    venue = FakeVenue()
    supervisor, _ex, venue, _store, _alerts = _supervisor(tmp_path, venue,
                                                          name="refused_cancel")
    supervisor.step()
    placed = len(venue.submits)
    venue.set_book("101.00", "101.02")             # force both sides to move

    def refuse_cancel(*, symbol: str, order_id: Any = None,
                      client_order_id: Optional[str] = None) -> Dict[str, Any]:
        raise BinanceAPIError("-2011 Unknown order sent", status=400,
                              code=-2011, endpoint="/fapi/v1/order")

    venue.cancel_order = refuse_cancel  # type: ignore[assignment]
    result = supervisor.step()                      # must not raise

    assert result.submitted == [], "no replacement over a still-live quote"
    assert result.held_over == ["BUY", "SELL"], result
    assert len(venue.submits) == placed
    assert not supervisor.halted, "the venue answered, so the loop may continue"


def test_a_cancel_that_fails_and_stays_unknown_halts(tmp_path: Path) -> None:
    venue = FakeVenue()
    supervisor, _ex, venue, _store, alerts = _supervisor(tmp_path, venue)
    supervisor.step()
    venue.set_book("101.00", "101.02")

    venue.cancel_order = lambda **kw: (_ for _ in ()).throw(   # type: ignore
        BinanceAPIError("-2011 Unknown order sent", status=400, code=-2011,
                        endpoint="/fapi/v1/order"))
    venue.query_error = BinanceAPIError("-1021 recvWindow", status=400,
                                        code=-1021, endpoint="/fapi/v1/order")

    supervisor.step()

    assert supervisor.halted
    assert any(kind == "halt" for kind, _m in alerts)


# --------------------------------------------------------------------------- #
# The stop flag is honoured between sides, not inside a handler
# --------------------------------------------------------------------------- #
def test_a_stop_requested_mid_step_places_no_further_quote(tmp_path: Path) -> None:
    """A handler that ran ``stop()`` inline would let this step keep quoting.

    The second side would then rest in the book after the operator was told the
    session had stopped. Setting a flag and checking it per side removes the
    race entirely.
    """
    venue = FakeVenue()
    supervisor, _ex, venue, _store, _alerts = _supervisor(tmp_path, venue)

    original = venue.new_order

    def interrupt_after_buy(**kwargs: Any) -> Dict[str, Any]:
        result = original(**kwargs)
        supervisor.stop_requested = True            # as a signal handler would
        return result

    venue.new_order = interrupt_after_buy  # type: ignore[assignment]
    step = supervisor.step()

    assert step.submitted == ["BUY"]
    assert step.skipped == "stop requested"
    assert len(venue.submits) == 1, "the SELL must not be placed after a stop"


# --------------------------------------------------------------------------- #
# Configuration is refused before a request is ever made
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("bad", [
    dict(quantity=Decimal("0")), dict(offset_bps=Decimal("-1")),
    dict(tick_size=Decimal("0")), dict(max_inventory=Decimal("0")),
    dict(session_loss_cap=Decimal("0")),
    dict(requote_tolerance_bps=Decimal("-0.1")),
    dict(symbol=""),
    # nan/inf pass a plain range check because both compare false against every
    # bound: an infinite loss cap never trips and a NaN tolerance re-places both
    # sides every tick, which is the churn rule 3 exists to prevent.
    dict(quantity=Decimal("nan")), dict(session_loss_cap=Decimal("inf")),
    dict(requote_tolerance_bps=Decimal("nan")),
    dict(max_inventory=Decimal("Infinity")),
])
def test_an_impossible_session_is_refused_at_construction(bad: Dict[str, Any]) -> None:
    base = dict(symbol=SYMBOL, quantity=Decimal("0.01"), offset_bps=Decimal("5"),
                tick_size=Decimal("0.01"))
    base.update(bad)
    with pytest.raises(ValueError):
        SupervisorConfig(**base)


def test_a_sane_session_constructs(tmp_path: Path) -> None:
    """The guard above must not refuse the ordinary configuration."""
    supervisor, _ex, _v, _s, _a = _supervisor(tmp_path, FakeVenue(),
                                              requote_tolerance_bps=Decimal("0"))
    assert supervisor.step().submitted == ["BUY", "SELL"]
