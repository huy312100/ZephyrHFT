"""Offline tests for the live execution layer.

Nothing here touches a socket. The client is a fake that can be told to fail in
each of the ways that matter (timeout after the venue may have accepted, venue
rejection, rate limit, an order that vanished while the process was down), so
the assertions are about *behaviour under ambiguity* rather than about HTTP.
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional

import pytest

from python_analytics.exchange.binance_futures import (
    BinanceAPIError,
    BinanceRateLimitError,
    BinanceTransportError,
    OpenOrder,
    OrderValidationError,
    Position,
)
from python_analytics.live import (
    AlphaGateError,
    LiveExecutor,
    LiveStoreError,
    OrderStore,
    alpha_gate_verdict,
    require_alpha_go,
)


# --------------------------------------------------------------------------- #
# Fakes
# --------------------------------------------------------------------------- #
class FakeClient:
    """Records submits and replays scripted failures."""

    def __init__(
        self,
        *,
        open_orders: Optional[List[OpenOrder]] = None,
        position: Optional[Position] = None,
    ) -> None:
        self.orders: List[OpenOrder] = list(open_orders or [])
        self.position = position
        self.submitted: List[Dict[str, Any]] = []
        self.new_order_error: Optional[Exception] = None
        self.query_result: Any = None
        self.cancel_error: Optional[Exception] = None
        self.flatten_error: Optional[Exception] = None
        self.cancelled = 0
        self.flattened = 0

    def new_order(self, **kwargs: Any) -> Dict[str, Any]:
        if self.new_order_error is not None:
            raise self.new_order_error
        self.submitted.append(kwargs)
        return {
            "orderId": 4242,
            "status": "NEW",
            "clientOrderId": kwargs["client_order_id"],
        }

    def query_order(
        self,
        *,
        symbol: str,
        client_order_id: Optional[str] = None,
        order_id: Any = None,
    ) -> Dict[str, Any]:
        if isinstance(self.query_result, Exception):
            raise self.query_result
        if self.query_result is not None:
            return self.query_result
        return {"orderId": 4242, "status": "NEW"}

    def open_orders(self, symbol: str) -> List[OpenOrder]:
        return list(self.orders)

    def positions(self, symbol: str) -> List[Position]:
        return [] if self.position is None else [self.position]

    def cancel_all_orders(self, symbol: str) -> Dict[str, Any]:
        if self.cancel_error is not None:
            raise self.cancel_error
        self.cancelled += 1
        self.orders = []
        return {}

    def flatten(self, symbol: str) -> Dict[str, Any]:
        if self.flatten_error is not None:
            raise self.flatten_error
        self.flattened += 1
        return {}


def _open_order(
    client_order_id: str = "manual-1",
    *,
    side: str = "BUY",
    price: str = "80000",
    quantity: str = "0.002",
    order_id: int = 7,
) -> OpenOrder:
    return OpenOrder(
        symbol="BTCUSDT",
        order_id=order_id,
        client_order_id=client_order_id,
        side=side,
        order_type="LIMIT",
        price=Decimal(price),
        stop_price=Decimal("0"),
        quantity=Decimal(quantity),
        executed_quantity=Decimal("0"),
        status="NEW",
    )


def _position(quantity: str = "0.002") -> Position:
    return Position(
        symbol="BTCUSDT",
        quantity=Decimal(quantity),
        entry_price=Decimal("80000"),
        mark_price=Decimal("80100"),
        unrealized_pnl=Decimal("0.2"),
        leverage=Decimal("3"),
    )


@pytest.fixture()
def store(tmp_path: Path) -> OrderStore:
    with OrderStore(tmp_path / "orders.sqlite3") as opened:
        yield opened


def _executor(
    store: OrderStore,
    client: Optional[FakeClient] = None,
    symbol: str = "BTCUSDT",
) -> "tuple[LiveExecutor, FakeClient]":
    fake = client if client is not None else FakeClient()
    return LiveExecutor(fake, store, symbol, clock=lambda: 1_700_000_000.0), fake


# --------------------------------------------------------------------------- #
# Store: durability and illegal transitions
# --------------------------------------------------------------------------- #
def test_intent_survives_a_reopen(tmp_path: Path) -> None:
    path = tmp_path / "orders.sqlite3"
    with OrderStore(path) as first:
        first.record_intent(
            client_order_id="z-btcusdt-1-1",
            symbol="BTCUSDT",
            side="BUY",
            price="79980",
            quantity="0.002",
        )
    with OrderStore(path) as second:
        row = second.get("z-btcusdt-1-1")
    assert row is not None
    assert row["status"] == "INTENT"
    assert row["price"] == "79980"


def test_duplicate_client_order_id_is_refused(store: OrderStore) -> None:
    store.record_intent(
        client_order_id="dup", symbol="BTCUSDT", side="BUY", price="1", quantity="1"
    )
    with pytest.raises(LiveStoreError):
        store.record_intent(
            client_order_id="dup",
            symbol="BTCUSDT",
            side="BUY",
            price="1",
            quantity="1",
        )


def test_terminal_status_cannot_be_revived(store: OrderStore) -> None:
    store.record_intent(
        client_order_id="dead", symbol="BTCUSDT", side="BUY", price="1", quantity="1"
    )
    store.mark_status("dead", "FILLED")
    with pytest.raises(LiveStoreError):
        store.mark_status("dead", "SUBMITTED")


def test_unknown_status_is_refused(store: OrderStore) -> None:
    store.record_intent(
        client_order_id="x", symbol="BTCUSDT", side="BUY", price="1", quantity="1"
    )
    with pytest.raises(LiveStoreError):
        store.mark_status("x", "TOTALLY_FINE")


def test_marking_an_unrecorded_id_is_refused(store: OrderStore) -> None:
    with pytest.raises(LiveStoreError):
        store.mark_status("never-seen", "SUBMITTED")


def test_active_orders_excludes_terminal_rows(store: OrderStore) -> None:
    for cid in ("a", "b"):
        store.record_intent(
            client_order_id=cid, symbol="BTCUSDT", side="BUY", price="1", quantity="1"
        )
    store.mark_status("b", "CANCELLED")
    assert [row["client_order_id"] for row in store.active_orders()] == ["a"]


def test_events_are_returned_newest_first(store: OrderStore) -> None:
    store.record_event("first")
    store.record_event("second", "detail")
    kinds = [event["kind"] for event in store.events()]
    assert kinds == ["second", "first"]


# --------------------------------------------------------------------------- #
# Submit: the ambiguous-failure paths
# --------------------------------------------------------------------------- #
def test_submit_records_intent_then_venue_id(store: OrderStore) -> None:
    executor, client = _executor(store)
    result = executor.submit_post_only(side="BUY", quantity="0.002", price="79980")

    assert result.accepted
    assert result.status == "SUBMITTED"
    assert result.venue_order_id == 4242
    sent = client.submitted[0]
    assert sent["post_only"] is True
    assert sent["order_type"] == "LIMIT"
    row = store.get(result.client_order_id)
    assert row is not None
    assert row["status"] == "SUBMITTED"
    assert row["venue_order_id"] == 4242
    assert row["symbol"] == "BTCUSDT"


def test_transport_error_is_resolved_by_query(store: OrderStore) -> None:
    client = FakeClient()
    client.new_order_error = BinanceTransportError("read timeout")
    client.query_result = {"orderId": 99, "status": "NEW"}
    executor, _ = _executor(store, client)

    result = executor.submit_post_only(side="SELL", quantity="0.002", price="80100")

    # The whole point: a timeout must not leave the order as a guess.
    assert result.status == "SUBMITTED"
    assert result.note == "resolved after transport error"
    row = store.get(result.client_order_id)
    assert row is not None
    assert row["status"] == "SUBMITTED"
    assert row["venue_order_id"] == 99
    kinds = [event["kind"] for event in store.events()]
    assert "transport_error" in kinds
    assert "order_resolved" in kinds


def test_transport_error_with_no_venue_record_is_lost(store: OrderStore) -> None:
    client = FakeClient()
    client.new_order_error = BinanceTransportError("connection reset")
    client.query_result = BinanceAPIError("order does not exist", code=-2013)
    executor, _ = _executor(store, client)

    result = executor.submit_post_only(side="BUY", quantity="0.002", price="79980")

    assert result.status == "LOST"
    assert not result.accepted
    row = store.get(result.client_order_id)
    assert row is not None
    assert row["status"] == "LOST"


def test_transport_error_whose_lookup_fails_stays_unknown(store: OrderStore) -> None:
    client = FakeClient()
    client.new_order_error = BinanceTransportError("read timeout")
    client.query_result = BinanceTransportError("dns still down")
    executor, _ = _executor(store, client)

    result = executor.submit_post_only(side="BUY", quantity="0.002", price="79980")

    # The lookup failed too. Guessing either way is worse than leaving a row
    # that reconciliation will chase.
    assert result.status == "UNKNOWN"
    assert result.retryable
    row = store.get(result.client_order_id)
    assert row is not None
    assert row["status"] == "UNKNOWN"
    assert "resolve_failed" in [event["kind"] for event in store.events()]


def test_server_error_submit_is_resolved_not_rejected(store: OrderStore) -> None:
    client = FakeClient()
    client.new_order_error = BinanceAPIError("service unavailable", status=503)
    client.query_result = {"orderId": 77, "status": "NEW"}
    executor, _ = _executor(store, client)

    result = executor.submit_post_only(side="BUY", quantity="0.002", price="79980")

    # 5xx is "I do not know", not "no": the matcher may hold the order.
    assert result.status == "SUBMITTED"
    assert result.accepted
    row = store.get(result.client_order_id)
    assert row is not None
    assert row["status"] == "SUBMITTED"
    assert row["venue_order_id"] == 77
    kinds = [event["kind"] for event in store.events()]
    assert "ambiguous_submit" in kinds
    assert "order_rejected" not in kinds


def test_backend_timeout_code_is_ambiguous_not_rejected(store: OrderStore) -> None:
    client = FakeClient()
    client.new_order_error = BinanceAPIError(
        "timeout waiting for response from backend", code=-1007
    )
    client.query_result = BinanceAPIError("order does not exist", code=-2013)
    executor, _ = _executor(store, client)

    result = executor.submit_post_only(side="BUY", quantity="0.002", price="79980")

    assert result.status == "LOST"
    kinds = [event["kind"] for event in store.events()]
    assert "ambiguous_submit" in kinds
    assert "order_rejected" not in kinds


def test_server_error_whose_lookup_fails_stays_unknown(store: OrderStore) -> None:
    client = FakeClient()
    client.new_order_error = BinanceAPIError("bad gateway", status=502)
    client.query_result = BinanceAPIError("still down", status=503)
    executor, _ = _executor(store, client)

    result = executor.submit_post_only(side="BUY", quantity="0.002", price="79980")

    assert result.status == "UNKNOWN"
    assert result.retryable
    row = store.get(result.client_order_id)
    assert row is not None
    assert row["status"] == "UNKNOWN"


def test_post_only_rejection_is_recorded_not_raised(store: OrderStore) -> None:
    client = FakeClient()
    client.new_order_error = BinanceAPIError("would immediately trigger", code=-2010)
    executor, _ = _executor(store, client)

    result = executor.submit_post_only(side="BUY", quantity="0.002", price="80500")

    assert result.status == "REJECTED"
    assert not result.accepted
    row = store.get(result.client_order_id)
    assert row is not None
    assert "would immediately trigger" in str(row["note"])


def test_rate_limit_is_reported_as_retryable(store: OrderStore) -> None:
    client = FakeClient()
    client.new_order_error = BinanceRateLimitError("too many requests", status=429)
    executor, _ = _executor(store, client)

    result = executor.submit_post_only(side="BUY", quantity="0.002", price="79980")

    assert result.retryable
    assert result.status == "REJECTED"
    assert "rate_limited" in [e["kind"] for e in store.events()]


def test_non_positive_quantity_never_reaches_the_venue(store: OrderStore) -> None:
    executor, client = _executor(store)
    with pytest.raises(OrderValidationError):
        executor.submit_post_only(side="BUY", quantity="0", price="79980")
    assert client.submitted == []
    assert store.orders() == []


def test_client_order_ids_are_unique_and_venue_legal(store: OrderStore) -> None:
    executor, _ = _executor(store)
    first = executor.next_client_order_id()
    second = executor.next_client_order_id()
    assert first != second
    assert len(first) <= 36
    assert first.startswith("z-btcusdt-")


def test_resubmitting_the_same_client_id_is_refused(store: OrderStore) -> None:
    executor, client = _executor(store)
    executor.submit_post_only(
        side="BUY", quantity="0.002", price="79980", client_order_id="fixed-id"
    )
    with pytest.raises(LiveStoreError):
        executor.submit_post_only(
            side="BUY", quantity="0.002", price="79980", client_order_id="fixed-id"
        )
    assert len(client.submitted) == 1


# --------------------------------------------------------------------------- #
# Reconcile: adopt what we did not place, resolve what vanished
# --------------------------------------------------------------------------- #
def test_reconcile_adopts_an_unknown_venue_order(store: OrderStore) -> None:
    client = FakeClient(open_orders=[_open_order("manual-1", side="SELL")])
    executor, _ = _executor(store, client)

    report = executor.reconcile()

    assert [o.client_order_id for o in report.adopted] == ["manual-1"]
    assert not report.clean
    row = store.get("manual-1")
    assert row is not None
    assert row["status"] == "ADOPTED"
    assert row["venue_order_id"] == 7
    # Adoption must be passive: killing a manual order would be worse.
    assert client.cancelled == 0


def test_reconcile_resolves_a_vanished_order_that_filled(store: OrderStore) -> None:
    client = FakeClient()
    executor, _ = _executor(store, client)
    result = executor.submit_post_only(
        side="BUY", quantity="0.002", price="79980"
    )
    client.orders = []  # the matcher filled it while we were not looking
    client.query_result = {"orderId": 4242, "status": "FILLED"}

    report = executor.reconcile()

    assert report.resolved == {result.client_order_id: "FILLED"}
    row = store.get(result.client_order_id)
    assert row is not None
    assert row["status"] == "FILLED"


def test_reconcile_marks_a_vanished_order_lost(store: OrderStore) -> None:
    client = FakeClient()
    executor, _ = _executor(store, client)
    result = executor.submit_post_only(side="BUY", quantity="0.002", price="79980")
    client.orders = []
    client.query_result = BinanceAPIError("order does not exist", code=-2013)

    report = executor.reconcile()

    assert report.resolved == {result.client_order_id: "LOST"}
    assert report.unresolved == [result.client_order_id]
    row = store.get(result.client_order_id)
    assert row is not None
    assert row["status"] == "LOST"


def test_reconcile_is_clean_when_the_books_agree(store: OrderStore) -> None:
    client = FakeClient()
    executor, _ = _executor(store, client)
    result = executor.submit_post_only(side="BUY", quantity="0.002", price="79980")
    client.orders = [_open_order(result.client_order_id, side="BUY")]

    report = executor.reconcile()

    assert report.clean
    assert report.adopted == []
    assert report.vanished == []
    assert report.position is None


def test_reconcile_promotes_local_orders_the_venue_holds(store: OrderStore) -> None:
    client = FakeClient()
    executor, _ = _executor(store, client)
    for cid, status in (("crashed-intent", "INTENT"), ("ambiguous", "UNKNOWN")):
        store.record_intent(
            client_order_id=cid, symbol="BTCUSDT", side="BUY",
            price="79980", quantity="0.002",
        )
        if status != "INTENT":
            store.mark_status(cid, status)
    client.orders = [
        _open_order("crashed-intent", order_id=7),
        _open_order("ambiguous", order_id=8),
    ]

    report = executor.reconcile()

    # The venue holding the order proves the submit landed even if the local
    # write did not: promote it instead of resolving it as vanished.
    assert report.clean
    assert report.adopted == []
    assert report.vanished == []
    for cid, order_id in (("crashed-intent", 7), ("ambiguous", 8)):
        row = store.get(cid)
        assert row is not None
        assert row["status"] == "SUBMITTED"
        assert row["venue_order_id"] == order_id


def test_reconcile_reports_the_position(store: OrderStore) -> None:
    client = FakeClient(position=_position("0.002"))
    executor, _ = _executor(store, client)

    report = executor.reconcile()

    assert report.position is not None
    assert report.position.quantity == Decimal("0.002")
    assert "BTCUSDT BUY" in report.describe()


# --------------------------------------------------------------------------- #
# Panic: the kill path must not lie about what it achieved
# --------------------------------------------------------------------------- #
def test_panic_cancels_orders_and_flattens(store: OrderStore) -> None:
    client = FakeClient(position=_position("0.002"))
    executor, _ = _executor(store, client)
    result = executor.submit_post_only(side="BUY", quantity="0.002", price="79980")
    client.query_result = {"orderId": 4242, "status": "CANCELED"}

    errors = executor.panic("signal: SIGINT")

    assert errors == []
    assert client.cancelled == 1
    assert client.flattened == 1
    row = store.get(result.client_order_id)
    assert row is not None
    assert row["status"] == "CANCELLED"
    assert "panic" in [event["kind"] for event in store.events()]
    # A cancel-all is followed by a per-order query: an order can fill in the
    # same instant, and the book of record should say so, not assume CANCELLED.
    assert "order_resolved" in [event["kind"] for event in store.events()]


def test_panic_reports_an_order_the_venue_still_holds(store: OrderStore) -> None:
    client = FakeClient()
    executor, _ = _executor(store, client)
    result = executor.submit_post_only(side="BUY", quantity="0.002", price="79980")
    client.query_result = {"orderId": 4242, "status": "NEW"}

    errors = executor.panic("signal: SIGINT")

    # cancel_all returned, but the venue still reports the quote working: that
    # is exposure, so the kill switch must not report a clean shutdown.
    assert any("still SUBMITTED" in error for error in errors)
    row = store.get(result.client_order_id)
    assert row is not None
    assert row["status"] == "SUBMITTED"


def test_panic_does_not_flatten_a_flat_book(store: OrderStore) -> None:
    executor, client = _executor(store)
    errors = executor.panic("startup check")
    assert errors == []
    assert client.flattened == 0


def test_panic_reports_a_failed_cancel_and_keeps_orders_unknown(
    store: OrderStore,
) -> None:
    client = FakeClient()
    client.cancel_error = BinanceAPIError("maintenance", status=503)
    executor, _ = _executor(store, client)
    result = executor.submit_post_only(side="BUY", quantity="0.002", price="79980")

    errors = executor.panic("adverse event")

    assert errors and "cancel_all_orders" in errors[0]
    row = store.get(result.client_order_id)
    assert row is not None
    assert row["status"] == "UNKNOWN"


# --------------------------------------------------------------------------- #
# Arming gate: the report decides, not a config flag
# --------------------------------------------------------------------------- #
def _write_report(
    tmp_path: Path,
    *,
    net: float = 12.5,
    alpha_passed: bool = True,
    generated_at: Optional[datetime] = None,
    headline: Optional[str] = None,
    drop_timestamp: bool = False,
) -> Path:
    """Mimics the shape of ``reports/production_readiness_report.json``."""

    stamp = generated_at or datetime.now(timezone.utc)
    payload: Dict[str, Any] = {
        "generated_at": stamp.isoformat(),
        "alpha_verdict": headline or ("GO" if alpha_passed else "NO-GO"),
        "summary": {"alpha_failed": 0 if alpha_passed else 1},
        "checks": [
            {
                "phase": "alpha",
                "id": "alpha.net_pnl",
                "passed": alpha_passed,
                "detail": "net after costs",
            },
            {
                "phase": "alpha",
                "id": "alpha.fee_assumption",
                "passed": True,
                "detail": "maker 2 bps",
            },
            # A failing structure check must not be read as an alpha failure.
            {"phase": "structure", "id": "structure.docs", "passed": False},
        ],
        "evidence": {"alpha": {"net_pnl": net, "fees_paid": 13.73}},
    }
    if drop_timestamp:
        payload.pop("generated_at")
    path = tmp_path / "readiness.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def test_gate_passes_a_fresh_positive_verdict(tmp_path: Path) -> None:
    report = _write_report(tmp_path)
    ok, detail = alpha_gate_verdict(json.loads(report.read_text(encoding="utf-8")))
    assert ok, detail
    assert "NO-GO" not in detail


def test_gate_blocks_the_committed_no_go_shape(tmp_path: Path) -> None:
    report = _write_report(tmp_path, net=-16.4743, alpha_passed=False)
    with pytest.raises(AlphaGateError) as excinfo:
        require_alpha_go(report)
    assert "alpha checks failed" in str(excinfo.value)


def test_gate_rejects_a_positive_net_with_failing_checks(tmp_path: Path) -> None:
    report = _write_report(tmp_path, net=5.0, alpha_passed=False)
    ok, detail = alpha_gate_verdict(json.loads(report.read_text(encoding="utf-8")))
    assert not ok
    assert "alpha.net_pnl" in detail


def test_gate_rejects_a_zero_net_even_when_checks_pass(tmp_path: Path) -> None:
    report = _write_report(tmp_path, net=0.0)
    ok, detail = alpha_gate_verdict(json.loads(report.read_text(encoding="utf-8")))
    assert not ok
    assert "needs > 0" in detail


def test_gate_rejects_a_boolean_net(tmp_path: Path) -> None:
    report = _write_report(tmp_path, net=True)
    ok, detail = alpha_gate_verdict(json.loads(report.read_text(encoding="utf-8")))
    assert not ok
    # float(True) == 1.0 would otherwise arm a live session on a typo.
    assert "boolean" in detail


@pytest.mark.parametrize("net", [float("inf"), float("-inf"), float("nan")])
def test_gate_rejects_a_non_finite_net(tmp_path: Path, net: float) -> None:
    report = _write_report(tmp_path, net=net)
    ok, detail = alpha_gate_verdict(json.loads(report.read_text(encoding="utf-8")))
    assert not ok
    assert "finite" in detail


def test_gate_honours_a_red_headline_over_passing_checks(tmp_path: Path) -> None:
    report = _write_report(tmp_path, net=12.5, headline="NO-GO")
    ok, detail = alpha_gate_verdict(json.loads(report.read_text(encoding="utf-8")))
    assert not ok
    assert "headline" in detail


def test_gate_rejects_a_stale_go(tmp_path: Path) -> None:
    report = _write_report(
        tmp_path, generated_at=datetime.now(timezone.utc) - timedelta(days=30)
    )
    with pytest.raises(AlphaGateError) as excinfo:
        require_alpha_go(report, max_age_hours=168.0)
    assert "stale" in str(excinfo.value)


def test_gate_rejects_a_report_without_a_timestamp(tmp_path: Path) -> None:
    report = _write_report(tmp_path, drop_timestamp=True)
    with pytest.raises(AlphaGateError):
        require_alpha_go(report)


def test_gate_refuses_a_missing_report(tmp_path: Path) -> None:
    with pytest.raises(AlphaGateError) as excinfo:
        require_alpha_go(tmp_path / "nope.json")
    assert "not found" in str(excinfo.value)


def test_gate_override_is_written_to_the_audit_trail(
    tmp_path: Path, store: OrderStore
) -> None:
    report = _write_report(tmp_path, net=-16.4743, alpha_passed=False)
    message = require_alpha_go(
        report,
        override_reason="manual smoke test with 200 USDT",
        store=store,
    )
    assert "override" in message
    events = [event for event in store.events() if event["kind"] == "alpha_override"]
    assert events and "manual smoke test" in str(events[0]["detail"])


def test_gate_records_a_blocked_attempt(tmp_path: Path, store: OrderStore) -> None:
    report = _write_report(tmp_path, net=-16.4743, alpha_passed=False)
    with pytest.raises(AlphaGateError):
        require_alpha_go(report, store=store)
    kinds = [event["kind"] for event in store.events()]
    assert "alpha_gate_blocked" in kinds
