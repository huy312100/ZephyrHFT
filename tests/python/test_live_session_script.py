"""Offline tests for the session harness's arming guardrails.

The supervisor is machinery; what these tests protect is the rule that
machinery does not earn money for us. A production session still needs the
alpha gate, ``--live`` still has to be said out loud, and the kill-on-stop path
must not depend on either.
"""
from __future__ import annotations

import json
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Dict, List

import pytest

from python_analytics.live import AlphaGateError

from scripts import run_live_session as script


def _production() -> SimpleNamespace:
    return SimpleNamespace(is_testnet=False, environment="production")


def _testnet() -> SimpleNamespace:
    return SimpleNamespace(is_testnet=True, environment="testnet")


# --------------------------------------------------------------------------- #
# Environment
# --------------------------------------------------------------------------- #
def test_production_needs_live_to_be_said_out_loud() -> None:
    assert script._production_refused(_production(), SimpleNamespace(live=False))
    assert not script._production_refused(_production(), SimpleNamespace(live=True))
    assert not script._production_refused(_testnet(), SimpleNamespace(live=False))


def test_a_missing_environment_is_a_usage_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from python_analytics.exchange import binance_futures as bf

    def refuse(*_a: Any, **_k: Any) -> Any:
        raise bf.BinanceConfigError("BINANCE_API_KEY not set")

    monkeypatch.setattr(bf.BinanceFuturesConfig, "from_env", classmethod(
        lambda cls, *a, **k: refuse()))
    args = script.parse_args(["--store", str(tmp_path / "o.sqlite3")])
    assert script.run(args) == script.EXIT_USAGE


# --------------------------------------------------------------------------- #
# The alpha gate
# --------------------------------------------------------------------------- #
def test_production_without_a_green_alpha_gate_is_refused(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from python_analytics.exchange import binance_futures as bf

    monkeypatch.setattr(bf.BinanceFuturesConfig, "from_env", classmethod(
        lambda cls, *a, **k: _production()))

    def refuse(*_a: Any, **_k: Any) -> str:
        raise AlphaGateError("alpha checks failed: alpha.net_pnl, alpha.fee_wall")

    monkeypatch.setattr(script, "require_alpha_go", refuse)
    args = script.parse_args(["--live", "--store", str(tmp_path / "o.sqlite3")])
    assert script.run(args) == script.EXIT_ALPHA_NO_GO


def test_a_testnet_session_runs_without_the_gate_but_reports_it(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Rehearsal is the point of testnet; the verdict is logged, not required."""
    from python_analytics.exchange import binance_futures as bf

    monkeypatch.setattr(bf.BinanceFuturesConfig, "from_env", classmethod(
        lambda cls, *a, **k: _testnet()))
    report = tmp_path / "report.json"
    report.write_text(json.dumps({"alpha_verdict": "NO-GO", "checks": []}),
                      encoding="utf-8")

    reached = {}

    def stop_after_the_gate(*_a: Any, **_k: Any) -> None:
        reached["past_gate"] = True
        raise RuntimeError("end of test")

    monkeypatch.setattr(script, "BinanceFuturesClient", stop_after_the_gate)
    args = script.parse_args(["--report", str(report),
                              "--store", str(tmp_path / "o.sqlite3")])
    with pytest.raises(RuntimeError):
        script.run(args)
    assert reached.get("past_gate"), "testnet must not be gated on alpha"


# --------------------------------------------------------------------------- #
# Defaults
# --------------------------------------------------------------------------- #
def test_the_session_is_bounded_by_default() -> None:
    """An unbounded loop on a laptop is not a service."""
    args = script.parse_args([])
    assert args.iterations > 0, "the default session must end on its own"


def test_documented_exit_codes_are_distinct() -> None:
    codes = [script.EXIT_CLEAN, script.EXIT_HALTED, script.EXIT_USAGE,
             script.EXIT_ALPHA_NO_GO, script.EXIT_INTERRUPTED]
    assert len(set(codes)) == len(codes), codes
    assert script.EXIT_ALPHA_NO_GO == 3, "the alpha refusal must be its own code"


# --------------------------------------------------------------------------- #
# A fake venue that is also the client, so run() can be driven end to end
# --------------------------------------------------------------------------- #
class SessionVenue:
    """Enough of ``BinanceFuturesClient`` for the script's own paths.

    Defined here rather than imported: this repository's live test modules each
    own their double, so a change to one harness cannot break another.
    """

    def __init__(self) -> None:
        self.bid, self.ask = Decimal("100.00"), Decimal("100.02")
        self.resting: list = []
        #: Every new_order the venue was actually asked for, including ones later
        #: cancelled: counting these is how a test proves no work was performed.
        self.placed: list = []
        self.cancelled: list = []
        self.cancel_all_calls = 0
        self.flatten_calls = 0
        self.position_qty = Decimal("0")
        self.cancel_refused = False
        self._next = 1

    def exchange_info(self) -> Dict[str, Any]:
        filters = [
            {"filterType": "PRICE_FILTER", "tickSize": "0.01"},
            {"filterType": "LOT_SIZE", "stepSize": "0.001", "minQty": "0.001"},
            {"filterType": "MIN_NOTIONAL", "notional": "5"},
        ]
        return {"symbols": [{"symbol": "BTCUSDT", "status": "TRADING",
                             "filters": filters}]}

    def book_ticker(self, symbol: str) -> Dict[str, str]:
        return {"bidPrice": str(self.bid), "askPrice": str(self.ask)}

    def new_order(self, **kwargs: Any) -> Dict[str, Any]:
        order_id = self._next
        self._next += 1
        self.placed.append(dict(kwargs))
        self.resting.append({"clientOrderId": kwargs["client_order_id"],
                             "orderId": order_id, "side": kwargs["side"],
                             "price": Decimal(kwargs["price"]),
                             "origQty": Decimal(kwargs["quantity"]),
                             "symbol": kwargs["symbol"], "type": "LIMIT",
                             "executedQty": Decimal("0"), "status": "NEW"})
        return {"orderId": order_id, "status": "NEW",
                "clientOrderId": kwargs["client_order_id"]}

    def query_order(self, *, symbol: str, client_order_id: Any = None,
                    order_id: Any = None) -> Dict[str, Any]:
        for row in self.resting:
            if row["clientOrderId"] == client_order_id:
                return {"orderId": row["orderId"], "status": "NEW"}
        return {"orderId": 0, "status": "FILLED"}

    def open_orders(self, symbol: str) -> list:
        from python_analytics.exchange.binance_futures import OpenOrder
        return [OpenOrder(symbol=r["symbol"], order_id=r["orderId"],
                          client_order_id=r["clientOrderId"], side=r["side"],
                          order_type=r["type"], price=r["price"],
                          stop_price=Decimal("0"), quantity=r["origQty"],
                          executed_quantity=r["executedQty"],
                          status=r["status"]) for r in self.resting]

    def cancel_order(self, *, symbol: str, order_id: Any = None,
                     client_order_id: Any = None) -> Dict[str, Any]:
        if self.cancel_refused:
            from python_analytics.exchange.binance_futures import BinanceAPIError
            raise BinanceAPIError("-2011 Unknown order sent", status=400,
                                  code=-2011, endpoint="/fapi/v1/order")
        self.cancelled.append(client_order_id)
        self.resting = [r for r in self.resting
                        if r["clientOrderId"] != client_order_id]
        return {"status": "CANCELED"}

    def cancel_all_orders(self, symbol: str) -> Dict[str, Any]:
        self.cancel_all_calls += 1
        self.resting = []
        return {}

    def flatten(self, symbol: str) -> Dict[str, Any]:
        self.flatten_calls += 1
        return {}

    def positions(self, symbol: Any = None, **kwargs: Any) -> list:
        from python_analytics.exchange.binance_futures import Position
        if self.position_qty == 0:
            return []
        return [Position(symbol="BTCUSDT", quantity=self.position_qty,
                         entry_price=Decimal("100"), mark_price=Decimal("100"),
                         unrealized_pnl=Decimal("0"), leverage=Decimal("1"))]


def _drive(tmp_path: Path, venue: SessionVenue, argv: List[str],
           monkeypatch: pytest.MonkeyPatch) -> int:
    """Runs the script's own ``run()`` against ``venue`` as the client."""
    from python_analytics.exchange import binance_futures as bf

    monkeypatch.setattr(bf.BinanceFuturesConfig, "from_env", classmethod(
        lambda cls, *a, **k: _testnet()))
    monkeypatch.setattr(script, "BinanceFuturesClient", lambda *a, **k: venue)
    report = tmp_path / "report.json"
    report.write_text(json.dumps({"alpha_verdict": "NO-GO", "checks": []}),
                      encoding="utf-8")
    return script.run(script.parse_args(
        ["--report", str(report), "--store", str(tmp_path / "s.sqlite3"),
         "--quantity", "0.01", "--offset-bps", "5", "--max-inventory", "0.5",
         "--loss-cap", "10"] + argv))


# --------------------------------------------------------------------------- #
# Cleanup on every path, and an exit code that cannot claim success early
# --------------------------------------------------------------------------- #
def test_a_session_that_ends_on_its_limit_leaves_nothing_resting(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The iteration limit is the ordinary exit, and the ordinary leak.

    Ending 'cleanly' with two quotes still in the book is the state a dead
    process must not leave behind, so cleanup runs on the clean path too.
    """
    venue = SessionVenue()
    code = _drive(tmp_path, venue, ["--iterations", "2", "--interval-s", "0"],
                  monkeypatch)
    assert code == script.EXIT_CLEAN
    assert venue.resting == [], "a finished session leaves its quotes cancelled"
    assert len(venue.cancelled) == 2


def test_a_cancel_refused_at_cleanup_is_not_reported_as_clean(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Exit 0 while the venue still holds our order is the forbidden outcome."""
    venue = SessionVenue()
    venue.cancel_refused = True
    code = _drive(tmp_path, venue, ["--iterations", "1", "--interval-s", "0"],
                  monkeypatch)
    assert code == script.EXIT_HALTED, "a stuck quote must not exit 0"
    assert venue.resting, "the venue really still holds it in this scenario"


def test_a_bad_number_is_a_usage_refusal_not_a_traceback(
    tmp_path: Path, capsys: pytest.CaptureFixture
) -> None:
    """``--loss-cap 5o`` must earn exit 2, the documented configuration refusal.

    ``Decimal('5o')`` raises ``InvalidOperation``, an ``ArithmeticError`` that no
    handler in the script catches -- so a typo would exit 1, which the contract
    reserves for a halted session, and print a traceback instead of a usage line.
    """
    for flag, value in (("--loss-cap", "5o"), ("--quantity", "nan"),
                        ("--offset-bps", "inf"), ("--tick-size", "x")):
        with pytest.raises(SystemExit) as exit_info:
            script.parse_args([flag, value])
        assert exit_info.value.code == 2, f"{flag}={value}"


def test_the_run_reports_an_unresolved_order_instead_of_success(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The final reconcile is a verdict, not a log line."""
    venue = SessionVenue()

    # An order the venue holds but never answers for: UNKNOWN at the end.
    def never_found(*, symbol: str, client_order_id: Any = None,
                    order_id: Any = None) -> Dict[str, Any]:
        raise bf_error()

    from python_analytics.exchange.binance_futures import BinanceAPIError

    def bf_error() -> BinanceAPIError:
        return BinanceAPIError("-2013 Order does not exist", status=400,
                               code=-2013, endpoint="/fapi/v1/order")

    original_query = venue.query_order

    def flaky(**kwargs: Any) -> Dict[str, Any]:
        # Landed on the venue, but the local row is missing from openOrders.
        row = original_query(**kwargs)
        return {"orderId": row["orderId"], "status": "NEW"}

    venue.query_order = flaky  # type: ignore[assignment]
    venue.open_orders = lambda symbol: []  # the venue "has nothing"  # type: ignore

    code = _drive(tmp_path, venue, ["--iterations", "1", "--interval-s", "0"],
                  monkeypatch)
    assert code == script.EXIT_HALTED, \
        "an unresolved order must never report success"


def test_the_signal_handler_only_raises_the_flag_and_places_nothing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Behaviour, not source text.

    Asserting on the file would keep passing if registration were removed, or if
    the handler moved its cleanup into a helper -- and the thing that actually
    matters is that firing the handler performs no venue work and no store write,
    because a handler that does can interrupt a step mid-flight and place the
    remaining side after the operator asked the session to stop.
    """
    import signal as signal_module
    from python_analytics.live import LiveSupervisor

    installed: Dict[int, Any] = {}
    restored: Dict[int, Any] = {}
    real_signal = signal_module.signal

    def record(sig, handler) -> Any:
        # The script installs its own handler and then restores the previous one,
        # so a plain assignment here would end up holding the *restored* handler
        # (in a test run, CPython's default_int_handler) and calling it would
        # raise KeyboardInterrupt instead of exercising the session's handler.
        target = installed if sig not in installed else restored
        target[sig] = handler
        return real_signal(sig, handler)

    supervisors: List[Any] = []

    class Spy(LiveSupervisor):
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            super().__init__(*args, **kwargs)
            supervisors.append(self)

    monkeypatch.setattr(signal_module, "signal", record)
    monkeypatch.setattr(script, "LiveSupervisor", Spy)
    venue = SessionVenue()
    _drive(tmp_path, venue, ["--iterations", "1", "--interval-s", "0"], monkeypatch)

    assert signal_module.SIGINT in installed, "no handler was ever installed"
    handler = installed[signal_module.SIGINT]
    # The script's own closure, not a restored default: this is the object the
    # operator's Ctrl-C will actually reach.
    assert getattr(handler, "__name__", "") == "on_signal", handler
    supervisor = supervisors[-1]
    placed, cancelled = len(venue.placed), len(venue.cancelled)

    handler(signal_module.SIGINT, None)

    assert supervisor.stop_requested is True
    assert len(venue.placed) == placed, "the handler must not place a quote"
    assert len(venue.cancelled) == cancelled, "nor cancel one from inside itself"
    assert restored.get(signal_module.SIGINT) is not None, \
        "the session never gave the process its handlers back"


def test_handlers_stay_ours_until_the_cleanup_is_done(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The window this used to leave open: Ctrl-C *during* cancellation.

    Restoring the previous dispositions as soon as the loop ended put
    ``SIG_DFL`` back while quotes were still being cancelled, so a Ctrl-C in
    that interval raised inside the cleanup and exited 130 with orders left
    resting. Observed from inside the cleanup itself, which is the only place
    that can prove the ordering.
    """
    import signal as signal_module

    venue = SessionVenue()
    original_cancel = venue.cancel_order
    seen: Dict[str, Any] = {}

    def probing_cancel(**kwargs: Any) -> Dict[str, Any]:
        seen["during_cleanup"] = signal_module.getsignal(signal_module.SIGINT)
        return original_cancel(**kwargs)

    venue.cancel_order = probing_cancel  # type: ignore[assignment]
    code = _drive(tmp_path, venue, ["--iterations", "1", "--interval-s", "0"],
                  monkeypatch)

    assert code == script.EXIT_CLEAN
    assert seen, "cleanup never cancelled, so it proved nothing"
    # Asserting "not SIG_DFL" would pass even with the bug: restoring early puts
    # back whatever the test runner had, which is a handler but not *this*
    # session's. The invariant is that cleanup is still interruptible by the
    # code that owns the orders, so the handler must be the session's own.
    during = seen["during_cleanup"]
    assert getattr(during, "__name__", "") == "on_signal", during
    assert venue.resting == [], "and the cleanup still finished the job"


def test_the_session_restores_the_signal_dispositions_it_borrowed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A CLI must not leave the process with handlers pointing at its own corpse.

    ``run()`` installs SIGINT/SIGTERM handlers; if it does not put back what was
    there before, every later caller in the same process -- including a test
    runner -- silently inherits a closure over a finished session.
    """
    import signal as signal_module

    # The sentinels themselves are borrowed state: restore whatever was here
    # before, or this guard leaks the very defect it tests for.
    previous = {sig: signal_module.getsignal(sig)
                for sig in (signal_module.SIGINT, signal_module.SIGTERM)}
    sentinels = {sig: (lambda *_a: None) for sig in previous}
    try:
        for sig, handler in sentinels.items():
            signal_module.signal(sig, handler)

        venue = SessionVenue()
        _drive(tmp_path, venue, ["--iterations", "1", "--interval-s", "0"],
               monkeypatch)

        for sig, handler in sentinels.items():
            assert signal_module.getsignal(sig) is handler, \
                f"{sig!r} handler was not restored after the session ended"
    finally:
        for sig, handler in previous.items():
            signal_module.signal(sig, handler)
