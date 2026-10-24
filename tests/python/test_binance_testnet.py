"""Tests for the Binance Futures testnet micro-pilot and its REST client.

Everything here is offline: a fake transport stands in for HTTPS, and a fake
client stands in for the pilot's exchange calls. The suite covers endpoint and
credential resolution, HMAC signing, exchange-filter validation, depth-frame
parsing, and both the dry-run and submit lifecycles.
"""
from __future__ import annotations

import json
import logging
import urllib.parse
import urllib.request
from decimal import Decimal
from typing import Any, Dict, List, Tuple

import pytest

import scripts.run_testnet_pilot as pilot
from python_analytics.exchange.binance_futures import (
    API_KEY_VAR,
    API_SECRET_VAR,
    BinanceAPIError,
    BinanceConfigError,
    BinanceCredentials,
    BinanceFuturesClient,
    BinanceFuturesConfig,
    BinanceTransportError,
    MissingCredentialsError,
    OrderValidationError,
    SymbolFilters,
    build_limit_order,
    decimal_text,
    parse_depth_tick,
)
from python_analytics.utils.config import (
    EXCHANGE_ENDPOINTS,
    ConfigError,
    DataIngestionSettings,
    binance_endpoints,
    resolve_binance_environment,
)

TESTNET_REST = "https://testnet.binancefuture.com"
TESTNET_WS = "wss://stream.binancefuture.com/ws"
SIGNED_VECTOR = (
    "symbol=BTCUSDT&side=BUY&type=LIMIT&timeInForce=GTC&quantity=0.001"
    "&price=10000&timestamp=1700000000000&recvWindow=5000"
    "&signature=bc2929e49fe8565f2fe2c0a3b4c41f741a67063e0e237f9fc195e4b772f58b47"
)


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
class FakeTransport:
    """Records requests and replays queued ``(status, payload)`` responses."""

    def __init__(self, responses: List[Tuple[int, Any]]):
        self.responses = list(responses)
        self.requests: List[urllib.request.Request] = []

    def __call__(
        self, request: urllib.request.Request, timeout: float
    ) -> Tuple[int, bytes]:
        self.requests.append(request)
        if not self.responses:
            raise AssertionError(f"unexpected request: {request.full_url}")
        status, payload = self.responses.pop(0)
        if isinstance(payload, bytes):
            return status, payload
        return status, json.dumps(payload).encode("utf-8")


def _credentials() -> BinanceCredentials:
    return BinanceCredentials(api_key="test-key-1234", api_secret="test-secret")


def _config(environment: str = "testnet") -> BinanceFuturesConfig:
    endpoints = binance_endpoints(environment, environ={})
    return BinanceFuturesConfig(
        environment=environment,
        rest_url=endpoints["rest"],
        ws_url=endpoints["ws"],
        credentials=_credentials(),
    )


def _client(
    transport: FakeTransport, *, clock: float = 1_700_000_000.0
) -> BinanceFuturesClient:
    return BinanceFuturesClient(
        _config(), transport=transport, clock=lambda: clock
    )


def _exchange_info() -> Dict[str, Any]:
    return {
        "symbols": [
            {
                "symbol": "BTCUSDT",
                "pricePrecision": 2,
                "quantityPrecision": 3,
                "filters": [
                    {
                        "filterType": "PRICE_FILTER",
                        "minPrice": "556.80",
                        "maxPrice": "4529764",
                        "tickSize": "0.10",
                    },
                    {
                        "filterType": "LOT_SIZE",
                        "minQty": "0.001",
                        "maxQty": "1000",
                        "stepSize": "0.001",
                    },
                    {
                        "filterType": "MIN_NOTIONAL",
                        "notional": "100",
                    },
                    {
                        "filterType": "PERCENT_PRICE",
                        "multiplierUp": "1.0500",
                        "multiplierDown": "0.9500",
                        "multiplierDecimal": "4",
                    },
                ],
            }
        ]
    }


def _account(available: str = "15000.00000000") -> Dict[str, Any]:
    return {
        "availableBalance": available,
        "totalWalletBalance": "15000.00000000",
        "assets": [{"asset": "USDT", "availableBalance": available}],
    }


def _filters() -> SymbolFilters:
    return SymbolFilters.from_exchange_info(_exchange_info(), "BTCUSDT")


# --------------------------------------------------------------------------- #
# Environment and configuration resolution
# --------------------------------------------------------------------------- #
def test_environment_defaults_to_testnet():
    assert resolve_binance_environment(environ={}) == "testnet"
    assert binance_endpoints(environ={}) == {"rest": TESTNET_REST, "ws": TESTNET_WS}


def test_environment_is_case_and_whitespace_insensitive():
    assert (
        resolve_binance_environment(environ={"BINANCE_ENV": " TESTNET "})
        == "testnet"
    )
    assert resolve_binance_environment("Production", environ={}) == "production"


def test_environment_rejects_unknown_values():
    with pytest.raises(ConfigError, match="BINANCE_ENV"):
        resolve_binance_environment("staging", environ={})


def test_production_endpoints():
    endpoints = binance_endpoints("production", environ={})
    assert endpoints["rest"] == "https://fapi.binance.com"
    assert endpoints["ws"] == "wss://fstream.binance.com/ws"


def test_testnet_exchange_endpoint_is_available_to_ingestion():
    assert (
        EXCHANGE_ENDPOINTS["binance_futures_testnet"]
        == "wss://stream.binancefuture.com/ws"
    )
    settings = DataIngestionSettings(exchange="binance_futures_testnet")
    assert settings.stream_url == "wss://stream.binancefuture.com/ws/btcusdt@depth@100ms"


def test_config_reads_credentials_from_environment_only():
    config = BinanceFuturesConfig.from_env(
        {
            "BINANCE_API_KEY": "  key-1234  ",
            "BINANCE_API_SECRET": "  secret-value  ",
        }
    )
    assert config.environment == "testnet"
    assert config.rest_url == TESTNET_REST
    assert config.ws_url == TESTNET_WS
    assert config.credentials.api_key == "key-1234"
    assert config.credentials.api_secret == "secret-value"
    assert config.is_testnet


def test_config_defaults_missing_environment_to_testnet():
    config = BinanceFuturesConfig.from_env(
        {"BINANCE_API_KEY": "key", "BINANCE_API_SECRET": "secret"}
    )
    assert config.environment == "testnet"


def test_config_rejects_invalid_environment():
    with pytest.raises(BinanceConfigError, match="BINANCE_ENV"):
        BinanceFuturesConfig.from_env(
            {
                "BINANCE_ENV": "staging",
                "BINANCE_API_KEY": "key",
                "BINANCE_API_SECRET": "secret",
            }
        )


def test_missing_credentials_are_named_explicitly():
    with pytest.raises(MissingCredentialsError) as excinfo:
        BinanceFuturesConfig.from_env({})
    message = str(excinfo.value)
    assert API_KEY_VAR in message and API_SECRET_VAR in message

    with pytest.raises(MissingCredentialsError, match=API_SECRET_VAR):
        BinanceFuturesConfig.from_env({API_KEY_VAR: "key"})


def test_credentials_and_config_mask_secrets():
    config = _config()
    for rendered in (repr(config), str(config), repr(config.credentials)):
        assert "test-secret" not in rendered
        assert "test-key-1234" not in rendered
    assert "***1234" in repr(config)
    assert "***" in repr(config.credentials)


def test_stream_url_lowercases_symbol_and_accepts_bare_stream_type():
    config = _config()
    assert config.stream_url("BTCUSDT") == f"{TESTNET_WS}/btcusdt@depth@100ms"
    assert (
        config.stream_url("BTCUSDT", "depth@100ms")
        == f"{TESTNET_WS}/btcusdt@depth@100ms"
    )
    with pytest.raises(BinanceConfigError, match="stream type"):
        config.stream_url("BTCUSDT", "  ")


# --------------------------------------------------------------------------- #
# Signing and request plumbing
# --------------------------------------------------------------------------- #
def test_signed_query_matches_known_hmac_vector():
    client = _client(FakeTransport([]))
    query = client.signed_query(
        {
            "symbol": "BTCUSDT",
            "side": "BUY",
            "type": "LIMIT",
            "timeInForce": "GTC",
            "quantity": Decimal("0.001"),
            "price": Decimal("10000"),
        }
    )
    assert query == SIGNED_VECTOR


def test_signed_request_sets_header_and_never_leaks_secret():
    transport = FakeTransport([(200, _account())])
    client = _client(transport)
    client.account()

    request = transport.requests[0]
    headers = {key.lower(): value for key, value in request.header_items()}
    parts = urllib.parse.urlsplit(request.full_url)
    query = urllib.parse.parse_qs(parts.query)

    assert request.get_method() == "GET"
    assert parts.path == "/fapi/v3/account"
    assert headers["x-mbx-apikey"] == "test-key-1234"
    assert "timestamp" in query and "recvWindow" in query and "signature" in query
    assert query["recvWindow"] == ["5000"]
    assert "test-secret" not in request.full_url


def test_public_request_has_no_api_key_header():
    transport = FakeTransport([(200, {"serverTime": 1})])
    client = _client(transport)
    client.ping()
    headers = {key.lower(): value for key, value in transport.requests[0].header_items()}
    assert "x-mbx-apikey" not in headers


def test_sync_time_tracks_exchange_offset():
    transport = FakeTransport([(200, {"serverTime": 1_700_000_000_500})])
    client = _client(transport)
    assert client.time_synced is False
    assert client.sync_time() == 500
    assert client.time_synced is True
    assert client.timestamp_ms() == 1_700_000_000_500


def test_account_falls_back_to_v2_on_404():
    transport = FakeTransport(
        [
            (404, {"code": -1, "msg": "not found"}),
            (200, _account()),
        ]
    )
    client = _client(transport)
    assert client.account()["availableBalance"] == "15000.00000000"
    paths = [urllib.parse.urlsplit(r.full_url).path for r in transport.requests]
    assert paths == ["/fapi/v3/account", "/fapi/v2/account"]


def test_api_error_exposes_status_and_code():
    transport = FakeTransport(
        [(400, {"code": -2019, "msg": "Margin is insufficient."})]
    )
    client = _client(transport)
    with pytest.raises(BinanceAPIError) as excinfo:
        client.account()
    assert excinfo.value.status == 400
    assert excinfo.value.code == -2019
    assert "Margin is insufficient." in str(excinfo.value)
    assert excinfo.value.endpoint == "/fapi/v3/account"


def test_non_json_error_body_is_still_reported():
    transport = FakeTransport([(502, b"<html>bad gateway</html>")])
    client = _client(transport)
    with pytest.raises(BinanceAPIError, match="bad gateway"):
        client.ping()


def test_transport_failure_is_wrapped():
    def failing(request: urllib.request.Request, timeout: float):
        raise OSError("connection refused")

    client = BinanceFuturesClient(
        _config(), transport=failing, clock=lambda: 1_700_000_000.0
    )
    with pytest.raises(BinanceTransportError, match="connection refused"):
        client.ping()


def test_order_helpers_require_an_identifier():
    client = _client(FakeTransport([]))
    with pytest.raises(BinanceConfigError, match="order_id"):
        client.query_order(symbol="BTCUSDT")


def test_new_order_encodes_a_signed_limit_order():
    transport = FakeTransport([(200, {"orderId": 7, "status": "NEW"})])
    client = _client(transport)
    client.new_order(
        symbol="btcusdt",
        side="buy",
        order_type="limit",
        quantity=Decimal("0.001"),
        price=Decimal("119400.0"),
        time_in_force="gtc",
        client_order_id="zephyr-pilot-1",
    )

    request = transport.requests[0]
    query = urllib.parse.parse_qs(urllib.parse.urlsplit(request.full_url).query)
    assert request.get_method() == "POST"
    assert urllib.parse.urlsplit(request.full_url).path == "/fapi/v1/order"
    assert query["symbol"] == ["BTCUSDT"]
    assert query["side"] == ["BUY"]
    assert query["type"] == ["LIMIT"]
    assert query["timeInForce"] == ["GTC"]
    assert query["quantity"] == ["0.001"]
    assert query["price"] == ["119400"]
    assert query["newClientOrderId"] == ["zephyr-pilot-1"]
    assert "signature" in query and "timestamp" in query


def test_cancel_order_uses_delete_with_the_order_id():
    transport = FakeTransport([(200, {"orderId": 7, "status": "CANCELED"})])
    client = _client(transport)
    client.cancel_order(symbol="BTCUSDT", order_id=7)

    request = transport.requests[0]
    query = urllib.parse.parse_qs(urllib.parse.urlsplit(request.full_url).query)
    assert request.get_method() == "DELETE"
    assert urllib.parse.urlsplit(request.full_url).path == "/fapi/v1/order"
    assert query["symbol"] == ["BTCUSDT"]
    assert query["orderId"] == ["7"]
    assert "signature" in query


# --------------------------------------------------------------------------- #
# Exchange filters, quantization and order planning
# --------------------------------------------------------------------------- #
def test_symbol_filters_parse_exchange_info():
    filters = _filters()
    assert filters.symbol == "BTCUSDT"
    assert filters.tick_size == Decimal("0.10")
    assert filters.step_size == Decimal("0.001")
    assert filters.min_quantity == Decimal("0.001")
    assert filters.max_quantity == Decimal("1000")
    assert filters.min_notional == Decimal("100")
    assert filters.quantity_precision == 3
    assert filters.price_precision == 2
    assert filters.multiplier_down == Decimal("0.9500")
    assert "minNotional=100" in filters.describe()


def test_symbol_filters_reject_unknown_symbol_and_missing_payload():
    from python_analytics.exchange.binance_futures import UnknownSymbolError

    with pytest.raises(UnknownSymbolError, match="ETHUSDT"):
        SymbolFilters.from_exchange_info(_exchange_info(), "ETHUSDT")
    with pytest.raises(pilot.BinanceError, match="symbols"):
        SymbolFilters.from_exchange_info({"symbols": None}, "BTCUSDT")


def test_quantize_quantity_and_price_round_down():
    filters = _filters()
    assert filters.quantize_quantity("0.0019") == Decimal("0.001")
    assert filters.quantize_quantity("1.2345") == Decimal("1.234")
    assert filters.quantize_price("119999.99") == Decimal("119999.9")


def test_required_quantity_respects_min_notional_and_min_qty():
    filters = _filters()
    assert filters.required_quantity("200000") == Decimal("0.001")
    # 100 USDT / 1,000,000 = 0.0001, but minQty is 0.001.
    assert filters.required_quantity("1000000") == Decimal("0.001")
    # 100 USDT / 40,000 = 0.0025 -> rounded up to the next step.
    assert filters.required_quantity("40000") == Decimal("0.003")


def test_validate_accepts_a_compliant_order():
    _filters().validate(
        Decimal("0.001"), price=Decimal("119400.0"), reference_price=Decimal("120000")
    )


def test_validate_rejects_bad_quantity_and_price():
    filters = _filters()
    with pytest.raises(OrderValidationError, match="minQty"):
        filters.validate(Decimal("0.0005"))
    with pytest.raises(OrderValidationError, match="stepSize"):
        filters.validate(Decimal("0.0015"))
    with pytest.raises(OrderValidationError, match="tickSize"):
        filters.validate(
            Decimal("0.001"),
            price=Decimal("119400.05"),
            reference_price=Decimal("120000"),
        )
    with pytest.raises(OrderValidationError, match="PERCENT_PRICE"):
        filters.validate(
            Decimal("0.001"),
            price=Decimal("110000.0"),
            reference_price=Decimal("120000"),
        )


def test_validate_rejects_notional_below_minimum():
    from dataclasses import replace

    filters = replace(
        _filters(),
        multiplier_up=Decimal("0"),
        multiplier_down=Decimal("0"),
    )
    with pytest.raises(OrderValidationError, match="minNotional"):
        filters.validate(Decimal("0.001"), price=Decimal("90000"))


def test_build_limit_order_is_passive_and_quantized():
    plan = build_limit_order(
        _filters(),
        side="buy",
        quantity="0.0019",
        reference_price="120000",
        offset_bps=50,
        now_ms=1_700_000_000_000,
    )
    assert plan.side == "BUY"
    assert plan.quantity == Decimal("0.001")
    assert plan.price == Decimal("119400.0")
    assert plan.price < plan.reference_price
    assert plan.notional == Decimal("119.4")
    assert plan.client_order_id == "zephyr-pilot-1700000000000"
    assert plan.order_params()["type"] == "LIMIT"
    assert plan.order_params()["newOrderRespType"] == "RESULT"

    sell = build_limit_order(
        _filters(),
        side="SELL",
        quantity="0.001",
        reference_price="120000",
        offset_bps=50,
        client_order_id="zephyr-test",
    )
    assert sell.price == Decimal("120600.0")
    assert sell.client_order_id == "zephyr-test"

    # A sub-tick offset must still round away from the reference, never onto it.
    sell_tiny = build_limit_order(
        _filters(),
        side="SELL",
        quantity="0.001",
        reference_price="120000.05",
        offset_bps=0.0001,
    )
    assert sell_tiny.price > sell_tiny.reference_price


def test_build_limit_order_rejects_invalid_inputs():
    with pytest.raises(OrderValidationError, match="side"):
        build_limit_order(
            _filters(), side="HOLD", quantity="0.001", reference_price="120000"
        )
    with pytest.raises(OrderValidationError, match="offset_bps"):
        build_limit_order(
            _filters(),
            side="BUY",
            quantity="0.001",
            reference_price="120000",
            offset_bps=-1,
        )
    with pytest.raises(OrderValidationError, match="offset_bps"):
        build_limit_order(
            _filters(),
            side="BUY",
            quantity="0.001",
            reference_price="120000",
            offset_bps=0,
        )
    with pytest.raises(OrderValidationError, match="client order id"):
        build_limit_order(
            _filters(),
            side="BUY",
            quantity="0.001",
            reference_price="120000",
            client_order_id="x" * 37,
        )


def test_decimal_text_avoids_exponents_and_trailing_zeros():
    assert decimal_text(Decimal("1E+2")) == "100"
    assert decimal_text(Decimal("0.0010")) == "0.001"
    assert decimal_text(Decimal("0.000")) == "0"
    assert decimal_text(Decimal("-0")) == "0"


# --------------------------------------------------------------------------- #
# Depth-stream parsing
# --------------------------------------------------------------------------- #
def test_parse_depth_tick_handles_raw_and_combined_frames():
    raw = {
        "e": "depthUpdate",
        "E": 1_700_000_000_000,
        "u": 42,
        "b": [["100000.10", "1.500"]],
        "a": [["100000.20", "2.000"]],
    }
    tick = parse_depth_tick(raw)
    assert tick is not None
    assert tick.bid == Decimal("100000.10")
    assert tick.ask == Decimal("100000.20")
    assert tick.spread == Decimal("0.10")
    assert tick.update_id == 42
    assert tick.event_ms == 1_700_000_000_000
    assert parse_depth_tick({"stream": "btcusdt@depth@100ms", "data": raw}) == tick


def test_parse_depth_tick_returns_none_for_unusable_frames():
    assert parse_depth_tick({"e": "error", "m": "oops"}) is None
    assert parse_depth_tick({"b": [], "a": []}) is None
    assert parse_depth_tick({"b": [["bad", "1"]], "a": [["1", "1"]]}) is None
    assert parse_depth_tick({"b": ["100", "1"], "a": [["1", "1"]]}) is None
    assert parse_depth_tick("nope") is None


# --------------------------------------------------------------------------- #
# Pilot orchestration (fake client, no network)
# --------------------------------------------------------------------------- #
class FakeClient:
    def __init__(self, *, available: str = "15000", mark: str = "120000") -> None:
        self.available = available
        self.mark = mark
        self.calls: List[Tuple[str, Dict[str, Any]]] = []
        self._cancelled = False

    def sync_time(self) -> int:
        self.calls.append(("sync_time", {}))
        return 12

    def ping(self) -> Dict[str, Any]:
        self.calls.append(("ping", {}))
        return {}

    def exchange_info(self) -> Dict[str, Any]:
        self.calls.append(("exchange_info", {}))
        return _exchange_info()

    def mark_price(self, symbol: str) -> Decimal:
        self.calls.append(("mark_price", {"symbol": symbol}))
        return Decimal(self.mark)

    def account(self) -> Dict[str, Any]:
        self.calls.append(("account", {}))
        return _account(self.available)

    def new_order(self, **kwargs: Any) -> Dict[str, Any]:
        self.calls.append(("new_order", kwargs))
        return {"orderId": 123, "status": "NEW"}

    def query_order(self, **kwargs: Any) -> Dict[str, Any]:
        self.calls.append(("query_order", kwargs))
        return {
            "orderId": 123,
            "status": "CANCELED" if self._cancelled else "NEW",
            "executedQty": "0",
            "avgPrice": "0",
        }

    def cancel_order(self, **kwargs: Any) -> Dict[str, Any]:
        self.calls.append(("cancel_order", kwargs))
        self._cancelled = True
        return {"orderId": 123, "status": "CANCELED"}


def test_parse_args_rejects_invalid_inputs():
    for argv in (
        ["--quantity", "abc"],
        ["--quantity", "0"],
        ["--quantity", "-0.001"],
        ["--order-offset-bps", "0"],
        ["--timeout", "0"],
        ["--recv-window", "0"],
        ["--symbol", " "],
    ):
        with pytest.raises(SystemExit) as excinfo:
            pilot.parse_args(argv)
        assert excinfo.value.code == 2


def test_run_dry_run_never_posts_an_order(caplog):
    caplog.set_level(logging.INFO)
    client = FakeClient()
    code = pilot.run(_config(), pilot.parse_args(["--skip-l2"]), client=client)
    names = [name for name, _ in client.calls]

    assert code == 0
    assert "new_order" not in names
    assert "cancel_order" not in names
    assert "pilot verdict: PASS" in caplog.text
    assert "dry run only" in caplog.text


def test_run_logs_mask_the_api_key_and_never_the_secret(caplog):
    caplog.set_level(logging.INFO)
    pilot.run(_config(), pilot.parse_args(["--skip-l2"]), client=FakeClient())

    assert "test-secret" not in caplog.text
    assert "test-key-1234" not in caplog.text
    assert "***1234" in caplog.text


def test_run_submit_places_queries_and_cancels(caplog):
    caplog.set_level(logging.INFO)
    client = FakeClient()
    code = pilot.run(
        _config(),
        pilot.parse_args(["--skip-l2", "--order-test", "submit"]),
        client=client,
    )

    calls = [name for name, _ in client.calls]
    assert code == 0
    assert calls.count("new_order") == 1
    assert calls.count("query_order") == 2
    assert calls.count("cancel_order") == 1

    new_order = next(payload for name, payload in client.calls if name == "new_order")
    assert new_order["symbol"] == "BTCUSDT"
    assert new_order["side"] == "BUY"
    assert new_order["order_type"] == "LIMIT"
    assert new_order["time_in_force"] == "GTC"
    assert new_order["quantity"] == Decimal("0.001")
    assert new_order["price"] == Decimal("119400.0")
    assert new_order["client_order_id"].startswith("zephyr-pilot-")
    assert "orderId=123 status=CANCELED" in caplog.text


def test_run_cancels_the_order_when_the_first_query_fails():
    class FailingQueryClient(FakeClient):
        def query_order(self, **kwargs: Any) -> Dict[str, Any]:
            self.calls.append(("query_order", kwargs))
            raise BinanceAPIError("gateway timeout", status=504)

    client = FailingQueryClient()
    code = pilot.run(
        _config(),
        pilot.parse_args(["--skip-l2", "--order-test", "submit"]),
        client=client,
    )
    calls = [name for name, _ in client.calls]
    assert code == 1
    assert calls.count("new_order") == 1
    assert calls.count("cancel_order") == 1


def test_run_refuses_to_submit_without_balance():
    client = FakeClient(available="0")
    code = pilot.run(
        _config(),
        pilot.parse_args(["--skip-l2", "--order-test", "submit"]),
        client=client,
    )
    names = [name for name, _ in client.calls]
    assert code == 1
    assert "new_order" not in names


def test_run_reports_quantity_below_min_notional():
    client = FakeClient()
    code = pilot.run(
        _config(),
        pilot.parse_args(["--skip-l2", "--quantity", "0.0001"]),
        client=client,
    )
    assert code == 1
    assert [name for name, _ in client.calls].count("new_order") == 0


def test_run_reports_api_failure_without_raising():
    class BrokenClient(FakeClient):
        def account(self) -> Dict[str, Any]:
            raise BinanceAPIError("bad signature", status=401, code=-1022)

    code = pilot.run(
        _config(), pilot.parse_args(["--skip-l2"]), client=BrokenClient()
    )
    assert code == 1


def test_main_requires_credentials(monkeypatch):
    for name in (API_KEY_VAR, API_SECRET_VAR, "BINANCE_ENV"):
        monkeypatch.delenv(name, raising=False)
    assert pilot.main(["--skip-l2"]) == 2


def test_main_refuses_production(monkeypatch):
    monkeypatch.setenv(API_KEY_VAR, "key")
    monkeypatch.setenv(API_SECRET_VAR, "secret")
    monkeypatch.setenv("BINANCE_ENV", "production")
    assert pilot.main(["--skip-l2"]) == 2


def test_main_runs_end_to_end_with_injected_client(monkeypatch, caplog):
    caplog.set_level(logging.INFO)
    monkeypatch.setenv(API_KEY_VAR, "key-1234")
    monkeypatch.setenv(API_SECRET_VAR, "secret-value")
    monkeypatch.delenv("BINANCE_ENV", raising=False)
    client = FakeClient()
    monkeypatch.setattr(pilot, "build_client", lambda config, args: client)

    assert pilot.main(["--skip-l2"]) == 0
    assert "wss://stream.binancefuture.com/ws/btcusdt@depth20@100ms" in caplog.text
    assert "secret-value" not in caplog.text
