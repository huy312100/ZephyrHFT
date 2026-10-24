"""Offline tests for the live-trading primitives on the futures REST client.

The testnet pilot only needs "place, query, cancel". A real-money session also
needs the order semantics that keep it survivable: post-only quotes, reduce-only
exits, a venue-side stop that outlives the process, leverage/margin setup, and
typed position/order snapshots to reconcile against. Every test here is offline
— the transport is injected, so no credential and no network is involved.
"""
from __future__ import annotations

import json
import logging
import urllib.parse
import urllib.request
from decimal import Decimal
from typing import Any, Dict, List, Tuple

import pytest

from python_analytics.costs import DEFAULT_MAKER_BPS
from python_analytics.exchange.binance_futures import (
    POST_ONLY_TIME_IN_FORCE,
    BinanceAPIError,
    BinanceCredentials,
    BinanceError,
    BinanceFuturesClient,
    BinanceFuturesConfig,
    BinanceRateLimitError,
    OrderValidationError,
    parse_commission_rate,
    parse_open_orders,
    parse_positions,
)
from python_analytics.utils.config import binance_endpoints


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


def _client(transport: FakeTransport) -> BinanceFuturesClient:
    endpoints = binance_endpoints("testnet", environ={})
    config = BinanceFuturesConfig(
        environment="testnet",
        rest_url=endpoints["rest"],
        ws_url=endpoints["ws"],
        credentials=BinanceCredentials(
            api_key="test-key-1234", api_secret="test-secret"
        ),
    )
    return BinanceFuturesClient(
        config, transport=transport, clock=lambda: 1_700_000_000.0
    )


def _query(transport: FakeTransport, index: int = 0) -> Dict[str, List[str]]:
    url = urllib.parse.urlsplit(transport.requests[index].full_url)
    return urllib.parse.parse_qs(url.query)


def _path(transport: FakeTransport, index: int = 0) -> str:
    return urllib.parse.urlsplit(transport.requests[index].full_url).path


def _account_payload(
    *,
    amount: str = "0.002",
    entry: str = "80000",
    leverage: str = "3",
    isolated: str = "true",
) -> Dict[str, Any]:
    """Mirrors an ``/fapi/v2/account`` or ``/fapi/v3/account`` response.

    Two venue quirks matter here: the account endpoint reports unrealised PnL as
    ``unrealizedProfit`` (``positionRisk`` is the one that spells it
    ``unRealizedProfit``), and it carries no ``markPrice`` at all. ``entryPrice``
    / ``leverage`` / ``isolated`` are documented on v2 and returned by v3 too.
    """

    return {
        "availableBalance": "1000.00000000",
        "totalWalletBalance": "1000.00000000",
        "assets": [{"asset": "USDT", "availableBalance": "1000.00000000"}],
        "positions": [
            {
                "symbol": "BTCUSDT",
                "positionAmt": amount,
                "entryPrice": entry,
                "unrealizedProfit": "2.00",
                "leverage": leverage,
                "isolated": isolated,
                "positionSide": "BOTH",
            },
            {
                "symbol": "ETHUSDT",
                "positionAmt": "0",
                "entryPrice": "0",
                "unrealizedProfit": "0",
                "leverage": "5",
                "isolated": "false",
                "positionSide": "BOTH",
            },
        ],
    }


def _position_risk_payload() -> List[Dict[str, Any]]:
    """Mirrors ``/fapi/v2/positionRisk``: mark price and the other PnL spelling."""

    return [
        {
            "symbol": "BTCUSDT",
            "positionAmt": "0.002",
            "entryPrice": "80000",
            "markPrice": "81000",
            "unRealizedProfit": "2.00",
            "leverage": "3",
            "isolated": "true",
            "positionSide": "BOTH",
        }
    ]


# --------------------------------------------------------------------------- #
# Position / order snapshots used for reconciliation
# --------------------------------------------------------------------------- #
def test_parse_positions_drops_flat_symbols_and_signs_quantity():
    positions = parse_positions(_account_payload())

    assert [position.symbol for position in positions] == ["BTCUSDT"]
    btc = positions[0]
    assert btc.quantity == Decimal("0.002")
    assert btc.side == "BUY"
    assert btc.closing_side() == "SELL"
    assert btc.mark_price == Decimal("0")  # the account endpoint sends no markPrice
    assert btc.notional == Decimal("160.000")  # so the entry price is used instead
    assert btc.leverage == Decimal("3")
    assert btc.isolated is True
    assert btc.unrealized_pnl == Decimal("2.00")
    assert "BTCUSDT BUY" in btc.describe()


def test_parse_positions_reads_the_unrealized_profit_key_the_account_uses():
    btc = parse_positions(_account_payload(entry="80000"))[0]

    assert btc.unrealized_pnl == Decimal("2.00")
    assert btc.entry_price == Decimal("80000")


def test_parse_positions_accepts_the_position_risk_pnl_spelling():
    btc = parse_positions({"positions": _position_risk_payload()})[0]

    assert btc.unrealized_pnl == Decimal("2.00")
    assert btc.mark_price == Decimal("81000")
    assert btc.notional == Decimal("162.000")


def test_parse_positions_tolerates_a_documented_v3_row():
    """A row with only the v3 fields (no entryPrice/leverage/isolated) must parse."""

    payload = {
        "positions": [
            {
                "symbol": "BTCUSDT",
                "positionSide": "BOTH",
                "positionAmt": "0.002",
                "unrealizedProfit": "2.00",
                "isolatedMargin": "0.00000000",
                "notional": "162",
                "isolatedWallet": "0",
                "initialMargin": "0",
                "maintMargin": "0",
                "updateTime": 0,
            }
        ]
    }

    btc = parse_positions(payload)[0]

    assert btc.quantity == Decimal("0.002")
    assert btc.unrealized_pnl == Decimal("2.00")
    assert btc.entry_price == Decimal("0")
    assert btc.leverage == Decimal("0")
    assert btc.isolated is False
    assert btc.notional == Decimal("0.000")


def test_parse_positions_can_include_flat_entries():
    positions = parse_positions(_account_payload(), include_flat=True)

    assert len(positions) == 2
    eth = positions[1]
    assert eth.side == "FLAT"
    assert eth.is_flat is True
    with pytest.raises(OrderValidationError, match="nothing to close"):
        eth.closing_side()


def test_parse_positions_handles_a_short_position():
    short = parse_positions(_account_payload(amount="-0.5"))[0]

    assert short.side == "SELL"
    assert short.closing_side() == "BUY"
    assert short.entry_price == Decimal("80000")
    assert short.notional == Decimal("40000.0")


def test_parse_positions_rejects_a_malformed_payload():
    with pytest.raises(BinanceError, match="positions"):
        parse_positions({"positions": "nope"})


def test_parse_positions_returns_nothing_without_a_positions_block():
    assert parse_positions({"availableBalance": "10"}) == []


def test_parse_open_orders_normalises_types_and_tracks_remaining():
    orders = parse_open_orders(
        [
            {
                "orderId": 5,
                "clientOrderId": "zephyr-1",
                "symbol": "btcusdt",
                "side": "buy",
                "type": "limit",
                "price": "80000.0",
                "stopPrice": "0",
                "origQty": "0.002",
                "executedQty": "0.001",
                "status": "PARTIALLY_FILLED",
                "timeInForce": "GTX",
                "reduceOnly": False,
                "closePosition": False,
            }
        ]
    )

    assert len(orders) == 1
    order = orders[0]
    assert order.symbol == "BTCUSDT"
    assert order.order_id == 5
    assert order.side == "BUY"
    assert order.order_type == "LIMIT"
    assert order.remaining_quantity == Decimal("0.001")
    assert order.time_in_force == "GTX"
    assert order.reduce_only is False
    assert "clientId=zephyr-1" in order.describe()


def test_parse_open_orders_accepts_an_empty_response():
    assert parse_open_orders([]) == []


def test_parse_open_orders_rejects_a_non_list_payload():
    with pytest.raises(BinanceError, match="openOrders"):
        parse_open_orders({"code": -1})


# --------------------------------------------------------------------------- #
# Commission rates: check the cost assumption instead of guessing it
# --------------------------------------------------------------------------- #
def test_parse_commission_rate_converts_venue_fractions_to_bps():
    rate = parse_commission_rate({
        "symbol": "btcusdt",
        "makerCommissionRate": "0.0002",
        "takerCommissionRate": "0.0005",
    })
    assert rate.symbol == "BTCUSDT"
    assert rate.maker_rate == Decimal("0.0002")
    assert rate.taker_bps == pytest.approx(5.0)
    assert rate.maker_bps == pytest.approx(DEFAULT_MAKER_BPS)
    assert rate.is_rebate is False
    assert "maker=0.0002 (+2.0000 bps)" in rate.describe()


def test_parse_commission_rate_flags_a_real_rebate_tier():
    rate = parse_commission_rate({
        "symbol": "ETHUSDT",
        "makerCommissionRate": "-0.000010",
        "takerCommissionRate": "0.000400",
    })
    assert rate.is_rebate is True
    assert rate.maker_bps == pytest.approx(-0.1)
    assert rate.describe().endswith("REBATE")


def test_parse_commission_rate_refuses_a_missing_rate():
    """A silent zero would make every strategy look free to run."""

    with pytest.raises(BinanceError, match="takerCommissionRate"):
        parse_commission_rate({"symbol": "BTCUSDT", "makerCommissionRate": "0.0002"})
    with pytest.raises(BinanceError, match="not an object"):
        parse_commission_rate([0.0002, 0.0005])


def test_parse_commission_rate_requires_a_symbol():
    with pytest.raises(BinanceError, match="symbol"):
        parse_commission_rate({"makerCommissionRate": "0.0002",
                               "takerCommissionRate": "0.0005"})
    assert parse_commission_rate({"makerCommissionRate": "0.0002",
                                  "takerCommissionRate": "0.0005"},
                                 symbol="btcusdt").symbol == "BTCUSDT"


def test_client_fetches_the_signed_commission_rate():
    transport = FakeTransport([(200, {"symbol": "BTCUSDT",
                                      "makerCommissionRate": "0.0002",
                                      "takerCommissionRate": "0.0005"})])

    rate = _client(transport).commission_rate("btcusdt")

    assert _path(transport) == "/fapi/v1/commissionRate"
    query = _query(transport)
    assert query["symbol"] == ["BTCUSDT"]
    assert "signature" in query  # a USER_DATA endpoint, so it must be signed
    assert rate.maker_bps == pytest.approx(2.0)
    assert rate.taker_bps == pytest.approx(5.0)


# --------------------------------------------------------------------------- #
# Order semantics a real-money session depends on
# --------------------------------------------------------------------------- #
def test_post_only_encodes_gtx():
    transport = FakeTransport([(200, {"orderId": 1, "status": "NEW"})])

    _client(transport).new_order(
        symbol="btcusdt",
        side="buy",
        order_type="limit",
        quantity=Decimal("0.002"),
        price=Decimal("80000"),
        post_only=True,
        client_order_id="zephyr-quote-1",
    )

    query = _query(transport)
    assert _path(transport) == "/fapi/v1/order"
    assert query["timeInForce"] == [POST_ONLY_TIME_IN_FORCE]
    assert query["quantity"] == ["0.002"]
    assert query["price"] == ["80000"]


def test_post_only_rejects_a_conflicting_time_in_force():
    transport = FakeTransport([])

    with pytest.raises(OrderValidationError, match="post_only"):
        _client(transport).new_order(
            symbol="BTCUSDT",
            side="BUY",
            order_type="LIMIT",
            quantity="0.002",
            price="80000",
            time_in_force="GTC",
            post_only=True,
        )

    assert transport.requests == []


def test_quantity_is_required_unless_closing_a_position():
    transport = FakeTransport([])

    with pytest.raises(OrderValidationError, match="quantity is required"):
        _client(transport).new_order(
            symbol="BTCUSDT", side="BUY", order_type="LIMIT", price="80000"
        )

    assert transport.requests == []


def test_stop_market_close_position_encodes_the_venue_side_stop():
    transport = FakeTransport([(200, {"orderId": 9, "status": "NEW"})])

    _client(transport).stop_market_close_position(
        symbol="btcusdt",
        side="sell",
        stop_price=Decimal("78000"),
        client_order_id="zephyr-stop-1",
    )

    query = _query(transport)
    assert _path(transport) == "/fapi/v1/order"
    assert query["type"] == ["STOP_MARKET"]
    assert query["side"] == ["SELL"]
    assert query["closePosition"] == ["true"]
    assert query["stopPrice"] == ["78000"]
    assert query["workingType"] == ["MARK_PRICE"]
    assert "quantity" not in query
    assert "reduceOnly" not in query


def test_close_position_rejects_a_quantity_and_reduce_only():
    transport = FakeTransport([])
    client = _client(transport)

    with pytest.raises(OrderValidationError, match="must not set quantity"):
        client.new_order(
            symbol="BTCUSDT",
            side="SELL",
            order_type="STOP_MARKET",
            quantity="0.002",
            stop_price="78000",
            close_position=True,
        )
    with pytest.raises(OrderValidationError, match="mutually exclusive"):
        client.new_order(
            symbol="BTCUSDT",
            side="SELL",
            order_type="STOP_MARKET",
            stop_price="78000",
            close_position=True,
            reduce_only=True,
        )

    assert transport.requests == []


def test_stop_price_must_be_positive():
    with pytest.raises(OrderValidationError, match="stop_price"):
        _client(FakeTransport([])).stop_market_close_position(
            symbol="BTCUSDT", side="SELL", stop_price="0"
        )


def test_close_position_market_sets_reduce_only():
    transport = FakeTransport([(200, {"orderId": 11, "status": "NEW"})])

    _client(transport).close_position_market(
        symbol="BTCUSDT", side="SELL", quantity=Decimal("0.002")
    )

    query = _query(transport)
    assert query["type"] == ["MARKET"]
    assert query["reduceOnly"] == ["true"]
    assert query["quantity"] == ["0.002"]


def test_flatten_uses_the_opposite_side_of_the_position():
    transport = FakeTransport(
        [(200, _account_payload()), (200, {"orderId": 12, "status": "NEW"})]
    )

    _client(transport).flatten("btcusdt")

    assert _path(transport, 0) == "/fapi/v3/account"
    assert _path(transport, 1) == "/fapi/v1/order"
    query = _query(transport, 1)
    assert query["side"] == ["SELL"]
    assert query["reduceOnly"] == ["true"]
    assert query["quantity"] == ["0.002"]


def test_flatten_refuses_when_the_position_is_flat():
    transport = FakeTransport([(200, _account_payload(amount="0"))])

    with pytest.raises(OrderValidationError, match="nothing to flatten"):
        _client(transport).flatten("BTCUSDT")

    assert len(transport.requests) == 1


# --------------------------------------------------------------------------- #
# Account setup and reconciliation calls
# --------------------------------------------------------------------------- #
def test_set_leverage_posts_an_integer():
    transport = FakeTransport(
        [(200, {"symbol": "BTCUSDT", "leverage": 3, "maxNotionalValue": "1000000"})]
    )

    _client(transport).set_leverage("btcusdt", 3)

    assert _path(transport) == "/fapi/v1/leverage"
    query = _query(transport)
    assert query["symbol"] == ["BTCUSDT"]
    assert query["leverage"] == ["3"]


@pytest.mark.parametrize("value", ["0", "126", "2.5", "abc"])
def test_set_leverage_rejects_invalid_values(value):
    transport = FakeTransport([])

    with pytest.raises(OrderValidationError, match="leverage"):
        _client(transport).set_leverage("BTCUSDT", value)

    assert transport.requests == []


def test_set_margin_mode_posts_isolated():
    transport = FakeTransport([(200, {"code": 200, "msg": "success"})])

    _client(transport).set_margin_mode("btcusdt", "isolated")

    assert _path(transport) == "/fapi/v1/marginType"
    assert _query(transport)["marginType"] == ["ISOLATED"]


def test_set_margin_mode_treats_an_unchanged_mode_as_success(caplog):
    caplog.set_level(logging.INFO)
    transport = FakeTransport(
        [(400, {"code": -4046, "msg": "No need to change margin type."})]
    )

    result = _client(transport).set_margin_mode("BTCUSDT", "ISOLATED")

    assert result["code"] == -4046
    assert "already on ISOLATED margin" in caplog.text
    assert caplog.records[-1].levelno < 30


def test_set_margin_mode_propagates_other_errors():
    transport = FakeTransport(
        [(400, {"code": -4168, "msg": "Margin type cannot be changed"})]
    )

    with pytest.raises(BinanceAPIError, match="Margin type"):
        _client(transport).set_margin_mode("BTCUSDT", "CROSSED")


def test_set_margin_mode_rejects_an_unknown_mode():
    transport = FakeTransport([])

    with pytest.raises(OrderValidationError, match="margin mode"):
        _client(transport).set_margin_mode("BTCUSDT", "PORTFOLIO")

    assert transport.requests == []


def test_open_orders_requests_the_symbol_and_parses_records():
    transport = FakeTransport(
        [
            (
                200,
                [
                    {
                        "orderId": 4,
                        "symbol": "BTCUSDT",
                        "side": "SELL",
                        "type": "LIMIT",
                        "price": "81000",
                        "origQty": "0.002",
                        "executedQty": "0",
                        "status": "NEW",
                    }
                ],
            )
        ]
    )

    orders = _client(transport).open_orders("btcusdt")

    assert _path(transport) == "/fapi/v1/openOrders"
    assert _query(transport)["symbol"] == ["BTCUSDT"]
    assert len(orders) == 1
    assert orders[0].side == "SELL"
    assert orders[0].status == "NEW"
    assert orders[0].remaining_quantity == Decimal("0.002")


def test_positions_filters_to_one_symbol():
    transport = FakeTransport([(200, _account_payload())])

    positions = _client(transport).positions("btcusdt")

    assert [position.symbol for position in positions] == ["BTCUSDT"]
    assert _path(transport) == "/fapi/v3/account"
    assert len(transport.requests) == 1  # no mark-price request unless asked for


def test_positions_fills_mark_prices_from_premium_index():
    transport = FakeTransport(
        [
            (200, _account_payload()),
            (
                200,
                {"symbol": "BTCUSDT", "markPrice": "81000.0", "indexPrice": "80990.0"},
            ),
        ]
    )

    positions = _client(transport).positions("btcusdt", with_mark_price=True)

    assert _path(transport, 1) == "/fapi/v1/premiumIndex"
    assert _query(transport, 1)["symbol"] == ["BTCUSDT"]
    btc = positions[0]
    assert btc.mark_price == Decimal("81000.0")
    assert btc.notional == Decimal("162.000")


def test_positions_fills_every_mark_price_in_one_request():
    payload = _account_payload()
    payload["positions"][1]["positionAmt"] = "-1"
    payload["positions"][1]["entryPrice"] = "3000"
    transport = FakeTransport(
        [
            (200, payload),
            (200, [{"symbol": "BTCUSDT", "markPrice": "81000"}]),
        ]
    )

    positions = _client(transport).positions(with_mark_price=True)

    assert "symbol" not in _query(transport, 1)  # one request covers all symbols
    assert [item.symbol for item in positions] == ["BTCUSDT", "ETHUSDT"]
    assert positions[0].mark_price == Decimal("81000")
    assert positions[1].mark_price == Decimal("0")  # unresolved: keeps payload value
    assert positions[1].notional == Decimal("3000")


@pytest.mark.parametrize("status", [429, 418])
def test_rate_limit_statuses_get_a_dedicated_error(status):
    transport = FakeTransport([(status, {"code": -1003, "msg": "Too many requests"})])

    with pytest.raises(BinanceRateLimitError) as caught:
        _client(transport).ping()

    assert caught.value.status == status
    assert caught.value.is_ban is (status == 418)
    assert isinstance(caught.value, BinanceAPIError)
