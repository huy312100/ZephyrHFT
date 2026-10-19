"""Integration tests for the pybind11 module and Python layer."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from python_analytics.backtesting import replay_lines
from python_analytics.native import hft_native
from python_analytics.native.factory import build_engine, engine_config, risk_limits
from python_analytics.strategies import InventoryMarketMaker
from python_analytics.utils import load_settings
from python_analytics.utils.config import ConfigError

DEPTH_JSON = (
    '{"e":"depthUpdate","E":1700000000000,"U":1,"u":1,"s":"BTCUSDT",'
    '"b":[["29999.50","1.5"],["29999.00","2.0"]],'
    '"a":[["30000.50","2.5"],["30001.00","1.0"]]}'
)
TRADE_JSON = (
    '{"e":"trade","E":1700000000200,"T":1700000000201,'
    '"p":"30000.25","q":"0.10","m":false}'
)


def test_fixed_point_and_buffers() -> None:
    assert hft_native.__version__ == "0.1.0"
    price = hft_native.Price.from_string("29999.50")
    assert price is not None and str(price) == "29999.50000000"
    assert hft_native.ring_buffer_smoke(20_000)


def test_json_and_websocket_helpers() -> None:
    update = hft_native.parse_depth_json(DEPTH_JSON)
    assert len(update.bids) == 2 and len(update.asks) == 2
    assert hft_native.classify_json(DEPTH_JSON) == hft_native.FeedMessageType.DEPTH_UPDATE
    trade = hft_native.parse_trade_json(TRADE_JSON)
    assert trade.is_buyer_maker == 0 and trade.aggressor == hft_native.Side.BUY
    trade.recv_ts = 1700000000202
    assert trade.recv_ts == 1700000000202

    payload = b"hello"
    mask = b"\x01\x02\x03\x04"
    masked = bytes(value ^ mask[index % 4] for index, value in enumerate(payload))
    consumed, opcode, decoded, fin, was_masked = hft_native.decode_ws_frame(
        bytes([0x81, 0x80 | len(payload)]) + mask + masked
    )
    assert consumed == 11 and decoded == payload and fin and was_masked


def test_engine_flow() -> None:
    settings = load_settings()
    engine = build_engine(settings)
    update = hft_native.parse_depth_json(DEPTH_JSON)
    engine.apply_depth(update)
    assert engine.bbo().bid_price == hft_native.Price.from_string("29999.5")
    assert engine.bbo().spread() == hft_native.Price.from_string("1.0")
    assert engine.bbo().spread_bps() == pytest.approx(10_000.0 / 30_000.0)
    assert engine.bbo().exchange_ts == 1_700_000_000_000
    assert engine.bbo().sequence == 1

    request = hft_native.OrderRequest()
    request.side = hft_native.Side.BUY
    request.type = hft_native.OrderType.MARKET
    request.quantity = hft_native.Quantity.from_string("0.5")
    ack = engine.submit(request)
    assert ack.accepted() and ack.filled == hft_native.Quantity.from_string("0.5")
    assert engine.position().net_quantity == hft_native.Quantity.from_string("0.5")
    assert engine.position().average_entry_price == hft_native.Price.from_string("30000.5")
    assert engine.position().unrealized_pnl == hft_native.Notional.from_string("-0.25")

    sell = hft_native.OrderRequest()
    sell.side = hft_native.Side.SELL
    sell.type = hft_native.OrderType.MARKET
    sell.quantity = hft_native.Quantity.from_string("0.5")
    sell_ack = engine.submit(sell)
    assert sell_ack.accepted()
    assert engine.position().net_quantity.raw() == 0
    assert engine.position().realized_pnl == hft_native.Notional.from_string("-0.5")
    assert engine.position().unrealized_pnl.raw() == 0

    fill = engine.drain_fill()
    assert fill is not None and fill.order_id == ack.order_id
    second = engine.drain_fill()
    assert second is not None and second.order_id == sell_ack.order_id
    assert engine.drain_fill() is None


def test_strategy_quotes_and_cancels() -> None:
    settings = load_settings()
    engine = build_engine(settings)
    update = hft_native.parse_depth_json(DEPTH_JSON)
    engine.apply_depth(update)

    strategy = InventoryMarketMaker(settings.strategy)
    strategy.on_depth(engine, update)
    assert engine.metrics().open_orders == 2
    quote = strategy.quote(engine)
    assert quote is not None and quote.is_valid()
    tick = engine.config().tick_size.raw()
    center = engine.config().center.raw()
    assert (quote.bid_price_raw - center) % tick == 0
    assert (quote.ask_price_raw - center) % tick == 0

    strategy.cancel_all(engine)
    assert engine.metrics().open_orders == 0


def test_replay_pipeline() -> None:
    settings = load_settings()
    result = replay_lines([DEPTH_JSON, TRADE_JSON], settings)
    assert result.stats["depth_events"] == 1
    assert result.stats["trade_events"] == 1
    assert result.stats["book_updates"] >= 1
    assert result.stats["malformed_events"] == 0
    assert result.stats["bbo"]["valid"] is True
    json.dumps(result.as_dict())  # must stay serialisable


def test_config_validation(tmp_path: Path) -> None:
    missing = tmp_path / "missing.yaml"
    with pytest.raises(ConfigError):
        load_settings(missing)

    bad = tmp_path / "bad.yaml"
    bad.write_text("engine:\n  tick_size: nope\n", encoding="utf-8")
    with pytest.raises(ConfigError):
        load_settings(bad)

    config = engine_config(load_settings())
    limits = risk_limits(load_settings())
    assert config.tick_size.raw() > 0 and limits.max_open_orders > 0
