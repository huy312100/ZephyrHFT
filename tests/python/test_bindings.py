"""Integration tests for the pybind11 module and Python layer."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from python_analytics.backtesting import replay_lines
from python_analytics.native import hft_native
from python_analytics.native.factory import (
    build_engine,
    build_risk_manager,
    engine_config,
    pretrade_config,
    risk_limits,
)
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
    assert hft_native.__version__ == "0.2.0"
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


def test_risk_manager_guards() -> None:
    h = hft_native
    config = h.RiskManagerConfig()
    config.enabled = True
    config.max_position = 10.0
    config.max_order_size = 5.0
    config.max_price_deviation_pct = 0.01
    config.order_rate_limit = 0.0  # guard tests opt out of rate limiting
    config.order_burst = 0.0

    risk = h.RiskManager(config)
    assert "max_position=10.000000" in repr(risk.config())

    def check(price, quantity, side, position, mid):
        return risk.validate_order("BTCUSDT", price, quantity, side, position, mid)

    buy, sell = h.Side.BUY, h.Side.SELL
    assert check(30_000.0, 1.0, buy, 0.0, 30_000.0) == h.RiskStatus.APPROVED
    assert check(30_000.0, 6.0, buy, 0.0, 30_000.0) == h.RiskStatus.REJECTED_MAX_ORDER_SIZE
    assert check(30_000.0, 1.0, buy, 9.5, 30_000.0) == h.RiskStatus.REJECTED_POSITION_LIMIT
    assert check(30_000.0, 1.0, sell, -9.5, 30_000.0) == h.RiskStatus.REJECTED_POSITION_LIMIT
    assert check(30_000.0, 4.0, sell, 9.0, 30_000.0) == h.RiskStatus.APPROVED
    assert check(31_000.0, 1.0, buy, 0.0, 30_000.0) == h.RiskStatus.REJECTED_FAT_FINGER
    assert check(30_000.0, 1.0, buy, 0.0, 0.0) == h.RiskStatus.REJECTED_FAT_FINGER
    assert check(0.0, 1.0, buy, 0.0, 30_000.0) == h.RiskStatus.REJECTED_INVALID_ORDER
    assert check(float("nan"), 1.0, buy, 0.0, 30_000.0) == h.RiskStatus.REJECTED_INVALID_ORDER

    # Status helpers are exposed to Python for logging and metrics.
    assert h.risk_status_name(h.RiskStatus.REJECTED_FAT_FINGER) == "REJECTED_FAT_FINGER"
    assert h.is_approved(h.RiskStatus.APPROVED)
    assert not h.is_approved(h.RiskStatus.REJECTED_RATE_LIMIT)

    # Clock-free snapshots use the steady clock and render readably.
    snapshot = risk.stats()
    assert isinstance(snapshot, h.RiskManagerStats)
    assert snapshot.evaluated == 9 and snapshot.approved == 2 and snapshot.rejected == 7
    assert "evaluated=9" in repr(snapshot)
    assert risk.tokens_available() == pytest.approx(0.0)

    # Limits are tunable at runtime and reflected by config().
    risk.max_position = 25.0
    risk.max_order_size = 2.0
    risk.max_price_deviation_pct = 0.05
    assert risk.max_position == 25.0 and risk.config().max_order_size == 2.0
    assert check(31_000.0, 1.0, buy, 0.0, 30_000.0) == h.RiskStatus.APPROVED

    # The disabled config approves even absurd orders.
    dormant = h.RiskManager(h.RiskManagerConfig.disabled())
    assert not dormant.enabled
    assert dormant.validate_order("BTCUSDT", 1.0, 1e9, buy, 1e9, 0.0) == h.RiskStatus.APPROVED
    assert dormant.stats(0).evaluated == 0


def test_risk_manager_rate_limit_and_stats() -> None:
    h = hft_native
    config = h.RiskManagerConfig()
    config.max_position = 0.0
    config.max_order_size = 0.0
    config.max_price_deviation_pct = 0.0
    config.order_rate_limit = 50.0
    config.order_burst = 5.0

    risk = h.RiskManager(config)
    risk.reset(1_000_000_000)
    statuses = [
        risk.validate_order_at(1_000_000_000, "BTCUSDT", 30_000.0, 0.1, h.Side.BUY, 0.0, 30_000.0)
        for _ in range(6)
    ]
    assert statuses[:5] == [h.RiskStatus.APPROVED] * 5
    assert statuses[5] == h.RiskStatus.REJECTED_RATE_LIMIT
    # 50 orders/s => one token per 20 ms.
    assert risk.validate_order_at(
        1_020_000_000, "BTCUSDT", 30_000.0, 0.1, h.Side.BUY, 0.0, 30_000.0
    ) == h.RiskStatus.APPROVED
    assert risk.tokens_available(1_020_000_000) == pytest.approx(0.0)
    # A full second of idleness refills to the burst capacity, never beyond.
    assert risk.tokens_available(2_000_000_000) == pytest.approx(5.0)
    assert risk.rate_limiter().rate_per_sec() == pytest.approx(50.0)

    stats = risk.stats(1_020_000_000)
    assert stats.evaluated == 7
    assert stats.approved == 6
    assert stats.rejected == 1
    assert stats.count(h.RiskStatus.REJECTED_RATE_LIMIT) == 1
    assert stats.by_status[h.RiskStatus.APPROVED.value] == 6


def test_pretrade_settings_wire_into_engine_config() -> None:
    settings = load_settings()
    config = pretrade_config(settings)
    assert config.enabled is False  # replay-safe default
    assert config.max_position == pytest.approx(10.0)
    assert config.order_rate_limit == pytest.approx(50.0)
    assert config.order_burst == pytest.approx(20.0)

    assert build_risk_manager(settings).enabled is False
    engine = build_engine(settings)
    assert engine.risk_manager_config().enabled is False
    assert engine.risk_manager().enabled is False
    assert engine.risk_stats().evaluated == 0

    # Live tuning through the engine handle reaches the same object.
    engine.risk_manager().max_order_size = 0.25
    assert engine.risk_manager_config().max_order_size == pytest.approx(0.25)
