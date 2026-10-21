"""Unit tests for the local paper-trading matching engine and market feed."""
from __future__ import annotations

import asyncio

import pytest

from python_analytics.native import hft_native
from python_analytics.simulator import (
    MarketDataFeed,
    PaperMatchingEngine,
    parse_depth_message,
)


def make_book() -> "hft_native.L2Book":
    return hft_native.L2Book(
        hft_native.Price.from_string("0.5"),
        hft_native.Price.from_string("100"),
    )


def make_pair(**kwargs):
    book = make_book()
    engine = PaperMatchingEngine(book, **kwargs)
    return book, engine


def set_bbo(book: "hft_native.L2Book", bid: float, bid_qty: float,
            ask: float, ask_qty: float) -> None:
    book.clear()
    book.apply_level(hft_native.Side.BUY, hft_native.Price.from_string(str(bid)),
                     hft_native.Quantity.from_string(str(bid_qty)), 0)
    book.apply_level(hft_native.Side.SELL, hft_native.Price.from_string(str(ask)),
                     hft_native.Quantity.from_string(str(ask_qty)), 0)


def place(engine: PaperMatchingEngine, side: str, price: float,
          quantity: float):
    request = hft_native.OrderRequest()
    request.side = hft_native.Side.BUY if side == "BUY" else hft_native.Side.SELL
    request.type = hft_native.OrderType.POST_ONLY
    request.price = hft_native.Price.from_string(str(price))
    request.quantity = hft_native.Quantity.from_string(str(quantity))
    return engine.submit(request)


def test_pretrade_risk_manager_rejects_orders() -> None:
    config = hft_native.RiskManagerConfig()
    config.enabled = True
    config.max_position = 1.0
    config.max_order_size = 0.5
    config.max_price_deviation_pct = 0.01
    config.order_rate_limit = 100.0
    config.order_burst = 100.0
    risk = hft_native.RiskManager(config)

    book, engine = make_pair(risk_manager=risk, symbol="TESTUSDT")
    set_bbo(book, 99.5, 5.0, 100.5, 5.0)  # mid == 100.0
    assert engine.risk_manager() is risk

    oversized = place(engine, "BUY", 99.5, 2.0)  # above max_order_size
    assert not oversized.accepted()
    assert oversized.reason == "REJECTED_MAX_ORDER_SIZE"
    assert engine.active_bid is None

    fat = place(engine, "BUY", 105.0, 0.1)  # 5% away from a 100.0 mid
    assert not fat.accepted()
    assert fat.reason == "REJECTED_FAT_FINGER"

    # Rejected orders must not mutate engine state.
    assert engine.inventory == 0.0
    assert engine.active_bid is None and engine.active_ask is None

    good = place(engine, "BUY", 99.5, 0.4)
    assert good.accepted() and good.order_id > 0
    assert engine.active_bid is not None

    # The gate can be detached (or swapped) at runtime.
    engine.set_risk_manager(None)
    assert engine.risk_manager() is None
    assert place(engine, "SELL", 100.5, 5.0).accepted()


def test_bid_fills_when_market_ask_crosses() -> None:
    book, engine = make_pair()
    set_bbo(book, 99.0, 5.0, 100.0, 5.0)

    ack = place(engine, "BUY", 100.0, 1.0)
    assert ack.accepted() and ack.order_id > 0

    fills = engine.on_market_tick()
    assert len(fills) == 1
    assert fills[0].side == "BUY"
    assert fills[0].price == pytest.approx(100.0)
    assert fills[0].quantity == pytest.approx(1.0)
    assert engine.inventory == pytest.approx(1.0)
    assert engine.active_bid is None  # fully filled


def test_ask_fills_when_market_bid_crosses() -> None:
    book, engine = make_pair()
    set_bbo(book, 100.0, 5.0, 101.0, 5.0)

    place(engine, "SELL", 99.5, 0.5)
    fills = engine.on_market_tick()

    assert len(fills) == 1
    assert fills[0].side == "SELL"
    assert fills[0].price == pytest.approx(99.5)
    assert engine.inventory == pytest.approx(-0.5)
    assert engine.active_ask is None


def test_partial_fill_uses_available_volume() -> None:
    book, engine = make_pair()
    set_bbo(book, 99.0, 5.0, 100.0, 0.3)  # only 0.3 available at the touch

    place(engine, "BUY", 100.5, 1.0)
    fills = engine.on_market_tick()

    assert len(fills) == 1
    assert fills[0].quantity == pytest.approx(0.3)
    assert engine.inventory == pytest.approx(0.3)
    assert engine.active_bid is not None
    assert engine.active_bid.remaining == pytest.approx(0.7)


def test_round_trip_pnl_and_maker_rebate() -> None:
    book, engine = make_pair(
        maker_fee=-0.0001, taker_fee=0.0005, initial_cash=100000.0,
    )

    # Buy 1.0 @ 100 (maker; 0.01% rebate received).
    set_bbo(book, 99.0, 5.0, 100.0, 5.0)
    place(engine, "BUY", 100.0, 1.0)
    engine.on_market_tick()

    assert engine.inventory == pytest.approx(1.0)
    assert engine.cash == pytest.approx(100000.0 - 100.0 + 0.01)
    assert engine.realized_pnl == pytest.approx(0.01)  # rebate only
    assert engine.unrealized_pnl == pytest.approx((99.5 - 100.0) * 1.0)

    # Sell 1.0 @ 101 (maker; rebate received).
    set_bbo(book, 101.0, 5.0, 102.0, 5.0)
    place(engine, "SELL", 101.0, 1.0)
    engine.on_market_tick()

    assert engine.inventory == pytest.approx(0.0)
    assert engine.cash == pytest.approx(100001.0201)
    assert engine.realized_pnl == pytest.approx(1.0201)
    assert engine.unrealized_pnl == pytest.approx(0.0)
    assert engine.total_pnl == pytest.approx(1.0201)
    assert engine.equity == pytest.approx(100001.0201)
    assert engine.total_pnl == pytest.approx(engine.equity - 100000.0)


def test_taker_fee_applied_on_taker_fill() -> None:
    book, engine = make_pair(
        maker_fee=-0.0001, taker_fee=0.0005, initial_cash=0.0,
    )
    set_bbo(book, 99.0, 5.0, 100.0, 5.0)

    fill = engine.execute_taker("BUY", 1.0)
    assert fill is not None and fill.is_maker is False
    assert fill.price == pytest.approx(100.0)
    assert fill.fee == pytest.approx(100.0 * 0.0005)
    assert engine.cash == pytest.approx(-100.0 - 0.05)
    assert engine.inventory == pytest.approx(1.0)


def test_parse_depth_message_handles_both_formats() -> None:
    diff = ('{"e":"depthUpdate","E":1700000000000,"U":1,"u":1,"s":"BTCUSDT",'
            '"b":[["29999.5","1.5"]],"a":[["30000.5","2.5"]]}')
    update = parse_depth_message(diff)
    assert update is not None
    assert len(update.bids) == 1 and len(update.asks) == 1

    partial = '{"lastUpdateId":5,"bids":[["100.0","2.0"]],"asks":[["101.0","3.0"]]}'
    partial_update = parse_depth_message(partial)
    assert partial_update is not None
    assert partial_update.final_update_id == 5
    assert len(partial_update.bids) == 1

    trade = '{"e":"trade","p":"100.0","q":"0.1","m":false}'
    assert parse_depth_message(trade) is None


def set_bbo_at(book: "hft_native.L2Book", bid: float, bid_qty: float,
               ask: float, ask_qty: float, exchange_ts: int,
               sequence: int) -> None:
    """Sets the touch together with a market timestamp/sequence."""
    book.clear()
    book.apply_level(hft_native.Side.BUY, hft_native.Price.from_string(str(bid)),
                     hft_native.Quantity.from_string(str(bid_qty)), exchange_ts)
    book.apply_level(hft_native.Side.SELL, hft_native.Price.from_string(str(ask)),
                     hft_native.Quantity.from_string(str(ask_qty)), exchange_ts)
    book.set_sequence(sequence, exchange_ts, exchange_ts)


def test_trades_history_records_timestamp_side_price_size() -> None:
    book, engine = make_pair()
    exchange_ts = 1_700_000_123_456
    set_bbo_at(book, 99.0, 5.0, 100.0, 5.0, exchange_ts, 11)

    place(engine, "BUY", 100.0, 1.0)
    engine.on_market_tick()

    history = engine.trades_history
    assert len(history) == 1
    trade = history[0]
    assert trade["timestamp"] == pytest.approx(exchange_ts / 1000.0)
    assert trade["exchange_ts"] == exchange_ts
    assert trade["sequence"] == 11
    assert trade["side"] == "BUY"
    assert trade["price"] == pytest.approx(100.0)
    assert trade["size"] == pytest.approx(1.0)
    # Bookkeeping mirrors the portfolio state after the fill.
    assert trade["inventory"] == pytest.approx(1.0)
    assert engine.last_market_ts == exchange_ts

    # The history is a defensive copy: mutating it must not corrupt the engine.
    history.clear()
    assert len(engine.trades_history) == 1


def test_no_fill_when_market_does_not_cross() -> None:
    """Regression: a quiet market must produce zero trades, not phantom fills."""
    book, engine = make_pair()
    set_bbo(book, 99.0, 5.0, 100.0, 5.0)

    place(engine, "BUY", 98.0, 1.0)
    place(engine, "SELL", 101.0, 1.0)

    for _ in range(10):
        assert engine.on_market_tick() == []

    assert engine.trades_history == []
    assert engine.inventory == pytest.approx(0.0)
    assert engine.active_bid is not None and engine.active_ask is not None


def test_self_trade_prevention_cancels_resting_opposite() -> None:
    """A quote that would cross our own resting quote retires the older one."""
    book, engine = make_pair()  # default: cancel_resting
    set_bbo(book, 99.0, 5.0, 100.0, 5.0)

    bid_ack = place(engine, "BUY", 100.2, 1.0)
    assert bid_ack.accepted()
    ask_ack = place(engine, "SELL", 99.8, 1.0)  # 99.8 <= 100.2 -> self-cross

    assert ask_ack.accepted()
    assert engine.active_bid is None          # stale bid retired
    assert engine.active_ask is not None
    assert engine.self_trade_prevented == 1

    # No fictitious round trip: the market never crossed the surviving ask.
    assert engine.on_market_tick() == []
    assert engine.trades_history == []


def test_self_trade_prevention_reject_mode() -> None:
    book, engine = make_pair(self_trade_prevention="reject")
    set_bbo(book, 99.0, 5.0, 100.0, 5.0)

    assert place(engine, "BUY", 100.2, 1.0).accepted()
    rejected = place(engine, "SELL", 99.8, 1.0)

    assert not rejected.accepted()
    assert rejected.reason == "SELF_TRADE_PREVENTED"
    assert engine.active_bid is not None      # original quote untouched
    assert engine.active_ask is None
    assert engine.self_trade_prevented == 1


def test_self_trade_prevention_none_allows_crossed_pair() -> None:
    book, engine = make_pair(self_trade_prevention="none")
    set_bbo(book, 100.0, 5.0, 100.0, 5.0)

    place(engine, "BUY", 100.2, 1.0)
    place(engine, "SELL", 99.8, 1.0)

    # Both quotes rest; without the guard a single tick would fill both.
    assert engine.active_bid is not None and engine.active_ask is not None
    assert engine.self_trade_prevented == 0


def test_invalid_self_trade_prevention_mode_rejected() -> None:
    with pytest.raises(ValueError):
        make_pair(self_trade_prevention="sometimes")


def test_partial_reduction_keeps_cost_basis() -> None:
    """Reducing a position must not overwrite the remaining cost basis."""
    book, engine = make_pair(initial_cash=100000.0)
    set_bbo(book, 99.0, 5.0, 100.0, 5.0)

    place(engine, "BUY", 100.0, 2.0)
    engine.on_market_tick()
    assert engine.inventory == pytest.approx(2.0)
    assert engine.position().average_entry_price == pytest.approx(100.0)

    # Sell half at a higher price: inventory shrinks, basis stays at 100.
    set_bbo(book, 101.0, 5.0, 102.0, 5.0)
    place(engine, "SELL", 101.0, 1.0)
    engine.on_market_tick()

    assert engine.inventory == pytest.approx(1.0)
    assert engine.position().average_entry_price == pytest.approx(100.0)
    assert engine.unrealized_pnl == pytest.approx((101.5 - 100.0) * 1.0)
    # Core portfolio identity: equity == initial cash + total PnL.
    assert engine.equity == pytest.approx(100000.0 + engine.total_pnl)


def test_flip_through_zero_resets_cost_basis() -> None:
    book, engine = make_pair(initial_cash=100000.0)
    set_bbo(book, 99.0, 5.0, 100.0, 5.0)

    place(engine, "BUY", 100.0, 1.0)
    engine.on_market_tick()
    assert engine.inventory == pytest.approx(1.0)

    # Sell 2: closes the long and opens a 1.0 short at 101.
    set_bbo(book, 101.0, 5.0, 102.0, 5.0)
    place(engine, "SELL", 101.0, 2.0)
    engine.on_market_tick()

    assert engine.inventory == pytest.approx(-1.0)
    assert engine.position().average_entry_price == pytest.approx(101.0)


def test_market_data_feed_replays_ndjson(tmp_path) -> None:
    path = tmp_path / "feed.ndjson"
    path.write_text(
        '{"e":"depthUpdate","E":1,"U":1,"u":1,"s":"BTCUSDT",'
        '"b":[["100.0","1.0"]],"a":[["101.0","1.0"]]}\n'
        "not-json\n"
        '{"e":"trade","E":2,"p":"100.5","q":"0.1","m":false}\n',
        encoding="utf-8",
    )
    book = make_book()
    seen = []

    def on_depth(update) -> None:
        seen.append(update.final_update_id)

    feed = MarketDataFeed(str(path), book, on_depth, loop=False, replay_delay=0.0)
    asyncio.run(feed.run())

    assert seen == [1]  # only the depth line triggers the hook
    bbo = book.top()
    assert bbo.is_valid()
    assert float(bbo.bid_price) == pytest.approx(100.0)
    assert float(bbo.ask_price) == pytest.approx(101.0)
    assert feed.malformed == 1  # the "not-json" line

