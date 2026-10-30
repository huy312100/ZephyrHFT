"""Maker fill models: what earns a fill, and what must not.

The three models are not interchangeable. ``touch`` is the legacy book-crossing
model, ``queue`` infers trades from depth depletion, and ``trade`` consumes the
aggregated tape. These tests pin the semantics that make the difference legible:
a quote that merely rests is not handed a fill, queue priority is respected, and
a depth-only archive cannot manufacture fills under the tape model.

The last section pins the *price window*: a ladder anchored on a stale constant
reports an empty book for a live feed, which reads like a quiet market and can
quietly invalidate an entire study.
"""
from __future__ import annotations

import pytest

from python_analytics.native import hft_native as n


def _depth(ts: int, bids, asks):
    body = ",".join('["%s","%s"]' % (price, qty) for price, qty in bids)
    body_a = ",".join('["%s","%s"]' % (price, qty) for price, qty in asks)
    line = (
        '{"e":"depthUpdate","E":%d,"U":1,"u":1,"s":"BTCUSDT","b":[%s],"a":[%s]}'
        % (ts, body, body_a)
    )
    return n.parse_depth_json(line)


def _trade(ts: int, price: str, qty: str, *, buyer_is_maker: bool = True):
    line = '{"e":"aggTrade","p":"%s","q":"%s","T":%d,"m":%s}' % (
        price, qty, ts, "true" if buyer_is_maker else "false")
    return n.parse_trade_json(line)


def _engine(model):
    return n.PaperEngine(
        n.Price.from_string("0.5"),
        n.Price.from_string("30000"),
        2.0,
        5.0,
        n.Notional.from_string("100000"),
        model,
    )


_INITIAL_CASH = 100000.0


def _post(engine, side, price: str, quantity: str, ts: int):
    request = n.OrderRequest()
    request.side = side
    request.type = n.OrderType.POST_ONLY
    request.price = n.Price.from_string(price)
    request.quantity = n.Quantity.from_string(quantity)
    request.timestamp = ts
    return engine.submit(request)


def _drain(engine):
    fills = []
    while True:
        fill = engine.drain_fill()
        if fill is None:
            return fills
        fills.append((str(fill.price), str(fill.quantity), int(fill.is_maker)))


def test_touch_fills_full_size_when_the_ask_reaches_the_bid():
    engine = _engine(n.FillModel.TOUCH)
    engine.process_update(_depth(1000, [("30000", "1.0")], [("30000.5", "1.0")]))
    ack = _post(engine, n.Side.BUY, "30000", "0.5", 1000)
    assert ack.accepted()
    assert _drain(engine) == []

    engine.process_update(_depth(1100, [("30000", "1.0")], [("30000", "1.0")]))
    fills = _drain(engine)
    # The legacy model hands over the whole size at the first cross, without
    # any evidence that someone actually traded at this price.
    assert len(fills) == 1
    assert fills[0][1] == "0.50000000"
    assert fills[0][2] == 1


def test_trade_model_requires_the_displayed_queue_to_be_consumed_first():
    engine = _engine(n.FillModel.TRADE)
    engine.process_update(_depth(1000, [("30000", "1.0")], [("30000.5", "1.0")]))
    assert _post(engine, n.Side.BUY, "30000", "0.5", 1000).accepted()

    # A seller lifts 0.4 into a 1.0 queue: we are behind it and stay unfilled.
    engine.process_trade(_trade(1100, "30000", "0.4"))
    assert _drain(engine) == []

    # 0.6 more exactly clears the queue. Back of the queue means nothing left.
    engine.process_trade(_trade(1200, "30000", "0.6"))
    assert _drain(engine) == []

    # The next print is ours, and only the printed size fills.
    engine.process_trade(_trade(1300, "30000", "0.2"))
    fills = _drain(engine)
    assert len(fills) == 1
    assert fills[0][1] == "0.20000000"

    # A print through our price completes the order.
    engine.process_trade(_trade(1400, "29999.5", "1.0"))
    fills = _drain(engine)
    assert len(fills) == 1
    assert fills[0][1] == "0.30000000"


def test_trade_model_ignores_prints_that_cannot_reach_the_order():
    engine = _engine(n.FillModel.TRADE)
    # A resting bid well below the touch keeps the book valid while our quote
    # sits inside the spread at a price with no displayed depth.
    engine.process_update(_depth(1000, [("29990", "1.0")], [("30000.5", "1.0")]))
    assert _post(engine, n.Side.BUY, "30000", "0.5", 1000).accepted()

    # A seller printing above our bid never trades with us.
    engine.process_trade(_trade(1100, "30000.5", "5.0"))
    assert _drain(engine) == []

    # A seller printing at our bid does, and fills the whole quote.
    engine.process_trade(_trade(1200, "30000", "0.5"))
    fills = _drain(engine)
    assert len(fills) == 1
    assert fills[0][1] == "0.50000000"


def test_trade_model_allocates_a_print_by_price_priority():
    engine = _engine(n.FillModel.TRADE)
    engine.process_update(
        _depth(1000, [("29990", "1.0")], [("30000.5", "1.0")]))
    assert _post(engine, n.Side.BUY, "29999.5", "0.5", 1000).accepted()
    assert _post(engine, n.Side.BUY, "30000", "0.5", 1000).accepted()

    # 0.5 trades at 30000: the better-priced bid takes it, the far one gets none.
    engine.process_trade(_trade(1100, "30000", "0.5"))
    fills = _drain(engine)
    assert fills == [("30000.00000000", "0.50000000", 1)]

    # A sweep to 29999.5 fills the remaining order at its own price.
    engine.process_trade(_trade(1200, "29999.5", "0.5"))
    fills = _drain(engine)
    assert fills == [("29999.50000000", "0.50000000", 1)]


def test_depth_only_archive_cannot_fabricate_tape_fills():
    engine = _engine(n.FillModel.TRADE)
    engine.process_update(_depth(1000, [("30000", "1.0")], [("30000.5", "1.0")]))
    assert _post(engine, n.Side.BUY, "30000", "0.5", 1000).accepted()

    # The touch crosses our bid on every frame, which the legacy model treats as
    # a fill. With no tape there is no evidence anyone traded with us.
    for ts in range(1100, 2000, 100):
        engine.process_update(_depth(ts, [("30000", "1.0")], [("30000", "1.0")]))

    metrics = engine.metrics()
    assert metrics.fills == 0
    assert metrics.trade_events == 0


def test_queue_model_fills_once_depth_at_the_price_is_consumed():
    engine = _engine(n.FillModel.QUEUE)
    # A second bid keeps the book two-sided after 30000 is swept, so the last
    # frame reaches ``match_resting_queue`` instead of returning at the BBO.
    engine.process_update(
        _depth(1000, [("30000", "1.0"), ("29999.5", "1.0")], [("30000.5", "1.0")]))
    assert _post(engine, n.Side.BUY, "30000", "0.5", 1000).accepted()

    # Partial depletion only advances the queue: 0.4 of the 1.0 ahead of us.
    engine.process_update(
        _depth(1100, [("30000", "0.6"), ("29999.5", "1.0")], [("30000.5", "1.0")]))
    assert _drain(engine) == []

    # Depth is re-added: nothing traded, so the queue ahead of us is unchanged.
    engine.process_update(
        _depth(1200, [("30000", "1.0"), ("29999.5", "1.0")], [("30000.5", "1.0")]))
    assert _drain(engine) == []

    # The level is swept: 1.0 traded clears the 0.6 still ahead of us and the
    # remaining 0.4 reaches the order. Nothing behind it is left over.
    engine.process_update(
        _depth(1300, [("30000", "0.0"), ("29999.5", "1.0")], [("30000.5", "1.0")]))
    assert _drain(engine) == [("30000.00000000", "0.40000000", 1)]


def test_a_re_anchor_does_not_replay_a_dropped_level_as_a_fill():
    """A level dropped by the re-anchor is not evidence that anything traded."""
    engine = _engine(n.FillModel.QUEUE)
    engine.process_update(_depth(1000, [("30000", "10.0")], [("30000.5", "1.0")]))
    assert _post(engine, n.Side.BUY, "30000", "0.5", 1000).accepted()

    # A print consumes half of the queue ahead of the order without reaching it.
    engine.process_trade(_trade(1050, "30000", "5.0"))
    assert _drain(engine) == []

    # The market gaps past the window. The re-anchor drops the level the order
    # sits on, and the queue model reads a fall in displayed size as traded
    # volume -- so the drop must not be replayed as a fill at a price the book
    # no longer holds.
    engine.process_update(_depth(1100, [("33000", "1.0")], [("33000.5", "1.0")]))
    assert engine.metrics().book_recenters == 1
    assert _drain(engine) == []
    assert float(engine.account().realized_pnl()) == pytest.approx(0.0)


def test_fill_model_survives_reset_and_is_reported():
    engine = _engine(n.FillModel.QUEUE)
    assert engine.fill_model() == n.FillModel.QUEUE
    engine.reset()
    assert engine.fill_model() == n.FillModel.QUEUE
    engine.set_fill_model(n.FillModel.TRADE)
    assert engine.fill_model() == n.FillModel.TRADE


@pytest.mark.parametrize("model", ["TOUCH", "QUEUE", "TRADE"])
def test_ledger_stays_coherent_under_every_model(model):
    engine = _engine(getattr(n.FillModel, model))
    engine.process_update(_depth(1000, [("30000", "1.0")], [("30000.5", "1.0")]))
    assert _post(engine, n.Side.BUY, "30000", "0.5", 1000).accepted()
    for ts in range(1100, 1600, 100):
        engine.process_update(_depth(ts, [("30000", "1.0")], [("30000", "1.0")]))
        engine.process_trade(_trade(ts, "30000", "0.2"))
    account = engine.account()
    # The gate's definition: equity change minus what the ledger says it earned.
    residual = (float(account.equity()) - _INITIAL_CASH
                - float(account.realized_pnl())
                - float(account.unrealized_pnl()))
    assert abs(residual) < 1e-6


# --------------------------------------------------------------------------- #
# The price window must follow the market
# --------------------------------------------------------------------------- #
def test_a_stale_anchor_is_moved_onto_the_feed():
    """A ladder pinned to a constant is blind; the feed's touch re-anchors it."""
    engine = _engine(n.FillModel.TOUCH)
    # The engine was built for 30000; the live market is at 81200 now.
    engine.process_update(_depth(1000, [("81199.5", "1.5")], [("81200.0", "2.0")]))

    bbo = engine.bbo()
    assert float(bbo.bid_price) == pytest.approx(81199.5)
    assert float(bbo.ask_price) == pytest.approx(81200.0)
    metrics = engine.metrics()
    assert metrics.book_recenters == 1
    assert metrics.rejected_levels == 0


def test_reframing_settles_instead_of_thrashing():
    engine = _engine(n.FillModel.TOUCH)
    for ts in range(1000, 2000, 100):
        engine.process_update(_depth(ts, [("81199.5", "1.5")], [("81200.0", "2.0")]))
    assert engine.metrics().book_recenters == 1


def test_a_populated_book_re_anchors_when_the_feed_leaves_the_window():
    """A book that holds levels is not safe: it can still go stale."""
    engine = _engine(n.FillModel.TOUCH)
    engine.process_update(_depth(1000, [("30000", "1.0")], [("30000.5", "1.0")]))
    assert engine.metrics().book_recenters == 0

    # The market gaps 3000 USDT, past the window's 2048. No level of this frame
    # fits. The ladder used to stay put and keep publishing 30000/30000.5 as a
    # valid BBO while rejecting every level of the live feed.
    engine.process_update(_depth(1100, [("33000", "1.0")], [("33000.5", "1.0")]))
    metrics = engine.metrics()
    assert metrics.book_recenters == 1
    assert metrics.rejected_levels == 0  # the frame landed, it was not refused
    bbo = engine.bbo()
    assert float(bbo.bid_price) == pytest.approx(33000.0)
    assert float(bbo.ask_price) == pytest.approx(33000.5)


def test_re_anchoring_keeps_the_levels_the_new_window_still_holds():
    engine = _engine(n.FillModel.TOUCH)
    engine.process_update(
        _depth(1000, [("31000", "2.0"), ("29900", "3.0")], [("31000.5", "1.0")]))

    # A one-sided frame 3000 USDT away re-anchors the window on 33000.5, so its
    # span covers 30952.5 to 35048.5. The bid at 31000 and the ask at 31000.5
    # still fit and are carried across; 29900 does not and is dropped.
    engine.process_update(_depth(1100, [], [("33000.5", "1.0")]))
    assert float(engine.center()) == pytest.approx(33000.5)

    bbo = engine.bbo()
    assert bbo.is_valid()
    assert float(bbo.bid_price) == pytest.approx(31000.0)
    # Retained makes it the *best* ask: the frame's 33000.5 is now the far one.
    assert float(bbo.ask_price) == pytest.approx(31000.5)
    assert engine.metrics().unpriced_frames == 0


def test_one_implausible_level_does_not_drag_the_anchor():
    """A stray price is a bad print, not evidence that the market has moved."""
    engine = _engine(n.FillModel.TOUCH)
    engine.process_update(_depth(1000, [("30000", "1.0")], [("30000.5", "1.0")]))

    # The touch is 1000 -- far outside the window -- but every other level of the
    # frame fits. Chasing it would move the anchor 29,000 away and lose the book
    # for frames the market never actually left.
    engine.process_update(_depth(
        1100, [("30000", "1.0"), ("29999.5", "1.0")], [("1000", "1.0")]))
    metrics = engine.metrics()
    assert metrics.book_recenters == 0
    assert float(engine.center()) == pytest.approx(30000.0)
    assert metrics.rejected_levels == 1  # the bad level is reported, not chased
    assert float(engine.bbo().bid_price) == pytest.approx(30000.0)


def test_an_even_split_frame_keeps_the_populated_book():
    """One fitting level and one stray is not proof that the market has moved."""
    engine = _engine(n.FillModel.TOUCH)
    engine.process_update(_depth(1000, [("30000", "2.0")], [("30000.5", "1.0")]))

    # Half of the frame fits and half does not: the tie must not move the anchor.
    # Chasing it would re-anchor on the midpoint of the good bid and the stray
    # ask, which drops the bid, the ask and every other level the book holds.
    engine.process_update(_depth(1100, [("30000", "2.0")], [("40000", "1.0")]))
    metrics = engine.metrics()
    assert metrics.book_recenters == 0
    assert float(engine.center()) == pytest.approx(30000.0)
    assert metrics.rejected_levels == 1
    assert float(engine.bbo().bid_price) == pytest.approx(30000.0)
    assert float(engine.bbo().ask_price) == pytest.approx(30000.5)


def test_a_wrong_tick_is_reported_instead_of_hidden():
    """The anchor may follow the feed; the tick must never be guessed."""
    engine = _engine(n.FillModel.TOUCH)  # tick = 0.5
    # Venue prices sit on a 0.1 grid, so no anchor holds both at a 0.5 tick.
    engine.process_update(_depth(1000, [("81199.9", "1.5")], [("81200.2", "2.0")]))

    assert not engine.bbo().is_valid()
    metrics = engine.metrics()
    assert metrics.rejected_levels == 2
    assert metrics.book_recenters == 1  # anchored once, then left alone


def test_reset_restores_the_configured_anchor():
    engine = _engine(n.FillModel.TOUCH)
    engine.process_update(_depth(1000, [("81199.5", "1.5")], [("81200.0", "2.0")]))
    # The anchor is the frame's mid (81199.75) floored onto the 0.5 grid.
    assert float(engine.center()) == pytest.approx(81199.5)

    engine.reset()
    assert float(engine.center()) == pytest.approx(30000.0)
    assert not engine.bbo().is_valid()
    assert engine.metrics().book_recenters == 0


def test_the_live_engine_re_anchors_too():
    """The paper engine is not the only one quoting against the ladder."""
    engine = n.TradingEngine()
    engine.apply_depth(_depth(1000, [("81199.5", "1.5")], [("81200.0", "2.0")]))

    bbo = engine.book().top()
    assert float(bbo.bid_price) == pytest.approx(81199.5)
    metrics = engine.metrics()
    assert metrics.book_recenters == 1
    assert metrics.rejected_levels == 0
