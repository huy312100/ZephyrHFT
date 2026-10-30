"""Unit tests for the Avellaneda-Stoikov strategy and analytics layer."""
from __future__ import annotations

import math

import pytest

from python_analytics.native import hft_native
from python_analytics.native.factory import build_engine
from python_analytics.strategies import (
    AvellanedaStoikovStrategy,
    MarketConfig,
    MarketType,
    MicroPriceCalculator,
)
from python_analytics.utils import load_settings

DEPTH_JSON = (
    '{"e":"depthUpdate","E":1700000000000,"U":1,"u":1,"s":"BTCUSDT",'
    '"b":[["29999.50","1.5"],["29999.00","2.0"]],'
    '"a":[["30000.50","2.5"],["30001.00","1.0"]]}'
)


def _config(**overrides) -> MarketConfig:
    params = {
        "gamma": 0.1,
        "sigma": 0.1,
        "k": 1.5,
        "inventory_limit": 5.0,
        "tick_size": 0.01,
        "market_type": MarketType.STOCK,
        "time_horizon": 100.0,
    }
    params.update(overrides)
    return MarketConfig(**params)


def test_reservation_price_shifts_down_for_long_inventory() -> None:
    """A long position (q > 0) lowers the reservation price for aggressive selling."""
    strategy = AvellanedaStoikovStrategy(_config())
    flat = strategy.reservation_price(100.0, 0.0, 50.0)
    long = strategy.reservation_price(100.0, 10.0, 50.0)

    assert long < flat
    # R = S - q * gamma * sigma**2 * (T - t)
    assert long == pytest.approx(100.0 - 10.0 * 0.1 * (0.1 ** 2) * 50.0)


def test_reservation_price_shifts_up_for_short_inventory() -> None:
    """A short position (q < 0) raises the reservation price for aggressive buying."""
    strategy = AvellanedaStoikovStrategy(_config())
    flat = strategy.reservation_price(100.0, 0.0, 50.0)
    short = strategy.reservation_price(100.0, -10.0, 50.0)

    assert short > flat
    assert short == pytest.approx(100.0 + 10.0 * 0.1 * (0.1 ** 2) * 50.0)


def test_micro_price_and_ofi_accuracy() -> None:
    """Micro-price weighting and OFI must match synthetic top-of-book updates."""
    calc = MicroPriceCalculator()

    # P_micro = (Ask_Vol * Bid_Price + Bid_Vol * Ask_Price) / (Bid_Vol + Ask_Vol)
    micro = calc.micro_price(100.0, 10.0, 101.0, 8.0)
    assert micro == pytest.approx((8.0 * 100.0 + 10.0 * 101.0) / 18.0)

    # Division-by-zero fallback: with no depth on either side use the mid.
    assert calc.micro_price(100.0, 0.0, 101.0, 0.0) == pytest.approx(100.5)

    # First tick has no prior state, so OFI is zero.
    assert calc.update(100.0, 10.0, 101.0, 8.0) == pytest.approx(0.0)

    # Bid ticks up (+12) and ask ticks down (+6): OFI = 12 - 6 = 6.
    assert calc.update(100.5, 12.0, 100.8, 6.0) == pytest.approx(6.0)

    # Same prices, bid size -3 and ask size +4: OFI = -3 - 4 = -7.
    assert calc.update(100.5, 9.0, 100.8, 10.0) == pytest.approx(-7.0)
    assert calc.ofi == pytest.approx(-7.0)
    assert calc.ticks == 3


def test_risk_guard_blocks_notional_limit() -> None:
    """The C++ RiskGuard must reject orders exceeding the notional cap."""
    engine = build_engine(load_settings())
    engine.apply_depth(hft_native.parse_depth_json(DEPTH_JSON))

    limits = hft_native.RiskLimits()
    limits.max_order_notional = hft_native.Notional.from_string("1000")
    engine.set_risk_limits(limits)

    request = hft_native.OrderRequest()
    request.side = hft_native.Side.BUY
    request.type = hft_native.OrderType.MARKET
    request.quantity = hft_native.Quantity.from_string("0.5")

    ack = engine.submit(request)
    assert not ack.accepted()
    assert ack.reason == hft_native.RejectReason.NOTIONAL_LIMIT


def test_stock_rounds_quotes_to_tick() -> None:
    """STOCK venues must snap quotes onto the configured tick grid."""
    config = _config(tick_size=0.01, market_type="STOCK")
    strategy = AvellanedaStoikovStrategy(config)

    bid, ask = strategy.adjusted_quotes(100.12345, 0.0, 50.0)

    # Both quotes must land exactly on the tick grid.
    assert math.isclose(bid / 0.01, round(bid / 0.01), abs_tol=1e-9)
    assert math.isclose(ask / 0.01, round(ask / 0.01), abs_tol=1e-9)
    # Explicit nearest-tick rounding (round half away from zero).
    assert strategy.round_to_tick(100.12345) == pytest.approx(100.12)
    assert strategy.round_to_tick(100.125) == pytest.approx(100.13)
    # The string adapter normalises to the MarketType enum.
    assert strategy._config.market_type == MarketType.STOCK


def test_crypto_quotes_land_on_the_tick_grid() -> None:
    """A quote between two ticks is not representable, so it must not be sent.

    Crypto used to skip the adapter entirely, which produced quotes like
    30000.8008 on a 0.5 tick: the ladder cannot hold them and a real venue
    rejects them. The only reason such an order ever "filled" was a fill model
    that ignored the book it was trading against.
    """
    config = _config(tick_size=0.5, market_type="CRYPTO")
    strategy = AvellanedaStoikovStrategy(config)

    for micro in (30000.9, 30001.0, 30001.4, 30001.9):
        bid, ask = strategy.adjusted_quotes(micro, 0.0, 59.9)
        for quote in (bid, ask):
            assert math.isclose((quote - 30000) / 0.5, round((quote - 30000) / 0.5),
                                abs_tol=1e-9)
        # Outward rounding can only widen the market, never cross it.
        assert ask > bid


def test_a_quote_already_on_the_grid_keeps_its_price() -> None:
    """``80000.2 / 0.1`` is ``800001.9999999999``, so blind rounding loses a tick.

    A quote the venue can already represent must not be moved: snapping it down
    silently changes the price, and therefore which orders fill.
    """
    config = _config(tick_size=0.1, market_type="CRYPTO")
    strategy = AvellanedaStoikovStrategy(config)

    assert strategy.snap_to_tick(80000.2, round_down=True) == 80000.2
    assert strategy.snap_to_tick(80000.2, round_down=False) == 80000.2
    assert strategy.snap_to_tick(80000.0, round_down=True) == 80000.0

    # A genuinely off-grid quote still rounds in the direction asked for, so
    # snapping can only widen the market. ``abs`` is a thousandth of a tick:
    # ``approx``'s default relative tolerance is 0.08 at this price, which is
    # 800 ticks wide and would accept a quote left a whole tick off grid.
    assert strategy.snap_to_tick(80000.25, round_down=True) == pytest.approx(
        80000.2, rel=0, abs=1e-7)
    assert strategy.snap_to_tick(80000.25, round_down=False) == pytest.approx(
        80000.3, rel=0, abs=1e-7)


def test_a_half_tick_quote_is_not_mistaken_for_a_grid_price() -> None:
    """The on-grid tolerance must not scale with the price-to-tick ratio.

    Ten thousand steps a side is enough to break a step-relative tolerance: at
    ``80000`` on a ``1e-4`` grid the step count is ``8e8``, so ``1e-9`` of it is
    most of a tick and a half-tick quote would be declared representable. The
    tolerance is the price's own float resolution instead, which stays nine
    orders of magnitude below half a tick and cannot absorb a real offset.
    """
    config = _config(tick_size=0.0001, market_type="CRYPTO")
    strategy = AvellanedaStoikovStrategy(config)

    # Half a tick off the grid: snapped outward, never submitted as it stands.
    # The tolerance is a tenth of the offset being detected, with the relative
    # term off, so a quote left where it started or moved by one tick fails.
    assert strategy.snap_to_tick(80000.00005, round_down=True) == pytest.approx(
        80000.0, rel=0, abs=1e-9)
    assert strategy.snap_to_tick(80000.00005, round_down=False) == pytest.approx(
        80000.0001, rel=0, abs=1e-9)

    # And a real grid price survives the same extremes untouched.
    assert strategy.snap_to_tick(80000.0001, round_down=True) == pytest.approx(
        80000.0001, rel=0, abs=1e-9)
    assert strategy.snap_to_tick(80000.0001, round_down=False) == pytest.approx(
        80000.0001, rel=0, abs=1e-9)


def test_crypto_quote_never_collapses_into_a_crossed_market() -> None:
    """A spread narrower than one tick still yields a valid two-sided quote."""
    # The committed fit (k=5.0, sigma=0.02) quotes ~0.20 wide, well under the
    # 0.5 tick, so a nearest-tick rule would map both sides onto one price.
    config = _config(tick_size=0.5, market_type="CRYPTO",
                     k=5.0, sigma=0.02, time_horizon=60.0)
    strategy = AvellanedaStoikovStrategy(config)

    assert strategy.optimal_half_spread(59.9) < config.tick_size
    bid, ask = strategy.adjusted_quotes(30001.0, 0.0, 59.9)
    assert ask - bid >= config.tick_size
    assert ask > bid
