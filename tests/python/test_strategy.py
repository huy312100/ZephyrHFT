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
