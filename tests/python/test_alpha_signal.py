"""Unit tests for the Phase 3 alpha-signal coupling and microstructure metrics."""
from __future__ import annotations

import math

import pytest

from python_analytics.strategies import (
    AvellanedaStoikovStrategy,
    MarketConfig,
    MarketType,
    OfiSignal,
    SpreadDepthStats,
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


def test_ofi_signal_is_zero_during_warmup_and_clipped_after() -> None:
    signal = OfiSignal(decay=0.9, cap=2.0, warmup=3)
    assert signal.update(1.0) == 0.0
    assert signal.update(1.0) == 0.0  # samples 1 and 2 are below warmup
    # Sample 3 is the first real estimate (not part of warmup).
    assert math.isfinite(signal.update(1.0))
    # A large outlier after warmup must be clipped to the cap.
    assert signal.update(1000.0) == pytest.approx(2.0)
    assert signal.value == pytest.approx(2.0)


def test_ofi_signal_validation() -> None:
    with pytest.raises(ValueError):
        OfiSignal(decay=1.0)
    with pytest.raises(ValueError):
        OfiSignal(cap=0.0)


def test_reservation_price_ofi_skew() -> None:
    strategy = AvellanedaStoikovStrategy(_config(), ofi_sensitivity=1.0)
    flat = strategy.reservation_price(100.0, 0.0, 50.0)
    # Positive OFI (buy pressure) raises the indifference price by sensitivity*z.
    assert strategy.reservation_price(100.0, 0.0, 50.0, ofi_z=2.0) == pytest.approx(
        flat + 2.0
    )
    assert strategy.reservation_price(100.0, 0.0, 50.0, ofi_z=-1.5) == pytest.approx(
        flat - 1.5
    )


def test_reservation_price_zero_sensitivity_unchanged() -> None:
    strategy = AvellanedaStoikovStrategy(_config(), ofi_sensitivity=0.0)
    assert strategy.reservation_price(100.0, 0.0, 50.0, ofi_z=5.0) == pytest.approx(
        100.0
    )


def test_time_spread_term_widens_with_horizon() -> None:
    base = AvellanedaStoikovStrategy(_config(), use_time_spread=True)
    legacy = AvellanedaStoikovStrategy(_config(), use_time_spread=False)
    # At a non-zero horizon the time-decaying term widens the per-side spread.
    assert base.optimal_half_spread(time_remaining=50.0, sigma=0.1) > legacy.optimal_half_spread(
        time_remaining=50.0, sigma=0.1
    )


def test_spread_depth_stats() -> None:
    stats = SpreadDepthStats()
    state = stats.update(100.0, 10.0, 101.0, 8.0)
    assert state["spread"] == pytest.approx(1.0)
    assert state["spread_bps"] == pytest.approx(1.0 / 100.5 * 10_000.0)
    assert state["depth_imbalance"] == pytest.approx(2.0 / 18.0)
    # Micro price weights each touch by the opposite side's volume.
    assert state["micro"] == pytest.approx((8.0 * 100.0 + 10.0 * 101.0) / 18.0)
    assert stats.state["mid"] == pytest.approx(100.5)
