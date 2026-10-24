"""Unit tests for the Avellaneda-Stoikov parameter optimizer."""
from __future__ import annotations

import math

import pytest

from python_analytics.simulator.paper_engine import synthetic_depth_messages
from python_analytics.strategies.optimizer import (
    backtest_parameters,
    grid_search,
    sortino_ratio,
)
from python_analytics.strategies.config import MarketConfig, MarketType


def test_sortino_ratio_accuracy() -> None:
    # Symmetric up/down returns: mean 0.005, downside deviation sqrt(0.00005).
    returns = [0.02, -0.01, 0.02, -0.01]
    assert sortino_ratio(returns, periods_per_year=1.0) == pytest.approx(
        0.005 / math.sqrt(0.00005), rel=1e-6
    )
    # No downside => zero Sortino.
    assert sortino_ratio([0.01, 0.02, 0.03]) == 0.0
    # Degenerate inputs.
    assert sortino_ratio([0.5]) == 0.0
    assert sortino_ratio([]) == 0.0


def test_backtest_parameters_single_run() -> None:
    generator = synthetic_depth_messages(mid=100.0, tick=0.5, seed=2)
    lines = [next(generator) for _ in range(250)]
    # ``k=5.0`` models a liquid book: the corrected Avellaneda-Stoikov spread
    # ``(1/gamma)*ln(1+gamma/k)`` is then narrow enough that the synthetic
    # ±3-tick jumps trade through our resting quotes.
    config = MarketConfig(
        gamma=0.1, sigma=0.02, k=5.0, tick_size=0.5,
        market_type=MarketType.CRYPTO, time_horizon=60.0, order_quantity=0.1,
    )
    metrics = backtest_parameters(config, lines, center=100.0)
    assert math.isfinite(metrics["net_pnl"])
    assert math.isfinite(metrics["sharpe"])
    assert math.isfinite(metrics["sortino"])
    assert 0.0 <= metrics["max_drawdown"] <= 1.0
    # A moving market must cross our resting quotes at least once.
    assert metrics["total_trades"] > 0


def test_grid_search_returns_best_params() -> None:
    generator = synthetic_depth_messages(mid=100.0, tick=0.5, seed=1)
    lines = [next(generator) for _ in range(300)]
    result = grid_search(
        {"gamma": [0.1, 0.5], "k": [2.0, 5.0]},
        lines,
        metric="net_pnl",
        tick_size=0.5,
        center=100.0,
        order_quantity=0.1,
    )
    assert result.metric == "net_pnl"
    assert set(result.best_params) == {"gamma", "k"}
    assert len(result.grid) == 4
    for row in result.grid:
        assert math.isfinite(row["net_pnl"])
        assert math.isfinite(row["sharpe"])
        assert math.isfinite(row["sortino"])
        assert 0.0 <= row["max_drawdown"] <= 1.0
    # The best row is the first (grid is sorted by the metric descending).
    assert result.grid[0]["net_pnl"] == pytest.approx(result.best_score)
    assert any(row["total_trades"] > 0 for row in result.grid)


def test_grid_search_rejects_empty_inputs() -> None:
    with pytest.raises(ValueError):
        grid_search({}, ["line"], metric="sharpe")
    with pytest.raises(ValueError):
        grid_search({"gamma": [0.1]}, [], metric="sharpe")
