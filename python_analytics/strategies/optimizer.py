"""Avellaneda-Stoikov parameter optimisation via historical grid search.

Replays recorded NDJSON market data through the local paper-trading engine and
scores each ``(gamma, sigma, k, time_horizon)`` combination on Sharpe, Sortino,
maximum drawdown and net PnL.
"""
from __future__ import annotations

import itertools
import math
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional, Sequence

from ..backtesting.metrics import max_drawdown, sharpe_ratio
from ..native import hft_native
from ..simulator.paper_engine import (
    PaperMatchingEngine,
    apply_depth_update,
    parse_depth_message,
)
from .config import MarketConfig, MarketType
from .market_maker import AvellanedaStoikovStrategy

__all__ = [
    "OptimizationResult",
    "backtest_parameters",
    "grid_search",
    "sortino_ratio",
]


def sortino_ratio(returns: Iterable[float], periods_per_year: float = 1.0,
                  target: float = 0.0) -> float:
    """Sortino ratio using downside deviation below ``target`` (0 by default)."""
    values = [float(value) for value in returns]
    if len(values) < 2:
        return 0.0
    mean = sum(values) / len(values)
    downside = sum(min(value - target, 0.0) ** 2 for value in values) / len(values)
    deviation = math.sqrt(downside)
    if deviation <= 0.0:
        return 0.0
    return ((mean - target) / deviation) * math.sqrt(periods_per_year)


@dataclass
class OptimizationResult:
    metric: str
    best_params: Dict[str, float]
    best_score: float
    grid: List[Dict[str, Any]] = field(default_factory=list)


def _equity_returns(curve: Sequence[float]) -> List[float]:
    out: List[float] = []
    for index in range(1, len(curve)):
        if curve[index - 1] > 0.0:
            out.append((curve[index] - curve[index - 1]) / curve[index - 1])
    return out


def _infer_center(lines: Sequence[str], tick_size: float) -> float:
    for line in lines:
        update = parse_depth_message(line)
        if update is None or not update.bids or not update.asks:
            continue
        try:
            bid = float(hft_native.Price.from_string(update.bids[0][0]))
            ask = float(hft_native.Price.from_string(update.asks[0][0]))
        except ValueError:
            continue
        mid = (bid + ask) / 2.0
        return round(mid / tick_size) * tick_size
    return 30000.0


def backtest_parameters(
    config: MarketConfig,
    lines: Sequence[str],
    *,
    center: Optional[float] = None,
    maker_fee: float = -0.0001,
    taker_fee: float = 0.0005,
    initial_cash: float = 0.0,
) -> Dict[str, Any]:
    """Replays ``lines`` through the paper engine for a single configuration."""
    center = center if center is not None else _infer_center(lines, config.tick_size)
    book = hft_native.L2Book(
        hft_native.Price.from_string(f"{config.tick_size:.8f}"),
        hft_native.Price.from_string(f"{center:.8f}"),
    )
    engine = PaperMatchingEngine(
        book, maker_fee=maker_fee, taker_fee=taker_fee, initial_cash=initial_cash,
    )
    strategy = AvellanedaStoikovStrategy(config)

    for line in lines:
        update = parse_depth_message(line)
        if update is None:
            continue
        apply_depth_update(book, update)
        engine.on_market_tick()
        strategy.on_depth(engine, update)

    engine.mark_to_market(engine.mark_price())
    returns = _equity_returns(engine.equity_curve)
    return {
        "net_pnl": engine.total_pnl,
        "realized_pnl": engine.realized_pnl,
        "unrealized_pnl": engine.unrealized_pnl,
        "sharpe": sharpe_ratio(returns, periods_per_year=1.0),
        "sortino": sortino_ratio(returns, periods_per_year=1.0),
        "max_drawdown": max_drawdown(engine.equity_curve),
        "total_trades": len(engine.fills),
        "final_equity": engine.equity,
    }


def grid_search(
    param_grid: Dict[str, Sequence[float]],
    lines: Iterable[str],
    *,
    metric: str = "sharpe",
    tick_size: float = 0.5,
    center: Optional[float] = None,
    inventory_limit: float = 5.0,
    order_quantity: float = 0.05,
    maker_fee: float = -0.0001,
    taker_fee: float = 0.0005,
    initial_cash: float = 0.0,
) -> OptimizationResult:
    """Grid search over A-S parameters, scored by ``metric`` (higher is better).

    ``param_grid`` maps parameter names to candidate values; supported keys are
    ``gamma``, ``sigma``, ``k`` and ``time_horizon``.
    """
    if not param_grid:
        raise ValueError("param_grid must contain at least one parameter axis")

    materialized = list(lines)
    if not materialized:
        raise ValueError("no market data provided")

    center = center if center is not None else _infer_center(materialized, tick_size)
    keys = list(param_grid.keys())
    rows: List[Dict[str, Any]] = []
    for combo in itertools.product(*(param_grid[key] for key in keys)):
        params = dict(zip(keys, combo))
        config = MarketConfig(
            gamma=float(params.get("gamma", 0.1)),
            sigma=float(params.get("sigma", 0.02)),
            k=float(params.get("k", 1.5)),
            time_horizon=float(params.get("time_horizon", 60.0)),
            tick_size=tick_size,
            market_type=MarketType.CRYPTO,
            inventory_limit=inventory_limit,
            order_quantity=order_quantity,
        )
        metrics = backtest_parameters(
            config, materialized, center=center,
            maker_fee=maker_fee, taker_fee=taker_fee, initial_cash=initial_cash,
        )
        rows.append({**params, **metrics})

    if metric not in rows[0]:
        raise ValueError(f"unknown metric: {metric!r}")

    rows.sort(key=lambda row: row[metric], reverse=True)
    best = rows[0]
    return OptimizationResult(
        metric=metric,
        best_params={key: float(best[key]) for key in keys},
        best_score=float(best[metric]),
        grid=rows,
    )

