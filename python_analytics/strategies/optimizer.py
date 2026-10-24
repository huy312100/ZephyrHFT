"""Avellaneda-Stoikov parameter optimisation over the C++ paper engine.

Replays recorded NDJSON market data through the zero-allocation Phase 2
``paper_engine`` (via ``BulkReplayer``) and scores each
``(gamma, sigma, k, time_horizon, ofi_sensitivity)`` combination on Sharpe,
Sortino, maximum drawdown, inventory variance and net PnL. Grid, random and
Bayesian search are provided; ``scipy`` is an optional dependency for the
Bayesian path.
"""
from __future__ import annotations

import itertools
import math
import random
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence

from ..backtesting.metrics import sortino_ratio as _sortino
from ..backtesting.replay import BulkReplayer
from ..native import hft_native
from .config import MarketConfig, MarketType
from .market_maker import AvellanedaStoikovStrategy

__all__ = [
    "OptimizationResult",
    "backtest_parameters",
    "bayesian_search",
    "composite_score",
    "grid_search",
    "random_search",
    "sortino_ratio",
]


def sortino_ratio(returns: Iterable[float], periods_per_year: float = 1.0,
                  target: float = 0.0) -> float:
    """Sortino ratio using downside deviation below ``target`` (0 by default)."""
    return _sortino(returns, periods_per_year=periods_per_year, target=target)


@dataclass
class OptimizationResult:
    metric: str
    best_params: Dict[str, float]
    best_score: float
    grid: List[Dict[str, Any]] = field(default_factory=list)


def _infer_center(lines: Sequence[str], tick_size: float) -> float:
    """Infers a ladder centre from the first usable frame's best bid/ask."""
    for line in lines:
        try:
            update = hft_native.parse_depth_json(line)
        except RuntimeError:
            continue
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


def composite_score(metrics: Mapping[str, Any], *,
                    w_sharpe: float = 1.0, w_drawdown: float = 1.0,
                    w_inventory: float = 1.0, w_fill: float = 1.0) -> float:
    """Risk-adjusted composite objective (higher is better).

    ``score = w_sharpe*sharpe - w_drawdown*max_drawdown - w_inventory*inventory_std
             + w_fill*fill_rate``

    ``max_drawdown`` and ``fill_rate`` are fractions in ``[0, 1]`` and
    ``inventory_std`` is the population standard deviation of the signed
    position (base units). Weights let a caller trade off return vs. drawdown
    vs. inventory risk vs. execution.
    """
    sharpe = float(metrics.get("sharpe", 0.0))
    drawdown = float(metrics.get("max_drawdown", 0.0))
    inventory = float(metrics.get("inventory_std", 0.0))
    fill = float(metrics.get("fill_rate", 0.0))
    return (w_sharpe * sharpe - w_drawdown * drawdown
            - w_inventory * inventory + w_fill * fill)


def backtest_parameters(
    config: MarketConfig,
    lines: Sequence[str],
    *,
    center: Optional[float] = None,
    maker_fee: float = -0.0001,
    taker_fee: float = 0.0005,
    initial_cash: float = 0.0,
    ofi_sensitivity: float = 0.0,
    use_time_spread: bool = True,
) -> Dict[str, Any]:
    """Replays ``lines`` through the C++ paper engine for one configuration.

    Returns the legacy keys ``net_pnl``/``sharpe``/``sortino``/``max_drawdown``/
    ``total_trades``/``final_equity`` plus Phase 3 keys ``fill_rate``,
    ``inventory_std``, ``fees_paid`` and the honest ``max_drawdown_pct``.
    """
    center = center if center is not None else _infer_center(lines, config.tick_size)
    # Fees arrive as fractions (e.g. -0.0001 == -1 bps). Keep the exact bps value
    # instead of rounding to an integer so sub-basis-point rates survive.
    maker_bps = float(maker_fee) * 10_000.0
    taker_bps = float(taker_fee) * 10_000.0
    strategy = AvellanedaStoikovStrategy(
        config, ofi_sensitivity=ofi_sensitivity, use_time_spread=use_time_spread
    )
    replayer = BulkReplayer(
        tick_size=f"{config.tick_size:.8f}",
        center=f"{center:.8f}",
        maker_bps=maker_bps,
        taker_bps=taker_bps,
        initial_cash=initial_cash,
        frames_per_hook=1,
        order_quantity=str(config.order_quantity),
    )
    summary = replayer.replay_lines(lines, strategy=strategy)
    stats = summary.stats
    return {
        "net_pnl": stats["gross_pnl"],
        "realized_pnl": stats["realized_pnl"],
        "unrealized_pnl": stats["unrealized_pnl"],
        "sharpe": stats["sharpe_ratio"],
        "sortino": stats["sortino_ratio"],
        "max_drawdown": min(1.0, stats["max_drawdown_pct"] / 100.0),
        "total_trades": int(stats["fills"]),
        "final_equity": summary.equity_curve[-1] if summary.equity_curve else initial_cash,
        "fill_rate": stats["fill_rate_pct"] / 100.0,
        "inventory_std": stats["inventory_std"],
        "fees_paid": stats["fees_paid"],
        "max_drawdown_pct": stats["max_drawdown_pct"],
    }


def _evaluate(params: Mapping[str, float], lines: Sequence[str], *,
              center: float, tick_size: float, inventory_limit: float,
              order_quantity: float, maker_fee: float, taker_fee: float,
              initial_cash: float, use_time_spread: bool) -> Dict[str, Any]:
    """Runs one candidate and attaches the composite score."""
    ofi_sensitivity = float(params.get("ofi_sensitivity", 0.0))
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
        config, lines, center=center, maker_fee=maker_fee, taker_fee=taker_fee,
        initial_cash=initial_cash, ofi_sensitivity=ofi_sensitivity,
        use_time_spread=use_time_spread,
    )
    row: Dict[str, Any] = {**params, **metrics}
    row["composite"] = composite_score(row)
    return row


def _rank(rows: List[Dict[str, Any]], keys: Sequence[str],
          metric: str) -> OptimizationResult:
    """Sorts rows by ``metric`` (or the composite score) and builds a result."""
    sort_key = "composite" if metric == "composite" else metric
    if sort_key not in rows[0]:
        raise ValueError(f"unknown metric: {metric!r}")
    rows.sort(key=lambda row: row[sort_key], reverse=True)
    best = rows[0]
    return OptimizationResult(
        metric=metric,
        best_params={key: float(best[key]) for key in keys},
        best_score=float(best[sort_key]),
        grid=rows,
    )


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
    use_time_spread: bool = True,
) -> OptimizationResult:
    """Grid search over A-S parameters, scored by ``metric`` (higher is better).

    ``param_grid`` maps parameter names to candidate values; supported keys are
    ``gamma``, ``sigma``, ``k``, ``time_horizon`` and ``ofi_sensitivity``.
    ``metric`` may be any returned metric name or ``"composite"``.
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
        rows.append(_evaluate(
            params, materialized, center=center, tick_size=tick_size,
            inventory_limit=inventory_limit, order_quantity=order_quantity,
            maker_fee=maker_fee, taker_fee=taker_fee, initial_cash=initial_cash,
            use_time_spread=use_time_spread,
        ))
    return _rank(rows, keys, metric)


def _latin_sample(bounds: Sequence[float], rng: random.Random) -> float:
    """Uniform draw within ``[low, high]`` (stratified approximation)."""
    low, high = float(bounds[0]), float(bounds[-1])
    if high <= low:
        return low
    return low + rng.random() * (high - low)


def random_search(
    param_space: Dict[str, Sequence[float]],
    lines: Iterable[str],
    *,
    n_iterations: int = 32,
    metric: str = "composite",
    seed: Optional[int] = None,
    tick_size: float = 0.5,
    center: Optional[float] = None,
    inventory_limit: float = 5.0,
    order_quantity: float = 0.05,
    maker_fee: float = -0.0001,
    taker_fee: float = 0.0005,
    initial_cash: float = 0.0,
    use_time_spread: bool = True,
) -> OptimizationResult:
    """Latin-hypercube-style random search over continuous parameter ranges.

    ``param_space`` maps parameter names to ``[low, high]`` ranges. Each axis is
    sampled uniformly and independently; the resulting design is space-filling
    without requiring scipy/numpy. Set ``seed`` for reproducibility.
    """
    if not param_space:
        raise ValueError("param_space must contain at least one parameter axis")
    if n_iterations < 1:
        raise ValueError("n_iterations must be positive")

    materialized = list(lines)
    if not materialized:
        raise ValueError("no market data provided")
    center = center if center is not None else _infer_center(materialized, tick_size)
    keys = list(param_space.keys())
    rng = random.Random(seed)

    rows: List[Dict[str, Any]] = []
    for _ in range(n_iterations):
        params = {key: _latin_sample(param_space[key], rng) for key in keys}
        rows.append(_evaluate(
            params, materialized, center=center, tick_size=tick_size,
            inventory_limit=inventory_limit, order_quantity=order_quantity,
            maker_fee=maker_fee, taker_fee=taker_fee, initial_cash=initial_cash,
            use_time_spread=use_time_spread,
        ))
    return _rank(rows, keys, metric)


def bayesian_search(
    param_space: Dict[str, Sequence[float]],
    lines: Iterable[str],
    *,
    n_calls: int = 20,
    n_init: int = 5,
    metric: str = "composite",
    seed: Optional[int] = None,
    tick_size: float = 0.5,
    center: Optional[float] = None,
    inventory_limit: float = 5.0,
    order_quantity: float = 0.05,
    maker_fee: float = -0.0001,
    taker_fee: float = 0.0005,
    initial_cash: float = 0.0,
    use_time_spread: bool = True,
) -> OptimizationResult:
    """Bayesian optimisation over ``param_space`` using ``scikit-optimize``.

    Falls back to a multistart ``scipy.optimize`` L-BFGS-B search when only
    scipy is installed. Raises ``ImportError`` if neither is available — use
    ``grid_search`` or ``random_search`` instead.
    """
    materialized = list(lines)
    if not materialized:
        raise ValueError("no market data provided")
    center = center if center is not None else _infer_center(materialized, tick_size)
    keys = list(param_space.keys())
    dimensions = [(float(v[0]), float(v[-1]))
                  for v in (param_space[k] for k in keys)]

    def objective(point: Sequence[float]) -> float:
        params = dict(zip(keys, (float(value) for value in point)))
        row = _evaluate(
            params, materialized, center=center, tick_size=tick_size,
            inventory_limit=inventory_limit, order_quantity=order_quantity,
            maker_fee=maker_fee, taker_fee=taker_fee, initial_cash=initial_cash,
            use_time_spread=use_time_spread,
        )
        return -float(row["composite"]) if metric == "composite" else -float(row[metric])

    try:
        from skopt import gp_minimize
    except ImportError:
        return _scipy_minimize(objective, dimensions, keys, metric,
                               n_calls, n_init, seed)

    result = gp_minimize(
        objective, dimensions, n_calls=n_calls, n_random_starts=n_init,
        random_state=seed,
    )
    best = {key: float(value) for key, value in zip(keys, result.x)}
    rows = [_evaluate(
        best, materialized, center=center, tick_size=tick_size,
        inventory_limit=inventory_limit, order_quantity=order_quantity,
        maker_fee=maker_fee, taker_fee=taker_fee, initial_cash=initial_cash,
        use_time_spread=use_time_spread,
    )]
    return OptimizationResult(
        metric=metric, best_params=best, best_score=-float(result.fun), grid=rows,
    )


def _scipy_minimize(objective, dimensions: Sequence[Sequence[float]],
                    keys: Sequence[str], metric: str, n_calls: int,
                    n_init: int, seed: Optional[int]) -> OptimizationResult:
    """Multistart L-BFGS-B fallback when only scipy is installed."""
    try:
        from scipy.optimize import minimize
    except ImportError as exc:  # pragma: no cover - env dependent
        raise ImportError(
            "bayesian_search requires scikit-optimize or scipy; "
            "use grid_search or random_search instead"
        ) from exc

    bounds = [(float(v[0]), float(v[1])) for v in dimensions]
    rng = random.Random(seed)
    best_score = -math.inf
    best_point = [0.5 * (float(v[0]) + float(v[1])) for v in dimensions]
    for _ in range(max(1, n_init)):
        x0 = [float(v[0]) + rng.random() * (float(v[1]) - float(v[0]))
              for v in dimensions]
        result = minimize(objective, x0, bounds=bounds, method="L-BFGS-B",
                          options={"maxiter": max(1, n_calls // max(1, n_init))})
        if -float(result.fun) > best_score:
            best_score = -float(result.fun)
            best_point = list(result.x)

    best = {key: float(value) for key, value in zip(keys, best_point)}
    return OptimizationResult(metric=metric, best_params=best,
                              best_score=best_score, grid=[])
