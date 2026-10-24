"""Metric extraction and lightweight performance statistics."""
from __future__ import annotations

import math
from typing import Any, Dict, Iterable, List, Mapping, Sequence

from ..native import hft_native


def summarize(engine: "hft_native.TradingEngine",
              fills: Sequence[Mapping[str, Any]],
              malformed: int = 0) -> Dict[str, Any]:
    """Returns a JSON-serialisable snapshot of engine state and flow metrics."""
    metrics = engine.metrics()
    position = engine.position()
    volume = sum(float(item["quantity"]) for item in fills)
    notional = sum(float(item["price"]) * float(item["quantity"]) for item in fills)
    average_price = notional / volume if volume > 0 else 0.0
    return {
        "depth_events": metrics.depth_events,
        "trade_events": metrics.trade_events,
        "book_updates": metrics.book_updates,
        "orders_submitted": metrics.orders_submitted,
        "orders_rejected": metrics.orders_rejected,
        "orders_cancelled": metrics.orders_cancelled,
        "open_orders": metrics.open_orders,
        "fills": metrics.fills,
        "fill_count": len(fills),
        "filled_quantity": str(metrics.filled_quantity),
        "traded_notional": str(metrics.traded_notional),
        "average_fill_price": average_price,
        "net_quantity": str(position.net_quantity),
        "average_entry_price": str(position.average_entry_price),
        "realized_pnl": str(position.realized_pnl),
        "unrealized_pnl": str(position.unrealized_pnl),
        "latency_p50_ns": metrics.p50_ns,
        "latency_p99_ns": metrics.p99_ns,
        "latency_p999_ns": metrics.p999_ns,
        "last_update_ts": metrics.last_update_ts,
        "malformed_events": malformed,
        "bbo": {
            "valid": engine.bbo().is_valid(),
            "bid": str(engine.bbo().bid_price),
            "ask": str(engine.bbo().ask_price),
            "spread_bps": engine.bbo().spread_bps(),
        },
    }


def sharpe_ratio(returns: Iterable[float], periods_per_year: float = 252.0) -> float:
    """Annualised Sharpe ratio with zero risk-free rate; 0 for degenerate input."""
    values = [float(value) for value in returns]
    if len(values) < 2:
        return 0.0
    mean = sum(values) / len(values)
    variance = sum((value - mean) ** 2 for value in values) / (len(values) - 1)
    if variance <= 0.0:
        return 0.0
    return (mean / math.sqrt(variance)) * math.sqrt(periods_per_year)


def max_drawdown(equity: Iterable[float]) -> float:
    """Largest peak-to-trough decline, expressed as a positive fraction."""
    peak = -math.inf
    worst = 0.0
    for value in equity:
        peak = max(peak, value)
        if peak > 0:
            worst = max(worst, (peak - value) / peak)
    return worst


def fill_rows(fills: List["hft_native.Fill"]) -> List[Dict[str, Any]]:
    """Normalises native fills into report-friendly dictionaries."""
    rows: List[Dict[str, Any]] = []
    for fill in fills:
        rows.append(
            {
                "order_id": fill.order_id,
                "price": str(fill.price),
                "quantity": str(fill.quantity),
                "fee": str(fill.fee),
                "side": str(fill.side),
                "is_maker": fill.is_maker,
                "timestamp": fill.timestamp,
            }
        )
    return rows


# --------------------------------------------------------------------------- #
# Phase 3 — quantitative performance analytics                                #
# --------------------------------------------------------------------------- #


def sortino_ratio(returns: Iterable[float], periods_per_year: float = 252.0,
                  target: float = 0.0) -> float:
    """Annualised Sortino ratio using downside deviation below ``target``.

    A value of ``0.0`` is returned for degenerate input (fewer than two returns
    or zero downside deviation), which is the common convention for a strategy
    with no observed downside.
    """
    values = [float(value) for value in returns]
    if len(values) < 2:
        return 0.0
    mean = sum(values) / len(values)
    downside = sum(min(value - target, 0.0) ** 2 for value in values) / len(values)
    deviation = math.sqrt(downside)
    if deviation <= 0.0:
        return 0.0
    return ((mean - target) / deviation) * math.sqrt(periods_per_year)


def _side_sign(side: Any) -> int:
    """Maps a native/string side to ``+1`` (buy) or ``-1`` (sell)."""
    text = str(side).upper()
    return 1 if "BUY" in text or text == "1" else -1


def round_trips(fills: Sequence[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    """Reconstructs FIFO open→close pairs from a fill log.

    Each round trip reports gross PnL (price differential, fees excluded), entry
    and exit timestamps/prices, the matched quantity and the opening side. The
    FIFO queue is keyed by side, so a fill first closes the oldest opposite-side
    lots and any excess becomes a new opening lot (position flips are handled).
    """
    trips: List[Dict[str, Any]] = []
    lots: List[Dict[str, Any]] = []
    for fill in fills:
        side = _side_sign(fill.get("side", "BUY"))
        quantity = float(fill["quantity"])
        price = float(fill["price"])
        timestamp = int(fill.get("timestamp", 0) or 0)
        remaining = quantity

        def close(open_side: int, open_price: float, open_ts: int, take: float) -> None:
            gross_pnl = (
                (price - open_price) * take if open_side > 0
                else (open_price - price) * take
            )
            trips.append(
                {
                    "entry_ts": open_ts,
                    "exit_ts": timestamp,
                    "entry_price": open_price,
                    "exit_price": price,
                    "quantity": take,
                    "side": "BUY" if open_side > 0 else "SELL",
                    "gross_pnl": gross_pnl,
                }
            )

        # Consume opposite-side lots FIFO.
        while remaining > 0 and lots and _side_sign(lots[0]["side"]) != side:
            lot = lots[0]
            take = min(float(lot["quantity"]), remaining)
            close(_side_sign(lot["side"]), float(lot["price"]), int(lot["timestamp"]),
                  take)
            lot["quantity"] = float(lot["quantity"]) - take
            remaining -= take
            if float(lot["quantity"]) <= 1e-12:
                lots.pop(0)
        if remaining > 1e-12:
            lots.append(
                {
                    "side": "BUY" if side > 0 else "SELL",
                    "quantity": remaining,
                    "price": price,
                    "timestamp": timestamp,
                }
            )
    return trips


def win_rate(trips: Sequence[Mapping[str, Any]]) -> float:
    """Fraction of round trips with positive gross PnL (0.0 with no trips)."""
    if not trips:
        return 0.0
    wins = sum(1 for trip in trips if float(trip["gross_pnl"]) > 0.0)
    return wins / len(trips)


def profit_factor(trips: Sequence[Mapping[str, Any]]) -> float:
    """Gross profit / gross loss; ``inf``-safe (capped at 0.0 when no losses)."""
    gross_profit = 0.0
    gross_loss = 0.0
    for trip in trips:
        pnl = float(trip["gross_pnl"])
        if pnl > 0.0:
            gross_profit += pnl
        else:
            gross_loss += -pnl
    if gross_loss <= 0.0:
        return 0.0 if gross_profit <= 0.0 else math.inf
    return gross_profit / gross_loss


def holding_time_seconds(trips: Sequence[Mapping[str, Any]]) -> float:
    """Mean open→close holding time in seconds (0.0 with no completed trips)."""
    if not trips:
        return 0.0
    durations = [
        max(0.0, (int(trip["exit_ts"]) - int(trip["entry_ts"])) / 1e9)
        for trip in trips
    ]
    return sum(durations) / len(durations)


def inventory_turnover(traded_notional: float,
                       equity_curve: Sequence[float]) -> float:
    """Traded notional per unit of average absolute equity (0.0 if equity is 0)."""
    values = [abs(float(value)) for value in equity_curve]
    if not values:
        return 0.0
    average_equity = sum(values) / len(values)
    if average_equity <= 1e-12:
        return 0.0
    return float(traded_notional) / average_equity


def fill_rate(filled_quantity: float, submitted_quantity: float) -> float:
    """Fraction of submitted quantity that filled (0.0 with no submissions)."""
    if submitted_quantity <= 0.0:
        return 0.0
    return min(1.0, float(filled_quantity) / submitted_quantity)


def distribution(values: Sequence[float],
                 percentiles: Sequence[float] = (50.0, 90.0, 95.0, 99.0)) -> Dict[str, float]:
    """Empirical distribution summary of a (non-negative) sample."""
    samples = sorted(float(value) for value in values)
    if not samples:
        return {"mean": 0.0, "max": 0.0}
    out: Dict[str, float] = {
        "mean": sum(samples) / len(samples),
        "max": samples[-1],
    }
    for pct in percentiles:
        index = min(len(samples) - 1, int(round((pct / 100.0) * (len(samples) - 1))))
        out[f"p{int(pct):02d}"] = samples[index]
    return out


def quant_summary(
    *,
    realized_pnl: float,
    unrealized_pnl: float,
    fees_paid: float,
    traded_notional: float,
    equity_curve: Sequence[float],
    fills: Sequence[Mapping[str, Any]],
    slippage_samples: Sequence[float],
    depth_events: int,
    elapsed_s: float,
    latency_ns: Mapping[str, int],
    submitted_quantity: float,
    periods_per_year: float = 252.0,
) -> Dict[str, Any]:
    """Produces the full financial/execution/system telemetry block.

    ``equity_curve`` is a sequence of mark-to-market account values sampled over
    the run; per-period returns are simple percentage changes. ``latency_ns`` is
    a mapping with ``p50``/``p90``/``p99``/``p999`` keys (the engine histogram).
    """
    returns: List[float] = []
    for index in range(1, len(equity_curve)):
        previous = float(equity_curve[index - 1])
        if previous > 0.0:
            returns.append((float(equity_curve[index]) - previous) / previous)

    trips = round_trips(list(fills))
    gross_pnl = realized_pnl + unrealized_pnl
    filled_quantity = sum(float(fill["quantity"]) for fill in fills)
    drawdown_fraction = max_drawdown(equity_curve)

    return {
        # Financial.
        "realized_pnl": realized_pnl,
        "unrealized_pnl": unrealized_pnl,
        "gross_pnl": gross_pnl,
        "sharpe_ratio": sharpe_ratio(returns, periods_per_year=periods_per_year),
        "sortino_ratio": sortino_ratio(returns, periods_per_year=periods_per_year),
        "max_drawdown_pct": drawdown_fraction * 100.0,
        "win_rate": win_rate(trips),
        "profit_factor": profit_factor(trips),
        "pnl_fee_ratio": (gross_pnl / fees_paid) if fees_paid != 0.0 else 0.0,
        # Execution & inventory.
        "avg_holding_time_s": holding_time_seconds(trips),
        "inventory_turnovers": inventory_turnover(traded_notional, equity_curve),
        "fill_rate_pct": fill_rate(filled_quantity, submitted_quantity) * 100.0,
        "filled_quantity": filled_quantity,
        "submitted_quantity": submitted_quantity,
        "fees_paid": fees_paid,
        "traded_notional": traded_notional,
        "slippage": distribution(slippage_samples),
        "fee_notional_ratio": (
            fees_paid / traded_notional if traded_notional > 0.0 else 0.0
        ),
        "round_trips": len(trips),
        # System.
        "depth_events": depth_events,
        "throughput_fps": (depth_events / elapsed_s) if elapsed_s > 0.0 else 0.0,
        "latency_ns": dict(latency_ns),
    }
