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
