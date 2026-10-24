"""Unit tests for the Phase 3 bulk replay engine and quantitative analytics."""
from __future__ import annotations

import math

import pytest

from python_analytics.backtesting.metrics import (
    profit_factor,
    quant_summary,
    round_trips,
    win_rate,
)
from python_analytics.backtesting.replay import BulkReplayer
from python_analytics.simulator.paper_engine import synthetic_depth_messages
from python_analytics.strategies import (
    AvellanedaStoikovStrategy,
    MarketConfig,
    MarketType,
)


def _lines(seed: int = 1, count: int = 200) -> list:
    generator = synthetic_depth_messages(mid=100.0, tick=0.5, seed=seed)
    return [next(generator) for _ in range(count)]


def _strategy() -> AvellanedaStoikovStrategy:
    config = MarketConfig(
        gamma=0.1, sigma=0.02, k=5.0, tick_size=0.5,
        market_type=MarketType.CRYPTO, time_horizon=60.0, order_quantity=0.1,
    )
    return AvellanedaStoikovStrategy(config, ofi_sensitivity=0.5)


def test_bulk_replay_produces_fills_and_telemetry() -> None:
    replayer = BulkReplayer(tick_size="0.5", center="100", maker_bps=-1,
                            taker_bps=5, frames_per_hook=1, order_quantity="0.1")
    summary = replayer.replay_lines(_lines(), strategy=_strategy())
    stats = summary.stats

    assert stats["depth_events"] == 200
    assert stats["gap_events"] == 0
    assert stats["fills"] > 0
    assert math.isfinite(stats["realized_pnl"])
    assert math.isfinite(stats["sharpe_ratio"])
    assert math.isfinite(stats["sortino_ratio"])
    assert stats["fill_rate_pct"] > 0.0
    assert len(summary.equity_curve) > 1
    assert "engine_p50_ns" in stats["latency_ns"]


def test_bulk_replay_is_deterministic() -> None:
    lines = _lines(seed=3, count=150)

    def run() -> tuple:
        replayer = BulkReplayer(tick_size="0.5", center="100", maker_bps=-1,
                                taker_bps=5, frames_per_hook=1,
                                order_quantity="0.1")
        summary = replayer.replay_lines(lines, strategy=_strategy())
        return summary.stats["realized_pnl"], tuple(summary.equity_curve)

    assert run() == run()


def test_round_trips_fifo_matching() -> None:
    fills = [
        {"side": "BUY", "quantity": 1.0, "price": 100.0, "timestamp": 0},
        {"side": "SELL", "quantity": 1.0, "price": 101.0, "timestamp": 1_000_000_000},
    ]
    trips = round_trips(fills)
    assert len(trips) == 1
    assert trips[0]["gross_pnl"] == 1.0
    assert trips[0]["entry_ts"] == 0
    assert trips[0]["exit_ts"] == 1_000_000_000
    assert win_rate(trips) == 1.0
    assert math.isinf(profit_factor(trips))


def test_round_trips_partial_close_and_flip() -> None:
    fills = [
        {"side": "BUY", "quantity": 2.0, "price": 100.0, "timestamp": 0},
        {"side": "SELL", "quantity": 3.0, "price": 101.0, "timestamp": 1_000_000_000},
    ]
    trips = round_trips(fills)
    # 2 units close against the long lot, 1 unit flips to a new short lot.
    assert len(trips) == 1
    assert trips[0]["quantity"] == 2.0
    assert trips[0]["gross_pnl"] == pytest.approx(2.0)


def test_quant_summary_key_coverage() -> None:
    summary = quant_summary(
        realized_pnl=1.0,
        unrealized_pnl=0.5,
        fees_paid=0.1,
        traded_notional=100.0,
        equity_curve=[100.0, 101.0, 100.5],
        fills=[{"side": "BUY", "quantity": 1.0, "price": 100.0, "timestamp": 0}],
        slippage_samples=[0.0, 0.1, 0.2],
        depth_events=10,
        elapsed_s=1.0,
        latency_ns={"engine_p50_ns": 1},
        submitted_quantity=1.0,
    )
    for key in (
        "realized_pnl", "unrealized_pnl", "gross_pnl", "sharpe_ratio",
        "sortino_ratio", "max_drawdown_pct", "win_rate", "profit_factor",
        "avg_holding_time_s", "inventory_turnovers", "fill_rate_pct",
        "fee_notional_ratio", "throughput_fps", "latency_ns",
    ):
        assert key in summary
    assert summary["gross_pnl"] == pytest.approx(1.5)
