"""Unit tests for the Phase 3 bulk replay engine and quantitative analytics."""
from __future__ import annotations

import math
from pathlib import Path

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


def test_fractional_bps_reach_the_ledger() -> None:
    """0.5 bps must survive to C++ (regression: ``int()`` truncated it to 0)."""
    from python_analytics.native import hft_native

    engine = hft_native.PaperEngine(
        hft_native.Price.from_string("0.5"),
        hft_native.Price.from_string("100"),
        -1.0,
        0.5,
        hft_native.Notional.from_string("100000"),
    )
    engine.process_update(hft_native.DepthUpdate(
        bids=[("100.0", "10")],
        asks=[("100.5", "10")],
        exchange_ts=1,
        first_update_id=1,
        final_update_id=1,
    ))
    request = hft_native.OrderRequest()
    request.side = hft_native.Side.BUY
    request.type = hft_native.OrderType.MARKET
    request.quantity = hft_native.Quantity.from_string("2")
    ack = engine.submit(request)

    assert ack.accepted()
    assert float(ack.average_price) == pytest.approx(100.5)
    # The configured rate round-trips as 0.5 bps, not 0 (or 1).
    assert engine.account().taker_bps() == pytest.approx(0.5)
    assert engine.account().maker_bps() == pytest.approx(-1.0)
    # 2 * 100.5 = 201 notional; 201 * 0.5 bps = 0.01005 (not zero, not 5 bps).
    assert float(engine.account().fees_paid()) == pytest.approx(0.01005, rel=1e-9)


def test_replayer_keeps_fractional_fees_unrounded() -> None:
    replayer = BulkReplayer(tick_size="0.5", center="100", maker_bps=-1.25,
                            taker_bps=0.5, frames_per_hook=1)
    assert replayer._maker_bps == pytest.approx(-1.25)
    assert replayer._taker_bps == pytest.approx(0.5)
    assert isinstance(replayer._taker_bps, float)


def test_run_backtest_cli_accepts_fractional_fees() -> None:
    from scripts.run_backtest import parse_args

    args = parse_args(["--maker-bps", "-1.25", "--taker-bps", "0.5"])
    assert args.maker_bps == pytest.approx(-1.25)
    assert args.taker_bps == pytest.approx(0.5)
    assert isinstance(args.taker_bps, float)
    # Backward compatibility: whole-basis-point flags still parse identically.
    legacy = parse_args(["--maker-bps", "-1", "--taker-bps", "5"])
    assert legacy.maker_bps == -1.0
    assert legacy.taker_bps == 5.0


def test_cli_fee_defaults_are_retail_tier_not_a_rebate() -> None:
    """Every entry point must default to what a retail VIP 0 account actually pays."""

    from python_analytics.costs import DEFAULT_MAKER_BPS, DEFAULT_TAKER_BPS
    from scripts.run_backtest import parse_args as backtest_args
    from scripts.run_live_paper import parse_args as live_paper_args
    from scripts.run_paper_trading import parse_args as paper_args
    from scripts.run_walk_forward import parse_args as walk_forward_args

    cases = [
        (backtest_args, "maker_bps", "taker_bps"),
        (walk_forward_args, "maker_bps", "taker_bps"),
        (paper_args, "maker_fee_bps", "taker_fee_bps"),
        (live_paper_args, "maker_fee_bps", "taker_fee_bps"),
    ]
    for parser, maker_attr, taker_attr in cases:
        defaults = parser([])
        assert getattr(defaults, maker_attr) == pytest.approx(DEFAULT_MAKER_BPS), \
            parser.__module__
        assert getattr(defaults, taker_attr) == pytest.approx(DEFAULT_TAKER_BPS), \
            parser.__module__
        assert getattr(defaults, maker_attr) >= 0.0, "a default rebate flatters every fill"


def test_export_report_accepts_json_path(tmp_path: Path) -> None:
    """``--report .../summary.json`` must not nest artefacts in a dir named .json."""
    from python_analytics.backtesting.report import export_report

    artefacts = export_report(
        tmp_path / "run_001" / "summary.json",
        summary={"realized_pnl": 1.0, "sharpe_ratio": float("inf")},
        fills=[{"side": "BUY", "quantity": 1.0, "price": 100.0}],
        equity_curve=[100000.0, 100001.0],
    )
    assert Path(artefacts["summary"]) == tmp_path / "run_001" / "summary.json"
    assert Path(artefacts["summary"]).is_file()
    assert Path(artefacts["fills"]) == tmp_path / "run_001" / "fills.csv"
    # Non-finite metrics are sanitised so the JSON stays valid.
    assert "null" in Path(artefacts["summary"]).read_text(encoding="utf-8")


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
