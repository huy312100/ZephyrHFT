#!/usr/bin/env python3
"""Phase 3 backtest runner: replay Phase 1 archives through the C++ paper engine.

Executes an alpha strategy per frame and emits the full quantitative telemetry
(financial, execution and system metrics) plus optional JSON/CSV/PNG exports.

Fee and rebate rates (--maker-bps / --taker-bps) are fractional basis points:
--taker-bps 0.5 charges 0.5 bps (0.005%), and a negative --maker-bps models a
maker rebate.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import List, Optional

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from python_analytics.backtesting.replay import BulkReplayer  # noqa: E402
from python_analytics.backtesting.report import export_report  # noqa: E402
from python_analytics.costs import DEFAULT_MAKER_BPS, DEFAULT_TAKER_BPS  # noqa: E402
from python_analytics.strategies import (  # noqa: E402
    AvellanedaStoikovStrategy,
    InventoryMarketMaker,
    MarketConfig,
    MarketType,
)
from python_analytics.utils import configure_logging, load_settings  # noqa: E402

DEFAULT_SOURCE = Path(__file__).resolve().parents[1] / "data" / "sample_events.ndjson"


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", default=str(DEFAULT_SOURCE),
                        help="Phase 1 archive or glob (e.g. data/raw_l2/*.ndjson.gz)")
    parser.add_argument("--tick-size", type=str, default=None)
    parser.add_argument("--center", type=str, default=None)
    parser.add_argument("--strategy", default="avellaneda_stoikov",
                        choices=("avellaneda_stoikov", "inventory_market_maker", "none"))
    # Avellaneda-Stoikov parameters.
    parser.add_argument("--gamma", type=float, default=0.1)
    parser.add_argument("--sigma", type=float, default=0.02)
    parser.add_argument("--k", type=float, default=1.5)
    parser.add_argument("--time-horizon", type=float, default=60.0)
    parser.add_argument("--inventory-limit", type=float, default=5.0)
    parser.add_argument("--ofi-sensitivity", type=float, default=0.0)
    parser.add_argument("--no-time-spread", action="store_true",
                        help="disable the gamma*sigma^2*(T-t) spread term")
    # Fees / cash / sizing. Fee rates are fractional basis points, so they are
    # parsed as floats (0.5 => 0.5 bps = 0.005%); negative maker_bps is a rebate.
    # The default is the retail VIP 0 schedule: a maker *pays* 2 bps.
    parser.add_argument("--maker-bps", type=float, default=DEFAULT_MAKER_BPS,
                        help="maker fee in basis points; negative = rebate "
                             "(default: 2.0 = retail VIP 0, i.e. 0.02%% paid)")
    parser.add_argument("--taker-bps", type=float, default=DEFAULT_TAKER_BPS,
                        help="taker fee in basis points; fractions allowed "
                             "(default: 5.0, i.e. 0.05%%); e.g. 0.5 = 0.005%%")
    parser.add_argument("--initial-cash", type=float, default=100000.0,
                        help="starting cash for drawdown/return metrics "
                             "(default: 100000.0)")
    parser.add_argument("--order-quantity", type=str, default="0.01",
                        help="base-asset quantity per quote (default: 0.01)")
    # Replay controls.
    parser.add_argument("--batch-size", type=int, default=64)
    parser.add_argument("--frames-per-hook", type=int, default=1)
    parser.add_argument("--fill-model", default="touch",
                        choices=("touch", "queue", "trade"),
                        help="how a resting maker order earns a fill (default: touch, "
                             "the legacy book-crossing model)")
    parser.add_argument("--event-limit", type=int, default=0)
    # Reporting.
    parser.add_argument("--report", type=str, default=None,
                        help="output directory for summary.json/fills.csv/equity.png "
                             "(a .json file path is accepted and resolved to its parent)")
    parser.add_argument("--plot", action="store_true",
                        help="write PNG charts (requires matplotlib)")
    parser.add_argument("--log-level", default="INFO")
    return parser.parse_args(argv)


def build_strategy(args: argparse.Namespace, tick_size: str, center: str):
    if args.strategy == "none":
        return None
    if args.strategy == "inventory_market_maker":
        from python_analytics.utils import StrategySettings  # local import
        settings = StrategySettings(
            half_spread_ticks=2, order_quantity=args.order_quantity,
            max_inventory=str(args.inventory_limit), skew_ticks=1,
        )
        return InventoryMarketMaker(settings)

    config = MarketConfig(
        gamma=args.gamma,
        sigma=args.sigma,
        k=args.k,
        time_horizon=args.time_horizon,
        inventory_limit=args.inventory_limit,
        tick_size=float(tick_size),
        market_type=MarketType.CRYPTO,
        order_quantity=float(args.order_quantity),
    )
    return AvellanedaStoikovStrategy(
        config,
        ofi_sensitivity=args.ofi_sensitivity,
        use_time_spread=not args.no_time_spread,
    )


def run(args: argparse.Namespace) -> int:
    settings = load_settings()
    tick_size = args.tick_size or settings.engine.tick_size
    center = args.center or settings.engine.center

    strategy = build_strategy(args, tick_size, center)
    replayer = BulkReplayer(
        tick_size=tick_size,
        center=center,
        maker_bps=args.maker_bps,
        taker_bps=args.taker_bps,
        initial_cash=args.initial_cash,
        batch_size=args.batch_size,
        frames_per_hook=args.frames_per_hook,
        order_quantity=args.order_quantity,
        fill_model=args.fill_model,
    )

    if any(char in args.source for char in "*?["):
        summary = replayer.replay_archives(args.source, strategy, args.event_limit)
    else:
        summary = replayer.replay([args.source], strategy, args.event_limit)

    stats = summary.stats
    print("=" * 78)
    for key in ("depth_events", "gap_events", "fills", "round_trips",
                "orders_submitted", "orders_rejected"):
        print(f"  {key:<20}: {stats.get(key)}")
    print(f"  {'throughput':<20}: {stats['throughput_fps']:,.1f} frames/s")
    print(f"  {'realized_pnl':<20}: {stats['realized_pnl']:+,.4f}")
    print(f"  {'unrealized_pnl':<20}: {stats['unrealized_pnl']:+,.4f}")
    print(f"  {'gross_pnl':<20}: {stats['gross_pnl']:+,.4f}")
    print(f"  {'fees_paid':<20}: {stats['fees_paid']:+,.4f}")
    print(f"  {'sharpe_ratio':<20}: {stats['sharpe_ratio']:,.3f}")
    print(f"  {'sortino_ratio':<20}: {stats['sortino_ratio']:,.3f}")
    print(f"  {'max_drawdown_pct':<20}: {stats['max_drawdown_pct']:,.2f} %")
    print(f"  {'win_rate':<20}: {stats['win_rate']:,.2%}")
    print(f"  {'profit_factor':<20}: {stats['profit_factor']:,.3f}")
    print(f"  {'fill_rate_pct':<20}: {stats['fill_rate_pct']:,.2f} %")
    print(f"  {'avg_holding_time_s':<20}: {stats['avg_holding_time_s']:,.4f}")
    print(f"  {'inventory_std':<20}: {stats['inventory_std']:,.4f}")
    latency = stats["latency_ns"]
    print(f"  {'engine_p50/p99/p999':<20}: {latency['engine_p50_ns']} / "
          f"{latency['engine_p99_ns']} / {latency['engine_p999_ns']} ns")
    print("=" * 78)

    if args.report:
        # Accept both a directory and a file path (e.g. ".../summary.json"):
        # a ".json" target always resolves to its parent directory so artefacts
        # never end up nested under a directory named after the file.
        report_target = Path(args.report)
        report_dir = report_target.parent if report_target.suffix == ".json" \
            else report_target
        artefacts = export_report(
            report_dir,
            summary=summary.as_dict(),
            fills=summary.fills,
            equity_curve=summary.equity_curve,
            grid=None,
            plot=args.plot,
        )
        print("\nwrote artefacts:")
        for name, path in artefacts.items():
            print(f"  {name:<12}: {path}")
    return 0


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    configure_logging(args.log_level)
    return run(args)


if __name__ == "__main__":
    raise SystemExit(main())
