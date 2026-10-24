#!/usr/bin/env python3
"""Asynchronous local paper-trading simulator for the Avellaneda-Stoikov maker.

Runs a 60-second (configurable) live/mock simulation: a market-data feed updates
a C++ ``L2Book``, the ``PaperMatchingEngine`` matches resting quotes and tracks
PnL, and the ``AvellanedaStoikovStrategy`` re-quotes on every tick. A compact
status table is printed periodically and a summary report is emitted on exit.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import List, Optional

# Make the repository root importable when this script is run directly.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from python_analytics.native import hft_native
from python_analytics.native.factory import build_risk_manager
from python_analytics.simulator.paper_engine import (
    MarketDataFeed,
    PaperMatchingEngine,
)
from python_analytics.strategies import (
    AvellanedaStoikovStrategy,
    MarketConfig,
    MarketType,
    MicroPriceCalculator,
)
from python_analytics.utils import configure_logging, load_settings

DEFAULT_WS_URL = "wss://stream.binance.com:9443/ws/btcusdt@depth20@100ms"
DEFAULT_NDJSON = Path(__file__).resolve().parents[1] / "data" / "sample_events.ndjson"


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", default=DEFAULT_WS_URL,
                        help="WebSocket URL, NDJSON file path, or 'mock'")
    parser.add_argument("--offline", action="store_true",
                        help="force the offline NDJSON fallback feed")
    parser.add_argument("--duration", type=float, default=60.0,
                        help="simulation duration in seconds")
    parser.add_argument("--interval", type=float, default=1.0,
                        help="dashboard refresh interval in seconds")
    parser.add_argument("--replay-delay", type=float, default=0.05,
                        help="delay between replayed/synthetic ticks")
    parser.add_argument("--gamma", type=float, default=0.1)
    parser.add_argument("--sigma", type=float, default=0.02)
    parser.add_argument("--k", type=float, default=1.5)
    parser.add_argument("--inventory-limit", type=float, default=5.0)
    parser.add_argument("--order-quantity", type=float, default=0.05)
    parser.add_argument("--time-horizon", type=float, default=60.0)
    parser.add_argument("--tick-size", type=str, default=None)
    parser.add_argument("--center", type=str, default=None)
    parser.add_argument("--maker-fee-bps", type=float, default=-1.0)
    parser.add_argument("--taker-fee-bps", type=float, default=5.0)
    parser.add_argument("--initial-cash", type=float, default=0.0)
    parser.add_argument("--self-trade-prevention", default="cancel_resting",
                        choices=("none", "cancel_resting", "reject"),
                        help="treatment of a quote crossing our own resting quote")
    parser.add_argument("--trades-out", default=None,
                        help="optional path to dump the trade history as JSON")
    parser.add_argument("--log-level", default="INFO")
    return parser.parse_args(argv)


def build_market_config(args: argparse.Namespace, tick_size: float) -> MarketConfig:
    return MarketConfig(
        gamma=args.gamma,
        sigma=args.sigma,
        k=args.k,
        inventory_limit=args.inventory_limit,
        tick_size=tick_size,
        market_type=MarketType.CRYPTO,
        time_horizon=args.time_horizon,
        order_quantity=args.order_quantity,
    )


def _fmt_order(order) -> str:
    if order is None:
        return "-"
    return f"{order.side} {order.price:,.6f} x {order.remaining:,.6f}"


def render_dashboard(engine: PaperMatchingEngine,
                     strategy: AvellanedaStoikovStrategy,
                     config: MarketConfig,
                     elapsed: float) -> None:
    bbo = engine.bbo()
    best_bid = float(bbo.bid_price) if bbo.is_valid() else 0.0
    best_ask = float(bbo.ask_price) if bbo.is_valid() else 0.0
    micro = 0.0
    if bbo.is_valid():
        micro = MicroPriceCalculator.micro_price(
            best_bid, float(bbo.bid_quantity), best_ask, float(bbo.ask_quantity)
        )
    remaining = max(0.0, config.time_horizon - elapsed)
    reservation = strategy.reservation_price(micro, engine.inventory, remaining)

    lines = [
        "=" * 78,
        f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}  elapsed {elapsed:6.1f}s",
        "-" * 78,
        f"  Best Bid      : {best_bid:,.6f}",
        f"  Best Ask      : {best_ask:,.6f}",
        f"  Micro-Price   : {micro:,.6f}",
        f"  Reservation   : {reservation:,.6f}",
        f"  Position (q)  : {engine.inventory:+,.6f}",
        f"  Active Quotes : bid {_fmt_order(engine.active_bid)}"
        f" | ask {_fmt_order(engine.active_ask)}",
        f"  Trades        : {len(engine.trades_history)} executed"
        f" | self-match prevented {engine.self_trade_prevented}",
        f"  Cumulative PnL: {engine.total_pnl:+,.6f} "
        f"(realized {engine.realized_pnl:+,.6f}, unrealized {engine.unrealized_pnl:+,.6f})",
        "-" * 78,
    ]
    print("\n".join(lines))


def render_summary(engine: PaperMatchingEngine, elapsed: float) -> None:
    summary = engine.summary()
    print()
    print("=" * 78)
    print("  PAPER TRADING SUMMARY")
    print("=" * 78)
    print(f"  Duration        : {elapsed:.1f}s")
    print(f"  Total Trades    : {summary['total_trades']}")
    print(f"  Win Rate        : {summary['win_rate'] * 100:.1f}%")
    print(f"  Sharpe (approx) : {summary['sharpe']:.3f}")
    print(f"  Final PnL       : {summary['total_pnl']:+,.6f}")
    print(f"    realized      : {summary['realized_pnl']:+,.6f}")
    print(f"    unrealized    : {summary['unrealized_pnl']:+,.6f}")
    print(f"  Max Drawdown    : {summary['max_drawdown'] * 100:.2f}%"
          f"  ({summary['max_drawdown_abs']:,.6f} absolute)")
    print(f"  Total Fees      : {summary['total_fees']:+,.6f}")
    print(f"  Final Position  : {summary['inventory']:+,.6f}")
    print(f"  Cash Balance    : {summary['cash']:+,.6f}")
    print(f"  Self-Match Stop : {summary['self_trade_prevented']}")
    risk = engine.risk_manager()
    if risk is not None:
        stats = risk.stats()
        print(f"  Pre-Trade Guard : {stats.rejected} rejected / {stats.evaluated} checked"
              f"  (tokens left: {stats.tokens_available:.1f})")

    recent = engine.trades_history[-10:]
    if recent:
        print("-" * 78)
        print("  RECENT TRADES  (timestamp, side, price, size, realized PnL)")
        print("-" * 78)
        for trade in recent:
            if trade["exchange_ts"] > 0:
                stamp = datetime.fromtimestamp(
                    trade["timestamp"]).strftime("%H:%M:%S.%f")[:-3]
            else:
                stamp = f"{trade['timestamp']:.3f}"
            print(f"  {stamp}  {trade['side']:<4}  {trade['price']:>12,.6f}"
                  f"  {trade['size']:>10,.6f}  {trade['realized_pnl']:+10,.6f}")
    print("=" * 78)


async def run(args: argparse.Namespace) -> None:
    settings = load_settings()
    tick_size_text = args.tick_size or settings.engine.tick_size
    center_text = args.center or settings.engine.center
    tick_size = float(hft_native.Price.from_string(tick_size_text))

    book = hft_native.L2Book(
        hft_native.Price.from_string(tick_size_text),
        hft_native.Price.from_string(center_text),
    )
    engine = PaperMatchingEngine(
        book,
        maker_fee=args.maker_fee_bps / 10_000.0,
        taker_fee=args.taker_fee_bps / 10_000.0,
        initial_cash=args.initial_cash,
        self_trade_prevention=args.self_trade_prevention,
        risk_manager=(
            build_risk_manager(settings) if settings.engine.pretrade.enabled else None
        ),
        symbol=settings.engine.symbol,
    )
    config = build_market_config(args, tick_size)
    strategy = AvellanedaStoikovStrategy(config)

    def handle_depth(update) -> None:
        engine.on_market_tick()
        strategy.on_depth(engine, update)

    source = str(DEFAULT_NDJSON) if args.offline else args.source
    feed = MarketDataFeed(
        source,
        book,
        handle_depth,
        loop=not source.startswith(("ws://", "wss://")),
        replay_delay=args.replay_delay,
        fallback=str(DEFAULT_NDJSON),
    )

    feed_task = asyncio.create_task(feed.run())
    started = time.monotonic()
    next_report = started
    try:
        while True:
            await asyncio.sleep(0.05)
            now = time.monotonic()
            elapsed = now - started
            if elapsed >= args.duration:
                break
            if now >= next_report:
                render_dashboard(engine, strategy, config, elapsed)
                next_report = now + args.interval
    finally:
        feed.stop()
        feed_task.cancel()
        try:
            await feed_task
        except (asyncio.CancelledError, Exception):
            pass
        strategy.cancel_all(engine)
        engine.cancel_all()
        render_summary(engine, time.monotonic() - started)
        if args.trades_out:
            out_path = Path(args.trades_out)
            out_path.write_text(json.dumps(engine.trades_history, indent=2),
                                encoding="utf-8")
            print(f"  Trade history   : {len(engine.trades_history)} rows"
                  f" -> {out_path}")


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    configure_logging(args.log_level)
    try:
        asyncio.run(run(args))
    except KeyboardInterrupt:
        print("\nInterrupted by user; shutting down cleanly.")
        return 130
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
