#!/usr/bin/env python3
"""Command-line entry point for the HFT engine demo and replay runner."""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from typing import Iterable, List, Optional

try:
    from python_analytics.backtesting import replay_lines
    from python_analytics.native import hft_native
    from python_analytics.native.factory import build_engine
    from python_analytics.strategies import InventoryMarketMaker
    from python_analytics.utils import configure_logging, load_settings
    from python_analytics.utils.config import AppSettings
except ImportError as exc:  # pragma: no cover - exercised on a broken install
    sys.stderr.write(
        "Unable to import the native extension or analytics package.\n"
        "Build it first:\n"
        "  cmake -S cpp_engine -B build \\\n"
        "    -Dpybind11_DIR=\"$(python -m pybind11 --cmakedir)\"\n"
        "  cmake --build build --parallel\n"
        f"Original error: {exc}\n"
    )
    raise SystemExit(2) from exc


def demo_events() -> Iterable[str]:
    """Small deterministic stream used by ``--demo``."""
    yield (
        '{"e":"depthUpdate","E":1700000000000,"U":1,"u":1,"s":"BTCUSDT",'
        '"b":[["29999.50","1.5"],["29999.00","2.0"]],'
        '"a":[["30000.50","2.5"],["30001.00","1.0"]]}'
    )
    yield (
        '{"e":"depthUpdate","E":1700000000100,"U":2,"u":2,"s":"BTCUSDT",'
        '"b":[["29999.50","2.0"]],"a":[["30000.50","3.0"]]}'
    )
    yield '{"e":"trade","E":1700000000200,"T":1700000000201,"p":"30000.25","q":"0.10","m":false}'


def run_demo(settings: AppSettings) -> dict:
    events = list(demo_events())
    update = hft_native.parse_depth_json(events[0])
    engine = build_engine(settings)
    engine.apply_depth(update)
    strategy = InventoryMarketMaker(settings.strategy)
    strategy.on_depth(engine, update)

    # Demonstrate a taker fill against the best ask.
    request = hft_native.OrderRequest()
    request.side = hft_native.Side.BUY
    request.type = hft_native.OrderType.MARKET
    request.quantity = hft_native.Quantity.from_string(settings.strategy.order_quantity)
    request.timestamp = time.monotonic_ns()
    ack = engine.submit(request)

    fills = []
    while True:
        fill = engine.drain_fill()
        if fill is None:
            break
        fills.append(
            {
                "order_id": fill.order_id,
                "price": str(fill.price),
                "quantity": str(fill.quantity),
                "side": str(fill.side),
            }
        )

    result = replay_lines(events, settings, strategy=None)
    strategy.cancel_all(engine)
    metrics = engine.metrics()
    return {
        "demo": {
            "taker_accepted": ack.accepted(),
            "taker_reason": str(ack.reason),
            "taker_filled": str(ack.filled),
            "taker_avg_price": str(ack.avg_price),
            "fills": fills,
            "position": str(engine.position().net_quantity),
            "average_entry_price": str(engine.position().average_entry_price),
            "realized_pnl": str(engine.position().realized_pnl),
            "unrealized_pnl": str(engine.position().unrealized_pnl),
        },
        "strategy": strategy.snapshot(),
        "replay": result.as_dict(),
        "engine_metrics": {
            "depth_events": metrics.depth_events,
            "orders_submitted": metrics.orders_submitted,
            "orders_cancelled": metrics.orders_cancelled,
            "open_orders": metrics.open_orders,
            "latency_p50_ns": metrics.p50_ns,
            "latency_p99_ns": metrics.p99_ns,
            "book_valid": engine.bbo().is_valid(),
            "bid": str(engine.bbo().bid_price),
            "ask": str(engine.bbo().ask_price),
        },
    }


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=None,
                        help="YAML settings path (default: config/settings.yaml)")
    parser.add_argument("--events", type=Path, default=None,
                        help="NDJSON file of exchange messages to replay")
    parser.add_argument("--limit", type=int, default=None,
                        help="maximum number of messages to process")
    parser.add_argument("--demo", action="store_true",
                        help="run the built-in deterministic demo")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args(argv)

    configure_logging(args.log_level)
    try:
        settings = load_settings(args.config)
    except Exception as exc:
        sys.stderr.write(f"configuration error: {exc}\n")
        return 2

    if args.events is not None:
        from python_analytics.backtesting import replay_file

        strategy = InventoryMarketMaker(settings.strategy)
        result = replay_file(args.events, settings, strategy, args.limit)
        print(json.dumps(result.as_dict(), indent=2, default=str))
        return 0

    # Demo mode is the default when no event file is supplied.
    print(json.dumps(run_demo(settings), indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
