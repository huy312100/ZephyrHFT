#!/usr/bin/env python3
"""Phase 2 live-forward paper trading: Phase 1 NDJSON -> C++ paper engine.

Streams a Phase 1 archive (``.ndjson.gz`` or ``.ndjson``) through the C++
``paper_engine`` and submits alternating taker orders so slippage, fees, PnL
and per-frame latency are all observable in real time.
"""
from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path
from typing import List, Optional

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from python_analytics.bridge import PaperSession, feed_archive  # noqa: E402
from python_analytics.native import hft_native  # noqa: E402
from python_analytics.utils import configure_logging, load_settings  # noqa: E402

DEFAULT_NDJSON = Path(__file__).resolve().parents[1] / "data" / "sample_events.ndjson"


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", default=str(DEFAULT_NDJSON),
                        help="Phase 1 archive (.ndjson.gz or .ndjson)")
    parser.add_argument("--event-limit", type=int, default=0,
                        help="stop after N depth events (0 = whole file)")
    parser.add_argument("--batch-size", type=int, default=64,
                        help="lines per C++ bulk-ingest batch")
    parser.add_argument("--tick-size", type=str, default=None)
    parser.add_argument("--center", type=str, default=None)
    parser.add_argument("--maker-fee-bps", type=float, default=0.0)
    parser.add_argument("--taker-fee-bps", type=float, default=5.0)
    parser.add_argument("--initial-cash", type=float, default=0.0)
    parser.add_argument("--order-quantity", type=str, default="0.05")
    parser.add_argument("--report-every", type=int, default=5,
                        help="print telemetry every N batches")
    parser.add_argument("--log-level", default="INFO")
    return parser.parse_args(argv)


def render(metrics: dict, frames: int, elapsed: float) -> None:
    rate = frames / elapsed if elapsed > 0 else 0.0
    print(
        f"frames={frames:>6}  rate={rate:6.1f}/s  "
        f"us/frame(p50={metrics['p50_ns'] / 1000:7.2f} "
        f"p99={metrics['p99_ns'] / 1000:7.2f})  "
        f"cash={metrics['cash']:>12,.2f}  equity={metrics['equity']:>12,.2f}  "
        f"net={metrics['net_quantity']:+.4f}  "
        f"slippage={metrics['slippage_total']:,.4f}  gaps={metrics['gap_events']}"
    )


def run(args: argparse.Namespace) -> int:
    settings = load_settings()
    tick_size = args.tick_size or settings.engine.tick_size
    center = args.center or settings.engine.center

    session = PaperSession.create(
        tick_size=tick_size,
        center=center,
        maker_bps=int(args.maker_fee_bps),
        taker_bps=int(args.taker_fee_bps),
        initial_cash=args.initial_cash,
    )

    state = {"frames": 0, "batches": 0, "buy_next": True, "started": time.monotonic()}

    def on_batch(batch: dict) -> None:
        state["batches"] += 1
        state["frames"] += batch["depth"]
        side = hft_native.Side.BUY if state["buy_next"] else hft_native.Side.SELL
        state["buy_next"] = not state["buy_next"]
        result = session.market(side, args.order_quantity)
        if not result["accepted"]:
            print(f"  reject: {result['reason']}", file=sys.stderr)
        if state["batches"] % args.report_every == 0:
            render(session.metrics(), state["frames"],
                   time.monotonic() - state["started"])
        if args.event_limit and state["frames"] >= args.event_limit:
            raise StopIteration

    try:
        feed_archive(session.engine, Path(args.source),
                     batch_size=args.batch_size, on_batch=on_batch)
    except StopIteration:
        pass

    metrics = session.metrics()
    render(metrics, state["frames"], time.monotonic() - state["started"])
    print("=" * 78)
    print("  fills            :", metrics["fills"])
    print("  filled qty       :", metrics["filled_quantity"])
    print("  traded notional  :", metrics["traded_notional"])
    print("  slippage total   :", metrics["slippage_total"])
    print("  fees paid        :", metrics["fees_paid"])
    print("  realized pnl     :", metrics["realized_pnl"])
    print("  unrealized pnl   :", metrics["unrealized_pnl"])
    print("  equity           :", metrics["equity"])
    print("  net position     :", metrics["net_quantity"])
    print("  latency p99      :", f"{metrics['p99_ns'] / 1000:.2f} us")
    print("=" * 78)
    return 0


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    configure_logging(args.log_level)
    return run(args)


if __name__ == "__main__":
    raise SystemExit(main())
