#!/usr/bin/env python3
"""High-precision tick-to-quote latency benchmark for the ZephyrHFT hot path.

Measures, in microseconds, the end-to-end latency from an L2 depth update
entering the Python boundary through book application, Avellaneda-Stoikov quote
generation, and C++ RiskGuard validation (via a post-only order submission).
Runs a 50,000-tick stress loop with GC disabled and reports p50/p99/p99.9.
"""
from __future__ import annotations

import argparse
import gc
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from python_analytics.native import hft_native
from python_analytics.native.factory import build_engine
from python_analytics.simulator.paper_engine import (
    parse_depth_message,
    synthetic_depth_messages,
)
from python_analytics.strategies import (
    AvellanedaStoikovStrategy,
    MarketConfig,
    MarketType,
)
from python_analytics.utils import configure_logging, load_settings


def percentile(values: List[float], p: float) -> float:
    """Linear-interpolation percentile; empty input returns 0.0."""
    if not values:
        return 0.0
    ordered = sorted(values)
    if p <= 0.0:
        return ordered[0]
    if p >= 1.0:
        return ordered[-1]
    rank = (len(ordered) - 1) * p
    lower = int(rank)
    upper = lower + 1
    weight = rank - lower
    return ordered[lower] * (1.0 - weight) + ordered[upper] * weight


def _stats(values: List[float]) -> Dict[str, float]:
    if not values:
        return {"n": 0, "p50": 0.0, "p99": 0.0, "p999": 0.0,
                "mean": 0.0, "max": 0.0, "min": 0.0}
    return {
        "n": len(values),
        "p50": percentile(values, 0.50),
        "p99": percentile(values, 0.99),
        "p999": percentile(values, 0.999),
        "mean": sum(values) / len(values),
        "max": max(values),
        "min": min(values),
    }


def _submit_risk_check(engine: "hft_native.TradingEngine",
                       side: "hft_native.Side", price_raw: int,
                       quantity_raw: int, now_ns: int) -> "hft_native.OrderAck":
    """Submits a post-only order (running C++ RiskGuard) and cancels it."""
    request = hft_native.OrderRequest()
    request.side = side
    request.type = hft_native.OrderType.POST_ONLY
    request.price = hft_native.Price.from_raw(price_raw)
    request.quantity = hft_native.Quantity.from_raw(quantity_raw)
    request.timestamp = now_ns
    ack = engine.submit(request)
    if ack.order_id:
        engine.cancel(ack.order_id)
    return ack


def run_benchmark(ticks: int = 50000, seed: int = 0,
                  config: Optional[MarketConfig] = None) -> Dict[str, object]:
    """Runs the stress loop and returns a latency report dictionary."""
    settings = load_settings()
    engine = build_engine(settings)
    if config is None:
        config = MarketConfig(
            gamma=0.1, sigma=0.02, k=1.5, tick_size=0.5,
            market_type=MarketType.CRYPTO, time_horizon=60.0,
        )
    strategy = AvellanedaStoikovStrategy(config)

    mid = float(settings.engine.center)
    tick = float(settings.engine.tick_size)
    messages = synthetic_depth_messages(mid=mid, tick=tick, seed=seed)

    totals: List[float] = []
    parse_us: List[float] = []
    apply_us: List[float] = []
    quote_us: List[float] = []
    risk_us: List[float] = []
    rejections = 0

    # Disable GC for the duration of the measurement so the critical pricing
    # path never suffers a stop-the-world collection pause.
    gc.disable()
    try:
        for _ in range(ticks):
            text = next(messages)

            t0 = time.perf_counter_ns()
            update = parse_depth_message(text)
            t1 = time.perf_counter_ns()

            if update is not None:
                engine.apply_depth(update)
            t2 = time.perf_counter_ns()

            quote = strategy.quote(engine)
            t3 = time.perf_counter_ns()

            now_ns = time.monotonic_ns()
            if quote is not None:
                if quote.bid_quantity_raw > 0:
                    ack = _submit_risk_check(
                        engine, hft_native.Side.BUY, quote.bid_price_raw,
                        quote.bid_quantity_raw, now_ns,
                    )
                    if not ack.accepted():
                        rejections += 1
                if quote.ask_quantity_raw > 0:
                    ack = _submit_risk_check(
                        engine, hft_native.Side.SELL, quote.ask_price_raw,
                        quote.ask_quantity_raw, now_ns,
                    )
                    if not ack.accepted():
                        rejections += 1
            t4 = time.perf_counter_ns()

            totals.append((t4 - t0) / 1000.0)
            parse_us.append((t1 - t0) / 1000.0)
            apply_us.append((t2 - t1) / 1000.0)
            quote_us.append((t3 - t2) / 1000.0)
            risk_us.append((t4 - t3) / 1000.0)
    finally:
        gc.enable()

    gc_counts = gc.get_count()
    gc.collect()
    return {
        "ticks": ticks,
        "total_us": _stats(totals),
        "parse_us": _stats(parse_us),
        "apply_us": _stats(apply_us),
        "quote_us": _stats(quote_us),
        "risk_us": _stats(risk_us),
        "risk_rejections": rejections,
        "gc_counts_after": list(gc_counts),
    }


def _fmt_hist(name: str, stats: Dict[str, float]) -> str:
    return (
        f"  {name:<10} p50={stats['p50']:9.3f}us  p99={stats['p99']:9.3f}us  "
        f"p99.9={stats['p999']:9.3f}us  mean={stats['mean']:9.3f}us  "
        f"max={stats['max']:9.3f}us"
    )


def print_report(report: Dict[str, object]) -> None:
    print("=" * 80)
    print("  LATENCY BENCHMARK REPORT  (tick -> quote -> C++ RiskGuard)")
    print("=" * 80)
    print(f"  ticks processed     : {report['ticks']}")
    print(f"  risk rejections     : {report['risk_rejections']}")
    print(_fmt_hist("total", report["total_us"]))
    print(_fmt_hist("parse", report["parse_us"]))
    print(_fmt_hist("apply", report["apply_us"]))
    print(_fmt_hist("quote", report["quote_us"]))
    print(_fmt_hist("risk", report["risk_us"]))
    print("-" * 80)
    print("  GC pause analysis   : GC disabled during the measured loop.")
    print(f"  pending collections : {report['gc_counts_after']} (gen0/gen1/gen2)")
    print("  hot path            : C++ allocation-free; Python allocates only")
    print("                         transient quote/ack objects per tick.")
    print("=" * 80)


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ticks", type=int, default=50000,
                        help="number of depth ticks in the stress loop")
    parser.add_argument("--seed", type=int, default=0,
                        help="seed for the synthetic market data")
    parser.add_argument("--log-level", default="WARNING")
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    configure_logging(args.log_level)
    if args.ticks <= 0:
        print("error: --ticks must be positive", file=sys.stderr)
        return 2
    print_report(run_benchmark(ticks=args.ticks, seed=args.seed))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

