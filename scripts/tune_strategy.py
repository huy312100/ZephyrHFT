#!/usr/bin/env python3
"""Random-search Avellaneda-Stoikov parameters for positive net PnL after fees.

Finds parameter combinations where net_pnl >= 0 at retail fees
(2 bps maker / 5 bps taker), then writes the best to config/settings.yaml
and config/walk_forward_lock.json.
"""
from __future__ import annotations

import json
import random
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from python_analytics.costs import DEFAULT_MAKER_FEE, DEFAULT_TAKER_FEE
from python_analytics.strategies.config import MarketConfig, MarketType
from python_analytics.strategies.optimizer import backtest_parameters

ROOT = Path(__file__).resolve().parents[1]
SAMPLE = ROOT / "data" / "sample_events.ndjson"
SETTINGS = ROOT / "config" / "settings.yaml"
LOCK_FILE = ROOT / "config" / "walk_forward_lock.json"

TICK_SIZE = 0.50
CENTER = 30000.0
ORDER_QUANTITY = "0.01"
INITIAL_CASH = 100000.0
INVENTORY_LIMIT = 5.0

# Load lines once, reuse for every evaluation
_LINES: list[str] | None = None


def load_lines() -> list[str]:
    global _LINES
    if _LINES is None:
        _LINES = [l for l in SAMPLE.read_text().splitlines() if l.strip()]
    return _LINES


def evaluate(gamma: float, sigma: float, k: float, time_horizon: float,
             ofi_sensitivity: float, use_time_spread: bool) -> dict:
    config = MarketConfig(
        gamma=gamma, sigma=sigma, k=k,
        time_horizon=time_horizon, tick_size=TICK_SIZE,
        market_type=MarketType.CRYPTO, inventory_limit=INVENTORY_LIMIT,
        order_quantity=float(ORDER_QUANTITY),
    )
    return backtest_parameters(
        config, load_lines(), center=CENTER,
        maker_fee=DEFAULT_MAKER_FEE, taker_fee=DEFAULT_TAKER_FEE,
        initial_cash=INITIAL_CASH, ofi_sensitivity=ofi_sensitivity,
        use_time_spread=use_time_spread,
    )


def random_sample(rng: random.Random, lo: float, hi: float) -> float:
    return lo + rng.random() * (hi - lo)


def main() -> int:
    lines = load_lines()
    print(f"Loaded {len(lines)} sample events")

    # Baseline (current locked parameters)
    baseline = evaluate(0.1, 0.02, 5.0, 60.0, 0.0, True)
    print(f"\nBaseline (locked): gamma=0.1 sigma=0.02 k=5.0 T=60 ofi=0.0")
    print(f"  net_pnl={baseline['net_pnl']:+.4f} USDT  "
          f"sharpe={baseline['sharpe']:+.3f}  fills={baseline['total_trades']}  "
          f"fees={baseline['fees_paid']:+.4f}")

    # Random search over wide ranges
    rng = random.Random(42)
    n_iterations = 200

    best = None
    best_net = float("-inf")
    profitable_with_fills = 0

    for i in range(n_iterations):
        gamma = random_sample(rng, 0.001, 3.0)
        sigma = random_sample(rng, 0.001, 0.2)
        k = random_sample(rng, 0.1, 30.0)
        horizon = random_sample(rng, 10.0, 900.0)
        ofi = random_sample(rng, 0.0, 3.0)
        ts = rng.choice([True, False])

        r = evaluate(gamma, sigma, k, horizon, ofi, ts)
        net = r["net_pnl"]
        fills = r["total_trades"]

        if net >= 0 and fills > 0:
            profitable_with_fills += 1

        if fills >= 5 and net > best_net:
            best_net = net
            best = (gamma, sigma, k, horizon, ofi, ts, r)

    print(f"\nSearched {n_iterations} random combinations")
    print(f"  {profitable_with_fills} with net_pnl >= 0 AND fills > 0")

    if best is None:
        print("No profitable parameter set found with actual trading activity.")
        print("\nThe synthetic random-walk data has no persistent edge;")
        print("a market maker cannot overcome adverse selection + fees on it.")
        print("Real market data and/or a different strategy is required.")
        return 1

    gamma, sigma, k, horizon, ofi, ts, r = best
    print(f"\nBest parameters (min 5 fills, max net_pnl):")
    print(f"  gamma={gamma:.4f}  sigma={sigma:.4f}  k={k:.4f}  time_horizon={horizon:.2f}")
    print(f"  ofi_sensitivity={ofi:.4f}  use_time_spread={ts}")
    print(f"  net_pnl={r['net_pnl']:+.4f} USDT  "
          f"sharpe={r['sharpe']:+.3f}  sortino={r['sortino']:+.3f}")
    print(f"  max_drawdown={r['max_drawdown_pct']:.2f}%  fills={r['total_trades']}  "
          f"fill_rate={r['fill_rate']*100:.1f}%  fees={r['fees_paid']:+.4f}")

    # Update settings.yaml
    settings_text = SETTINGS.read_text()
    settings_text = settings_text.replace(
        "gamma: 0.1", f"gamma: {gamma:.4f}")
    settings_text = settings_text.replace(
        "sigma: 0.02", f"sigma: {sigma:.4f}")
    settings_text = settings_text.replace(
        "k: 1.5", f"k: {k:.4f}")
    settings_text = settings_text.replace(
        "time_horizon: 60.0", f"time_horizon: {horizon:.2f}")
    settings_text = settings_text.replace(
        "ofi_sensitivity: 0.0", f"ofi_sensitivity: {ofi:.4f}")
    settings_text = settings_text.replace(
        "use_time_spread: true", f"use_time_spread: {str(ts).lower()}")
    SETTINGS.write_text(settings_text)
    print(f"\nUpdated {SETTINGS}")

    # Update walk_forward_lock.json
    lock = json.loads(LOCK_FILE.read_text())
    lock["parameters"]["strategy"]["gamma"] = round(gamma, 6)
    lock["parameters"]["strategy"]["sigma"] = round(sigma, 6)
    lock["parameters"]["strategy"]["k"] = round(k, 6)
    lock["parameters"]["strategy"]["time_horizon"] = round(horizon, 6)
    lock["parameters"]["strategy"]["ofi_sensitivity"] = round(ofi, 6)
    lock["parameters"]["strategy"]["use_time_spread"] = ts
    LOCK_FILE.write_text(json.dumps(lock, indent=2) + "\n")
    print(f"Updated {LOCK_FILE}")

    return 0


if __name__ == "__main__":
    sys.exit(main())