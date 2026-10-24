#!/usr/bin/env python3
"""Phase 4 walk-forward runner: OOS validation under a frozen parameter set.

Replays a chronological list of Phase 1 archives (or fixed-size event windows
inside them) through the C++ paper engine, one fresh strategy per window, all
of them built from a single immutable :class:`LockedParameters`. Nothing is
re-optimised between windows, so the resulting PnL/Sharpe/drawdown are true
out-of-sample numbers.

Guarantees enforced before any number is reported:

* **Parameter lock** — every window's strategy is read back before and after
  the replay and compared with the expected lock; drift aborts the run.
  ``--lock-file`` pins the run to a previously persisted fit and refuses to
  start if the CLI disagrees with it.
* **No look-ahead** — a window only ever receives its own slice, the ingested
  line count must match, and exchange timestamps must not go backwards.
* **Ledger coherence** — ``end - start`` equity must equal realized +
  unrealized PnL to fixed-point tolerance.

Exit codes: ``0`` success, ``1`` ``--min-sharpe``/``--min-pnl`` gate failed,
``2`` usage / parameter-lock / missing-input error, ``3`` validation violation.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import List, Optional

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from python_analytics.backtesting.walk_forward import (  # noqa: E402
    LockedParameters,
    LookAheadError,
    ParameterLockError,
    WalkForwardError,
    WalkForwardRunner,
    read_lock_file,
    write_lock_file,
)
from python_analytics.costs import DEFAULT_MAKER_BPS, DEFAULT_TAKER_BPS  # noqa: E402
from python_analytics.utils import configure_logging, load_settings  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SOURCE = ROOT / "data" / "sample_events.ndjson"
DEFAULT_REPORT_DIR = ROOT / "reports"

EXIT_OK = 0
EXIT_GATE_FAILED = 1
EXIT_USAGE = 2
EXIT_INVALID = 3


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--source", nargs="+", default=[str(DEFAULT_SOURCE)],
                        help="archives in chronological order; globs are expanded "
                             "in sorted order (default: data/sample_events.ndjson)")
    parser.add_argument("--window-events", type=int, default=0,
                        help="events per window; 0 = one window per archive")
    # Frozen strategy parameters (must match --lock-file when one is supplied).
    parser.add_argument("--gamma", type=float, default=0.1)
    parser.add_argument("--sigma", type=float, default=0.02)
    parser.add_argument("--k", type=float, default=1.5)
    parser.add_argument("--time-horizon", type=float, default=60.0)
    parser.add_argument("--inventory-limit", type=float, default=5.0)
    parser.add_argument("--ofi-sensitivity", type=float, default=0.0)
    parser.add_argument("--ofi-decay", type=float, default=0.95)
    parser.add_argument("--ofi-cap", type=float, default=3.0)
    parser.add_argument("--no-time-spread", action="store_true",
                        help="disable the gamma*sigma^2*(T-t) spread term")
    parser.add_argument("--market-type", default="CRYPTO",
                        choices=("CRYPTO", "STOCK", "COMMODITIES",
                                 "crypto", "stock", "commodities"),
                        help="venue adapter class (phase 3 added it to "
                             "MarketConfig; the values must be real adapters)")
    parser.add_argument("--tick-size", type=str, default=None)
    parser.add_argument("--center", type=str, default=None)
    # Fees / cash / sizing: fractional basis points, negative maker = rebate.
    # Retail VIP 0 pays 2 bps as a maker, so that is the default.
    parser.add_argument("--maker-bps", type=float, default=DEFAULT_MAKER_BPS,
                        help="maker fee in fractional basis points "
                             "(default: 2.0 = retail VIP 0; negative = rebate)")
    parser.add_argument("--taker-bps", type=float, default=DEFAULT_TAKER_BPS,
                        help="taker fee in fractional basis points "
                             "(default: 5.0 = retail VIP 0)")
    parser.add_argument("--initial-cash", type=float, default=100000.0)
    parser.add_argument("--order-quantity", type=str, default="0.01")
    # Replay controls.
    parser.add_argument("--batch-size", type=int, default=64)
    parser.add_argument("--frames-per-hook", type=int, default=1)
    parser.add_argument("--periods-per-year", type=float, default=252.0,
                        help="annualisation factor for Sharpe/Sortino")
    parser.add_argument("--ledger-tolerance", type=float, default=1e-2,
                        help="max |equity change - realized - unrealized| per window")
    # Lock handling.
    parser.add_argument("--lock-file", type=str, default=None,
                        help="load a persisted lock and require the CLI to match it")
    parser.add_argument("--from-lock", action="store_true",
                        help="adopt the parameters stored in --lock-file instead "
                             "of restating them on the command line (used by CI "
                             "to replay the committed lock verbatim)")
    parser.add_argument("--write-lock", type=str, default=None,
                        help="persist the effective parameter lock to this path")
    # Reporting and gating.
    parser.add_argument("--report-dir", type=str, default=str(DEFAULT_REPORT_DIR),
                        help="directory for <stem>.json and <stem>.md")
    parser.add_argument("--stem", type=str, default="walk_forward")
    parser.add_argument("--no-report", action="store_true",
                        help="console output only (no artefacts written)")
    parser.add_argument("--min-sharpe", type=float, default=None,
                        help="exit 1 when the OOS Sharpe is below this value")
    parser.add_argument("--min-pnl", type=float, default=None,
                        help="exit 1 when net OOS PnL is below this value")
    parser.add_argument("--log-level", default="INFO")
    return parser.parse_args(argv)


def build_parameters(args: argparse.Namespace) -> LockedParameters:
    """Merges CLI flags with settings defaults into one immutable lock."""
    settings = load_settings()
    return LockedParameters(
        gamma=args.gamma,
        sigma=args.sigma,
        k=args.k,
        time_horizon=args.time_horizon,
        inventory_limit=args.inventory_limit,
        ofi_sensitivity=args.ofi_sensitivity,
        ofi_decay=args.ofi_decay,
        ofi_cap=args.ofi_cap,
        use_time_spread=not args.no_time_spread,
        tick_size=args.tick_size or settings.engine.tick_size,
        center=args.center or settings.engine.center,
        market_type=args.market_type.upper(),
        maker_bps=args.maker_bps,
        taker_bps=args.taker_bps,
        initial_cash=args.initial_cash,
        order_quantity=args.order_quantity,
    )


def run(args: argparse.Namespace) -> int:
    if args.from_lock and not args.lock_file:
        raise ValueError("--from-lock requires --lock-file")

    if args.from_lock:
        # The lock is the single source of truth: nothing on the command line
        # can widen a limit or re-tune a parameter between windows.
        parameters = read_lock_file(args.lock_file)
        lock_source = str(args.lock_file)
    else:
        parameters = build_parameters(args)
        lock_source = "cli"
        if args.lock_file:
            locked = read_lock_file(args.lock_file)
            parameters.assert_matches(locked,
                                      context=f"--lock-file {args.lock_file}")
            parameters = locked
            lock_source = str(args.lock_file)

    if args.write_lock:
        write_lock_file(parameters, args.write_lock, metadata={
            "source": list(args.source),
            "window_events": args.window_events,
        })

    runner = WalkForwardRunner(
        parameters,
        window_events=args.window_events,
        batch_size=args.batch_size,
        frames_per_hook=args.frames_per_hook,
        periods_per_year=args.periods_per_year,
        ledger_tolerance=args.ledger_tolerance,
        lock_source=lock_source,
    )

    def show(window) -> None:
        print(f"  window {window.index:>3} {window.label:<40} "
              f"{window.events:>7,} ev  pnl {window.net_pnl:>+12,.4f}  "
              f"trades {window.trades:>5}  end {window.end_equity:>14,.4f}")

    print("=" * 100)
    print(f"  parameter lock: {parameters.fingerprint} ({lock_source})")
    print(f"  sources       : {len(args.source)} pattern(s), "
          f"window_events={args.window_events or 'per-archive'}")
    print("-" * 100)
    report = runner.run(args.source, progress=show)
    print("-" * 100)
    for line in report.summary_lines():
        print(f"  {line}")
    print("=" * 100)

    if not args.no_report:
        json_path, md_path = report.write(args.report_dir, args.stem)
        print(f"  wrote: {json_path}")
        print(f"  wrote: {md_path}")

    aggregate = report.aggregate
    failures: List[str] = []
    if args.min_sharpe is not None and aggregate["sharpe_ratio"] < args.min_sharpe:
        failures.append(f"sharpe {aggregate['sharpe_ratio']:.3f} < {args.min_sharpe:.3f}")
    if args.min_pnl is not None and aggregate["net_pnl"] < args.min_pnl:
        failures.append(f"net_pnl {aggregate['net_pnl']:+.4f} < {args.min_pnl:+.4f}")
    if failures:
        for failure in failures:
            print(f"  GATE FAILED: {failure}", file=sys.stderr)
        return EXIT_GATE_FAILED
    return EXIT_OK


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    configure_logging(args.log_level)
    try:
        return run(args)
    except (ParameterLockError, FileNotFoundError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return EXIT_USAGE
    except (LookAheadError, WalkForwardError) as exc:
        print(f"validation failure: {exc}", file=sys.stderr)
        return EXIT_INVALID


if __name__ == "__main__":
    raise SystemExit(main())
