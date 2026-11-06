#!/usr/bin/env python3
"""Run a bounded market-making session through the supervisor.

``run_live_trader.py`` is deliberately single-shot: it reconciles, places one
quote, or kills everything. This is the loop the strategy needs to actually run
— re-quoting, inventory limits, a session loss cap — built on ``LiveSupervisor``
so those rules live in one tested place rather than in a script.

Safety rails, in the same order of force as the rest of the live layer:

* ``BINANCE_ENV`` defaults to testnet; a production run needs ``--live``.
* Production additionally needs a **fresh alpha GO** in ``--report``. Nothing
  here weakens that: the supervisor is machinery, and machinery does not earn.
* ``--iterations`` bounds the session by default. An unbounded loop on an
  operator's laptop is not a service; a supervisor is.
* Every exit path goes through ``supervisor.stop()``, so a Ctrl-C or a hit loss
  cap leaves no quote resting in a book nobody is watching.

Exit codes: ``0`` session finished clean, ``1`` halted (loss cap, unresolved
order, failed cancel) or an ordinary submit rejection, ``2`` configuration or
environment refusal, ``3`` the alpha gate refused to arm, ``130`` interrupted.
"""
from __future__ import annotations

import argparse
import signal
import sys
from decimal import Decimal
from pathlib import Path
from typing import Any, List, Optional

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from python_analytics.exchange.binance_futures import (  # noqa: E402
    BinanceConfigError,
    BinanceFuturesClient,
    BinanceFuturesConfig,
    SymbolFilters,
)
from python_analytics.live import (  # noqa: E402
    AlphaGateError,
    LiveExecutionError,
    LiveExecutor,
    LiveSupervisor,
    OrderStore,
    SupervisorConfig,
    require_alpha_go,
)
from python_analytics.utils import configure_logging  # noqa: E402

import logging

logger = logging.getLogger("zephyr.live.session")

DEFAULT_SYMBOL = "BTCUSDT"
DEFAULT_STORE = "state/live_orders.sqlite3"
DEFAULT_REPORT = "reports/production_readiness_report.json"

EXIT_CLEAN = 0
EXIT_HALTED = 1
EXIT_USAGE = 2
EXIT_ALPHA_NO_GO = 3
EXIT_INTERRUPTED = 130


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run a bounded live/paper market-making session.",
        epilog=("Credentials come from BINANCE_API_KEY / BINANCE_API_SECRET only. "
                "BINANCE_ENV defaults to testnet; --live is required for "
                "production and still needs the alpha gate."),
    )
    parser.add_argument("--symbol", default=DEFAULT_SYMBOL)
    parser.add_argument("--quantity", default="0.002",
                        help="base units quoted on each side")
    parser.add_argument("--offset-bps", default="5.0",
                        help="distance from the mid on each side")
    parser.add_argument("--tick-size", default=None,
                        help="venue price grid (default: read from exchangeInfo)")
    parser.add_argument("--requote-tolerance-bps", default="0.25",
                        help="leave a resting quote alone unless its target "
                             "moves further than this")
    parser.add_argument("--max-inventory", default="0.05",
                        help="net base units before the growing side is suppressed")
    parser.add_argument("--loss-cap", default="50",
                        help="session PnL (quote currency) that trips the kill path")
    parser.add_argument("--iterations", type=int, default=60,
                        help="bounded by default; 0 runs until stopped")
    parser.add_argument("--interval-s", type=float, default=1.0)
    parser.add_argument("--store", default=DEFAULT_STORE)
    parser.add_argument("--report", default=DEFAULT_REPORT)
    parser.add_argument("--max-report-age-hours", type=float, default=168.0)
    parser.add_argument("--override-alpha", default="",
                        help="sentence recorded in the audit trail; required to "
                             "trade a red study")
    parser.add_argument("--flatten-on-stop", action="store_true",
                        help="close the position as well as cancelling quotes")
    parser.add_argument("--live", action="store_true",
                        help="permit BINANCE_ENV=production; still needs the gate")
    parser.add_argument("--recv-window", type=int, default=5000)
    parser.add_argument("--log-level", default="INFO")
    return parser.parse_args(argv)


def _production_refused(config: BinanceFuturesConfig, args: argparse.Namespace) -> bool:
    return not config.is_testnet and not args.live


def run(args: argparse.Namespace) -> int:
    try:
        venue = BinanceFuturesConfig.from_env()
    except BinanceConfigError as exc:
        logger.error("configuration error: %s", exc)
        return EXIT_USAGE

    if _production_refused(venue, args):
        logger.error(
            "refusing to run: BINANCE_ENV=%s. This harness talks to the testnet "
            "unless --live is passed explicitly.", venue.environment)
        return EXIT_USAGE

    store = OrderStore(args.store)
    armed = ""
    if venue.is_testnet:
        # Testnet rehearses the loop; the alpha verdict is logged, not required.
        from python_analytics.live import alpha_gate_verdict
        try:
            import json
            report = json.loads(Path(args.report).read_text(encoding="utf-8"))
            ok, why = alpha_gate_verdict(report)
            logger.warning("testnet session; alpha gate says %s: %s",
                           "GO" if ok else "NO-GO", why)
        except (OSError, ValueError) as exc:
            logger.warning("testnet session; alpha report unreadable: %s", exc)
    else:
        try:
            armed = require_alpha_go(
                args.report, max_age_hours=args.max_report_age_hours,
                override_reason=args.override_alpha.strip() or None, store=store)
        except AlphaGateError as exc:
            logger.error("not armed: %s", exc)
            return EXIT_ALPHA_NO_GO
        logger.warning("armed: %s", armed)

    client = BinanceFuturesClient(venue, recv_window=args.recv_window)
    try:
        filters = SymbolFilters.from_exchange_info(client.exchange_info(),
                                                   args.symbol)
    except Exception as exc:
        logger.error("cannot read the venue filters for %s: %s", args.symbol, exc)
        return EXIT_USAGE
    tick = Decimal(args.tick_size) if args.tick_size else filters.tick_size
    if tick <= 0:
        logger.error("tick size must be positive, got %s", tick)
        return EXIT_USAGE

    config = SupervisorConfig(
        symbol=args.symbol,
        quantity=Decimal(args.quantity),
        offset_bps=Decimal(args.offset_bps),
        tick_size=tick,
        requote_tolerance_bps=Decimal(args.requote_tolerance_bps),
        max_inventory=Decimal(args.max_inventory),
        session_loss_cap=Decimal(args.loss_cap),
        max_iterations=args.iterations,
        interval_s=args.interval_s,
    )
    supervisor = LiveSupervisor(LiveExecutor(client, store, args.symbol),
                                client, config)

    def on_signal(_sig: int, _frame: Any) -> None:
        # Ask the loop to stop; the shutdown path is the same one the loss cap
        # uses, so an interrupted session leaves the book the same way.
        supervisor.stop("signal")

    signal.signal(signal.SIGINT, on_signal)
    signal.signal(signal.SIGTERM, on_signal)

    try:
        report = supervisor.run()
    except LiveExecutionError as exc:
        logger.error("session failed: %s", exc)
        supervisor.stop("failure", flatten=args.flatten_on_stop)
        return EXIT_HALTED
    finally:
        store.close()

    if not supervisor.halted:
        logger.info("session clean: %s", report.describe())
        return EXIT_CLEAN
    # Either way the session stopped taking care of itself, and the reason is
    # what the operator needs: a loss cap and a failed cancel are different
    # incidents even though both end the loop.
    logger.error("session halted: %s (%s)", report.describe(),
                 supervisor.halt_reason)
    return EXIT_HALTED


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    configure_logging(args.log_level)
    try:
        return run(args)
    except KeyboardInterrupt:
        return EXIT_INTERRUPTED


if __name__ == "__main__":
    raise SystemExit(main())
