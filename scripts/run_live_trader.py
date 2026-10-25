#!/usr/bin/env python3
"""Single-shot live execution harness: reconcile, optionally quote, or panic.

This is deliberately *not* an autonomous bot. It does the four things a live
session needs before strategy code is worth trusting:

1. open the durable order store at ``--store`` (created on first run),
2. reconcile the local book against the venue — adopting orders this process
   did not place and resolving orders that vanished while it was down,
3. optionally place **one** post-only quote at ``--offset-bps`` from the book
   mid, going through :class:`~python_analytics.live.LiveExecutor`, and
4. ``--cancel-all``: cancel the symbol and flatten the position, the same kill
   path a signal handler would take.

Safety rails, in order of force:

* ``BINANCE_ENV`` defaults to testnet; a production run requires ``--live``.
* On production, arming requires a **fresh alpha GO** in
  ``--report``. The committed study is red, so the only way through is an
  explicit ``--override-alpha "<reason>"``, and that sentence is written to the
  order store's audit trail.
* ``--submit`` is opt-in and places exactly one order; nothing loops.

Example::

    export BINANCE_ENV=testnet
    export BINANCE_API_KEY=...  BINANCE_API_SECRET=...
    python3 scripts/run_live_trader.py                      # reconcile only
    python3 scripts/run_live_trader.py --submit --side BUY --quantity 0.002
    python3 scripts/run_live_trader.py --cancel-all         # kill switch
"""
from __future__ import annotations

import argparse
import json
import logging
import math
import signal
import sys
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Optional, Sequence

# Make the repository root importable when this script is run directly.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from python_analytics.exchange.binance_futures import (
    BinanceConfigError,
    BinanceError,
    BinanceFuturesClient,
    BinanceFuturesConfig,
    OrderValidationError,
    SymbolFilters,
    build_limit_order,
)
from python_analytics.live import (
    AlphaGateError,
    LiveExecutor,
    OrderStore,
    SubmissionResult,
    alpha_gate_verdict,
    require_alpha_go,
)
from python_analytics.utils import configure_logging

logger = logging.getLogger("zephyr.live_trader")

DEFAULT_SYMBOL = "BTCUSDT"
DEFAULT_STORE = "state/live_orders.sqlite3"
DEFAULT_REPORT = "reports/production_readiness_report.json"
DEFAULT_QUANTITY = "0.002"
DEFAULT_OFFSET_BPS = 5.0

#: Binance's post-only rejections. ``-2010`` is the generic new-order rejection
#: that also covers spot GTX ("Order would immediately trigger"), so the text
#: has to confirm it. ``-5022`` exists *only* for a USDⓈ-M futures post-only
#: order that could not rest as maker, so the code alone is proof. Either way
#: nothing rested and nothing is owed; every other rejection needs an operator.
POST_ONLY_REJECTION_CODE = -2010
POST_ONLY_REJECTION_TEXT = "would immediately trigger"
POST_ONLY_MAKER_REJECTION_CODES = frozenset({-5022})


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Reconcile a live futures account, place one post-only quote, "
        "or flatten everything.",
        epilog=(
            "Credentials come from BINANCE_API_KEY / BINANCE_API_SECRET only. "
            "BINANCE_ENV defaults to testnet; --live is required for production."
        ),
    )
    parser.add_argument("--symbol", default=DEFAULT_SYMBOL, help="futures symbol")
    parser.add_argument(
        "--store", default=DEFAULT_STORE, help="SQLite order-store path"
    )
    parser.add_argument(
        "--report",
        default=DEFAULT_REPORT,
        help="readiness report consulted before arming real money",
    )
    parser.add_argument(
        "--max-report-age-hours",
        type=float,
        default=168.0,
        help="a GO older than this is treated as no evidence at all",
    )
    parser.add_argument(
        "--override-alpha",
        default="",
        help="sentence recorded in the audit trail; required to trade a red study",
    )
    parser.add_argument(
        "--submit",
        action="store_true",
        help="place exactly one post-only quote (default: reconcile only)",
    )
    parser.add_argument(
        "--side", choices=("BUY", "SELL"), default="BUY", help="quote side"
    )
    parser.add_argument(
        "--quantity", default=DEFAULT_QUANTITY, help="order quantity in base units"
    )
    parser.add_argument(
        "--offset-bps",
        type=float,
        default=DEFAULT_OFFSET_BPS,
        help="passive distance from the book mid, in basis points",
    )
    parser.add_argument(
        "--cancel-all",
        action="store_true",
        help="cancel the symbol and flatten the position, then exit",
    )
    parser.add_argument(
        "--live",
        action="store_true",
        help="permit BINANCE_ENV=production; still requires the alpha gate",
    )
    parser.add_argument(
        "--recv-window", type=int, default=5000, help="signed-request recvWindow (ms)"
    )
    parser.add_argument(
        "--timeout", type=float, default=10.0, help="per-request HTTP timeout (s)"
    )
    parser.add_argument(
        "--log-level", default="INFO", help="DEBUG, INFO, WARNING, ERROR"
    )
    return parser


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = build_parser()
    args = parser.parse_args(argv)
    if not str(args.symbol or "").strip():
        parser.error("--symbol must not be empty")
    try:
        quantity = Decimal(str(args.quantity))
    except (InvalidOperation, ValueError):
        parser.error(f"--quantity is not a decimal: {args.quantity!r}")
    if not quantity.is_finite() or quantity <= 0:
        parser.error(f"--quantity must be a positive decimal: {args.quantity!r}")
    if args.offset_bps <= 0:
        parser.error("--offset-bps must be > 0 to keep the quote passive")
    if not math.isfinite(args.max_report_age_hours) or args.max_report_age_hours <= 0:
        # nan/inf would slip through a plain ``<= 0`` check and disable the
        # freshness test that keeps a stale GO from arming real money.
        parser.error("--max-report-age-hours must be finite and > 0")
    if args.recv_window <= 0:
        parser.error("--recv-window must be positive")
    if args.timeout <= 0:
        parser.error("--timeout must be positive")
    if args.submit and args.cancel_all:
        parser.error("--submit and --cancel-all are mutually exclusive")
    return args


def _production_refused(
    config: BinanceFuturesConfig, args: argparse.Namespace
) -> bool:
    """True when a production run lacks the explicit ``--live`` opt-in.

    ``--cancel-all`` is exempt: the kill switch must work on the environment it
    is needed on, and refusing to cancel an open position because an arming flag
    was omitted is the wrong failure mode. It still bypasses the alpha gate.
    """

    return not config.is_testnet and not (args.live or args.cancel_all)


def _post_only_crossing_rejected(result: SubmissionResult) -> bool:
    """True only for a venue-confirmed post-only crossing rejection.

    GTX refusing to cross means nothing rested, so the run is clean. Every other
    rejection — filters, margin, an unknown code — leaves the operator with a
    question and must exit non-zero. ``-5022`` is the futures post-only code and
    is unambiguous; ``-2010`` is generic, so its text has to agree.
    """

    if result.status != "REJECTED":
        return False
    if result.code in POST_ONLY_MAKER_REJECTION_CODES:
        return True
    if result.code != POST_ONLY_REJECTION_CODE:
        return False
    return POST_ONLY_REJECTION_TEXT in (result.note or "").lower()


def _mid_price(payload: object) -> Decimal:
    """Book mid from a ``bookTicker`` payload; refuses a one-sided book."""

    if not isinstance(payload, dict):
        raise OrderValidationError("bookTicker returned no object")
    try:
        bid = Decimal(str(payload.get("bidPrice") or "0"))
        ask = Decimal(str(payload.get("askPrice") or "0"))
    except (InvalidOperation, ValueError) as exc:
        raise OrderValidationError(f"unparseable bookTicker: {payload!r}") from exc
    if bid <= 0 or ask <= 0:
        raise OrderValidationError(f"bookTicker is one-sided: {payload!r}")
    return (bid + ask) / Decimal(2)


def _raise_interrupt(signum: int, frame: object) -> None:
    raise KeyboardInterrupt(f"signal {signum}")


def _install_signal_handlers() -> None:
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            signal.signal(sig, _raise_interrupt)
        except (ValueError, OSError):  # not the main thread, or unsupported
            logger.debug("could not install handler for %s", sig)


def _log_alpha_status(report_path: str) -> None:
    """Reports the study's verdict without enforcing it (testnet path)."""

    try:
        payload = json.loads(Path(report_path).read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        logger.info("alpha evidence unavailable (%s); fine on testnet", exc)
        return
    ok, detail = alpha_gate_verdict(payload)
    logger.info("alpha gate (%s): %s", "GO" if ok else "NO-GO", detail)


def run(
    config: BinanceFuturesConfig,
    args: argparse.Namespace,
    *,
    client: BinanceFuturesClient,
    store: OrderStore,
) -> int:
    executor = LiveExecutor(client, store, args.symbol)

    if args.cancel_all:
        # Clock sync matters for signed requests, but a failure must not stop
        # the kill path: report it alongside the panic result and still try.
        sync_error: Optional[str] = None
        if not client.time_synced:
            try:
                logger.info("clock offset: %+d ms", client.sync_time())
            except BinanceError as exc:
                sync_error = f"sync_time: {exc}"
                logger.error("clock sync failed before panic: %s", exc)
        errors = executor.panic("operator requested --cancel-all")
        for error in errors:
            logger.error("panic step failed: %s", error)
        if sync_error is not None:
            errors = [sync_error, *errors]
        return 0 if not errors else 1

    if not client.time_synced:
        logger.info("clock offset: %+d ms", client.sync_time())

    report = executor.reconcile()
    logger.info("%s", report.describe())
    if report.adopted:
        logger.warning(
            "adopted %d order(s) this process did not place: %s",
            len(report.adopted),
            [order.client_order_id for order in report.adopted],
        )
    if report.unresolved:
        logger.error("orders still in doubt after reconcile: %s", report.unresolved)
        return 1

    if not args.submit:
        logger.info("reconcile-only run complete (pass --submit to quote once)")
        return 0

    filters = SymbolFilters.from_exchange_info(client.exchange_info(), args.symbol)
    mid = _mid_price(client.book_ticker(args.symbol))
    plan = build_limit_order(
        filters,
        side=args.side,
        quantity=args.quantity,
        reference_price=mid,
        offset_bps=args.offset_bps,
    )
    logger.warning(
        "quoting %s %s %s @ %s (mid %s, %.1f bps away) on %s",
        plan.symbol,
        plan.side,
        plan.quantity,
        plan.price,
        mid,
        args.offset_bps,
        config.environment,
    )
    result = executor.submit_post_only(
        side=plan.side, quantity=plan.quantity, price=plan.price
    )
    logger.info("submit result: %s", result.describe())

    if result.accepted:
        return 0
    if _post_only_crossing_rejected(result):
        logger.warning("post-only quote would have crossed; nothing rested")
        return 0
    logger.error("submit did not leave a resting order: %s", result.describe())
    return 1


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    configure_logging(args.log_level)

    try:
        config = BinanceFuturesConfig.from_env()
    except BinanceConfigError as exc:
        logger.error("configuration error: %s", exc)
        return 2

    if _production_refused(config, args):
        logger.error(
            "refusing to run: BINANCE_ENV=%s. This harness talks to the testnet "
            "unless --live is passed explicitly; --cancel-all is the only "
            "production exception.",
            config.environment,
        )
        return 2

    store = OrderStore(args.store)
    try:
        if config.is_testnet or args.cancel_all:
            _log_alpha_status(args.report)
        else:
            try:
                verdict = require_alpha_go(
                    args.report,
                    max_age_hours=args.max_report_age_hours,
                    override_reason=args.override_alpha.strip() or None,
                    store=store,
                )
            except AlphaGateError as exc:
                logger.error("not armed: %s", exc)
                return 3
            logger.warning("armed: %s", verdict)

        client = BinanceFuturesClient(
            config, timeout=args.timeout, recv_window=args.recv_window
        )
        _install_signal_handlers()
        try:
            return run(config, args, client=client, store=store)
        except KeyboardInterrupt as exc:
            logger.warning("interrupt (%s): flattening before exit", exc)
            errors = LiveExecutor(client, store, args.symbol).panic(
                f"interrupt: {exc}"
            )
            for error in errors:
                logger.error("panic step failed: %s", error)
            return 130
    except (BinanceError, OrderValidationError) as exc:
        logger.error("live harness aborted: %s", exc)
        return 1
    finally:
        store.close()


if __name__ == "__main__":
    sys.exit(main())
