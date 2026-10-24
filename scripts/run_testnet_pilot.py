#!/usr/bin/env python3
"""Binance Futures **testnet** micro-pilot for ``BTCUSDT``.

The pilot exercises the full happy path against testnet only:

1. resolve endpoints from ``BINANCE_ENV`` and credentials from
   ``BINANCE_API_KEY`` / ``BINANCE_API_SECRET`` (never from YAML),
2. connect to REST, ping, and align the local clock,
3. read ``exchangeInfo`` filters (tick size, step size, minQty, minNotional,
   PERCENT_PRICE band) and validate the configured order size,
4. read the account balance and refuse to continue without testnet funds,
5. sample the L2 depth WebSocket for a few seconds, and
6. build a passive limit order and either stop at a dry run (default) or run the
   place -> query -> cancel lifecycle with ``--order-test submit``.

Safety rails: the script refuses to run when ``BINANCE_ENV`` is not ``testnet``,
credentials never appear in logs or ``repr`` output, and the order defaults to a
dry run. ``--order-test submit`` still only touches testnet funds.

Example::

    export BINANCE_ENV=testnet
    export BINANCE_API_KEY=...
    export BINANCE_API_SECRET=...
    python3 scripts/run_testnet_pilot.py --l2-seconds 15
    python3 scripts/run_testnet_pilot.py --skip-l2 --order-test submit
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
import time
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, List, Mapping, Optional, Sequence

# Make the repository root importable when this script is run directly.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from python_analytics.exchange.binance_futures import (
    API_SECRET_VAR,
    BinanceAPIError,
    BinanceConfigError,
    BinanceError,
    BinanceFuturesClient,
    BinanceFuturesConfig,
    DepthTick,
    LimitOrderPlan,
    OrderValidationError,
    SymbolFilters,
    build_limit_order,
    parse_depth_tick,
)
from python_analytics.utils import configure_logging

logger = logging.getLogger("zephyr.testnet_pilot")

PASS = "PASS"
FAIL = "FAIL"
SKIP = "SKIP"

DEFAULT_SYMBOL = "BTCUSDT"
DEFAULT_QUANTITY = "0.001"
DEFAULT_L2_SECONDS = 10.0
DEFAULT_MIN_TICKS = 5
DEFAULT_TICK_LOG_EVERY = 1.0
DEFAULT_ORDER_OFFSET_BPS = 50.0

# A partial-book stream (``depth20``) carries both sides of the top of book in
# every frame, unlike the diff stream (``depth``) where an update may touch only
# bids or only asks. The pilot only needs a reliable BBO sample, so it uses the
# partial stream.
L2_STREAM_TYPE = "depth20@100ms"


@dataclass(frozen=True)
class Check:
    name: str
    status: str
    detail: str


def _record(checks: List[Check], name: str, status: str, detail: str) -> None:
    checks.append(Check(name=name, status=status, detail=detail))
    log = logger.error if status == FAIL else logger.info
    log("[%s] %s: %s", status, name, detail)


def _decimal_field(payload: Mapping[str, Any], field_name: str) -> Decimal:
    raw = payload.get(field_name)
    try:
        return Decimal(str(raw))
    except (InvalidOperation, TypeError, ValueError):
        return Decimal("0")


def _usdt_balance(account: Mapping[str, Any], field_name: str) -> Decimal:
    """Reads a top-level balance, falling back to the USDT asset entry."""

    if account.get(field_name) is not None:
        return _decimal_field(account, field_name)
    assets = account.get("assets")
    if isinstance(assets, list):
        usdt = next(
            (
                item
                for item in assets
                if isinstance(item, Mapping) and item.get("asset") == "USDT"
            ),
            None,
        )
        if isinstance(usdt, Mapping):
            return _decimal_field(usdt, field_name)
    return Decimal("0")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Binance Futures testnet micro-pilot (BTCUSDT by default).",
        epilog=(
            "Requires BINANCE_API_KEY and BINANCE_API_SECRET in the environment. "
            "BINANCE_ENV defaults to testnet and production runs are refused."
        ),
    )
    parser.add_argument("--symbol", default=DEFAULT_SYMBOL, help="futures symbol")
    parser.add_argument(
        "--quantity",
        default=DEFAULT_QUANTITY,
        help="limit-order quantity in base units (default: 0.001 BTC)",
    )
    parser.add_argument(
        "--order-test",
        choices=("dry-run", "submit", "none"),
        default="dry-run",
        help="dry-run validates only; submit places/query/cancels on testnet",
    )
    parser.add_argument(
        "--order-offset-bps",
        type=float,
        default=DEFAULT_ORDER_OFFSET_BPS,
        help="passive offset from mark price in basis points",
    )
    parser.add_argument(
        "--l2-seconds",
        type=float,
        default=DEFAULT_L2_SECONDS,
        help="depth-stream sampling time in seconds",
    )
    parser.add_argument(
        "--min-ticks",
        type=int,
        default=DEFAULT_MIN_TICKS,
        help="minimum L2 frames required for the probe to pass",
    )
    parser.add_argument(
        "--tick-log-every",
        type=float,
        default=DEFAULT_TICK_LOG_EVERY,
        help="seconds between tick log lines",
    )
    parser.add_argument(
        "--skip-l2", action="store_true", help="skip the depth WebSocket probe"
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
    if args.l2_seconds <= 0:
        parser.error("--l2-seconds must be positive")
    if args.min_ticks < 0:
        parser.error("--min-ticks must be >= 0")
    if args.tick_log_every <= 0:
        parser.error("--tick-log-every must be positive")
    if args.order_offset_bps <= 0:
        parser.error("--order-offset-bps must be > 0")
    if args.recv_window <= 0:
        parser.error("--recv-window must be positive")
    if args.timeout <= 0:
        parser.error("--timeout must be positive")
    return args


@dataclass(frozen=True)
class L2ProbeResult:
    ticks: int
    malformed: int
    unusable: int
    duration_s: float
    max_gap_ms: Optional[int]
    last_tick: Optional[DepthTick]


async def probe_depth_stream(
    config: BinanceFuturesConfig,
    symbol: str,
    *,
    seconds: float,
    log_every: float = DEFAULT_TICK_LOG_EVERY,
    stream_type: str = L2_STREAM_TYPE,
) -> L2ProbeResult:
    """Samples a partial-book depth stream for ``seconds`` and summarises it."""

    try:
        import websockets
    except ImportError as exc:  # pragma: no cover - dependency is pinned
        raise BinanceError(
            "the 'websockets' package is required for the L2 probe: "
            "python -m pip install websockets"
        ) from exc

    url = config.stream_url(symbol, stream_type)
    logger.info("connecting to %s", url)
    try:
        socket = await websockets.connect(
            url,
            ping_interval=20,
            ping_timeout=20,
            close_timeout=0.5,
            open_timeout=10,
            max_queue=256,
            compression=None,
        )
    except Exception as exc:
        raise BinanceError(f"websocket connection failed: {exc}") from exc

    logger.info("websocket connected; sampling %s for %.1fs", symbol.upper(), seconds)
    started = time.monotonic()
    deadline = started + seconds
    ticks = 0
    malformed = 0
    unusable = 0
    max_gap_ms: Optional[int] = None
    last_event_ms: Optional[int] = None
    last_tick: Optional[DepthTick] = None
    last_log = 0.0

    try:
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            try:
                raw = await asyncio.wait_for(
                    socket.recv(), timeout=max(0.1, min(5.0, remaining))
                )
            except asyncio.TimeoutError:
                logger.debug("no depth frame within %.1fs", min(5.0, remaining))
                continue
            except Exception as exc:
                raise BinanceError(f"websocket receive failed: {exc}") from exc

            try:
                payload = json.loads(raw)
            except (TypeError, ValueError):
                malformed += 1
                if malformed <= 3:
                    logger.warning("malformed frame: %.120s", raw)
                continue

            tick = parse_depth_tick(payload)
            if tick is None:
                unusable += 1
                continue
            ticks += 1
            last_tick = tick
            if tick.event_ms is not None:
                if last_event_ms is not None:
                    gap = tick.event_ms - last_event_ms
                    if gap > 0:
                        max_gap_ms = gap if max_gap_ms is None else max(max_gap_ms, gap)
                last_event_ms = tick.event_ms

            now = time.monotonic()
            if ticks == 1 or now - last_log >= log_every:
                last_log = now
                logger.info(
                    "tick #%d event=%s bid=%s x %s | ask=%s x %s | spread=%s",
                    ticks,
                    tick.event_ms if tick.event_ms is not None else "n/a",
                    tick.bid,
                    tick.bid_quantity,
                    tick.ask,
                    tick.ask_quantity,
                    tick.spread,
                )
    finally:
        # Measure the sample window before the close handshake, which can take
        # up to ``close_timeout`` and must not inflate the tick rate.
        duration = time.monotonic() - started
        try:
            await socket.close()
        except Exception:  # pragma: no cover - close is best effort
            logger.debug("websocket close failed", exc_info=True)

    rate = ticks / duration if duration > 0 else 0.0
    logger.info(
        "L2 probe summary: %d ticks in %.2fs (%.2f/s), malformed=%d unusable=%d",
        ticks,
        duration,
        rate,
        malformed,
        unusable,
    )
    return L2ProbeResult(
        ticks=ticks,
        malformed=malformed,
        unusable=unusable,
        duration_s=duration,
        max_gap_ms=max_gap_ms,
        last_tick=last_tick,
    )


def _run_order_lifecycle(
    client: BinanceFuturesClient, plan: LimitOrderPlan
) -> str:
    """Places, queries, cancels and re-queries one passive testnet limit order."""

    order = client.new_order(
        symbol=plan.symbol,
        side=plan.side,
        order_type="LIMIT",
        quantity=plan.quantity,
        price=plan.price,
        time_in_force=plan.time_in_force,
        client_order_id=plan.client_order_id,
    )
    order_id = order.get("orderId") if isinstance(order, Mapping) else None
    if order_id is None:
        raise BinanceError(f"order response has no orderId: {order!r}")
    logger.info(
        "order placed: id=%s status=%s type=LIMIT %s %s @ %s",
        order_id,
        order.get("status", "n/a"),
        plan.side,
        plan.quantity,
        plan.price,
    )

    identifier = {"symbol": plan.symbol, "order_id": order_id}
    cancelled = False
    try:
        queried = client.query_order(**identifier)
        logger.info(
            "order query: id=%s status=%s executedQty=%s avgPrice=%s",
            queried.get("orderId", order_id),
            queried.get("status", "n/a"),
            queried.get("executedQty", "0"),
            queried.get("avgPrice", "0"),
        )

        try:
            client.cancel_order(**identifier)
            cancelled = True
        except BinanceAPIError as exc:
            logger.warning("cancel request was rejected (%s); checking final state", exc)

        final = client.query_order(**identifier)
        final_status = str(final.get("status") or "UNKNOWN").upper()
        logger.info("final order status: %s", final_status)
        if final_status == "NEW":
            raise BinanceError(
                "order is still open after cancel; inspect the testnet account "
                "before retrying"
            )
        if final_status == "FILLED":
            logger.warning(
                "testnet order filled instead of resting; no real funds were involved"
            )
    except Exception:
        # Never leave an untracked resting order behind on the testnet.
        if not cancelled:
            try:
                client.cancel_order(**identifier)
            except BinanceAPIError as exc:
                logger.warning(
                    "best-effort cancel for order %s failed (%s); check open "
                    "orders manually",
                    order_id,
                    exc,
                )
        logger.error("order lifecycle aborted for orderId=%s", order_id)
        raise
    return f"orderId={order_id} status={final_status}"


def _finish(checks: List[Check]) -> int:
    failures = [check for check in checks if check.status == FAIL]
    logger.info("-" * 78)
    logger.info("%-4s %-24s %s", "ST", "CHECK", "DETAIL")
    for check in checks:
        logger.info("%-4s %-24s %s", check.status, check.name, check.detail)
    logger.info("-" * 78)
    if failures:
        logger.error(
            "pilot verdict: FAIL (%d/%d checks failed)", len(failures), len(checks)
        )
        return 1
    logger.info("pilot verdict: PASS (%d checks)", len(checks))
    return 0


def run(
    config: BinanceFuturesConfig,
    args: argparse.Namespace,
    *,
    client: Optional[BinanceFuturesClient] = None,
) -> int:
    """Executes the pilot and returns a process exit code (0 = all checks pass)."""

    client = client or build_client(config, args)
    checks: List[Check] = []

    logger.info("=" * 78)
    logger.info("ZephyrHFT - Binance Futures testnet micro-pilot")
    logger.info("environment : %s", config.environment)
    logger.info("REST        : %s", config.rest_url)
    logger.info("WS          : %s", config.stream_url(args.symbol, L2_STREAM_TYPE))
    logger.info("API key     : %s", config.credentials.masked_key())
    logger.info("secret      : loaded from %s (never logged or persisted)", API_SECRET_VAR)
    logger.info(
        "symbol      : %s | quantity=%s | order-test=%s",
        args.symbol.upper(),
        args.quantity,
        args.order_test,
    )
    logger.info("=" * 78)

    try:
        offset = client.sync_time()
        client.ping()
    except BinanceError as exc:
        _record(checks, "REST connectivity", FAIL, str(exc))
        return _finish(checks)
    _record(
        checks,
        "REST connectivity",
        PASS,
        f"ping ok, clock offset {offset:+d} ms",
    )

    try:
        info = client.exchange_info()
        filters = SymbolFilters.from_exchange_info(info, args.symbol)
    except BinanceError as exc:
        _record(checks, "Exchange filters", FAIL, str(exc))
        return _finish(checks)
    logger.info("filters: %s", filters.describe())
    _record(checks, "Exchange filters", PASS, filters.describe())

    try:
        mark_price = client.mark_price(args.symbol)
    except BinanceError as exc:
        _record(checks, "Mark price", FAIL, str(exc))
        return _finish(checks)
    _record(checks, "Mark price", PASS, f"{args.symbol.upper()} mark={mark_price}")

    balance_ok = False
    try:
        account = client.account()
        available = _usdt_balance(account, "availableBalance")
        wallet = _usdt_balance(account, "totalWalletBalance")
    except BinanceError as exc:
        _record(checks, "Account balance", FAIL, str(exc))
    else:
        balance_ok = available > 0
        detail = f"wallet={wallet} USDT available={available} USDT"
        _record(
            checks,
            "Account balance",
            PASS if balance_ok else FAIL,
            detail if balance_ok else f"{detail} (testnet funds required)",
        )

    if args.skip_l2:
        _record(checks, "L2 depth stream", SKIP, "--skip-l2 requested")
    else:
        try:
            probe = asyncio.run(
                probe_depth_stream(
                    config,
                    args.symbol,
                    seconds=args.l2_seconds,
                    log_every=args.tick_log_every,
                )
            )
        except (BinanceError, OSError) as exc:
            _record(checks, "L2 depth stream", FAIL, str(exc))
        else:
            detail = (
                f"{probe.ticks} ticks in {probe.duration_s:.2f}s "
                f"(malformed={probe.malformed}, unusable={probe.unusable})"
            )
            if probe.last_tick is not None and probe.last_tick.spread is not None:
                detail += f", last spread={probe.last_tick.spread}"
            if probe.max_gap_ms is not None:
                detail += f", max event gap={probe.max_gap_ms} ms"
            passed = probe.ticks >= args.min_ticks
            _record(
                checks,
                "L2 depth stream",
                PASS if passed else FAIL,
                detail if passed else f"{detail} (< min-ticks {args.min_ticks})",
            )

    requested = filters.quantize_quantity(args.quantity)
    required = filters.required_quantity(mark_price)
    logger.info(
        "sizing: requested=%s quantized=%s minNotional requires >= %s at mark %s",
        args.quantity,
        requested,
        required,
        mark_price,
    )
    if requested <= 0 or requested < required:
        _record(
            checks,
            "Order sizing",
            FAIL,
            f"{requested} is below the {required} required by minNotional "
            f"{filters.min_notional}",
        )
    else:
        _record(
            checks,
            "Order sizing",
            PASS,
            f"{requested} satisfies minQty/minNotional (>= {required})",
        )

        if args.order_test == "none":
            _record(checks, "Order lifecycle", SKIP, "--order-test none")
        else:
            try:
                plan = build_limit_order(
                    filters,
                    side="BUY",
                    quantity=requested,
                    reference_price=mark_price,
                    offset_bps=args.order_offset_bps,
                )
            except OrderValidationError as exc:
                _record(checks, "Order lifecycle", FAIL, str(exc))
            else:
                logger.info(
                    "order plan: %s %s %s @ %s (mark %s, offset %.1f bps, "
                    "notional %s USDT, clientId=%s)",
                    plan.side,
                    plan.quantity,
                    plan.symbol,
                    plan.price,
                    plan.reference_price,
                    args.order_offset_bps,
                    plan.notional,
                    plan.client_order_id,
                )
                if args.order_test == "dry-run":
                    _record(
                        checks,
                        "Order lifecycle",
                        PASS,
                        f"dry run only: {plan.side} {plan.quantity} @ {plan.price}, "
                        "no POST sent",
                    )
                elif not balance_ok:
                    _record(
                        checks,
                        "Order lifecycle",
                        SKIP,
                        "submit skipped because the account has no available balance",
                    )
                else:
                    try:
                        result = _run_order_lifecycle(client, plan)
                    except (BinanceError, OrderValidationError) as exc:
                        _record(checks, "Order lifecycle", FAIL, str(exc))
                    else:
                        _record(checks, "Order lifecycle", PASS, result)

    return _finish(checks)


def build_client(
    config: BinanceFuturesConfig, args: argparse.Namespace
) -> BinanceFuturesClient:
    return BinanceFuturesClient(
        config, timeout=args.timeout, recv_window=args.recv_window
    )


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    configure_logging(args.log_level)

    try:
        config = BinanceFuturesConfig.from_env()
    except BinanceConfigError as exc:
        logger.error("configuration error: %s", exc)
        return 2

    if not config.is_testnet:
        logger.error(
            "refusing to run: BINANCE_ENV=%s. This micro-pilot only talks to the "
            "Binance Futures testnet; unset BINANCE_ENV or set it to 'testnet'.",
            config.environment,
        )
        return 2

    if args.order_test == "submit":
        logger.warning(
            "order-test=submit: a real order will be placed on the Binance testnet"
        )

    try:
        return run(config, args, client=build_client(config, args))
    except KeyboardInterrupt:
        logger.warning("interrupted by user")
        return 130
    except (BinanceError, OrderValidationError) as exc:
        logger.error("pilot aborted: %s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())
