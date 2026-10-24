"""Binance USDⓈ-M Futures configuration and a minimal signed REST client.

The testnet micro-pilot (``scripts/run_testnet_pilot.py``) uses this module to

* resolve REST/WebSocket endpoints from ``BINANCE_ENV`` (default ``testnet``),
* load credentials from ``BINANCE_API_KEY`` / ``BINANCE_API_SECRET`` only,
* sign requests with HMAC-SHA256 using nothing but the standard library, and
* validate order quantities/prices against the symbol's exchange filters.

Secrets stay in memory: ``repr`` output masks them, no log record ever includes
the API key or signature, and no credential is read from or written to YAML.

Only ``GET``, ``POST`` and ``DELETE`` are implemented because that is all the
pilot needs. Every request is synchronous; the asynchronous piece is the market
data stream, which the pilot drives with ``websockets``.
"""
from __future__ import annotations

import hashlib
import hmac
import json
import logging
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field, replace
from decimal import ROUND_CEILING, ROUND_DOWN, Decimal, InvalidOperation
from typing import Any, Callable, Dict, List, Mapping, Optional, Tuple

from python_analytics.utils.config import (
    BINANCE_DEFAULT_ENVIRONMENT,
    ConfigError,
    binance_endpoints,
    resolve_binance_environment,
)

logger = logging.getLogger(__name__)

API_KEY_VAR = "BINANCE_API_KEY"
API_SECRET_VAR = "BINANCE_API_SECRET"
DEFAULT_ENVIRONMENT = BINANCE_DEFAULT_ENVIRONMENT

DEFAULT_STREAM_TYPE = "depth@100ms"
DEFAULT_RECV_WINDOW_MS = 5000
DEFAULT_TIMEOUT_S = 10.0

#: Binance futures post-only time-in-force. ``GTX`` makes the venue *reject* an
#: order that would cross instead of filling it, so a passive quote can never
#: turn into a taker fill at the wrong fee tier.
POST_ONLY_TIME_IN_FORCE = "GTX"

#: HTTP statuses Binance uses for rate limiting (429) and IP bans (418).
RATE_LIMIT_STATUSES = frozenset({418, 429})


class BinanceError(RuntimeError):
    """Base class for exchange/transport failures raised by this module."""


class BinanceConfigError(ConfigError, BinanceError):
    """Raised when endpoint or credential configuration is unusable."""


class MissingCredentialsError(BinanceConfigError):
    """Raised when ``BINANCE_API_KEY`` / ``BINANCE_API_SECRET`` are absent."""


class BinanceTransportError(BinanceError):
    """Raised when the HTTP request itself fails (DNS, TLS, timeout, ...)."""


class BinanceAPIError(BinanceError):
    """Raised when Binance answers with an HTTP error or an error payload."""

    def __init__(
        self,
        message: str,
        *,
        status: Optional[int] = None,
        code: Optional[int] = None,
        endpoint: Optional[str] = None,
    ) -> None:
        self.status = status
        self.code = code
        self.endpoint = endpoint
        super().__init__(self._render(message))

    def _render(self, message: str) -> str:
        details = []
        if self.status is not None:
            details.append(f"HTTP {self.status}")
        if self.code is not None:
            details.append(f"code {self.code}")
        if self.endpoint:
            details.append(self.endpoint)
        suffix = f" ({', '.join(details)})" if details else ""
        return f"{message}{suffix}"


class OrderValidationError(ValueError):
    """Raised when an order would violate the symbol's exchange filters."""


class BinanceRateLimitError(BinanceAPIError):
    """Raised on HTTP 429 (rate limit) or 418 (IP ban).

    Kept separate from :class:`BinanceAPIError` so a trading loop can back off
    instead of treating the rejection as an order-level problem.
    """

    @property
    def is_ban(self) -> bool:
        return self.status == 418


class UnknownSymbolError(BinanceError):
    """Raised when ``exchangeInfo`` does not list the requested symbol."""


def mask_secret(value: str) -> str:
    """Masks a key/secret, keeping the last four characters for identification."""

    text = str(value or "")
    if len(text) <= 4:
        return "***"
    return f"***{text[-4:]}"


def _normalise_endpoint(url: str) -> str:
    return str(url or "").strip().rstrip("/")


@dataclass(frozen=True)
class BinanceCredentials:
    """API key/secret pair sourced exclusively from the environment."""

    api_key: str = field(repr=False)
    api_secret: str = field(repr=False)

    @classmethod
    def from_env(
        cls, environ: Optional[Mapping[str, str]] = None
    ) -> "BinanceCredentials":
        env = os.environ if environ is None else environ
        api_key = str(env.get(API_KEY_VAR, "") or "").strip()
        api_secret = str(env.get(API_SECRET_VAR, "") or "").strip()
        missing = [
            name
            for name, value in ((API_KEY_VAR, api_key), (API_SECRET_VAR, api_secret))
            if not value
        ]
        if missing:
            joined = " and ".join(missing)
            raise MissingCredentialsError(
                f"{joined} not set; export the {DEFAULT_ENVIRONMENT} key/secret "
                "before running the pilot (credentials are never read from files)"
            )
        return cls(api_key=api_key, api_secret=api_secret)

    def masked_key(self) -> str:
        return mask_secret(self.api_key)

    def __repr__(self) -> str:
        return f"BinanceCredentials(api_key={self.masked_key()!r}, api_secret='***')"

    __str__ = __repr__


@dataclass(frozen=True)
class BinanceFuturesConfig:
    """Resolved environment, endpoints and credentials for one pilot run."""

    environment: str
    rest_url: str
    ws_url: str
    credentials: BinanceCredentials = field(repr=False)

    @classmethod
    def from_env(
        cls,
        environ: Optional[Mapping[str, str]] = None,
        *,
        require_credentials: bool = True,
    ) -> "BinanceFuturesConfig":
        env = os.environ if environ is None else environ
        try:
            environment = resolve_binance_environment(environ=env)
            endpoints = binance_endpoints(environment, environ=env)
        except BinanceConfigError:
            raise
        except ConfigError as exc:
            raise BinanceConfigError(str(exc)) from exc
        credentials = (
            BinanceCredentials.from_env(env)
            if require_credentials
            else BinanceCredentials(api_key="", api_secret="")
        )
        return cls(
            environment=environment,
            rest_url=_normalise_endpoint(endpoints["rest"]),
            ws_url=_normalise_endpoint(endpoints["ws"]),
            credentials=credentials,
        )

    @property
    def is_testnet(self) -> bool:
        return self.environment == "testnet"

    def stream_url(
        self, symbol: str, stream_type: str = DEFAULT_STREAM_TYPE
    ) -> str:
        """Builds the combined-stream URL, e.g. ``.../ws/btcusdt@depth@100ms``."""

        normalised_symbol = str(symbol or "").strip().lower()
        normalised_stream = str(stream_type or "").strip().lower()
        if not normalised_symbol:
            raise BinanceConfigError("symbol must not be empty")
        if not normalised_stream:
            raise BinanceConfigError("stream type must not be empty")
        if not normalised_stream.startswith("@"):
            normalised_stream = f"@{normalised_stream}"
        return f"{self.ws_url}/{normalised_symbol}{normalised_stream}"

    def __repr__(self) -> str:
        return (
            "BinanceFuturesConfig("
            f"environment={self.environment!r}, rest_url={self.rest_url!r}, "
            f"ws_url={self.ws_url!r}, api_key={self.credentials.masked_key()!r})"
        )

    __str__ = __repr__


def decimal_text(value: Decimal) -> str:
    """Formats a ``Decimal`` without exponent or trailing zeros."""

    normalised = value.normalize()
    text = format(normalised, "f")
    if text in ("-0", ""):
        return "0"
    return text


def _as_decimal(value: Any, label: str) -> Decimal:
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise OrderValidationError(f"{label} is not a decimal: {value!r}") from exc
    if not result.is_finite():
        raise OrderValidationError(f"{label} must be finite: {value!r}")
    return result


@dataclass(frozen=True)
class DepthTick:
    """Best bid/ask snapshot parsed from a Binance depth stream frame."""

    event_ms: Optional[int]
    bid: Optional[Decimal]
    bid_quantity: Optional[Decimal]
    ask: Optional[Decimal]
    ask_quantity: Optional[Decimal]
    update_id: Optional[int] = None

    @property
    def spread(self) -> Optional[Decimal]:
        if self.bid is None or self.ask is None:
            return None
        return self.ask - self.bid


def parse_depth_tick(payload: Any) -> Optional[DepthTick]:
    """Extracts the best bid/ask from a raw or combined depth-stream frame.

    Returns ``None`` for frames without both sides (for example an error notice
    or a subscription ack) so the caller can count them as unusable instead of
    logging a bogus quote.
    """

    data = payload
    if isinstance(data, Mapping) and isinstance(data.get("data"), Mapping):
        data = data["data"]
    if not isinstance(data, Mapping):
        return None
    bids = data.get("b")
    asks = data.get("a")
    if not isinstance(bids, (list, tuple)) or not isinstance(asks, (list, tuple)):
        return None
    if not bids or not asks:
        return None
    bid_level, ask_level = bids[0], asks[0]
    if not isinstance(bid_level, (list, tuple)) or len(bid_level) < 2:
        return None
    if not isinstance(ask_level, (list, tuple)) or len(ask_level) < 2:
        return None
    try:
        bid = _as_decimal(bid_level[0], "bid price")
        bid_quantity = _as_decimal(bid_level[1], "bid quantity")
        ask = _as_decimal(ask_level[0], "ask price")
        ask_quantity = _as_decimal(ask_level[1], "ask quantity")
    except (OrderValidationError, IndexError, KeyError, TypeError):
        return None
    event_ms = data.get("E")
    update_id = data.get("u")
    return DepthTick(
        event_ms=int(event_ms) if isinstance(event_ms, (int, float)) else None,
        bid=bid,
        bid_quantity=bid_quantity,
        ask=ask,
        ask_quantity=ask_quantity,
        update_id=update_id if isinstance(update_id, int) else None,
    )


@dataclass(frozen=True)
class SymbolFilters:
    """Trading rules from ``exchangeInfo`` for one futures symbol."""

    symbol: str
    tick_size: Decimal
    step_size: Decimal
    min_quantity: Decimal
    max_quantity: Decimal
    min_notional: Decimal
    quantity_precision: int
    price_precision: int
    min_price: Decimal = Decimal("0")
    max_price: Decimal = Decimal("0")
    multiplier_up: Decimal = Decimal("0")
    multiplier_down: Decimal = Decimal("0")

    @classmethod
    def from_exchange_info(cls, info: Any, symbol: str) -> "SymbolFilters":
        wanted = str(symbol or "").strip().upper()
        entries = info.get("symbols") if isinstance(info, Mapping) else None
        if not isinstance(entries, list):
            raise BinanceError("exchangeInfo payload has no 'symbols' list")
        entry = next(
            (
                item
                for item in entries
                if isinstance(item, Mapping)
                and str(item.get("symbol", "")).upper() == wanted
            ),
            None,
        )
        if entry is None:
            raise UnknownSymbolError(f"{wanted} is not listed in exchangeInfo")
        raw_filters = entry.get("filters") or []
        by_type: Dict[str, Mapping[str, Any]] = {
            str(item.get("filterType")): item
            for item in raw_filters
            if isinstance(item, Mapping)
        }
        lot = by_type.get("LOT_SIZE") or by_type.get("MARKET_LOT_SIZE")
        if not lot:
            raise BinanceError(f"{wanted} has no LOT_SIZE filter")
        price_filter = by_type.get("PRICE_FILTER") or {}
        notional_filter = by_type.get("MIN_NOTIONAL") or {}
        percent_filter = by_type.get("PERCENT_PRICE") or {}
        filters = cls(
            symbol=wanted,
            tick_size=_as_decimal(price_filter.get("tickSize", "0"), "tickSize"),
            step_size=_as_decimal(lot.get("stepSize", "0"), "stepSize"),
            min_quantity=_as_decimal(lot.get("minQty", "0"), "minQty"),
            max_quantity=_as_decimal(lot.get("maxQty", "0"), "maxQty"),
            min_notional=_as_decimal(
                notional_filter.get("notional", notional_filter.get("minNotional", "0")),
                "minNotional",
            ),
            quantity_precision=int(entry.get("quantityPrecision") or 0),
            price_precision=int(entry.get("pricePrecision") or 0),
            min_price=_as_decimal(price_filter.get("minPrice", "0"), "minPrice"),
            max_price=_as_decimal(price_filter.get("maxPrice", "0"), "maxPrice"),
            multiplier_up=_as_decimal(
                percent_filter.get("multiplierUp", "0"), "multiplierUp"
            ),
            multiplier_down=_as_decimal(
                percent_filter.get("multiplierDown", "0"), "multiplierDown"
            ),
        )
        if filters.tick_size <= 0 or filters.step_size <= 0:
            raise BinanceError(f"{wanted} has a non-positive tickSize/stepSize")
        return filters

    def quantize_quantity(
        self, value: Any, *, rounding: str = ROUND_DOWN
    ) -> Decimal:
        quantity = _as_decimal(value, "quantity")
        if self.step_size <= 0:
            return quantity
        return (
            quantity / self.step_size
        ).to_integral_value(rounding=rounding) * self.step_size

    def quantize_price(
        self, value: Any, *, rounding: str = ROUND_DOWN
    ) -> Decimal:
        price = _as_decimal(value, "price")
        if self.tick_size <= 0:
            return price
        return (
            price / self.tick_size
        ).to_integral_value(rounding=rounding) * self.tick_size

    def required_quantity(self, reference_price: Any) -> Decimal:
        """Smallest step-aligned quantity that satisfies ``minNotional``."""

        reference = _as_decimal(reference_price, "reference price")
        if reference <= 0:
            raise OrderValidationError(f"reference price must be positive: {reference}")
        if self.min_notional <= 0:
            return self.min_quantity
        needed = self.quantize_quantity(
            self.min_notional / reference, rounding=ROUND_CEILING
        )
        return max(self.min_quantity, needed)

    def validate(
        self,
        quantity: Any,
        *,
        price: Any = None,
        reference_price: Any = None,
    ) -> None:
        """Raises :class:`OrderValidationError` if the order breaks the filters."""

        qty = _as_decimal(quantity, "quantity")
        if qty <= 0:
            raise OrderValidationError(f"quantity must be positive: {decimal_text(qty)}")
        if qty < self.min_quantity:
            raise OrderValidationError(
                f"quantity {decimal_text(qty)} is below minQty "
                f"{decimal_text(self.min_quantity)} for {self.symbol}"
            )
        if self.max_quantity > 0 and qty > self.max_quantity:
            raise OrderValidationError(
                f"quantity {decimal_text(qty)} exceeds maxQty "
                f"{decimal_text(self.max_quantity)} for {self.symbol}"
            )
        if qty % self.step_size != 0:
            raise OrderValidationError(
                f"quantity {decimal_text(qty)} is not a multiple of stepSize "
                f"{decimal_text(self.step_size)} for {self.symbol}"
            )

        reference = (
            _as_decimal(reference_price, "reference price")
            if reference_price is not None
            else None
        )
        order_price = _as_decimal(price, "price") if price is not None else None
        if order_price is not None:
            if order_price <= 0:
                raise OrderValidationError(
                    f"price must be positive: {decimal_text(order_price)}"
                )
            if self.min_price > 0 and order_price < self.min_price:
                raise OrderValidationError(
                    f"price {decimal_text(order_price)} is below minPrice "
                    f"{decimal_text(self.min_price)} for {self.symbol}"
                )
            if self.max_price > 0 and order_price > self.max_price:
                raise OrderValidationError(
                    f"price {decimal_text(order_price)} exceeds maxPrice "
                    f"{decimal_text(self.max_price)} for {self.symbol}"
                )
            if order_price % self.tick_size != 0:
                raise OrderValidationError(
                    f"price {decimal_text(order_price)} is not a multiple of tickSize "
                    f"{decimal_text(self.tick_size)} for {self.symbol}"
                )
            if reference is not None and reference > 0:
                if self.multiplier_down > 0:
                    lower = reference * self.multiplier_down
                    if order_price < lower:
                        raise OrderValidationError(
                            f"price {decimal_text(order_price)} is below the "
                            f"PERCENT_PRICE band ({decimal_text(lower)}) around "
                            f"{decimal_text(reference)}"
                        )
                if self.multiplier_up > 0:
                    upper = reference * self.multiplier_up
                    if order_price > upper:
                        raise OrderValidationError(
                            f"price {decimal_text(order_price)} is above the "
                            f"PERCENT_PRICE band ({decimal_text(upper)}) around "
                            f"{decimal_text(reference)}"
                        )

        notional_reference = order_price if order_price is not None else reference
        if self.min_notional > 0 and notional_reference is not None:
            notional = qty * notional_reference
            if notional < self.min_notional:
                raise OrderValidationError(
                    f"notional {decimal_text(notional)} is below minNotional "
                    f"{decimal_text(self.min_notional)} for {self.symbol}; use at "
                    f"least {decimal_text(self.required_quantity(notional_reference))}"
                )

    def describe(self) -> str:
        return (
            f"{self.symbol}: tickSize={decimal_text(self.tick_size)} "
            f"stepSize={decimal_text(self.step_size)} "
            f"minQty={decimal_text(self.min_quantity)} "
            f"minNotional={decimal_text(self.min_notional)} "
            f"pricePrecision={self.price_precision} "
            f"quantityPrecision={self.quantity_precision}"
        )


@dataclass(frozen=True)
class LimitOrderPlan:
    """A filter-validated passive limit order ready to submit."""

    symbol: str
    side: str
    quantity: Decimal
    price: Decimal
    reference_price: Decimal
    notional: Decimal
    client_order_id: str
    time_in_force: str = "GTC"

    def order_params(self) -> Dict[str, str]:
        return {
            "symbol": self.symbol,
            "side": self.side,
            "type": "LIMIT",
            "timeInForce": self.time_in_force,
            "quantity": decimal_text(self.quantity),
            "price": decimal_text(self.price),
            "newClientOrderId": self.client_order_id,
            "newOrderRespType": "RESULT",
        }


def build_limit_order(
    filters: SymbolFilters,
    *,
    side: str,
    quantity: Any,
    reference_price: Any,
    offset_bps: Any = 50,
    client_order_id: Optional[str] = None,
    now_ms: Optional[int] = None,
) -> LimitOrderPlan:
    """Builds a passive limit order ``offset_bps`` away from the reference.

    A BUY rests below the reference price and a SELL above it, so the order is
    never immediately marketable. The result is quantized to the exchange tick
    size / step size and validated (minNotional, PERCENT_PRICE band, ...).
    """

    normalised_side = str(side or "").strip().upper()
    if normalised_side not in ("BUY", "SELL"):
        raise OrderValidationError(f"side must be BUY or SELL: {side!r}")
    offset = _as_decimal(offset_bps, "offset_bps")
    if offset <= 0:
        raise OrderValidationError(
            f"offset_bps must be positive to keep the order passive: {offset}"
        )
    reference = _as_decimal(reference_price, "reference price")
    if reference <= 0:
        raise OrderValidationError(f"reference price must be positive: {reference}")
    if normalised_side == "BUY":
        price = filters.quantize_price(
            reference * (Decimal(1) - offset / Decimal(10000))
        )
        if price >= reference:
            price -= filters.tick_size
    else:
        price = filters.quantize_price(
            reference * (Decimal(1) + offset / Decimal(10000)),
            rounding=ROUND_CEILING,
        )
        if price <= reference:
            price += filters.tick_size
    qty = filters.quantize_quantity(quantity)
    filters.validate(qty, price=price, reference_price=reference)

    if client_order_id is None:
        stamp = int(now_ms if now_ms is not None else time.time() * 1000)
        client_order_id = f"zephyr-pilot-{stamp}"
    client_order_id = str(client_order_id)
    if len(client_order_id) > 36:
        raise OrderValidationError("client order id exceeds 36 characters")

    return LimitOrderPlan(
        symbol=filters.symbol,
        side=normalised_side,
        quantity=qty,
        price=price,
        reference_price=reference,
        notional=qty * price,
        client_order_id=client_order_id,
    )


@dataclass(frozen=True)
class Position:
    """One symbol's futures position, normalised from the account payload."""

    symbol: str
    quantity: Decimal
    entry_price: Decimal
    mark_price: Decimal
    unrealized_pnl: Decimal
    leverage: Decimal
    isolated: bool = False
    position_side: str = "BOTH"

    @property
    def is_flat(self) -> bool:
        return self.quantity == 0

    @property
    def side(self) -> str:
        if self.quantity > 0:
            return "BUY"
        if self.quantity < 0:
            return "SELL"
        return "FLAT"

    @property
    def notional(self) -> Decimal:
        """Quantity valued at the mark price, or at the entry price if unknown.

        Account payloads carry no ``markPrice`` (only ``positionRisk`` does), so
        a position parsed from one would otherwise report a notional of zero.
        """

        price = self.mark_price if self.mark_price > 0 else self.entry_price
        return abs(self.quantity) * price

    def closing_side(self) -> str:
        """The side that reduces this position; raises when it is already flat."""

        if self.quantity > 0:
            return "SELL"
        if self.quantity < 0:
            return "BUY"
        raise OrderValidationError(f"{self.symbol} is flat; nothing to close")

    def describe(self) -> str:
        return (
            f"{self.symbol} {self.side} qty={decimal_text(self.quantity)} "
            f"entry={decimal_text(self.entry_price)} "
            f"mark={decimal_text(self.mark_price)} "
            f"notional={decimal_text(self.notional)} "
            f"uPnL={decimal_text(self.unrealized_pnl)} "
            f"leverage={decimal_text(self.leverage)}x "
            f"{'isolated' if self.isolated else 'cross'}"
        )


@dataclass(frozen=True)
class OpenOrder:
    """One resting order, normalised from ``/fapi/v1/openOrders``."""

    symbol: str
    order_id: Optional[int]
    client_order_id: Optional[str]
    side: str
    order_type: str
    price: Decimal
    stop_price: Decimal
    quantity: Decimal
    executed_quantity: Decimal
    status: str
    reduce_only: bool = False
    time_in_force: Optional[str] = None
    close_position: bool = False

    @property
    def remaining_quantity(self) -> Decimal:
        remaining = self.quantity - self.executed_quantity
        return remaining if remaining > 0 else Decimal("0")

    def describe(self) -> str:
        stop = f" stop={decimal_text(self.stop_price)}" if self.stop_price > 0 else ""
        return (
            f"{self.symbol} {self.side} {self.order_type} "
            f"id={self.order_id} clientId={self.client_order_id} "
            f"qty={decimal_text(self.quantity)} "
            f"filled={decimal_text(self.executed_quantity)} "
            f"price={decimal_text(self.price)}{stop} status={self.status}"
        )


def _decimal_field(
    payload: Mapping[str, Any], key: str, *, default: str = "0"
) -> Decimal:
    raw = payload.get(key, default)
    try:
        return _as_decimal(raw, key)
    except OrderValidationError as exc:
        raise BinanceError(f"unparseable {key} in exchange payload: {raw!r}") from exc


def _flag_field(payload: Mapping[str, Any], key: str) -> bool:
    raw = payload.get(key)
    if isinstance(raw, bool):
        return raw
    return str(raw).strip().lower() == "true"


def parse_positions(payload: Any, *, include_flat: bool = False) -> List[Position]:
    """Extracts positions from an account payload.

    Flat entries are dropped by default: a trading loop that iterates open risk
    should never see the hundreds of zero-quantity rows an account payload
    carries for every symbol the account has ever touched.

    The venue is inconsistent about the unrealised-PnL key: an account payload
    (``/fapi/v2/account``, ``/fapi/v3/account``) reports ``unrealizedProfit``
    while ``/fapi/v2/positionRisk`` reports ``unRealizedProfit``. Both spellings
    are accepted, so either payload can feed a reconciliation. Account rows also
    carry no ``markPrice`` (that field is positionRisk-only), which is why
    ``BinanceFuturesClient.positions(..., with_mark_price=True)`` fills it from
    ``premiumIndex``.
    """

    if not isinstance(payload, Mapping):
        raise BinanceError("account payload is not an object")
    entries = payload.get("positions")
    if entries is None:
        return []
    if not isinstance(entries, list):
        raise BinanceError("account payload 'positions' is not a list")

    positions: List[Position] = []
    for entry in entries:
        if not isinstance(entry, Mapping):
            continue
        symbol = str(entry.get("symbol") or "").strip().upper()
        if not symbol:
            continue
        pnl_key = (
            "unrealizedProfit" if "unrealizedProfit" in entry else "unRealizedProfit"
        )
        position = Position(
            symbol=symbol,
            quantity=_decimal_field(entry, "positionAmt"),
            entry_price=_decimal_field(entry, "entryPrice"),
            mark_price=_decimal_field(entry, "markPrice"),
            unrealized_pnl=_decimal_field(entry, pnl_key),
            leverage=_decimal_field(entry, "leverage"),
            isolated=_flag_field(entry, "isolated"),
            position_side=str(entry.get("positionSide") or "BOTH").upper(),
        )
        if position.is_flat and not include_flat:
            continue
        positions.append(position)
    return positions


def parse_open_orders(payload: Any) -> List[OpenOrder]:
    """Normalises an ``/fapi/v1/openOrders`` payload into ``OpenOrder`` records."""

    if payload is None:
        return []
    if not isinstance(payload, list):
        raise BinanceError("openOrders payload is not a list")

    orders: List[OpenOrder] = []
    for entry in payload:
        if not isinstance(entry, Mapping):
            continue
        raw_id = entry.get("orderId")
        orders.append(
            OpenOrder(
                symbol=str(entry.get("symbol") or "").strip().upper(),
                order_id=raw_id if isinstance(raw_id, int) else None,
                client_order_id=entry.get("clientOrderId") or None,
                side=str(entry.get("side") or "").strip().upper(),
                order_type=str(entry.get("type") or "").strip().upper(),
                price=_decimal_field(entry, "price"),
                stop_price=_decimal_field(entry, "stopPrice"),
                quantity=_decimal_field(entry, "origQty"),
                executed_quantity=_decimal_field(entry, "executedQty"),
                status=str(entry.get("status") or "UNKNOWN").strip().upper(),
                reduce_only=_flag_field(entry, "reduceOnly"),
                time_in_force=entry.get("timeInForce") or None,
                close_position=_flag_field(entry, "closePosition"),
            )
        )
    return orders


Transport = Callable[[urllib.request.Request, float], Tuple[int, bytes]]


def _decode_payload(body: bytes) -> Any:
    text = body.decode("utf-8", "replace").strip() if body else ""
    if not text:
        return None
    try:
        return json.loads(text)
    except ValueError:
        return text


def _param_text(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, Decimal):
        return decimal_text(value)
    return str(value)


def _urllib_transport(
    request: urllib.request.Request, timeout: float
) -> Tuple[int, bytes]:
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return int(getattr(response, "status", 200) or 200), response.read()
    except urllib.error.HTTPError as exc:
        return int(exc.code), exc.read()
    except (urllib.error.URLError, OSError) as exc:
        origin = urllib.parse.urlsplit(request.full_url).netloc
        raise BinanceTransportError(f"request to {origin} failed: {exc}") from exc


class BinanceFuturesClient:
    """Synchronous, dependency-free REST client for Binance USDⓈ-M Futures."""

    def __init__(
        self,
        config: BinanceFuturesConfig,
        *,
        transport: Optional[Transport] = None,
        clock: Callable[[], float] = time.time,
        timeout: float = DEFAULT_TIMEOUT_S,
        recv_window: int = DEFAULT_RECV_WINDOW_MS,
    ) -> None:
        if timeout <= 0:
            raise BinanceConfigError(f"timeout must be positive: {timeout}")
        if recv_window <= 0:
            raise BinanceConfigError(f"recv_window must be positive: {recv_window}")
        self.config = config
        self.timeout = float(timeout)
        self.recv_window = int(recv_window)
        self._transport = transport or _urllib_transport
        self._clock = clock
        self._time_offset_ms = 0
        self._time_synced = False

    @property
    def time_offset_ms(self) -> int:
        return self._time_offset_ms

    @property
    def time_synced(self) -> bool:
        return self._time_synced

    def timestamp_ms(self) -> int:
        return int(self._clock() * 1000) + self._time_offset_ms

    def sync_time(self) -> int:
        """Aligns request timestamps with the exchange clock; returns the offset."""

        payload = self._request("GET", "/fapi/v1/time")
        server_ms = payload.get("serverTime") if isinstance(payload, Mapping) else None
        if not isinstance(server_ms, (int, float)) or server_ms <= 0:
            raise BinanceAPIError(
                "time endpoint returned no serverTime", endpoint="/fapi/v1/time"
            )
        offset = int(server_ms) - int(self._clock() * 1000)
        self._time_offset_ms = offset
        self._time_synced = True
        logger.debug("exchange clock offset: %+d ms", offset)
        return offset

    def signed_query(self, params: Mapping[str, Any]) -> str:
        """Encodes and HMAC-SHA256 signs parameters (exposed for tests)."""

        payload = {str(key): _param_text(value) for key, value in params.items()}
        payload["timestamp"] = str(self.timestamp_ms())
        payload["recvWindow"] = str(self.recv_window)
        query = urllib.parse.urlencode(payload)
        signature = hmac.new(
            self.config.credentials.api_secret.encode("utf-8"),
            query.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        return f"{query}&signature={signature}"

    def _request(
        self,
        method: str,
        path: str,
        params: Optional[Mapping[str, Any]] = None,
        *,
        signed: bool = False,
    ) -> Any:
        query = (
            self.signed_query(params or {})
            if signed
            else urllib.parse.urlencode(
                {str(key): _param_text(value) for key, value in (params or {}).items()}
            )
        )
        url = f"{self.config.rest_url}{path}"
        if query:
            url = f"{url}?{query}"
        headers = {
            "Accept": "application/json",
            "User-Agent": "ZephyrHFT-testnet-pilot/0.1",
        }
        if signed:
            headers["X-MBX-APIKEY"] = self.config.credentials.api_key
        data = b"" if method.upper() == "POST" else None
        request = urllib.request.Request(
            url, data=data, headers=headers, method=method.upper()
        )
        logger.debug("%s %s", method.upper(), path)
        try:
            status, body = self._transport(request, self.timeout)
        except BinanceError:
            raise
        except Exception as exc:  # pragma: no cover - defensive transport guard
            raise BinanceTransportError(f"{method.upper()} {path} failed: {exc}") from exc

        payload = _decode_payload(body)
        if status < 200 or status >= 300:
            code: Optional[int] = None
            if isinstance(payload, Mapping):
                message = str(
                    payload.get("msg") or payload.get("message") or f"HTTP {status}"
                )
                raw_code = payload.get("code")
                if isinstance(raw_code, bool):
                    raw_code = None
                if isinstance(raw_code, int):
                    code = raw_code
                elif isinstance(raw_code, str) and raw_code.lstrip("-").isdigit():
                    code = int(raw_code)
            else:
                message = str(payload or f"HTTP {status}")[:200]
            if status in RATE_LIMIT_STATUSES:
                raise BinanceRateLimitError(
                    message, status=status, code=code, endpoint=path
                )
            raise BinanceAPIError(
                message, status=status, code=code, endpoint=path
            )
        if payload is None:
            return {}
        if not isinstance(payload, (Mapping, list)):
            raise BinanceAPIError(
                f"expected JSON, received {str(payload)[:120]!r}",
                status=status,
                endpoint=path,
            )
        return payload

    def ping(self) -> Mapping[str, Any]:
        return self._request("GET", "/fapi/v1/ping")

    def exchange_info(self) -> Mapping[str, Any]:
        return self._request("GET", "/fapi/v1/exchangeInfo")

    def account(self) -> Mapping[str, Any]:
        """Fetches account state, preferring v3 and falling back to v2.

        Binance retired the v2 account endpoint on some hosts; both payloads
        expose ``availableBalance`` / ``totalWalletBalance`` and ``assets``, so
        the pilot can consume either without special-casing the version.
        """

        try:
            return self._request("GET", "/fapi/v3/account", signed=True)
        except BinanceAPIError as exc:
            if exc.status != 404:
                raise
            logger.info("account v3 unavailable (HTTP 404); falling back to v2")
        return self._request("GET", "/fapi/v2/account", signed=True)

    def mark_price(self, symbol: str) -> Decimal:
        payload = self._request(
            "GET", "/fapi/v1/premiumIndex", {"symbol": str(symbol).upper()}
        )
        raw = payload.get("markPrice") if isinstance(payload, Mapping) else None
        if raw is None:
            raise BinanceAPIError(
                "premiumIndex response has no markPrice",
                endpoint="/fapi/v1/premiumIndex",
            )
        try:
            return _as_decimal(raw, "markPrice")
        except OrderValidationError as exc:
            raise BinanceAPIError(
                f"invalid markPrice in response: {raw!r}",
                endpoint="/fapi/v1/premiumIndex",
            ) from exc

    def book_ticker(self, symbol: str) -> Mapping[str, Any]:
        return self._request(
            "GET", "/fapi/v1/ticker/bookTicker", {"symbol": str(symbol).upper()}
        )

    def new_order(
        self,
        *,
        symbol: str,
        side: str,
        order_type: str,
        quantity: Any = None,
        price: Any = None,
        stop_price: Any = None,
        time_in_force: Optional[str] = None,
        client_order_id: Optional[str] = None,
        reduce_only: Optional[bool] = None,
        post_only: bool = False,
        close_position: bool = False,
        working_type: Optional[str] = None,
    ) -> Mapping[str, Any]:
        """Submits one order, rejecting parameter combinations the venue refuses.

        ``post_only`` maps to ``timeInForce=GTX``: Binance then *rejects* an
        order that would cross instead of filling it, so a maker quote can never
        silently become a taker fill. ``close_position`` submits
        ``closePosition=true``, which the venue only accepts for stop-type orders
        and which must carry neither a quantity nor ``reduceOnly``.
        """

        params: Dict[str, Any] = {
            "symbol": str(symbol).upper(),
            "side": str(side).upper(),
            "type": str(order_type).upper(),
            "newOrderRespType": "RESULT",
        }
        if close_position:
            if quantity is not None:
                raise OrderValidationError(
                    "closePosition orders must not set quantity; the venue closes "
                    "the whole position"
                )
            if reduce_only is not None:
                raise OrderValidationError(
                    "closePosition and reduceOnly are mutually exclusive"
                )
            params["closePosition"] = "true"
        elif quantity is None:
            raise OrderValidationError(
                "quantity is required unless close_position is set"
            )
        else:
            params["quantity"] = _param_text(quantity)

        if price is not None:
            params["price"] = _param_text(price)
        if stop_price is not None:
            params["stopPrice"] = _param_text(stop_price)
        if post_only:
            requested = str(time_in_force or POST_ONLY_TIME_IN_FORCE).upper()
            if requested != POST_ONLY_TIME_IN_FORCE:
                raise OrderValidationError(
                    f"post_only requires time_in_force={POST_ONLY_TIME_IN_FORCE}, "
                    f"got {requested}"
                )
            time_in_force = POST_ONLY_TIME_IN_FORCE
        if time_in_force:
            params["timeInForce"] = str(time_in_force).upper()
        if client_order_id:
            params["newClientOrderId"] = str(client_order_id)
        if reduce_only is not None:
            params["reduceOnly"] = "true" if reduce_only else "false"
        if working_type:
            params["workingType"] = str(working_type).upper()
        return self._request("POST", "/fapi/v1/order", params, signed=True)

    @staticmethod
    def _order_params(
        symbol: str, order_id: Any = None, client_order_id: Optional[str] = None
    ) -> Dict[str, Any]:
        params: Dict[str, Any] = {"symbol": str(symbol).upper()}
        if order_id is not None:
            params["orderId"] = int(order_id)
        elif client_order_id:
            params["origClientOrderId"] = str(client_order_id)
        else:
            raise BinanceConfigError(
                "either order_id or client_order_id is required"
            )
        return params

    def query_order(
        self,
        *,
        symbol: str,
        order_id: Any = None,
        client_order_id: Optional[str] = None,
    ) -> Mapping[str, Any]:
        params = self._order_params(symbol, order_id, client_order_id)
        return self._request("GET", "/fapi/v1/order", params, signed=True)

    def cancel_order(
        self,
        *,
        symbol: str,
        order_id: Any = None,
        client_order_id: Optional[str] = None,
    ) -> Mapping[str, Any]:
        params = self._order_params(symbol, order_id, client_order_id)
        return self._request("DELETE", "/fapi/v1/order", params, signed=True)

    def cancel_all_orders(self, symbol: str) -> Mapping[str, Any]:
        return self._request(
            "DELETE",
            "/fapi/v1/allOpenOrders",
            {"symbol": str(symbol).upper()},
            signed=True,
        )

    def open_orders(self, symbol: str) -> List[OpenOrder]:
        """Lists the resting orders for ``symbol`` as typed records."""

        payload = self._request(
            "GET",
            "/fapi/v1/openOrders",
            {"symbol": str(symbol).upper()},
            signed=True,
        )
        return parse_open_orders(payload)

    def positions(
        self,
        symbol: Optional[str] = None,
        *,
        include_flat: bool = False,
        with_mark_price: bool = False,
    ) -> List[Position]:
        """Returns open positions, optionally filtered to one symbol.

        ``with_mark_price`` fills ``Position.mark_price`` from
        ``/fapi/v1/premiumIndex``, because an account payload does not carry it.
        That costs one extra request, so it is opt-in: a caller that only needs
        quantities and sides (a flatten, a cancel-all) can leave it off.
        """

        wanted = str(symbol).strip().upper() if symbol else None
        parsed = parse_positions(self.account(), include_flat=include_flat)
        if wanted is not None:
            parsed = [item for item in parsed if item.symbol == wanted]
        if with_mark_price and parsed:
            parsed = self._with_mark_prices(parsed)
        return parsed

    def _with_mark_prices(self, positions: List[Position]) -> List[Position]:
        """Overlays ``premiumIndex`` mark prices onto parsed positions.

        One request covers every symbol when more than one position is open. A
        symbol whose mark price cannot be resolved keeps whatever the payload
        carried (usually zero) instead of failing the whole reconciliation.
        """

        params = {"symbol": positions[0].symbol} if len(positions) == 1 else None
        payload = self._request("GET", "/fapi/v1/premiumIndex", params)
        entries = payload if isinstance(payload, list) else [payload]
        marks: Dict[str, Decimal] = {}
        for entry in entries:
            if not isinstance(entry, Mapping) or entry.get("markPrice") is None:
                continue
            mark_symbol = str(entry.get("symbol") or "").strip().upper()
            if not mark_symbol:
                continue
            try:
                marks[mark_symbol] = _decimal_field(entry, "markPrice")
            except BinanceError:
                continue
        return [
            replace(position, mark_price=marks[position.symbol])
            if position.symbol in marks
            else position
            for position in positions
        ]

    def set_leverage(self, symbol: str, leverage: Any) -> Mapping[str, Any]:
        """Sets the initial leverage for ``symbol`` (integer, 1..125)."""

        value = _as_decimal(leverage, "leverage")
        if value != value.to_integral_value():
            raise OrderValidationError(f"leverage must be an integer: {leverage!r}")
        if not Decimal(1) <= value <= Decimal(125):
            raise OrderValidationError(f"leverage must be within 1..125: {value}")
        return self._request(
            "POST",
            "/fapi/v1/leverage",
            {"symbol": str(symbol).upper(), "leverage": int(value)},
            signed=True,
        )

    def set_margin_mode(
        self, symbol: str, mode: str = "ISOLATED"
    ) -> Mapping[str, Any]:
        """Sets the margin mode; "no need to change" is treated as success.

        Binance answers ``-4046`` when the account already uses that mode. That
        is the state the caller asked for, so it must not abort a live start-up.
        """

        normalised = str(mode or "").strip().upper()
        if normalised not in ("ISOLATED", "CROSSED"):
            raise OrderValidationError(
                f"margin mode must be ISOLATED or CROSSED: {mode!r}"
            )
        try:
            return self._request(
                "POST",
                "/fapi/v1/marginType",
                {"symbol": str(symbol).upper(), "marginType": normalised},
                signed=True,
            )
        except BinanceAPIError as exc:
            if exc.code != -4046:
                raise
            logger.info(
                "%s already on %s margin (no change needed)", symbol, normalised
            )
            return {"code": -4046, "msg": "No need to change margin type."}

    def close_position_market(
        self,
        *,
        symbol: str,
        side: str,
        quantity: Any,
        client_order_id: Optional[str] = None,
    ) -> Mapping[str, Any]:
        """Closes ``quantity`` with a ``reduceOnly`` market order."""

        return self.new_order(
            symbol=symbol,
            side=side,
            order_type="MARKET",
            quantity=quantity,
            client_order_id=client_order_id,
            reduce_only=True,
        )

    def flatten(
        self, symbol: str, *, client_order_id: Optional[str] = None
    ) -> Mapping[str, Any]:
        """Closes the whole ``symbol`` position in one reduceOnly market order."""

        position = next(iter(self.positions(symbol)), None)
        if position is None:
            raise OrderValidationError(
                f"{symbol} has no open position; nothing to flatten"
            )
        return self.close_position_market(
            symbol=position.symbol,
            side=position.closing_side(),
            quantity=abs(position.quantity),
            client_order_id=client_order_id,
        )

    def stop_market_close_position(
        self,
        *,
        symbol: str,
        side: str,
        stop_price: Any,
        client_order_id: Optional[str] = None,
        working_type: str = "MARK_PRICE",
    ) -> Mapping[str, Any]:
        """Places the venue-side dead man's switch for an open position.

        The stop lives on Binance's matcher, so it still fires when this process
        is dead, asleep, or disconnected. ``working_type`` defaults to the mark
        price to keep a single wick on the last trade from flattening the book.
        """

        trigger = _as_decimal(stop_price, "stop_price")
        if trigger <= 0:
            raise OrderValidationError(f"stop_price must be positive: {trigger}")
        return self.new_order(
            symbol=symbol,
            side=side,
            order_type="STOP_MARKET",
            stop_price=trigger,
            client_order_id=client_order_id,
            close_position=True,
            working_type=working_type,
        )
