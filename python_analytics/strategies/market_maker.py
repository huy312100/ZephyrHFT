"""Inventory-aware two-sided market maker.

The strategy quotes symmetrically around the mid, then shifts both quotes by an
inventory skew so a long book is more likely to sell. All arithmetic is done on
raw fixed-point integers; no floating point touches prices or sizes.
"""
from __future__ import annotations

import math
import time
from typing import Optional

from ..native import hft_native
from ..utils.config import StrategySettings
from .base import Quote, Strategy
from .config import MarketConfig, MarketType
from .indicators import MicroPriceCalculator, OfiSignal


class InventoryMarketMaker(Strategy):
    name = "inventory_market_maker"

    def __init__(self, settings: StrategySettings) -> None:
        self._half_spread_ticks = settings.half_spread_ticks
        self._skew_ticks = settings.skew_ticks
        self._order_quantity = _quantity(settings.order_quantity)
        self._max_inventory = _quantity(settings.max_inventory)
        self._bid_id: Optional[int] = None
        self._ask_id: Optional[int] = None
        self._last_quote: Optional[Quote] = None
        self._quotes_sent = 0
        self._rejections = 0

    def quote(self, engine: "hft_native.TradingEngine") -> Optional[Quote]:
        bbo = engine.bbo()
        if not bbo.is_valid():
            return None
        tick = engine.config().tick_size.raw()
        if tick <= 0:
            return None

        mid = (bbo.bid_price.raw() + bbo.ask_price.raw()) // 2
        mid = _snap_to_grid(mid, engine.config().center.raw(), tick)
        inventory = engine.position().net_quantity.raw()
        skew = _inventory_skew(inventory, self._max_inventory,
                               self._skew_ticks) * tick

        bid = mid - self._half_spread_ticks * tick - skew
        ask = mid + self._half_spread_ticks * tick - skew

        # Never cross the touch; post-only orders must rest.
        best_ask = bbo.ask_price.raw()
        best_bid = bbo.bid_price.raw()
        if bid >= best_ask:
            bid = best_ask - tick
        if ask <= best_bid:
            ask = best_bid + tick
        if bid <= 0 or ask <= bid:
            return None

        at_cap = (self._max_inventory > 0 and
                  abs(inventory) >= self._max_inventory)
        size = int(self._order_quantity)
        if at_cap:
            # At the inventory cap, only quote the reducing side.
            if inventory > 0:
                return Quote(0, 0, ask, size)
            return Quote(bid, size, 0, 0)
        return Quote(bid, size, ask, size)

    def on_depth(self, engine: "hft_native.TradingEngine",
                 depth: "hft_native.DepthUpdate") -> None:
        self.on_timer(engine, time.monotonic_ns())

    def on_timer(self, engine: "hft_native.TradingEngine", now_ns: int) -> None:
        quote = self.quote(engine)
        if quote is None:
            self.cancel_all(engine)
            return
        if quote == self._last_quote:
            return

        self.cancel_all(engine)
        self._last_quote = quote
        if quote.bid_quantity_raw > 0:
            self._bid_id = self._submit(engine, hft_native.Side.BUY, quote.bid_price_raw,
                                        quote.bid_quantity_raw, now_ns)
        if quote.ask_quantity_raw > 0:
            self._ask_id = self._submit(engine, hft_native.Side.SELL, quote.ask_price_raw,
                                        quote.ask_quantity_raw, now_ns)

    def _submit(self, engine: "hft_native.TradingEngine", side: "hft_native.Side",
                price_raw: int, quantity_raw: int, now_ns: int) -> Optional[int]:
        request = hft_native.OrderRequest()
        request.side = side
        request.type = hft_native.OrderType.POST_ONLY
        request.price = hft_native.Price.from_raw(price_raw)
        request.quantity = hft_native.Quantity.from_raw(quantity_raw)
        request.timestamp = now_ns
        ack = engine.submit(request)
        self._quotes_sent += 1
        if not ack.accepted():
            self._rejections += 1
            return None
        return ack.order_id

    def cancel_all(self, engine: "hft_native.TradingEngine") -> None:
        if self._bid_id is not None:
            engine.cancel(self._bid_id)
            self._bid_id = None
        if self._ask_id is not None:
            engine.cancel(self._ask_id)
            self._ask_id = None
        self._last_quote = None

    def snapshot(self) -> dict[str, int]:
        return {
            "quotes_sent": self._quotes_sent,
            "rejections": self._rejections,
            "bid_live": int(self._bid_id is not None),
            "ask_live": int(self._ask_id is not None),
        }


def _snap_to_grid(value: int, center: int, tick: int) -> int:
    """Rounds a raw price to the nearest valid tick relative to the ladder center."""
    if tick <= 0:
        return value
    offset = value - center
    if offset >= 0:
        steps = (offset + tick // 2) // tick
    else:
        steps = -((-offset + tick // 2) // tick)
    return center + steps * tick


def _inventory_skew(inventory: int, max_inventory: int, max_steps: int) -> int:
    """Integer rounding of ``inventory / max_inventory * max_steps``."""
    if max_inventory <= 0 or max_steps <= 0:
        return 0
    if inventory >= max_inventory:
        return max_steps
    if inventory <= -max_inventory:
        return -max_steps
    numerator = inventory * max_steps
    if numerator >= 0:
        return (2 * numerator + max_inventory) // (2 * max_inventory)
    return -((-2 * numerator + max_inventory) // (2 * max_inventory))


def _quantity(text: str) -> int:
    value = hft_native.Quantity.from_string(text)
    if value is None:
        raise ValueError(f"invalid quantity: {text!r}")
    return value.raw()


# Fixed-point scale shared by Price/Quantity/Notional (8 decimal digits).
_FIXED_SCALE = hft_native.Price.from_int(1).raw()


class AvellanedaStoikovStrategy(Strategy):
    """Avellaneda-Stoikov inventory-aware market maker.

    Quotes are derived from an indifference (reservation) price that is pushed
    away from the micro-price by the current inventory, and an optimal spread
    that grows with risk aversion ``gamma`` and shrinks with book liquidity
    ``k``.

    Reservation price : ``R = S - q * gamma * sigma**2 * (T - t)``
    Optimal half-spread: ``delta = (1 / gamma) * ln(1 + gamma / k)``
    Raw quotes         : ``bid = R - delta / 2``, ``ask = R + delta / 2``

    The native ``TradingEngine`` owns the ``L2Book``, order manager and risk
    guard; the strategy reads book/position state and queries the engine's risk
    limits before generating orders. The engine re-checks every submission and
    remains authoritative.
    """

    name = "avellaneda_stoikov"

    def __init__(self, config: MarketConfig, *,
                 ofi_sensitivity: float = 0.0,
                 ofi_decay: float = 0.95,
                 ofi_cap: float = 3.0,
                 use_time_spread: bool = True) -> None:
        self._config = config
        self._calculator = MicroPriceCalculator()
        # Alpha coupling: an OFI z-score shifts the reservation price by
        # ``ofi_sensitivity * ofi_z`` (price units). Zero disables the skew so
        # the legacy Avellaneda-Stoikov quotes are reproduced exactly.
        self._ofi_sensitivity = float(ofi_sensitivity)
        self._ofi_signal = OfiSignal(decay=ofi_decay, cap=ofi_cap)
        self._use_time_spread = bool(use_time_spread)
        self._bid_id: Optional[int] = None
        self._ask_id: Optional[int] = None
        self._last_quote: Optional[Quote] = None
        self._quotes_sent = 0
        self._rejections = 0
        self._start_monotonic = time.monotonic()
        self._event_start_ns: Optional[int] = None
        self._last_micro: Optional[float] = None
        self._variance_ema = config.sigma ** 2
        self._vol_samples = 0

    # ------------------------------------------------------------------ maths
    @property
    def config(self) -> MarketConfig:
        """The frozen market configuration this instance was built from."""
        return self._config

    @property
    def ofi_sensitivity(self) -> float:
        """Alpha coupling in price units per OFI z-score (``0`` disables it)."""
        return self._ofi_sensitivity

    @property
    def ofi_signal(self) -> OfiSignal:
        """The live OFI normaliser (exposes ``decay``/``cap``)."""
        return self._ofi_signal

    @property
    def use_time_spread(self) -> bool:
        """Whether the ``gamma*sigma^2*(T-t)`` spread term is enabled."""
        return self._use_time_spread

    def reservation_price(self, micro_price: float, inventory: float,
                          time_remaining: float,
                          sigma: Optional[float] = None,
                          ofi_z: float = 0.0) -> float:
        """Indifference price ``R = S - q*gamma*sigma**2*(T-t) + ofi_sens*ofi_z``.

        The OFI term ``ofi_sensitivity * ofi_z`` skews the reservation toward the
        prevailing order-flow direction: positive OFI (buy pressure) raises the
        indifference price, making the maker willing to pay up to stay flat.
        """
        volatility = self._config.sigma if sigma is None else float(sigma)
        reservation = micro_price - inventory * self._config.gamma * (
            volatility ** 2
        ) * time_remaining
        if self._ofi_sensitivity != 0.0:
            reservation += self._ofi_sensitivity * float(ofi_z)
        return reservation

    def optimal_half_spread(self, time_remaining: Optional[float] = None,
                            sigma: Optional[float] = None) -> float:
        """Per-side optimal spread.

        ``delta = (1/gamma) * ln(1 + gamma/k)`` is the liquidity component. When
        ``time_remaining`` and ``sigma`` are supplied the time-decaying inventory
        component ``(gamma/2) * sigma**2 * (T - t)`` is added, so the full
        two-sided spread is ``gamma*sigma**2*(T-t) + (2/gamma)*ln(1+gamma/k)``.
        """
        gamma = self._config.gamma
        spread = (1.0 / gamma) * math.log1p(gamma / self._config.k)
        if (self._use_time_spread and time_remaining is not None
                and time_remaining > 0.0):
            volatility = self._config.sigma if sigma is None else float(sigma)
            spread += 0.5 * gamma * (volatility ** 2) * time_remaining
        return spread

    def raw_quotes(self, micro_price: float, inventory: float,
                   time_remaining: float,
                   sigma: Optional[float] = None,
                   ofi_z: float = 0.0) -> tuple[float, float]:
        """Unrounded ``(bid, ask)`` around the (OFI-skewed) reservation price."""
        reservation = self.reservation_price(
            micro_price, inventory, time_remaining, sigma=sigma, ofi_z=ofi_z
        )
        half = self.optimal_half_spread(time_remaining, sigma=sigma)
        return reservation - half, reservation + half

    def adjusted_quotes(self, micro_price: float, inventory: float,
                        time_remaining: float,
                        sigma: Optional[float] = None,
                        ofi_z: float = 0.0) -> tuple[float, float]:
        """Raw quotes snapped onto the venue's price grid.

        Every venue this engine trades quotes on a discrete tick grid, and an
        order priced between two ticks is not representable: the ladder cannot
        hold it, and a real exchange rejects it. Rounding is therefore applied
        to all market types, and it is directional — the bid rounds **down** and
        the ask rounds **up** — so snapping can only widen the market. Nearest-
        tick rounding could pull both sides into the same tick and produce a
        crossed or degenerate quote from a perfectly valid one.
        """
        bid, ask = self.raw_quotes(micro_price, inventory, time_remaining,
                                   sigma=sigma, ofi_z=ofi_z)
        tick = self._config.tick_size
        if tick > 0.0:
            bid = math.floor(bid / tick) * tick
            ask = math.ceil(ask / tick) * tick
        return bid, ask

    def round_to_tick(self, price: float) -> float:
        """Rounds a quote to the nearest ``tick_size`` (round half away from 0)."""
        tick = self._config.tick_size
        if tick <= 0.0:
            return price
        steps = price / tick
        if steps >= 0.0:
            return math.floor(steps + 0.5) * tick
        return math.ceil(steps - 0.5) * tick

    def in_session(self, seconds_since_midnight: float) -> bool:
        """Whether the market is open for a session-bound venue."""
        return (self._config.session_start
                <= seconds_since_midnight
                < self._config.session_end)

    # ------------------------------------------------------------- risk / io
    def _risk_adjusted_quantities(
        self,
        engine: "hft_native.TradingEngine",
        bid: float,
        ask: float,
        quantity: float,
        inventory: float,
    ) -> tuple[float, float]:
        """Scales quotes against the native risk limits before submission."""
        limits = engine.risk_limits()
        max_order_notional = float(limits.max_order_notional)
        max_order_quantity = float(limits.max_order_quantity)

        bid_qty = quantity
        ask_qty = quantity

        # Inventory guard: beyond the configured cap only quote the reducing
        # side, mirroring the C++ RiskGuard's worst-case exposure logic.
        if abs(inventory) >= self._config.inventory_limit:
            if inventory > 0.0:
                bid_qty = 0.0  # long -> sell only
            else:
                ask_qty = 0.0  # short -> buy only

        # Per-order notional cap (first-pass mirror; the engine re-checks).
        if bid_qty > 0.0 and bid > 0.0:
            bid_qty = min(bid_qty, max_order_notional / bid)
        if ask_qty > 0.0 and ask > 0.0:
            ask_qty = min(ask_qty, max_order_notional / ask)

        bid_qty = min(bid_qty, max_order_quantity)
        ask_qty = min(ask_qty, max_order_quantity)

        return max(0.0, bid_qty), max(0.0, ask_qty)

    def _time_remaining(self, now_ns: Optional[int] = None) -> float:
        """Seconds until ``T``, measured in event time when ``now_ns`` is given.

        Under replay the exchange ``E`` timestamp (nanoseconds) is authoritative;
        without it the strategy falls back to wall-clock time for live paper mode.
        """
        if now_ns and now_ns > 0:
            if self._event_start_ns is None:
                self._event_start_ns = now_ns
            elapsed = (now_ns - self._event_start_ns) / 1e9
        else:
            elapsed = time.monotonic() - self._start_monotonic
        return max(0.0, self._config.time_horizon - elapsed)

    def _observe_micro(self, micro_price: float) -> None:
        """Updates the dynamic micro-volatility estimate for crypto venues."""
        if micro_price <= 0.0:
            return
        if self._last_micro is None or self._last_micro <= 0.0:
            self._last_micro = micro_price
            return
        ret = (micro_price - self._last_micro) / self._last_micro
        self._last_micro = micro_price
        self._variance_ema = 0.95 * self._variance_ema + 0.05 * (ret ** 2)
        self._vol_samples += 1

    def _effective_sigma(self) -> float:
        if self._config.market_type == MarketType.CRYPTO and self._vol_samples >= 10:
            return math.sqrt(self._variance_ema)
        return self._config.sigma

    @staticmethod
    def _seconds_since_midnight() -> float:
        now = time.localtime()
        return now.tm_hour * 3600.0 + now.tm_min * 60.0 + now.tm_sec

    @staticmethod
    def _to_raw(value: float) -> int:
        return int(round(value * _FIXED_SCALE))

    def quote(self, engine: "hft_native.TradingEngine",
              now_ns: Optional[int] = None) -> Optional[Quote]:
        bbo = engine.bbo()
        if not bbo.is_valid():
            return None

        best_bid = float(bbo.bid_price)
        best_ask = float(bbo.ask_price)
        bid_volume = float(bbo.bid_quantity)
        ask_volume = float(bbo.ask_quantity)
        micro = self._calculator.micro_price(
            best_bid, bid_volume, best_ask, ask_volume
        )
        if micro <= 0.0:
            return None
        self._observe_micro(micro)

        # Alpha coupling: feed the raw OFI into the EMA-z-score signal so a
        # positive imbalance (buy pressure) raises the reservation price.
        ofi = self._calculator.update(best_bid, bid_volume, best_ask, ask_volume)
        ofi_z = self._ofi_signal.update(ofi)

        inventory = float(engine.position().net_quantity)
        time_remaining = self._time_remaining(now_ns)
        bid, ask = self.adjusted_quotes(
            micro, inventory, time_remaining, sigma=self._effective_sigma(),
            ofi_z=ofi_z,
        )

        # Session-bound venues stop quoting outside market hours.
        if self._config.market_type == MarketType.STOCK and not self.in_session(
            self._seconds_since_midnight()
        ):
            return None

        # Post-only quotes must rest behind the touch; never cross the book.
        tick = self._config.tick_size
        if bid >= best_ask:
            bid = best_ask - tick
        if ask <= best_bid:
            ask = best_bid + tick
        if not (0.0 < bid < ask):
            return None

        bid_qty, ask_qty = self._risk_adjusted_quantities(
            engine, bid, ask, self._config.order_quantity, inventory
        )
        if bid_qty <= 0.0 and ask_qty <= 0.0:
            return None

        return Quote(
            self._to_raw(bid) if bid_qty > 0.0 else 0,
            self._to_raw(bid_qty) if bid_qty > 0.0 else 0,
            self._to_raw(ask) if ask_qty > 0.0 else 0,
            self._to_raw(ask_qty) if ask_qty > 0.0 else 0,
        )

    # ------------------------------------------------------------ event loop
    def on_depth(self, engine: "hft_native.TradingEngine",
                 depth: "hft_native.DepthUpdate") -> None:
        exchange_ns = int(getattr(depth, "exchange_ts", 0) or 0)
        self.on_timer(engine, exchange_ns if exchange_ns > 0 else time.monotonic_ns())

    def on_timer(self, engine: "hft_native.TradingEngine", now_ns: int) -> None:
        quote = self.quote(engine, now_ns)
        if quote is None:
            self.cancel_all(engine)
            return
        if quote == self._last_quote:
            return

        self.cancel_all(engine)
        self._last_quote = quote
        if quote.bid_quantity_raw > 0:
            self._bid_id = self._submit(
                engine, hft_native.Side.BUY, quote.bid_price_raw,
                quote.bid_quantity_raw, now_ns,
            )
        if quote.ask_quantity_raw > 0:
            self._ask_id = self._submit(
                engine, hft_native.Side.SELL, quote.ask_price_raw,
                quote.ask_quantity_raw, now_ns,
            )

    def _submit(self, engine: "hft_native.TradingEngine",
                side: "hft_native.Side", price_raw: int, quantity_raw: int,
                now_ns: int) -> Optional[int]:
        request = hft_native.OrderRequest()
        request.side = side
        request.type = hft_native.OrderType.POST_ONLY
        request.price = hft_native.Price.from_raw(price_raw)
        request.quantity = hft_native.Quantity.from_raw(quantity_raw)
        request.timestamp = now_ns
        ack = engine.submit(request)
        self._quotes_sent += 1
        if not ack.accepted():
            self._rejections += 1
            return None
        return ack.order_id

    def cancel_all(self, engine: "hft_native.TradingEngine") -> None:
        if self._bid_id is not None:
            engine.cancel(self._bid_id)
            self._bid_id = None
        if self._ask_id is not None:
            engine.cancel(self._ask_id)
            self._ask_id = None
        self._last_quote = None

    def snapshot(self) -> dict:
        return {
            "quotes_sent": self._quotes_sent,
            "rejections": self._rejections,
            "bid_live": int(self._bid_id is not None),
            "ask_live": int(self._ask_id is not None),
            "market_type": str(self._config.market_type.value),
        }
