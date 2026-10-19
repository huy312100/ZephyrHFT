"""Inventory-aware two-sided market maker.

The strategy quotes symmetrically around the mid, then shifts both quotes by an
inventory skew so a long book is more likely to sell. All arithmetic is done on
raw fixed-point integers; no floating point touches prices or sizes.
"""
from __future__ import annotations

import time
from typing import Optional

from ..native import hft_native
from ..utils.config import StrategySettings
from .base import Quote, Strategy


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
