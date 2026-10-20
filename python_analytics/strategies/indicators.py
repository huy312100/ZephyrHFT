"""Micro-price and order-flow analytics for the strategy layer."""
from __future__ import annotations

from typing import Optional


class MicroPriceCalculator:
    """Weighted mid-price and top-of-book order-flow imbalance (OFI).

    The micro price weights each touch by the opposite side's volume, a
    first-order approximation of the unobservable efficient price when order
    sizes carry information. OFI follows Cont, Kukanov & Stoikov (2014): the
    bid/ask contributions track how top-of-book depth changes across ticks and
    the imbalance is ``OFI = bid_contribution - ask_contribution``.
    """

    def __init__(self) -> None:
        self._prev_bid_price: Optional[float] = None
        self._prev_bid_volume: Optional[float] = None
        self._prev_ask_price: Optional[float] = None
        self._prev_ask_volume: Optional[float] = None
        self._ofi: float = 0.0
        self._ticks: int = 0

    @staticmethod
    def micro_price(bid_price: float, bid_volume: float,
                    ask_price: float, ask_volume: float) -> float:
        """Weighted mid-price, safe against a book with no depth on either side."""
        total = bid_volume + ask_volume
        if total <= 0.0:
            if bid_price > 0.0 and ask_price > 0.0:
                return (bid_price + ask_price) / 2.0
            return 0.0
        return (ask_volume * bid_price + bid_volume * ask_price) / total

    def update(self, bid_price: float, bid_volume: float,
               ask_price: float, ask_volume: float) -> float:
        """Records a top-of-book tick and returns the resulting OFI."""
        if self._prev_bid_price is None or self._prev_ask_price is None:
            ofi = 0.0
        else:
            ofi = self._bid_contribution(bid_price, bid_volume) - \
                self._ask_contribution(ask_price, ask_volume)
        self._prev_bid_price = bid_price
        self._prev_bid_volume = bid_volume
        self._prev_ask_price = ask_price
        self._prev_ask_volume = ask_volume
        self._ofi = ofi
        self._ticks += 1
        return ofi

    @property
    def ofi(self) -> float:
        """Most recent order-flow imbalance."""
        return self._ofi

    @property
    def ticks(self) -> int:
        """Number of top-of-book ticks observed."""
        return self._ticks

    def _bid_contribution(self, bid_price: float, bid_volume: float) -> float:
        prev_price = self._prev_bid_price
        prev_volume = self._prev_bid_volume
        assert prev_price is not None and prev_volume is not None
        if bid_price > prev_price:
            return bid_volume
        if bid_price < prev_price:
            return -prev_volume
        return bid_volume - prev_volume

    def _ask_contribution(self, ask_price: float, ask_volume: float) -> float:
        prev_price = self._prev_ask_price
        prev_volume = self._prev_ask_volume
        assert prev_price is not None and prev_volume is not None
        if ask_price < prev_price:
            return ask_volume
        if ask_price > prev_price:
            return -prev_volume
        return ask_volume - prev_volume
