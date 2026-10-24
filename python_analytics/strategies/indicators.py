"""Micro-price and order-flow analytics for the strategy layer."""
from __future__ import annotations

import math
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


class OfiSignal:
    """Exponentially-weighted, z-scored order-flow imbalance (OFI) signal.

    OFI is noisy and unbounded in price units, so it is normalised against an
    EMA mean/variance pair and clipped to ``[-cap, cap]``. The resulting
    ``ofi_z`` is a dimensionless, stationary alpha term suitable for shifting a
    reservation price via ``ofi_sensitivity * ofi_z``.
    """

    def __init__(self, decay: float = 0.95, cap: float = 3.0,
                 warmup: int = 5) -> None:
        if not 0.0 < decay < 1.0:
            raise ValueError("decay must be in (0, 1)")
        if cap <= 0.0:
            raise ValueError("cap must be positive")
        self._decay = float(decay)
        self._cap = float(cap)
        self._warmup = int(warmup)
        self._mean = 0.0
        self._variance = 1.0
        self._samples = 0
        self._value = 0.0

    def update(self, ofi: float) -> float:
        """Ingests a raw OFI observation and returns the z-scored, clipped value."""
        value = float(ofi)
        self._samples += 1
        decay = self._decay
        self._mean = decay * self._mean + (1.0 - decay) * value
        deviation = value - self._mean
        self._variance = decay * self._variance + (1.0 - decay) * deviation ** 2
        if self._samples < self._warmup or self._variance <= 0.0:
            self._value = 0.0
        else:
            z_score = (value - self._mean) / math.sqrt(self._variance)
            self._value = max(-self._cap, min(self._cap, z_score))
        return self._value

    @property
    def value(self) -> float:
        """Most recent clipped z-score."""
        return self._value

    @property
    def samples(self) -> int:
        """Number of OFI observations ingested."""
        return self._samples

    def reset(self) -> None:
        """Re-initialises the estimator (used between backtest runs)."""
        self._mean = 0.0
        self._variance = 1.0
        self._samples = 0
        self._value = 0.0


class SpreadDepthStats:
    """Snapshot of top-of-book spread and depth-based microstructure metrics.

    Produces, per tick, the bid/ask spread (absolute and bps), the quoted
    depth on each side, the depth imbalance and both the mid and micro price.
    """

    def __init__(self) -> None:
        self._state: dict = {}

    def update(self, bid_price: float, bid_volume: float,
               ask_price: float, ask_volume: float) -> dict:
        """Computes and stores the latest top-of-book statistics."""
        spread = ask_price - bid_price if bid_price > 0.0 and ask_price > 0.0 else 0.0
        mid = (bid_price + ask_price) / 2.0 if bid_price > 0.0 and ask_price > 0.0 else 0.0
        spread_bps = (spread / mid * 10_000.0) if mid > 0.0 else 0.0
        total_depth = bid_volume + ask_volume
        depth_imbalance = (
            (bid_volume - ask_volume) / total_depth if total_depth > 0.0 else 0.0
        )
        self._state = {
            "bid_price": bid_price,
            "ask_price": ask_price,
            "bid_volume": bid_volume,
            "ask_volume": ask_volume,
            "spread": spread,
            "spread_bps": spread_bps,
            "mid": mid,
            "micro": MicroPriceCalculator.micro_price(
                bid_price, bid_volume, ask_price, ask_volume
            ),
            "depth_imbalance": depth_imbalance,
        }
        return self._state

    @property
    def state(self) -> dict:
        """Most recent statistics dictionary."""
        return self._state
