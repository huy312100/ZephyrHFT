"""Market configuration and adapter rules for the strategy layer.

The Avellaneda-Stoikov strategy is parameterised per market so the same engine
can quote crypto (24/7 continuous), equities (session-bound, tick-aligned) and
commodities (session/board conventions with tick alignment).
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Union


class MarketType(str, Enum):
    """Venue adapter class used to select quoting rules."""

    CRYPTO = "CRYPTO"
    STOCK = "STOCK"
    COMMODITIES = "COMMODITIES"


@dataclass
class MarketConfig:
    """Parameters for the Avellaneda-Stoikov market maker."""

    # Risk aversion of the market maker. Larger values tilt quotes harder away
    # from inventory and widen the spread.
    gamma: float = 0.1
    # Asset volatility per sqrt(unit time). For CRYPTO venues this can be
    # overridden by the dynamic micro-volatility estimator.
    sigma: float = 0.02
    # Order-book liquidity / order-arrival intensity parameter.
    k: float = 1.5
    # Maximum absolute inventory position the strategy is allowed to hold.
    inventory_limit: float = 5.0
    # Minimum price increment used for tick alignment on exchange-style venues.
    tick_size: float = 0.5
    # Adapter class: "CRYPTO", "STOCK" or "COMMODITIES".
    market_type: Union[MarketType, str] = MarketType.CRYPTO
    # Horizon (seconds) over which the reservation price decays to the
    # micro-price; ``T - t`` in the Avellaneda-Stoikov formula.
    time_horizon: float = 60.0
    # Base quantity (asset units) quoted on each side before risk scaling.
    order_quantity: float = 0.05
    # Session bounds (seconds since midnight) enforced for STOCK markets.
    session_start: float = 9.0 * 3600.0
    session_end: float = 16.0 * 3600.0

    def __post_init__(self) -> None:
        if not isinstance(self.market_type, MarketType):
            self.market_type = MarketType(str(self.market_type).upper())
        if self.gamma <= 0.0:
            raise ValueError("gamma must be positive")
        if self.sigma < 0.0:
            raise ValueError("sigma must be non-negative")
        if self.k <= 0.0:
            raise ValueError("k must be positive")
        if self.inventory_limit <= 0.0:
            raise ValueError("inventory_limit must be positive")
        if self.tick_size <= 0.0:
            raise ValueError("tick_size must be positive")
        if self.time_horizon <= 0.0:
            raise ValueError("time_horizon must be positive")
        if self.order_quantity <= 0.0:
            raise ValueError("order_quantity must be positive")
        if self.session_end <= self.session_start:
            raise ValueError("session_end must be after session_start")
