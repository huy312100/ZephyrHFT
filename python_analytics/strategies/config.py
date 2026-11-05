"""Market configuration and adapter rules for the strategy layer.

The Avellaneda-Stoikov strategy is parameterised per market so the same engine
can quote crypto (24/7 continuous), equities (session-bound, tick-aligned) and
commodities (session/board conventions with tick alignment).
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Optional, Union


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
    # Maximum distance of EACH quote from the micro price, in venue ticks.
    #
    # ``None`` leaves the Avellaneda-Stoikov maths untouched. A number caps it,
    # and the reason it needs capping is that every A-S term is in *price units*
    # while a venue's meaningful unit is its tick: the locked ``gamma``/``k``
    # profile puts the half-spread at 0.65 USDT, which on BTCUSDT (tick 0.1,
    # mid 80,240) is 6.5 ticks and 0.08 bps, but on ADAUSDT (tick 0.0001, mid
    # 0.2001) is 6,466 ticks and 32,313 bps -- an ask at three times the asset's
    # own price. Such a quote is not wide, it is a market that cannot exist, and
    # the replay fills nothing at all while looking like a dead venue.
    #
    # Not a locked field: the parameter lock is a hash over its schema, so
    # widening it would invalidate every committed fingerprint. Studies that set
    # it record the value in their artefact instead.
    max_quote_ticks: Optional[float] = None
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
        if self.max_quote_ticks is not None and self.max_quote_ticks < 1.0:
            # One tick per side is the smallest quote that survives snapping;
            # below it the cap and the grid cannot both be honoured, and a
            # degenerate one-tick-wide book would be sent as a real quote.
            raise ValueError("max_quote_ticks must be >= 1 tick, or None")
        if self.session_end <= self.session_start:
            raise ValueError("session_end must be after session_start")
