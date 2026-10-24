"""Fee assumptions shared by the backtests, the paper engine and the gates.

Every default in this repository models the **retail** Binance USDⓈ-M schedule:
2 bps maker, 5 bps taker. A negative maker rate is a *rebate*, which retail
accounts do not receive -- and assuming one silently converts a losing strategy
into a winning one, because a market maker that quotes inside the fee is only
"profitable" when somebody else subsidises it.

A study that genuinely owns a rebate or a volume tier must pass it explicitly
(``--maker-bps -1.0``); the readiness report records the rates it used, so the
assumption can never again hide inside a default.

This module imports nothing so both ``strategies`` and ``backtesting`` can use
it without a cycle.
"""
from __future__ import annotations

#: Binance USDⓈ-M VIP 0 maker fee in fractional bps (2 bps = 0.02% = 0.0002).
DEFAULT_MAKER_BPS = 2.0
#: Binance USDⓈ-M VIP 0 taker fee in fractional bps (5 bps = 0.05% = 0.0005).
DEFAULT_TAKER_BPS = 5.0
#: The same maker fee as a fraction, for APIs that take ``maker_fee``.
DEFAULT_MAKER_FEE = DEFAULT_MAKER_BPS / 10_000.0
#: The same taker fee as a fraction.
DEFAULT_TAKER_FEE = DEFAULT_TAKER_BPS / 10_000.0


def round_trip_cost_bps(maker_bps: float = DEFAULT_MAKER_BPS, *,
                        both_sides_maker: bool = True,
                        taker_bps: float = DEFAULT_TAKER_BPS) -> float:
    """Cost of opening and closing one position, in fractional basis points.

    ``both_sides_maker=True`` models a market maker that is passive on the way in
    *and* out; ``False`` models a taker entry with a taker exit. Negative values
    mean the round trip is paid for by a rebate.
    """

    if both_sides_maker:
        return 2.0 * float(maker_bps)
    return float(maker_bps) + float(taker_bps)
