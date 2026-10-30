"""Fee assumptions shared by the backtests, the paper engine and the gates.

Every default in this repository models the **retail** Binance USDⓈ-M schedule:
2 bps maker, 5 bps taker. A negative maker rate is a *rebate*, which retail
accounts do not receive -- and assuming one silently converts a losing strategy
into a winning one, because a market maker that quotes inside the fee is only
"profitable" when somebody else subsidises it.

A study that genuinely owns a rebate or a volume tier must pass it explicitly
(``--maker-bps -1.0``); the readiness report records the rates it used, so the
assumption can never again hide inside a default.

This module imports nothing from this package so both ``strategies`` and
``backtesting`` can use it without a cycle.
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Optional

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
    *and* out; ``False`` models a taker entry with a taker exit, where neither
    fill rested and the taker rate is therefore paid twice. Negative values mean
    the round trip is paid for by a rebate.
    """

    if both_sides_maker:
        return 2.0 * float(maker_bps)
    return 2.0 * float(taker_bps)


def gross_edge_bps(pre_fee_pnl: float,
                   traded_notional: float) -> Optional[float]:
    """Gross PnL in basis points of the notional it was earned on.

    A fee is charged per unit of notional, so this is the only unit in which a
    measured edge and a fee rate are directly comparable. A study whose gross
    edge is ``g`` bps breaks even at a maker fee of exactly ``g`` bps and loses
    ``maker_bps - g`` bps on every unit of notional once the fee goes above it.
    Returns ``None`` when nothing was traded: an empty run has no unit to charge
    a fee against, and reporting ``0.0`` would read as a measured edge. It also
    returns ``None`` when the study's numbers are not finite, in either
    direction: ``json.dumps`` writes ``NaN``/``Infinity`` as bare tokens, which
    are not valid JSON, so a corrupted artefact must not reach the report as a
    derived measurement.
    """
    notional = float(traded_notional)
    if not math.isfinite(notional) or notional <= 0.0:
        return None
    pnl = float(pre_fee_pnl)
    if not math.isfinite(pnl):
        return None
    edge = pnl / notional * 10_000.0
    return edge if math.isfinite(edge) else None


@dataclass(frozen=True)
class FeeWall:
    """Where a measured gross edge stops paying for its own fees.

    ``gross_bps`` is what the study earned *before* fees, per unit of notional
    traded, so a venue charging exactly that many bps is break-even. The number
    is structural rather than tunable: widening the quotes changes it, but only
    within a range, and past that range the extra width is paid for in adverse
    selection. ``maker_bps`` above ``gross_bps`` means every fill is a small
    donation, and that is a decision about the fee tier (or the venue), not
    about the strategy's parameters.
    """

    gross_bps: float
    maker_bps: float
    traded_notional: float
    fills: int = 0

    @classmethod
    def from_study(cls, *, pre_fee_pnl: float, traded_notional: float,
                   maker_bps: float, fills: int = 0) -> "Optional[FeeWall]":
        """Builds the wall from a study's pre-fee PnL and traded notional."""
        gross = gross_edge_bps(pre_fee_pnl, traded_notional)
        if gross is None:
            return None
        return cls(gross_bps=gross, maker_bps=float(maker_bps),
                   traded_notional=float(traded_notional), fills=int(fills))

    @property
    def net_bps(self) -> float:
        """Edge left after the fee, per unit of notional traded."""
        return self.gross_bps - self.maker_bps

    @property
    def viable(self) -> bool:
        """Whether the edge strictly covers the fee."""
        return self.net_bps > 0.0

    @property
    def fee_multiple(self) -> "Optional[float]":
        """How many times the fee exceeds the edge (``None`` if there is none).

        A tiny but finite edge can still overflow the quotient (``2.0 / 1e-309``
        is infinite), and an infinite multiple reaches the report as the
        nonstandard ``Infinity`` token, so it counts as "no multiple" here.
        """
        if self.gross_bps <= 0.0:
            return None
        multiple = self.maker_bps / self.gross_bps
        return multiple if math.isfinite(multiple) else None

    def describe(self) -> str:
        """One line naming both sides and the break-even fee between them."""
        return (f"gross edge {self.gross_bps:+.3f} bps vs {self.maker_bps:g} bps"
                f" maker fee | break-even maker fee {self.gross_bps:+.3f} bps")
