"""Strategy interfaces kept free of engine internals."""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:  # pragma: no cover - typing only
    from ..native import hft_native


@dataclass(frozen=True)
class Quote:
    """Two-sided quote in fixed-point raw units."""

    bid_price_raw: int
    bid_quantity_raw: int
    ask_price_raw: int
    ask_quantity_raw: int

    def is_valid(self) -> bool:
        return (
            self.bid_price_raw > 0
            and self.ask_price_raw > self.bid_price_raw
            and self.bid_quantity_raw > 0
            and self.ask_quantity_raw > 0
        )


class Strategy(ABC):
    """Event hooks invoked by the replay/live runners."""

    name = "strategy"

    @abstractmethod
    def on_depth(self, engine: "hft_native.TradingEngine",
                 depth: "hft_native.DepthUpdate") -> None:
        """Called after a depth update has been applied to the book."""

    def on_trade(self, engine: "hft_native.TradingEngine",
                 trade: "hft_native.TradeTick") -> None:
        """Called for every trade tick (no-op by default)."""

    def on_timer(self, engine: "hft_native.TradingEngine", now_ns: int) -> None:
        """Called periodically by the runner (no-op by default)."""

    def cancel_all(self, engine: "hft_native.TradingEngine") -> None:
        """Best-effort cancellation hook used on shutdown."""

    def snapshot(self) -> dict[str, Any]:
        """Returns JSON-serialisable diagnostics."""
        return {}
