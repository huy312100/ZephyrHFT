"""Phase 2 bridge: stream Phase 1 gzip NDJSON into the C++ ``paper_engine``.

The per-level parsing and book update happen inside ``hft_native`` (allocation
free); Python only reads compressed lines and hands them over in bulk batches,
so no per-level Python object is ever constructed.
"""
from __future__ import annotations

import gzip
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Dict, Iterable, Iterator, List, Optional

from .native import hft_native

logger = logging.getLogger(__name__)


def iter_gzip_ndjson(path: Path) -> Iterator[str]:
    with gzip.open(path, "rt", encoding="utf-8") as handle:
        for line in handle:
            stripped = line.strip()
            if stripped:
                yield stripped


def iter_ndjson(path: Path) -> Iterator[str]:
    with Path(path).open("r", encoding="utf-8") as handle:
        for line in handle:
            stripped = line.strip()
            if stripped:
                yield stripped


def build_paper_engine(
    tick_size: str,
    center: str,
    maker_bps: int = 0,
    taker_bps: int = 5,
    initial_cash: float = 0.0,
) -> "hft_native.PaperEngine":
    return hft_native.PaperEngine(
        hft_native.Price.from_string(tick_size),
        hft_native.Price.from_string(center),
        int(maker_bps),
        int(taker_bps),
        hft_native.Notional.from_string(f"{initial_cash:.8f}"),
    )


def ingest_batch(engine: "hft_native.PaperEngine", lines: List[str]) -> Dict[str, int]:
    stats = hft_native.ingest_lines(engine, lines)
    return {
        "lines": stats.lines,
        "depth": stats.depth_events,
        "trade": stats.trade_events,
        "malformed": stats.malformed,
        "gaps": stats.gaps,
    }


def feed_archive(
    engine: "hft_native.PaperEngine",
    source: Path,
    *,
    batch_size: int = 512,
    on_batch: Optional[Callable[[Dict[str, int]], None]] = None,
) -> Dict[str, int]:
    iterator = iter_gzip_ndjson(source) if str(source).endswith(".gz") else iter_ndjson(source)
    totals = {"lines": 0, "depth": 0, "trade": 0, "malformed": 0, "gaps": 0, "batches": 0}
    batch: List[str] = []
    for line in iterator:
        batch.append(line)
        if len(batch) >= batch_size:
            _drain(engine, batch, totals, on_batch)
            batch = []
    if batch:
        _drain(engine, batch, totals, on_batch)
    return totals


def _drain(
    engine: "hft_native.PaperEngine",
    batch: List[str],
    totals: Dict[str, int],
    on_batch: Optional[Callable[[Dict[str, int]], None]],
) -> None:
    stats = hft_native.ingest_lines(engine, batch)
    totals["lines"] += stats.lines
    totals["depth"] += stats.depth_events
    totals["trade"] += stats.trade_events
    totals["malformed"] += stats.malformed
    totals["gaps"] = stats.gaps
    totals["batches"] += 1
    if on_batch is not None:
        on_batch(
            {
                "lines": stats.lines,
                "depth": stats.depth_events,
                "trade": stats.trade_events,
                "malformed": stats.malformed,
                "gaps": stats.gaps,
            }
        )


@dataclass
class PaperSession:
    """Owns a C++ ``paper_engine`` plus Python-side fill buffers."""

    engine: "hft_native.PaperEngine"
    fills: List[Dict[str, float]] = None  # type: ignore[assignment]
    maker_bps: int = 0
    taker_bps: int = 5

    def __post_init__(self) -> None:
        if self.fills is None:
            self.fills = []

    @classmethod
    def create(
        cls,
        tick_size: str,
        center: str,
        maker_bps: int = 0,
        taker_bps: int = 5,
        initial_cash: float = 0.0,
    ) -> "PaperSession":
        return cls(
            build_paper_engine(tick_size, center, maker_bps, taker_bps, initial_cash),
            maker_bps=maker_bps,
            taker_bps=taker_bps,
        )

    def feed(self, lines: Iterable[str]) -> Dict[str, int]:
        return ingest_batch(self.engine, list(lines))

    def feed_file(self, path: Path, batch_size: int = 512) -> Dict[str, int]:
        return feed_archive(self.engine, path, batch_size=batch_size)

    def market(self, side: "hft_native.Side", quantity: str) -> Dict[str, float]:
        request = hft_native.OrderRequest()
        request.side = side
        request.type = hft_native.OrderType.MARKET
        request.quantity = hft_native.Quantity.from_string(quantity)
        ack = self.engine.submit(request)
        self._drain_fills()
        return {
            "accepted": ack.accepted(),
            "filled": float(ack.filled),
            "average_price": float(ack.average_price),
            "slippage": float(ack.slippage),
            "reason": str(ack.reason),
        }

    def _drain_fills(self) -> None:
        while True:
            fill = self.engine.drain_fill()
            if fill is None:
                return
            self.fills.append(
                {
                    "price": float(fill.price),
                    "quantity": float(fill.quantity),
                    "fee": float(fill.fee),
                    "side": "BUY" if fill.side == hft_native.Side.BUY else "SELL",
                    "is_maker": bool(fill.is_maker),
                }
            )

    def metrics(self) -> Dict[str, float]:
        metrics = self.engine.metrics()
        account = metrics.account
        return {
            "depth_events": metrics.depth_events,
            "gap_events": metrics.gap_events,
            "orders_submitted": metrics.orders_submitted,
            "orders_rejected": metrics.orders_rejected,
            "orders_cancelled": metrics.orders_cancelled,
            "open_orders": metrics.open_orders,
            "fills": metrics.fills,
            "filled_quantity": float(metrics.filled_quantity),
            "traded_notional": float(metrics.traded_notional),
            "slippage_total": float(metrics.slippage_total),
            "p50_ns": metrics.p50_ns,
            "p99_ns": metrics.p99_ns,
            "p999_ns": metrics.p999_ns,
            "last_frame_latency_ns": metrics.last_frame_latency_ns,
            "cash": float(account.cash),
            "fees_paid": float(account.fees_paid),
            "realized_pnl": float(account.realized_pnl),
            "unrealized_pnl": float(account.unrealized_pnl),
            "equity": float(account.equity),
            "net_quantity": float(account.net_quantity),
            "average_entry_price": float(account.average_entry_price),
            "maker_fills": account.maker_fills,
            "taker_fills": account.taker_fills,
            "fill_count": account.fill_count,
        }
