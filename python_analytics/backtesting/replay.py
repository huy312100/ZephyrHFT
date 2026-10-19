"""Deterministic offline replay of newline-delimited exchange messages."""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from ..native import factory, hft_native
from ..strategies.base import Strategy
from ..utils.config import AppSettings
from .metrics import fill_rows, summarize

logger = logging.getLogger(__name__)


@dataclass
class BacktestResult:
    stats: Dict[str, Any] = field(default_factory=dict)
    fills: List[Dict[str, Any]] = field(default_factory=list)
    strategy: Dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> Dict[str, Any]:
        return {"stats": self.stats, "fills": self.fills, "strategy": self.strategy}


def iter_jsonl(path: Path) -> Iterable[str]:
    with Path(path).open("r", encoding="utf-8") as handle:
        for line in handle:
            stripped = line.strip()
            if stripped:
                yield stripped


def replay_lines(lines: Iterable[str],
                 settings: AppSettings,
                 strategy: Optional[Strategy] = None,
                 event_limit: Optional[int] = None) -> BacktestResult:
    """Replays JSON messages through the native engine.

    Depth updates are applied before the strategy hook runs, so the strategy
    always sees the book state produced by the event it is reacting to.
    """
    engine = factory.build_engine(settings)
    limit = settings.backtest.event_limit if event_limit is None else max(0, event_limit)
    native_fills: List["hft_native.Fill"] = []
    malformed = 0
    processed = 0

    for line in lines:
        if processed >= limit:
            break
        processed += 1
        try:
            kind = hft_native.classify_json(line)
        except RuntimeError:
            malformed += 1
            continue

        if kind == hft_native.FeedMessageType.DEPTH_UPDATE:
            try:
                update = hft_native.parse_depth_json(line)
            except RuntimeError:
                malformed += 1
                continue
            if not engine.publish_depth(update):
                logger.warning("inbound queue full; dropping depth event")
                continue
            engine.poll()
            if strategy is not None:
                strategy.on_depth(engine, update)
        elif kind == hft_native.FeedMessageType.TRADE:
            try:
                trade = hft_native.parse_trade_json(line)
            except RuntimeError:
                malformed += 1
                continue
            if not engine.publish_trade(trade):
                logger.warning("inbound queue full; dropping trade event")
                continue
            engine.poll()
            if strategy is not None:
                strategy.on_trade(engine, trade)
        else:
            continue

        _drain(engine, native_fills, settings.backtest.record_fills)

    if strategy is not None:
        strategy.cancel_all(engine)
    _drain(engine, native_fills, settings.backtest.record_fills)

    rows = fill_rows(native_fills)
    return BacktestResult(
        stats=summarize(engine, rows, malformed=malformed),
        fills=rows,
        strategy=strategy.snapshot() if strategy is not None else {},
    )


def replay_file(path: Path,
                settings: AppSettings,
                strategy: Optional[Strategy] = None,
                event_limit: Optional[int] = None) -> BacktestResult:
    return replay_lines(iter_jsonl(path), settings, strategy, event_limit)


def _drain(engine: "hft_native.TradingEngine", sink: List["hft_native.Fill"],
           record: bool) -> None:
    while True:
        fill = engine.drain_fill()
        if fill is None:
            return
        if record:
            sink.append(fill)
