"""Deterministic offline replay of newline-delimited exchange messages."""
from __future__ import annotations

import glob
import logging
import math
import re
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Union

from ..bridge import iter_gzip_ndjson, iter_ndjson
from ..native import factory, hft_native
from ..strategies.base import Strategy
from ..utils.config import AppSettings, require_aligned_center
from ..costs import DEFAULT_MAKER_BPS, DEFAULT_TAKER_BPS
from .metrics import distribution, fill_rows, quant_summary, summarize

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


# --------------------------------------------------------------------------- #
# Phase 3 — bulk replay over the zero-allocation ``paper_engine``             #
# --------------------------------------------------------------------------- #

# Matches Binance's ``"u":<final_update_id>`` field without a full JSON parse.
_FINAL_ID_RE = re.compile(r'"u"\s*:\s*(\d+)')


def _stddev(values: Sequence[float]) -> float:
    """Population standard deviation of a sample (0.0 for empty/singleton)."""
    if len(values) < 2:
        return 0.0
    mean = sum(values) / len(values)
    variance = sum((value - mean) ** 2 for value in values) / len(values)
    return math.sqrt(variance)


def _extract_final_id(line: str) -> int:
    """Best-effort extraction of the depth frame's ``final_update_id``."""
    match = _FINAL_ID_RE.search(line)
    return int(match.group(1)) if match else 0


def _wide_risk_limits() -> "hft_native.RiskLimits":
    """Wide limits so the C++ paper engine does not clip strategy quotes."""
    limits = hft_native.RiskLimits()
    limits.max_order_notional = hft_native.Notional.from_string("1000000000")
    limits.max_order_quantity = hft_native.Quantity.from_string("100000")
    limits.max_position_notional = hft_native.Notional.from_string("1000000000")
    limits.max_open_orders = 1024
    return limits


# Maker fill models exposed by the native engine. ``touch`` is the legacy
# book-crossing model; ``queue`` infers trades from depth depletion; ``trade``
# fills from the aggregated tape. See ``cpp_engine/include/paper_engine.hpp``.
FILL_MODELS = {
    "touch": "TOUCH",
    "queue": "QUEUE",
    "trade": "TRADE",
}


def resolve_fill_model(name: str) -> "hft_native.FillModel":
    """Maps a CLI fill-model name to the native enum, rejecting typos loudly."""
    key = str(name).strip().lower()
    if key not in FILL_MODELS:
        choices = ", ".join(sorted(FILL_MODELS))
        raise ValueError(f"unknown fill model {name!r} (choose from {choices})")
    return getattr(hft_native.FillModel, FILL_MODELS[key])


class BacktestEngine:
    """Adapter exposing the ``TradingEngine``-style surface used by strategies.

    ``paper_engine`` (Phase 2) owns matching and the ledger but has no
    ``risk_limits()``. This thin wrapper supplies the wide-limits default and
    accumulates submitted quantity so the strategy objects run unmodified and
    the fill rate can be derived. All matching still happens in C++.
    """

    def __init__(self, engine: "hft_native.PaperEngine",
                 order_quantity: Union[str, float] = "0.05") -> None:
        self._engine = engine
        self._order_quantity = float(order_quantity)
        self._risk_limits = _wide_risk_limits()
        self._submitted_quantity = 0.0

    # -- TradingEngine-compatible surface ----------------------------------
    def bbo(self) -> "hft_native.Bbo":
        return self._engine.bbo()

    def position(self) -> "hft_native.PositionSnapshot":
        return self._engine.position()

    def risk_limits(self) -> "hft_native.RiskLimits":
        return self._risk_limits

    def submit(self, request: "hft_native.OrderRequest") -> "hft_native.paper_ack":
        self._submitted_quantity += float(request.quantity)
        return self._engine.submit(request)

    def cancel(self, order_id: int, timestamp: int = 0) -> bool:
        return self._engine.cancel(order_id, timestamp)

    def cancel_all(self, timestamp: int = 0) -> int:
        return self._engine.cancel_all(timestamp)

    def metrics(self) -> "hft_native.PaperMetrics":
        return self._engine.metrics()

    def mark_price(self) -> "hft_native.Price":
        return self._engine.mark_price()

    @property
    def submitted_quantity(self) -> float:
        """Cumulative base quantity submitted through this adapter."""
        return self._submitted_quantity

    @property
    def order_quantity(self) -> float:
        """Fallback per-order quantity used when no strategy is attached."""
        return self._order_quantity


def seed_from_snapshot(engine: "hft_native.PaperEngine",
                       snapshot: Mapping[str, Any]) -> int:
    """Seeds the C++ ladder from a REST snapshot and returns ``lastUpdateId``.

    ``snapshot`` is a mapping with ``bids``/``asks`` lists of ``[price, qty]``
    pairs and an optional ``lastUpdateId``. Levels are applied directly to
    ``engine.book()`` (absolute L2 semantics) so they do not count as depth
    events and do not advance the gap detector.
    """
    book = engine.book()
    for side, key in ((hft_native.Side.BUY, "bids"),
                      (hft_native.Side.SELL, "asks")):
        for level in snapshot.get(key, []):
            try:
                price = hft_native.Price.from_string(str(level[0]))
                quantity = hft_native.Quantity.from_string(str(level[1]))
            except (ValueError, TypeError, IndexError):
                continue
            book.apply_level(side, price, quantity, 0)
    return int(snapshot.get("lastUpdateId", 0) or 0)


@dataclass
class ReplaySummary:
    """Aggregated result of a Phase 3 bulk replay."""

    stats: Dict[str, Any] = field(default_factory=dict)
    fills: List[Dict[str, Any]] = field(default_factory=list)
    equity_curve: List[float] = field(default_factory=list)
    position_curve: List[float] = field(default_factory=list)
    per_file: List[Dict[str, Any]] = field(default_factory=list)
    elapsed_s: float = 0.0
    strategy: Dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> Dict[str, Any]:
        return {
            "stats": self.stats,
            "fills": self.fills,
            "equity_curve": self.equity_curve,
            "position_curve": self.position_curve,
            "per_file": self.per_file,
            "elapsed_s": self.elapsed_s,
            "strategy": self.strategy,
        }


class BulkReplayer:
    """Batch replay of Phase 1 archives through the C++ ``paper_engine``.

    Parsing and book mutation run allocation-free inside ``ingest_lines``; this
    class only assembles line batches and, when a ``strategy`` is supplied, runs
    its ``on_timer`` hook every ``frames_per_hook`` frames. Event time for the
    hook is the exchange ``E`` timestamp carried on the book's BBO; a synthetic
    clock advances deterministically when the archive has no timestamps.

    Determinism: for a fixed configuration and line sequence the ledger is
    bit-identical across runs. Only wall-clock timing and the boundary-latency
    distribution vary.
    """

    def __init__(self, *, tick_size: Union[str, float], center: Union[str, float],
                 maker_bps: float = DEFAULT_MAKER_BPS,
                 taker_bps: float = DEFAULT_TAKER_BPS,
                 initial_cash: float = 0.0, batch_size: int = 64,
                 frames_per_hook: int = 1,
                 seed: Optional[Mapping[str, Any]] = None,
                 order_quantity: Union[str, float] = "0.05",
                 fill_model: str = "touch") -> None:
        self._tick_size = str(tick_size)
        self._center = str(center)
        # A centre off the tick grid cannot index a single price, so it is
        # rejected here rather than after a silently empty study.
        require_aligned_center(self._center, self._tick_size)
        # Fee rates are fractional basis points: 0.5 is a valid 0.5 bps charge,
        # so they are kept as floats and passed straight to the C++ ledger, which
        # freezes them into micro-bps. Never round/truncate them here.
        self._maker_bps = float(maker_bps)
        self._taker_bps = float(taker_bps)
        self._initial_cash = float(initial_cash)
        self._batch_size = max(1, int(batch_size))
        self._frames_per_hook = max(1, int(frames_per_hook))
        self._seed = seed
        self._order_quantity = order_quantity
        # Resolved before the engine is built so a typo fails at construction.
        self._fill_model = str(fill_model).strip().lower()
        resolve_fill_model(self._fill_model)
        self._engine = self._build_engine()
        self._adapter = BacktestEngine(self._engine, order_quantity)
        self._snapshot_last_id = 0
        self._synth_ns = 0

    # ------------------------------------------------------------------ setup
    def _build_engine(self) -> "hft_native.PaperEngine":
        return hft_native.PaperEngine(
            hft_native.Price.from_string(self._tick_size),
            hft_native.Price.from_string(self._center),
            self._maker_bps,
            self._taker_bps,
            hft_native.Notional.from_string(f"{self._initial_cash:.8f}"),
            resolve_fill_model(self._fill_model),
        )

    @property
    def fill_model(self) -> str:
        """The maker fill model this replayer was configured with."""
        return self._fill_model

    @property
    def engine(self) -> "hft_native.PaperEngine":
        """The underlying C++ paper engine (advanced access)."""
        return self._engine

    def reset(self) -> None:
        """Clears book, ledger, histogram and counters (keeps tick/centre)."""
        self._engine.reset()
        self._adapter._submitted_quantity = 0.0
        self._snapshot_last_id = 0
        self._synth_ns = 0

    # ----------------------------------------------------------------- replay
    def replay(self, sources: Union[str, Path, Sequence[Union[str, Path]]],
               strategy: Optional[Strategy] = None,
               event_limit: int = 0) -> ReplaySummary:
        """Replays one or more archives; returns aggregated telemetry."""
        started = time.monotonic()
        self._prepare(event_limit)
        paths: List[Path] = [Path(sources)] if isinstance(sources, (str, Path)) \
            else [Path(source) for source in sources]
        per_file: List[Dict[str, Any]] = []
        for path in paths:
            info, _ = self._replay_source(path, strategy)
            per_file.append(info)
            if self._remaining is not None and self._remaining <= 0:
                break
        return self._finalize(strategy, started, per_file)

    def replay_lines(self, lines: Iterable[str],
                     strategy: Optional[Strategy] = None,
                     event_limit: int = 0) -> ReplaySummary:
        """Replays an in-memory line sequence (used by the optimizer)."""
        started = time.monotonic()
        self._prepare(event_limit)
        info, _ = self._replay_iterator(iter(lines), "<lines>", strategy)
        return self._finalize(strategy, started, [info])

    def replay_archives(self, pattern: Union[str, Path],
                        strategy: Optional[Strategy] = None,
                        event_limit: int = 0) -> ReplaySummary:
        """Replays every archive matching ``pattern`` in sorted order."""
        paths = sorted(glob.glob(str(pattern)))
        if not paths:
            raise FileNotFoundError(f"no archives match {pattern!r}")
        return self.replay(paths, strategy, event_limit)

    # --------------------------------------------------------------- internals
    def _prepare(self, event_limit: int = 0) -> None:
        self.reset()
        if self._seed is not None:
            self._snapshot_last_id = seed_from_snapshot(self._engine, self._seed)
        self._remaining = int(event_limit) if event_limit and event_limit > 0 else None
        self._totals = {"lines": 0, "depth": 0, "trade": 0, "malformed": 0, "gaps": 0}
        self._equity_curve: List[float] = []
        self._position_curve: List[float] = []
        self._slippage: List[float] = []
        self._boundary: List[float] = []
        self._native_fills: List["hft_native.Fill"] = []

    def _finalize(self, strategy: Optional[Strategy], started: float,
                  per_file: List[Dict[str, Any]]) -> ReplaySummary:
        if strategy is not None:
            strategy.cancel_all(self._adapter)
            self._drain_native_fills()

        account = self._engine.account()
        self._equity_curve.append(float(account.equity()))
        self._position_curve.append(float(account.net_quantity()))

        metrics = self._engine.metrics()
        fill_dicts = fill_rows(self._native_fills)
        elapsed = time.monotonic() - started
        boundary_dist = distribution(self._boundary)

        submitted = self._adapter.submitted_quantity
        if submitted <= 0.0:
            submitted = float(metrics.orders_submitted) * float(self._adapter.order_quantity)

        stats = quant_summary(
            realized_pnl=float(metrics.account.realized_pnl),
            unrealized_pnl=float(metrics.account.unrealized_pnl),
            fees_paid=float(metrics.account.fees_paid),
            traded_notional=float(metrics.traded_notional),
            equity_curve=self._equity_curve,
            fills=fill_dicts,
            slippage_samples=self._slippage,
            depth_events=metrics.depth_events,
            elapsed_s=elapsed,
            latency_ns={
                "engine_p50_ns": metrics.p50_ns,
                "engine_p99_ns": metrics.p99_ns,
                "engine_p999_ns": metrics.p999_ns,
                "boundary_p50_ns": int(boundary_dist.get("p50", 0)),
                "boundary_p90_ns": int(boundary_dist.get("p90", 0)),
                "boundary_p99_ns": int(boundary_dist.get("p99", 0)),
            },
            submitted_quantity=submitted,
        )
        stats["gap_events"] = metrics.gap_events
        stats["fills"] = metrics.fills
        stats["orders_submitted"] = metrics.orders_submitted
        stats["orders_rejected"] = metrics.orders_rejected
        stats["maker_fills"] = metrics.account.maker_fills
        stats["taker_fills"] = metrics.account.taker_fills
        stats["lines"] = self._totals["lines"]
        stats["malformed_events"] = self._totals["malformed"]
        stats["inventory_std"] = _stddev(self._position_curve)
        # Trade prints are the fill source under the tape model; surfacing the
        # count makes "zero fills" legible as "no tape" rather than "no signal".
        stats["trade_events"] = metrics.trade_events
        stats["fill_model"] = self._fill_model
        # Grid health. A blind ladder (stale anchor, wrong venue tick) rejects
        # every level and still reports a plausible-looking run, so the study has
        # to carry the counters that expose it. ``unpriced_frames`` is the one
        # that means it: wide feeds legitimately drop far-away levels, but a
        # frame that leaves no two-sided top is a book that is not the market.
        stats["rejected_levels"] = metrics.rejected_levels
        stats["book_recenters"] = metrics.book_recenters
        stats["unpriced_frames"] = metrics.unpriced_frames

        return ReplaySummary(
            stats=stats,
            fills=fill_dicts,
            equity_curve=self._equity_curve,
            position_curve=self._position_curve,
            per_file=per_file,
            elapsed_s=elapsed,
            strategy=strategy.snapshot() if strategy is not None else {},
        )

    # --------------------------------------------------------------- internals
    def _replay_source(self, path: Path, strategy: Optional[Strategy]) -> tuple:
        """Streams one archive, returning ``(info, remaining)``."""
        iterator = iter_gzip_ndjson(path) if str(path).endswith(".gz") \
            else iter_ndjson(path)
        return self._replay_iterator(iterator, str(path), strategy)

    def _replay_iterator(self, iterator: Iterable[str], label: str,
                         strategy: Optional[Strategy]) -> tuple:
        """Shared per-line loop; returns ``(info, remaining)``."""
        batch: List[str] = []
        file_lines = file_depth = file_malformed = 0
        gap_before = self._engine.metrics().gap_events
        remaining = self._remaining

        for line in iterator:
            if remaining is not None and remaining <= 0:
                break
            file_lines += 1
            if self._snapshot_last_id > 0:
                final_id = _extract_final_id(line)
                if final_id and final_id <= self._snapshot_last_id:
                    continue
            batch.append(line)
            if len(batch) >= self._frames_per_hook:
                depth, malformed = self._flush(batch, strategy)
                file_depth += depth
                file_malformed += malformed
                if remaining is not None:
                    remaining -= depth
                if remaining is not None and remaining <= 0:
                    break
        depth, malformed = self._flush(batch, strategy)
        file_depth += depth
        file_malformed += malformed
        if remaining is not None:
            remaining -= depth

        gap_after = self._engine.metrics().gap_events
        self._totals["lines"] += file_lines
        self._totals["depth"] += file_depth
        self._totals["malformed"] += file_malformed
        self._totals["gaps"] += gap_after - gap_before
        self._remaining = remaining
        return {
            "path": label,
            "lines": file_lines,
            "depth": file_depth,
            "malformed": file_malformed,
            "gaps": gap_after - gap_before,
        }, remaining

    def _flush(self, batch: List[str], strategy: Optional[Strategy]) -> tuple:
        """Ingests one batch and runs the strategy hook; returns (depth, bad)."""
        if not batch:
            return 0, 0
        start = time.perf_counter_ns()
        stats = hft_native.ingest_lines(self._engine, batch)
        self._boundary.append(time.perf_counter_ns() - start)
        batch.clear()
        self._after_frames(strategy)
        return stats.depth_events, stats.malformed

    def _after_frames(self, strategy: Optional[Strategy]) -> None:
        if strategy is None:
            return
        bbo = self._engine.bbo()
        event_ns = int(bbo.exchange_ts) if bbo.exchange_ts > 0 else self._next_synth_ns()
        strategy.on_timer(self._adapter, event_ns)
        account = self._engine.account()
        self._equity_curve.append(float(account.equity()))
        self._position_curve.append(float(account.net_quantity()))
        self._slippage.append(abs(float(self._engine.last_slippage())))
        before = len(self._native_fills)
        self._drain_native_fills()
        if len(self._native_fills) > before:
            self.on_frame(event_ns, self._native_fills[before:])
        else:
            self.on_frame(event_ns, ())

    def on_frame(self, event_ns: int, new_fills: Sequence["hft_native.Fill"]) -> None:
        """Hook invoked after every strategy hook, with the frame's new fills.

        Subclasses measure execution quality here (see
        :class:`~python_analytics.backtesting.markouts.MarkoutReplayer`): the
        engine's event clock and its post-match book are both available at this
        point, which is the only place a simulated fill can be paired with the
        mid that produced it. The default implementation does nothing.
        """

    def _next_synth_ns(self) -> int:
        """Deterministic 100 ms/frame clock for timestamp-less archives."""
        self._synth_ns += 100_000_000
        return self._synth_ns

    def _drain_native_fills(self) -> None:
        while True:
            fill = self._engine.drain_fill()
            if fill is None:
                return
            self._native_fills.append(fill)


def replay_archive(path: Union[str, Path], strategy: Optional[Strategy] = None, *,
                   tick_size: Union[str, float], center: Union[str, float],
                   **kwargs: Any) -> ReplaySummary:
    """Convenience wrapper for a single-archive replay."""
    return BulkReplayer(tick_size=tick_size, center=center, **kwargs).replay(
        [path], strategy
    )


def replay_archives(pattern: Union[str, Path], strategy: Optional[Strategy] = None, *,
                    tick_size: Union[str, float], center: Union[str, float],
                    **kwargs: Any) -> ReplaySummary:
    """Convenience wrapper for a multi-archive glob replay."""
    return BulkReplayer(tick_size=tick_size, center=center, **kwargs).replay_archives(
        pattern, strategy
    )
