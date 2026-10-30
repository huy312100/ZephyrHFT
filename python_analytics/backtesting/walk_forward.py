"""Phase 4 — walk-forward out-of-sample (OOS) validation.

The Phase 3 replayer answers "what did this parameter set earn on this
archive?". Walk-forward validation answers the question that actually gates a
deployment: "does a *frozen* parameter set keep earning on data it was never
fitted to?".

Three invariants are enforced mechanically rather than by convention:

``ParameterLock``
    Every window builds a **fresh** strategy from one immutable
    :class:`LockedParameters` instance. Before *and* after the window the live
    strategy is re-read through its public properties and compared with the
    expected lock; any drift raises :class:`ParameterLockError` before the
    window's ledger can enter the aggregate. Nothing in this module reads the
    optimiser, so a walk-forward run cannot re-tune between windows.
``No look-ahead``
    Windows are consumed from a single forward-only stream: window *n* is
    handed exactly its own slice and nothing else, the replayer's ingested-line
    counter must equal that slice, and exchange timestamps must not go backwards
    across windows. Violations raise :class:`LookAheadError`; they are never
    silently averaged into the report.
``Mark-to-market chaining``
    Window *n+1* starts with window *n*'s closing equity, and residual
    inventory is valued at the window's final mid. Unrealized PnL is therefore
    carried exactly once — never dropped, never double counted. Because each
    window opens flat, exposure left at a boundary is booked as a realized
    mark-to-market settlement (``boundary_settled_pnl``), and only the final
    window's inventory stays unrealized in the aggregate ledger.

``WalkForwardRunner.run()`` returns a :class:`WalkForwardReport` with per-window
telemetry and the aggregate OOS ledger (PnL, Sharpe/Sortino, drawdown, fill
ratio, win/loss ratio).
"""
from __future__ import annotations

import glob
import hashlib
import json
import logging
import math
import re
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterator, List, Mapping, Optional, Sequence, Tuple

from ..bridge import iter_gzip_ndjson, iter_ndjson
from ..strategies import AvellanedaStoikovStrategy, MarketConfig, MarketType
from ..costs import DEFAULT_MAKER_BPS, DEFAULT_TAKER_BPS, FeeWall, gross_edge_bps
from .metrics import (
    max_drawdown,
    profit_factor,
    round_trips,
    sharpe_ratio,
    sortino_ratio,
    win_rate,
)
from .replay import BulkReplayer, resolve_fill_model

logger = logging.getLogger(__name__)

# Exchange event timestamp (Binance `E`, milliseconds). A light regex is enough:
# the C++ parser already validates the payload, and this only feeds the
# chronology proof, so a full JSON decode per line would be wasted work.
_EVENT_TS = re.compile(r'"E"\s*:\s*(\d{1,20})')

# Per-window metric subset kept in the report: enough to audit a window without
# duplicating the whole quant_summary block for every slice.
_WINDOW_METRIC_KEYS = (
    "depth_events",
    "fills",
    "orders_submitted",
    "orders_rejected",
    "gap_events",
    "malformed_events",
    "fill_rate_pct",
    "filled_quantity",
    "submitted_quantity",
    "gross_pnl",
    "realized_pnl",
    "unrealized_pnl",
    "fees_paid",
    "traded_notional",
    "sharpe_ratio",
    "sortino_ratio",
    "max_drawdown_pct",
    "avg_holding_time_s",
    "throughput_fps",
    "latency_ns",
)


class WalkForwardError(RuntimeError):
    """Base class for validation failures that must abort a walk-forward run."""


class ParameterLockError(WalkForwardError):
    """Raised when a window's strategy drifted from the locked parameter set."""


class LookAheadError(WalkForwardError):
    """Raised when a window could observe data outside its own OOS slice."""


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def utc_timestamp() -> str:
    """ISO-8601 UTC stamp used in every artefact this package writes."""
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def resolve_sources(patterns: Sequence[Any]) -> List[Path]:
    """Expands globs, de-duplicates and validates the replay order.

    Glob expansion is sorted so a shell-quoted pattern is deterministic; the
    top-level order is preserved because the caller decides the chronology.
    """
    resolved: List[Path] = []
    for pattern in patterns:
        text = str(pattern)
        matches = sorted(glob.glob(text)) if any(ch in text for ch in "*?[") else [text]
        if not matches:
            raise FileNotFoundError(f"no archives match {text!r}")
        for match in matches:
            path = Path(match).expanduser()
            if not path.is_file():
                raise FileNotFoundError(f"archive not found: {path}")
            if path not in resolved:
                resolved.append(path)
    if not resolved:
        raise FileNotFoundError("no input archives supplied")
    return resolved


def iter_source_lines(path: Path) -> Iterator[str]:
    """Streams one archive without materialising it (gzip aware)."""
    return iter_gzip_ndjson(path) if str(path).endswith(".gz") else iter_ndjson(path)


def event_span(lines: Sequence[str]) -> Tuple[Optional[int], Optional[int]]:
    """Returns the first/last exchange timestamp (ms) found in a window."""
    first: Optional[int] = None
    last: Optional[int] = None
    for line in lines:
        match = _EVENT_TS.search(line)
        if match is None:
            continue
        stamp = int(match.group(1))
        if first is None:
            first = stamp
        last = stamp
    return first, last


def json_safe(value: Any) -> Any:
    """Recursively replaces non-finite floats with ``None``.

    ``profit_factor`` is legitimately infinite when a run has no losing trades;
    emitting ``Infinity`` would produce JSON that strict parsers reject, so the
    artefact records ``null`` instead.
    """
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, Mapping):
        return {str(key): json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_safe(item) for item in value]
    return value


def downsample(values: Sequence[float], limit: int = 2000) -> List[float]:
    """Evenly thins a series while always keeping its first and last sample."""
    series = [float(value) for value in values]
    if limit <= 0 or len(series) <= limit:
        return series
    if limit == 1:
        return [series[-1]]
    step = (len(series) - 1) / float(limit - 1)
    picked = [series[int(round(index * step))] for index in range(limit)]
    picked[-1] = series[-1]
    return picked


# --------------------------------------------------------------------------- #
# Parameter lock
# --------------------------------------------------------------------------- #
# Sections are the shape written to the lock file / report. Ordering is fixed
# so the canonical fingerprint is stable across processes.
_SECTION_FIELDS: Dict[str, Tuple[str, ...]] = {
    "strategy": (
        "gamma",
        "sigma",
        "k",
        "time_horizon",
        "inventory_limit",
        "ofi_sensitivity",
        "ofi_decay",
        "ofi_cap",
        "use_time_spread",
    ),
    "venue": ("tick_size", "center", "market_type"),
    "costs": ("maker_bps", "taker_bps"),
    "sizing": ("initial_cash", "order_quantity"),
}

# Fields restored by `LockedParameters.from_dict`, with their cast.
_CASTS: Dict[str, Any] = {
    "gamma": float,
    "sigma": float,
    "k": float,
    "time_horizon": float,
    "inventory_limit": float,
    "ofi_sensitivity": float,
    "ofi_decay": float,
    "ofi_cap": float,
    "use_time_spread": bool,
    "tick_size": str,
    "center": str,
    "market_type": str,
    "maker_bps": float,
    "taker_bps": float,
    "initial_cash": float,
    "order_quantity": str,
}


@dataclass(frozen=True)
class LockedParameters:
    """The complete, immutable parameter set a walk-forward run is locked to.

    A walk-forward study is only evidence if the strategy cannot adapt between
    windows, so every knob that changes a quote — risk aversion, volatility,
    liquidity, horizon, inventory cap, OFI coupling, venue grid, fees, size —
    lives here and nowhere else. ``--lock-file`` reloads this object from disk
    and refuses to run when the CLI disagrees with it.
    """

    gamma: float = 0.1
    sigma: float = 0.02
    k: float = 1.5
    time_horizon: float = 60.0
    inventory_limit: float = 5.0
    ofi_sensitivity: float = 0.0
    ofi_decay: float = 0.95
    ofi_cap: float = 3.0
    use_time_spread: bool = True
    tick_size: str = "0.5"
    center: str = "30000"
    market_type: str = "CRYPTO"
    maker_bps: float = DEFAULT_MAKER_BPS
    taker_bps: float = DEFAULT_TAKER_BPS
    initial_cash: float = 100000.0
    order_quantity: str = "0.01"

    def __post_init__(self) -> None:
        # Fail fast on nonsense rather than three windows into a run; the
        # MarketConfig validators already own the rules.
        object.__setattr__(self, "market_type", str(self.market_type).upper())
        if self.market_type not in {member.value for member in MarketType}:
            raise ValueError(
                f"unknown market_type {self.market_type!r}; expected one of "
                + ", ".join(member.value for member in MarketType))
        self.market_config()
        if self.taker_bps < 0.0:
            raise ValueError("taker_bps must be non-negative")
        if self.maker_bps < -100.0:
            raise ValueError("maker_bps must be >= -100 (100 bps rebate)")

    # ------------------------------------------------------------- building
    def market_config(self) -> MarketConfig:
        """Materialises the :class:`MarketConfig` used by every window."""
        return MarketConfig(
            gamma=float(self.gamma),
            sigma=float(self.sigma),
            k=float(self.k),
            time_horizon=float(self.time_horizon),
            inventory_limit=float(self.inventory_limit),
            tick_size=float(self.tick_size),
            market_type=MarketType(self.market_type),
            order_quantity=float(self.order_quantity),
        )

    def build_strategy(self) -> AvellanedaStoikovStrategy:
        """Creates a fresh strategy instance carrying the locked parameters."""
        return AvellanedaStoikovStrategy(
            self.market_config(),
            ofi_sensitivity=float(self.ofi_sensitivity),
            ofi_decay=float(self.ofi_decay),
            ofi_cap=float(self.ofi_cap),
            use_time_spread=bool(self.use_time_spread),
        )

    # ---------------------------------------------------------- serialising
    def strategy_lock(self) -> Dict[str, Any]:
        """Strategy-only lock, comparable with :func:`parameter_lock_of`."""
        return {name: _CASTS[name](getattr(self, name))
                for name in _SECTION_FIELDS["strategy"]}

    def as_dict(self) -> Dict[str, Any]:
        """Nested, JSON-stable view of every locked parameter."""
        return {
            section: {name: _CASTS[name](getattr(self, name)) for name in names}
            for section, names in _SECTION_FIELDS.items()
        }

    @property
    def fingerprint(self) -> str:
        """Short SHA-256 over the canonical parameter JSON."""
        payload = json.dumps(self.as_dict(), sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> "LockedParameters":
        """Rebuilds a lock from ``as_dict()`` output, rejecting unknown keys."""
        values: Dict[str, Any] = {}
        for section, names in _SECTION_FIELDS.items():
            block = payload.get(section, {})
            if not isinstance(block, Mapping):
                raise ParameterLockError(f"lock section {section!r} must be an object")
            unknown = sorted(set(block) - set(names))
            if unknown:
                raise ParameterLockError(
                    f"unknown lock field(s) in {section}: {', '.join(unknown)}")
            for name in names:
                if name not in block:
                    raise ParameterLockError(f"lock is missing {section}.{name}")
                values[name] = _CASTS[name](block[name])
        return cls(**values)

    def diff(self, other: "LockedParameters") -> List[str]:
        """Returns ``section.field: this=… other=…`` for every mismatch."""
        mine, theirs = self.as_dict(), other.as_dict()
        out: List[str] = []
        for section, names in _SECTION_FIELDS.items():
            for name in names:
                left, right = mine[section][name], theirs[section][name]
                if left != right:
                    out.append(f"{section}.{name}: {left!r} != {right!r}")
        return out

    def assert_matches(self, other: "LockedParameters",
                       *, context: str = "parameter lock") -> None:
        """Raises :class:`ParameterLockError` when the two sets differ."""
        differences = self.diff(other)
        if differences:
            raise ParameterLockError(
                f"{context} mismatch ({len(differences)} field(s)): "
                + "; ".join(differences))


def parameter_lock_of(strategy: AvellanedaStoikovStrategy) -> Dict[str, Any]:
    """Reads the live parameter set back through the strategy's public API.

    Used before and after every window: a strategy that mutated its own
    configuration (or was built from the wrong ``MarketConfig``) would produce
    a mismatch and abort the run.
    """
    config = strategy.config
    return {
        "gamma": float(config.gamma),
        "sigma": float(config.sigma),
        "k": float(config.k),
        "time_horizon": float(config.time_horizon),
        "inventory_limit": float(config.inventory_limit),
        "ofi_sensitivity": float(strategy.ofi_sensitivity),
        "ofi_decay": float(strategy.ofi_signal.decay),
        "ofi_cap": float(strategy.ofi_signal.cap),
        "use_time_spread": bool(strategy.use_time_spread),
    }


def write_lock_file(parameters: LockedParameters, path: Any,
                    *, metadata: Optional[Mapping[str, Any]] = None) -> Path:
    """Persists a lock so later runs can be proved to use the same fit."""
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "phase": 4,
        "kind": "walk_forward_parameter_lock",
        "written_at": utc_timestamp(),
        "fingerprint": parameters.fingerprint,
        "parameters": parameters.as_dict(),
        "metadata": dict(metadata or {}),
    }
    target.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n",
                      encoding="utf-8")
    logger.info("wrote parameter lock %s (%s)", target, parameters.fingerprint)
    return target


def read_lock_file(path: Any) -> LockedParameters:
    """Loads a lock and proves its fingerprint still matches its parameters."""
    source = Path(path)
    try:
        payload = json.loads(source.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise ParameterLockError(f"lock file not found: {source}") from exc
    except json.JSONDecodeError as exc:
        raise ParameterLockError(f"lock file {source} is not valid JSON: {exc}") from exc
    block = payload.get("parameters")
    if not isinstance(block, Mapping):
        raise ParameterLockError(f"lock file {source} has no 'parameters' object")
    parameters = LockedParameters.from_dict(block)
    recorded = payload.get("fingerprint")
    if recorded and recorded != parameters.fingerprint:
        raise ParameterLockError(
            f"lock file {source} fingerprint {recorded} does not match its "
            f"parameters ({parameters.fingerprint}); the file was edited by hand")
    return parameters


# --------------------------------------------------------------------------- #
# Window ledger
# --------------------------------------------------------------------------- #
@dataclass
class WalkForwardWindow:
    """One OOS slice: its own fills, equity path and summary statistics."""

    index: int
    label: str
    source: str
    offset: int
    events: int
    start_equity: float
    end_equity: float
    equity_curve: List[float] = field(repr=False, default_factory=list)
    fills: List[Dict[str, Any]] = field(repr=False, default_factory=list)
    trips: List[Dict[str, Any]] = field(repr=False, default_factory=list)
    stats: Dict[str, Any] = field(repr=False, default_factory=dict)
    first_event_ms: Optional[int] = None
    last_event_ms: Optional[int] = None

    @property
    def net_pnl(self) -> float:
        return float(self.end_equity) - float(self.start_equity)

    @property
    def net_pnl_pct(self) -> float:
        base = float(self.start_equity)
        return (self.net_pnl / base * 100.0) if base else 0.0

    @property
    def trades(self) -> int:
        return len(self.trips)

    @property
    def wins(self) -> int:
        return sum(1 for trip in self.trips if float(trip.get("gross_pnl", 0.0)) > 0.0)

    @property
    def losses(self) -> int:
        return sum(1 for trip in self.trips if float(trip.get("gross_pnl", 0.0)) < 0.0)

    @property
    def ledger_residual(self) -> float:
        """``end - start`` minus the ledger's own realized/unrealized split.

        The C++ ledger books fees into ``realized_pnl`` and cancels the open
        position's cost basis inside ``equity()``, so the two must agree to
        fixed-point rounding. A non-trivial residual means double counting.
        """
        realized = float(self.stats.get("realized_pnl", 0.0) or 0.0)
        unrealized = float(self.stats.get("unrealized_pnl", 0.0) or 0.0)
        return self.net_pnl - (realized + unrealized)

    def metrics(self) -> Dict[str, Any]:
        """The curated per-window metric block kept in artefacts."""
        return {key: self.stats.get(key) for key in _WINDOW_METRIC_KEYS if key in self.stats}

    def as_dict(self) -> Dict[str, Any]:
        return {
            "index": self.index,
            "label": self.label,
            "source": self.source,
            "offset": self.offset,
            "events": self.events,
            "first_event_ms": self.first_event_ms,
            "last_event_ms": self.last_event_ms,
            "start_equity": round(self.start_equity, 8),
            "end_equity": round(self.end_equity, 8),
            "net_pnl": round(self.net_pnl, 8),
            "net_pnl_pct": round(self.net_pnl_pct, 6),
            "trades": self.trades,
            "wins": self.wins,
            "losses": self.losses,
            "ledger_residual": round(self.ledger_residual, 8),
            "metrics": self.metrics(),
        }


def aggregate_windows(windows: Sequence[WalkForwardWindow], *,
                      initial_cash: float,
                      elapsed_s: float,
                      periods_per_year: float = 252.0) -> Dict[str, Any]:
    """Folds per-window ledgers into one continuous OOS record.

    The equity curve is spliced at window boundaries, dropping the first sample
    of each later window because it only repeats the previous close — keeping
    it would inject a zero return and bias Sharpe downward.
    """
    curve: List[float] = []
    for window in windows:
        segment = [float(value) for value in window.equity_curve]
        if not segment:
            continue
        if curve and abs(segment[0] - curve[-1]) <= 1e-6:
            segment = segment[1:]
        curve.extend(segment)

    trips = [trip for window in windows for trip in window.trips]
    depth_events = sum(int(window.stats.get("depth_events", 0) or 0) for window in windows)
    filled = sum(float(window.stats.get("filled_quantity", 0.0) or 0.0) for window in windows)
    submitted = sum(float(window.stats.get("submitted_quantity", 0.0) or 0.0)
                    for window in windows)
    realized = sum(float(window.stats.get("realized_pnl", 0.0) or 0.0) for window in windows)
    fees = sum(float(window.stats.get("fees_paid", 0.0) or 0.0) for window in windows)
    gross = sum(float(window.stats.get("gross_pnl", 0.0) or 0.0) for window in windows)
    notional_traded = sum(float(window.stats.get("traded_notional", 0.0) or 0.0)
                          for window in windows)
    unrealized_per_window = [float(window.stats.get("unrealized_pnl", 0.0) or 0.0)
                             for window in windows]
    # Each window starts flat (its cash is the previous window's closing equity),
    # so any inventory still open when a window ends is settled at that window's
    # final mark and only re-enters the ledger as cash. Those boundary
    # settlements are realized PnL; only the last window is still open at the
    # end of the run, which is why it is the sole contributor to unrealized.
    boundary_settled = sum(unrealized_per_window[:-1])
    unrealized = unrealized_per_window[-1] if unrealized_per_window else 0.0
    net_pnl = (curve[-1] - float(initial_cash)) if curve else 0.0
    # What the study earned before fees, per unit of notional traded. This is the
    # break-even maker fee: the single number a fee tier (or a venue) has to beat,
    # and the one that stays comparable when the quotes are widened or narrowed.
    edge = gross_edge_bps(net_pnl + fees, notional_traded)
    # Sharpe/Sortino are defined on per-period returns, not on equity *levels*
    # (levels are ~1e5 with tiny variance, which would inflate the ratio by
    # ~1e4). Same convention as ``metrics.quant_summary``.
    returns: List[float] = []
    for index in range(1, len(curve)):
        previous = curve[index - 1]
        if previous > 0.0:
            returns.append((curve[index] - previous) / previous)

    wins = sum(window.wins for window in windows)
    losses = sum(window.losses for window in windows)
    holding = [float(trip.get("holding_time_s", 0.0) or 0.0) for trip in trips]
    positive = sum(1 for window in windows if window.net_pnl > 0.0)
    negative = sum(1 for window in windows if window.net_pnl < 0.0)

    return {
        "windows": len(windows),
        "events": sum(window.events for window in windows),
        "depth_events": depth_events,
        "gap_events": sum(int(window.stats.get("gap_events", 0) or 0) for window in windows),
        "malformed_events": sum(int(window.stats.get("malformed_events", 0) or 0)
                                for window in windows),
        "initial_cash": round(float(initial_cash), 8),
        "final_equity": round(curve[-1], 8) if curve else round(float(initial_cash), 8),
        "net_pnl": round(net_pnl, 8),
        "net_pnl_pct": round(net_pnl / float(initial_cash) * 100.0, 6) if initial_cash else 0.0,
        "gross_pnl": round(gross, 8),
        "realized_pnl": round(realized + boundary_settled, 8),
        "in_window_realized_pnl": round(realized, 8),
        "boundary_settled_pnl": round(boundary_settled, 8),
        "unrealized_pnl": round(unrealized, 8),
        "fees_paid": round(fees, 8),
        "traded_notional": round(notional_traded, 8),
        "gross_edge_bps": None if edge is None else round(edge, 6),
        "fills": sum(int(window.stats.get("fills", 0) or 0) for window in windows),
        "trades": len(trips),
        "wins": wins,
        "losses": losses,
        "win_rate_pct": round(win_rate(trips) * 100.0, 4) if trips else 0.0,
        "win_loss_ratio": round(wins / losses, 4) if losses else None,
        "profit_factor": float(profit_factor(trips)) if trips else 0.0,
        "sharpe_ratio": round(sharpe_ratio(returns, periods_per_year), 6),
        "sortino_ratio": round(sortino_ratio(returns, periods_per_year), 6),
        "max_drawdown_pct": round(max_drawdown(curve) * 100.0, 6),
        "fill_ratio_pct": round(filled / submitted * 100.0, 4) if submitted > 0 else 0.0,
        "filled_quantity": round(filled, 8),
        "submitted_quantity": round(submitted, 8),
        "avg_holding_time_s": round(sum(holding) / len(holding), 6) if holding else 0.0,
        "positive_windows": positive,
        "negative_windows": negative,
        "window_hit_rate_pct": round(positive / len(windows) * 100.0, 4) if windows else 0.0,
        "best_window_pnl": round(max((w.net_pnl for w in windows), default=0.0), 8),
        "worst_window_pnl": round(min((w.net_pnl for w in windows), default=0.0), 8),
        "throughput_fps": round(depth_events / elapsed_s, 2) if elapsed_s > 0 else 0.0,
        "equity_samples": len(curve),
        "ledger_residual": round(net_pnl - (realized + boundary_settled + unrealized), 8),
    }


# --------------------------------------------------------------------------- #
# Runner
# --------------------------------------------------------------------------- #
class WalkForwardRunner:
    """Validates one frozen parameter set across chronological OOS windows.

    ``window_events=0`` means "one window per archive" — the natural unit when
    Phase 1 writes hourly files. A positive value re-slices the stream into
    fixed-size event blocks so a single long archive still yields several
    independent windows.
    """

    def __init__(self, parameters: LockedParameters, *,
                 window_events: int = 0,
                 batch_size: int = 64,
                 frames_per_hook: int = 1,
                 periods_per_year: float = 252.0,
                 ledger_tolerance: float = 1e-2,
                 max_curve_points: int = 2000,
                 fill_model: str = "touch",
                 lock_source: str = "cli") -> None:
        if window_events < 0:
            raise ValueError("window_events must be >= 0")
        if batch_size < 1:
            raise ValueError("batch_size must be >= 1")
        if frames_per_hook < 1:
            raise ValueError("frames_per_hook must be >= 1")
        if periods_per_year <= 0:
            raise ValueError("periods_per_year must be > 0")
        self._parameters = parameters
        self._window_events = int(window_events)
        self._batch_size = int(batch_size)
        self._frames_per_hook = int(frames_per_hook)
        self._periods_per_year = float(periods_per_year)
        self._ledger_tolerance = abs(float(ledger_tolerance))
        self._max_curve_points = int(max_curve_points)
        self._lock_source = lock_source
        # A measurement choice, deliberately outside the frozen parameter lock:
        # changing the fill model changes what the study measures, not what it
        # trades, and folding it into the fingerprint would invalidate every
        # committed lock for a model comparison.
        self._fill_model = str(fill_model).strip().lower()
        resolve_fill_model(self._fill_model)  # fail at construction, not mid-run

    @property
    def parameters(self) -> LockedParameters:
        return self._parameters

    # ------------------------------------------------------------ slicing
    def iter_slices(self, paths: Sequence[Path]) -> Iterator[Tuple[Path, int, List[str]]]:
        """Yields ``(path, offset, lines)`` exactly once per event.

        Streaming keeps memory flat, and a slice never contains a line already
        handed to an earlier slice — which is what makes the OOS claim
        checkable rather than aspirational.
        """
        for path in paths:
            if self._window_events <= 0:
                lines = list(iter_source_lines(path))
                if lines:
                    yield path, 0, lines
                continue
            buffer: List[str] = []
            offset = 0
            for line in iter_source_lines(path):
                buffer.append(line)
                if len(buffer) >= self._window_events:
                    yield path, offset, buffer
                    offset += len(buffer)
                    buffer = []
            if buffer:
                yield path, offset, buffer

    # ---------------------------------------------------------------- run
    def run(self, sources: Sequence[Any], *,
            progress: Optional[Any] = None) -> "WalkForwardReport":
        """Replays every window under the lock and returns the OOS evidence.

        Raises :class:`ParameterLockError` / :class:`LookAheadError` /
        :class:`WalkForwardError` instead of degrading the report: a
        walk-forward that cannot prove its invariants is not a result.
        """
        started = time.monotonic()
        paths = resolve_sources(sources)
        expected = self._parameters.strategy_lock()
        windows: List[WalkForwardWindow] = []
        equity = float(self._parameters.initial_cash)
        previous_last_ms: Optional[int] = None
        lock_checks = 0
        chronology_checks = 0

        for path, offset, lines in self.iter_slices(paths):
            strategy = self._parameters.build_strategy()
            self._assert_locked(strategy, expected, f"{path.name}#{offset} (pre-window)")
            lock_checks += 1

            replayer = BulkReplayer(
                tick_size=self._parameters.tick_size,
                center=self._parameters.center,
                maker_bps=self._parameters.maker_bps,
                taker_bps=self._parameters.taker_bps,
                initial_cash=equity,
                batch_size=self._batch_size,
                frames_per_hook=self._frames_per_hook,
                order_quantity=self._parameters.order_quantity,
                fill_model=self._fill_model,
            )
            summary = replayer.replay_lines(lines, strategy)

            self._assert_locked(strategy, expected, f"{path.name}#{offset} (post-window)")
            lock_checks += 1

            consumed = int(summary.stats.get("lines", 0) or 0)
            if consumed != len(lines):
                raise LookAheadError(
                    f"window {path.name}#{offset} ingested {consumed} lines but was "
                    f"handed {len(lines)}; the replayer read outside its slice")

            first_ms, last_ms = event_span(lines)
            if previous_last_ms is not None and first_ms is not None:
                chronology_checks += 1
                if first_ms < previous_last_ms:
                    raise LookAheadError(
                        f"window {path.name}#{offset} starts at {first_ms} ms, before the "
                        f"previous window closed at {previous_last_ms} ms; reorder the "
                        f"archives so time only moves forward")
            if last_ms is not None:
                previous_last_ms = last_ms

            end_equity = float(summary.equity_curve[-1])
            window = WalkForwardWindow(
                index=len(windows),
                label=f"{path.name}#{offset}",
                source=str(path),
                offset=offset,
                events=len(lines),
                start_equity=equity,
                end_equity=end_equity,
                equity_curve=[float(value) for value in summary.equity_curve],
                fills=list(summary.fills),
                trips=round_trips(summary.fills),
                stats=dict(summary.stats),
                first_event_ms=first_ms,
                last_event_ms=last_ms,
            )
            if abs(window.ledger_residual) > self._ledger_tolerance:
                raise WalkForwardError(
                    f"window {window.label} ledger residual "
                    f"{window.ledger_residual:+.6f} exceeds tolerance "
                    f"{self._ledger_tolerance}: equity change does not equal "
                    f"realized + unrealized PnL")
            windows.append(window)
            equity = end_equity
            if progress is not None:
                progress(window)

        if not windows:
            raise WalkForwardError("no events were replayed; nothing to validate")

        elapsed = time.monotonic() - started
        aggregate = aggregate_windows(
            windows,
            initial_cash=float(self._parameters.initial_cash),
            elapsed_s=elapsed,
            periods_per_year=self._periods_per_year,
        )
        # Recorded with the evidence, because a net PnL is only interpretable
        # next to the model that produced its fills.
        aggregate["fill_model"] = self._fill_model
        aggregate["trade_events"] = sum(
            int(window.stats.get("trade_events", 0) or 0) for window in windows)
        # Grid health travels with the PnL: a ladder that rejected the feed still
        # produces a tidy-looking run, and that has to be visible next to the
        # number rather than discovered later.
        aggregate["rejected_levels"] = sum(
            int(window.stats.get("rejected_levels", 0) or 0) for window in windows)
        aggregate["book_recenters"] = sum(
            int(window.stats.get("book_recenters", 0) or 0) for window in windows)
        aggregate["unpriced_frames"] = sum(
            int(window.stats.get("unpriced_frames", 0) or 0) for window in windows)
        tolerance = self._ledger_tolerance * max(1, len(windows))
        if abs(float(aggregate["ledger_residual"])) > tolerance:
            raise WalkForwardError(
                f"aggregate ledger residual {aggregate['ledger_residual']:+.6f} "
                f"exceeds tolerance {tolerance:.6f}")

        merged: List[float] = []
        for window in windows:
            segment = window.equity_curve
            if merged and segment and abs(segment[0] - merged[-1]) <= 1e-6:
                segment = segment[1:]
            merged.extend(segment)

        return WalkForwardReport(
            parameters=self._parameters,
            windows=windows,
            aggregate=aggregate,
            equity_curve=merged,
            sources=[str(path) for path in paths],
            window_events=self._window_events,
            elapsed_s=elapsed,
            lock_checks=lock_checks,
            lock_fingerprint=self._parameters.fingerprint,
            lock_source=self._lock_source,
            chronology_checks=chronology_checks,
            chronology_verified=chronology_checks > 0,
            max_curve_points=self._max_curve_points,
        )

    # ------------------------------------------------------------ internals
    @staticmethod
    def _assert_locked(strategy: AvellanedaStoikovStrategy,
                       expected: Mapping[str, Any], context: str) -> None:
        live = parameter_lock_of(strategy)
        drift = [f"{key}: {expected[key]!r} != {live[key]!r}"
                 for key in expected if live.get(key) != expected[key]]
        if drift:
            raise ParameterLockError(
                f"{context} drifted from the locked parameter set: " + "; ".join(drift))


# --------------------------------------------------------------------------- #
# Report
# --------------------------------------------------------------------------- #
def _num(value: Any, digits: int = 2) -> str:
    """Formats a number for Markdown, degrading to ``n/a`` when undefined."""
    if value is None:
        return "n/a"
    if isinstance(value, bool):
        return "yes" if value else "no"
    if isinstance(value, int):
        return f"{value:,}"
    if isinstance(value, float):
        if not math.isfinite(value):
            return "n/a"
        return f"{value:,.{digits}f}"
    return str(value)


@dataclass
class WalkForwardReport:
    """Aggregate OOS evidence plus the proofs that produced it."""

    parameters: LockedParameters
    windows: List[WalkForwardWindow]
    aggregate: Dict[str, Any]
    equity_curve: List[float]
    sources: List[str]
    window_events: int
    elapsed_s: float
    lock_checks: int
    lock_fingerprint: str
    lock_source: str
    chronology_checks: int
    chronology_verified: bool
    ledger_tolerance: float = 1e-2
    max_curve_points: int = 2000
    generated_at: str = field(default_factory=utc_timestamp)

    # ------------------------------------------------------------- artefacts
    def as_dict(self) -> Dict[str, Any]:
        """JSON-ready payload (non-finite floats become ``null``)."""
        payload = {
            "report": "phase4_walk_forward",
            "generated_at": self.generated_at,
            "elapsed_s": round(self.elapsed_s, 3),
            "validation": {
                "parameter_lock": {
                    "verified": True,
                    "fingerprint": self.lock_fingerprint,
                    "source": self.lock_source,
                    "checks": self.lock_checks,
                    "parameters": self.parameters.as_dict(),
                },
                "look_ahead": {
                    "verified": True,
                    "chronology_verified": self.chronology_verified,
                    "chronology_checks": self.chronology_checks,
                    "windows_checked": len(self.windows),
                },
                "ledger": {
                    "residual": self.aggregate.get("ledger_residual"),
                    "tolerance": round(self.ledger_tolerance * max(1, len(self.windows)), 8),
                },
            },
            "inputs": {
                "sources": list(self.sources),
                "window_events": self.window_events,
                "windows": len(self.windows),
            },
            "aggregate": dict(self.aggregate),
            "windows": [window.as_dict() for window in self.windows],
            "equity_curve": [round(value, 6) for value in
                             downsample(self.equity_curve, self.max_curve_points)],
        }
        return json_safe(payload)

    def to_json(self, path: Any) -> Path:
        target = Path(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(json.dumps(self.as_dict(), indent=2, sort_keys=True) + "\n",
                          encoding="utf-8")
        logger.info("wrote walk-forward JSON %s", target)
        return target

    def to_markdown(self, path: Any) -> Path:
        target = Path(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(self.to_markdown_text(), encoding="utf-8")
        logger.info("wrote walk-forward report %s", target)
        return target

    def write(self, directory: Any, stem: str = "walk_forward") -> Tuple[Path, Path]:
        """Writes ``<stem>.json`` and ``<stem>.md`` into ``directory``."""
        root = Path(directory)
        return self.to_json(root / f"{stem}.json"), self.to_markdown(root / f"{stem}.md")

    # ------------------------------------------------------------- narrative
    def fee_wall(self) -> "Optional[FeeWall]":
        """The measured gross edge priced against the locked maker fee.

        Read from the aggregate ledger rather than from the lock alone: a study
        that traded nothing has no unit to charge a fee against and returns
        ``None`` (see :func:`python_analytics.costs.gross_edge_bps`).
        """
        agg = self.aggregate
        return FeeWall.from_study(
            pre_fee_pnl=float(agg.get("net_pnl", 0.0) or 0.0)
            + float(agg.get("fees_paid", 0.0) or 0.0),
            traded_notional=float(agg.get("traded_notional", 0.0) or 0.0),
            maker_bps=float(self.parameters.maker_bps),
            fills=int(agg.get("fills", 0) or 0),
        )

    def summary_lines(self) -> List[str]:
        """Console block printed by ``scripts/run_walk_forward.py``."""
        agg = self.aggregate
        wall = self.fee_wall()
        wall_line = [f"fee wall: {wall.describe()}"] if wall is not None else []
        return [
            f"walk-forward: {agg['windows']} window(s) / {agg['events']} events / "
            f"{len(self.sources)} archive(s) in {self.elapsed_s:.2f}s",
            f"parameter lock: {self.lock_fingerprint} ({self.lock_source}), "
            f"{self.lock_checks} checks, verified",
            f"look-ahead guard: {self.chronology_checks} chronology check(s), "
            f"chronology {'verified' if self.chronology_verified else 'NOT verified'}",
            f"equity: {agg['initial_cash']:,.2f} -> {agg['final_equity']:,.2f} "
            f"(net {agg['net_pnl']:+,.2f} / {agg['net_pnl_pct']:+.4f}%)",
            f"sharpe {_num(agg['sharpe_ratio'], 3)} | sortino "
            f"{_num(agg['sortino_ratio'], 3)} | max drawdown "
            f"{_num(agg['max_drawdown_pct'], 3)}% | fill ratio "
            f"{_num(agg['fill_ratio_pct'], 2)}%",
            f"trades {agg['trades']} (wins {agg['wins']} / losses {agg['losses']}, "
            f"win rate {_num(agg['win_rate_pct'], 2)}%, profit factor "
            f"{_num(agg['profit_factor'], 3)})",
            f"fills {agg['fills']} under the {agg.get('fill_model', 'touch')} model | "
            f"tape {agg.get('trade_events', 0)} print(s) | book: "
            f"{agg.get('unpriced_frames', 0)} unpriced frame(s), "
            f"{agg.get('book_recenters', 0)} re-anchor(s)",
            *wall_line,
            f"realized {agg['realized_pnl']:+,.4f} (of which "
            f"{agg['boundary_settled_pnl']:+,.4f} boundary settlements) | "
            f"open unrealized {agg['unrealized_pnl']:+,.4f}",
            f"windows positive/negative: {agg['positive_windows']}/"
            f"{agg['negative_windows']} (hit rate {_num(agg['window_hit_rate_pct'], 1)}%)",
            f"ledger residual {agg['ledger_residual']:+.8f} "
            f"(tolerance {self.ledger_tolerance * max(1, len(self.windows)):.6f})",
        ]

    def to_markdown_text(self) -> str:
        """Human-readable companion to :meth:`as_dict`."""
        agg = self.aggregate
        lock = self.parameters.as_dict()
        # The fee wall belongs in the metrics table because it, not the net PnL,
        # is what a deployment decision has to clear.
        wall = self.fee_wall()
        wall_rows = [] if wall is None else [
            f"| Gross edge (pre-fee) | {wall.gross_bps:+.3f} bps |",
            f"| Break-even maker fee | {wall.gross_bps:+.3f} bps "
            f"(charged {wall.maker_bps:g} bps) |",
        ]
        lines: List[str] = [
            "# Walk-forward out-of-sample validation",
            "",
            f"- Generated: `{self.generated_at}`",
            f"- Elapsed: {self.elapsed_s:.2f}s",
            f"- Parameter lock: `{self.lock_fingerprint}` (source: {self.lock_source}), "
            f"{self.lock_checks} pre/post-window checks passed",
            f"- Look-ahead guard: {len(self.windows)} windows, "
            f"{self.chronology_checks} chronology checks, "
            f"{'verified' if self.chronology_verified else 'not verifiable (no timestamps)'}",
            f"- Ledger residual: {agg['ledger_residual']:+.8f} "
            f"(tolerance {self.ledger_tolerance * max(1, len(self.windows)):.6f})",
            "",
            "## Frozen parameters",
            "",
            "| Section | Parameter | Value |",
            "| --- | --- | --- |",
        ]
        for section, block in lock.items():
            for name, value in block.items():
                lines.append(f"| {section} | `{name}` | `{value}` |")

        lines += [
            "",
            "## Aggregate OOS performance",
            "",
            "| Metric | Value |",
            "| --- | --- |",
            f"| Windows | {agg['windows']} |",
            f"| Events replayed | {agg['events']:,} |",
            f"| Depth events | {agg['depth_events']:,} |",
            f"| Gaps / malformed | {agg['gap_events']} / {agg['malformed_events']} |",
            f"| Initial cash | {_num(agg['initial_cash'])} |",
            f"| Final equity | {_num(agg['final_equity'])} |",
            f"| Net PnL | {_num(agg['net_pnl'])} ({_num(agg['net_pnl_pct'], 4)}%) |",
            f"| Realized / unrealized | {_num(agg['realized_pnl'])} / "
            f"{_num(agg['unrealized_pnl'])} |",
            f"| Boundary mark-to-market settlements | "
            f"{_num(agg['boundary_settled_pnl'])} |",
            f"| Fees paid | {_num(agg['fees_paid'])} |",
            f"| Traded notional | {_num(agg['traded_notional'])} |",
            *wall_rows,
            f"| Fills | {agg['fills']:,} |",
            f"| Trades (round trips) | {agg['trades']:,} |",
            f"| Wins / losses | {agg['wins']} / {agg['losses']} |",
            f"| Win rate | {_num(agg['win_rate_pct'])}% |",
            f"| Win/loss ratio | {_num(agg['win_loss_ratio'])} |",
            f"| Profit factor | {_num(agg['profit_factor'], 3)} |",
            f"| Sharpe (annualised) | {_num(agg['sharpe_ratio'], 3)} |",
            f"| Sortino (annualised) | {_num(agg['sortino_ratio'], 3)} |",
            f"| Max drawdown | {_num(agg['max_drawdown_pct'], 3)}% |",
            f"| Fill ratio | {_num(agg['fill_ratio_pct'])}% |",
            f"| Avg holding time | {_num(agg['avg_holding_time_s'])}s |",
            f"| Positive / negative windows | {agg['positive_windows']} / "
            f"{agg['negative_windows']} |",
            f"| Window hit rate | {_num(agg['window_hit_rate_pct'], 1)}% |",
            f"| Best / worst window PnL | {_num(agg['best_window_pnl'])} / "
            f"{_num(agg['worst_window_pnl'])} |",
            f"| Throughput | {_num(agg['throughput_fps'], 0)} depth events/s |",
        ]
        return "\n".join(lines + self._window_table()) + "\n"

    def _window_table(self) -> List[str]:
        lines = [
            "",
            "## Window ledger",
            "",
            "| # | Window | Events | Start equity | End equity | Net PnL | PnL % | Trades | "
            "Win % | Sharpe | Max DD % | Fill % |",
            "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
        ]
        for window in self.windows:
            metrics = window.metrics()
            lines.append(
                f"| {window.index} | `{window.label}` | {window.events:,} | "
                f"{_num(window.start_equity)} | {_num(window.end_equity)} | "
                f"{_num(window.net_pnl)} | {_num(window.net_pnl_pct, 4)} | "
                f"{window.trades} | {_num(metrics.get('win_rate'), 2)} | "
                f"{_num(metrics.get('sharpe_ratio'), 3)} | "
                f"{_num(metrics.get('max_drawdown_pct'), 3)} | "
                f"{_num(metrics.get('fill_rate_pct'), 2)} |")
        lines += ["", "## Inputs", ""]
        lines += [f"- `{source}`" for source in self.sources]
        return lines
