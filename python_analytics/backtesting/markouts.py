"""Markout analysis: does a resting quote earn the spread or get picked off?

A market maker's PnL decomposes into spread capture, fees and **adverse
selection** — the cost of the mid moving against every fill. A study can post a
flat-looking PnL while being picked off on every quote: the fills look harmless
because the loss shows up as the *next* price move, not in the fill itself.
Markout is the instrument that separates those cases.

For a fill at price ``p`` on side ``s`` (``+1`` buy, ``-1`` sell) and the mid
``m(h)`` observed ``h`` seconds later::

    markout_bps(h) = s * (m(h) - p) / p * 10_000

At ``h = 0`` this is (approximately) the half-spread a maker captured by
resting. At longer horizons it measures how much of that capture survives. A
quote that is adversely selected shows markout decaying or turning negative,
and the fee it pays is a *further* deduction::

    net_edge_bps(h) = markout_bps(h) - maker_bps

Two clocks, never mixed
-----------------------
This module never guesses a timestamp's clock. The C++ paper engine stamps
fills with ``steady_clock`` (:func:`cpp_engine.types.now_ns`), which has an
arbitrary epoch and therefore **cannot** be joined to an archive's exchange
timestamps. That is why simulated markouts are measured in-loop by
:class:`MarkoutReplayer` on the engine's own event clock — the same clock the
capture carries — rather than by looking a fill's timestamp up in a file.
Venue-timestamped fills (a real session, exported as JSON) use
:func:`build_timeline` plus :func:`resolve_markouts` on the venue's clock.

Everything here is a diagnostic over recorded data; nothing in this module can
make a strategy profitable, and a green verdict is a precondition for trading,
not evidence of an edge.
"""
from __future__ import annotations

import json
import math
from bisect import bisect_left, bisect_right
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from ..costs import DEFAULT_MAKER_BPS, DEFAULT_TAKER_BPS
from .replay import BulkReplayer

__all__ = [
    "HORIZONS_S",
    "MarkoutConfig",
    "MarkoutFill",
    "MarkoutReplayer",
    "MarkoutRow",
    "MarkoutTimeline",
    "TopOfBook",
    "build_timeline",
    "horizon_label",
    "load_fills",
    "markout_verdict",
    "parse_fill_payload",
    "render_markdown",
    "resolve_markouts",
    "side_name",
    "side_sign",
    "summarize_markouts",
    "to_nanos",
]

#: Horizons, in seconds, a maker's fill is marked against. ``0`` is the fill
#: itself (spread capture); the positive horizons are how far the mid has moved
#: away by the time inventory could actually be worked out.
HORIZONS_S: Tuple[float, ...] = (0.0, 1.0, 10.0, 60.0)

NANOS_PER_S = 1_000_000_000
NANOS_PER_MS = 1_000_000

#: A magnitude at or above this is nanoseconds; below it, milliseconds. Binance
#: millisecond stamps sit near 1.7e12 and nanosecond stamps near 1.7e18, so
#: nothing real falls in the gap and the rule never has to guess.
_NANOS_THRESHOLD = 1e15


def side_sign(value: Any) -> float:
    """``+1`` for a buy, ``-1`` for a sell; accepts ``'Side.BUY'`` and ``1``."""
    text = str(value).upper()
    if "BUY" in text:
        return 1.0
    if "SELL" in text:
        return -1.0
    raise ValueError(f"unrecognised side {value!r}; expected BUY or SELL")


def side_name(value: Any) -> str:
    """Canonical ``'BUY'``/``'SELL'`` for any accepted side spelling."""
    return "BUY" if side_sign(value) > 0 else "SELL"


def horizon_label(seconds: float) -> str:
    """Stable dictionary key for a horizon (``1.0`` -> ``'1s'``)."""
    value = float(seconds)
    return f"{value:g}s" if value else "0s"


def _integer(value: Any) -> int:
    """Exact integer for ints and integer strings.

    Timestamps around 1.7e18 ns exceed float64's 53-bit integer range, so a
    ``float()`` round-trip silently shifts them by up to 256 ns. Integers and
    digit strings must therefore never pass through a float.
    """
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    text = str(value).strip()
    if text.lstrip("+-").isdigit():
        return int(text)
    return int(round(float(text)))


def to_nanos(value: Any) -> int:
    """Converts a venue timestamp to nanoseconds using the documented rule.

    Values of at least :data:`_NANOS_THRESHOLD` are already nanoseconds;
    anything smaller is milliseconds. Strings are accepted and parsed exactly,
    so a JSON payload never loses a nanosecond to a float round-trip.
    """
    stamp = float(value)
    if not math.isfinite(stamp) or stamp < 0:
        raise ValueError(f"unusable timestamp {value!r}")
    exact = _integer(value)
    return exact if exact >= _NANOS_THRESHOLD else exact * NANOS_PER_MS


@dataclass(frozen=True)
class MarkoutConfig:
    """What to measure, how strictly, and what counts as an edge.

    ``tolerance_s`` is the maximum distance between a horizon's target time and
    the sample actually used. Past it the horizon is reported as *stale* and
    excluded instead of interpolated: filling a data hole by extrapolating the
    last mid is how a feed outage turns into a fake edge.

    ``primary_horizon_s`` is the horizon the verdict is decided on. It defaults
    to the shortest positive horizon, because adverse selection is a short
    horizon phenomenon — a quote that is still fine one second later but wrong
    a minute later was not picked off, it was simply held too long.
    """

    horizons_s: Tuple[float, ...] = HORIZONS_S
    tolerance_s: float = 2.0
    maker_bps: float = DEFAULT_MAKER_BPS
    taker_bps: float = DEFAULT_TAKER_BPS
    min_samples: int = 30
    min_edge_bps: float = 0.0
    primary_horizon_s: Optional[float] = None

    def __post_init__(self) -> None:
        horizons = tuple(sorted({float(value) for value in self.horizons_s}))
        if not horizons:
            raise ValueError("horizons_s must contain at least one horizon")
        if horizons[0] < 0.0:
            raise ValueError("horizons_s must be non-negative")
        if float(self.tolerance_s) <= 0.0:
            raise ValueError("tolerance_s must be > 0")
        if int(self.min_samples) < 1:
            raise ValueError("min_samples must be >= 1")
        object.__setattr__(self, "horizons_s", horizons)
        object.__setattr__(self, "tolerance_s", float(self.tolerance_s))
        object.__setattr__(self, "min_samples", int(self.min_samples))
        object.__setattr__(self, "min_edge_bps", float(self.min_edge_bps))

        primary = self.primary_horizon_s
        if primary is None:
            positive = [value for value in horizons if value > 0.0]
            primary = positive[0] if positive else horizons[0]
        elif float(primary) not in horizons:
            raise ValueError(
                f"primary_horizon_s {primary!r} is not one of the measured "
                f"horizons {list(horizons)}")
        object.__setattr__(self, "primary_horizon_s", float(primary))

    def as_dict(self) -> Dict[str, Any]:
        return {
            "horizons_s": list(self.horizons_s),
            "tolerance_s": self.tolerance_s,
            "maker_bps": float(self.maker_bps),
            "taker_bps": float(self.taker_bps),
            "min_samples": self.min_samples,
            "min_edge_bps": self.min_edge_bps,
            "primary_horizon_s": float(self.primary_horizon_s or 0.0),
        }


@dataclass(frozen=True)
class MarkoutFill:
    """One fill, stamped on the same clock as the timeline it is marked against.

    ``symbol`` is optional because a replay fill has no venue symbol to report,
    but when both the fill and the timeline name a market they must agree: a
    BTC fill marked against an ETH mid is a number nobody can defend.
    """

    ts_ns: int
    side: str
    price: float
    quantity: float
    is_maker: bool = True
    symbol: Optional[str] = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "side", side_name(self.side))
        price = float(self.price)
        quantity = float(self.quantity)
        if not math.isfinite(price) or price <= 0.0:
            raise ValueError(f"fill price must be > 0, got {self.price!r}")
        if not math.isfinite(quantity) or quantity <= 0.0:
            raise ValueError(f"fill quantity must be > 0, got {self.quantity!r}")
        object.__setattr__(self, "price", price)
        object.__setattr__(self, "quantity", quantity)
        object.__setattr__(self, "ts_ns", int(self.ts_ns))
        object.__setattr__(self, "is_maker", bool(self.is_maker))
        object.__setattr__(self, "symbol",
                           None if self.symbol in (None, "") else str(self.symbol).upper())

    @property
    def sign(self) -> float:
        """``+1`` for a buy, ``-1`` for a sell."""
        return 1.0 if self.side == "BUY" else -1.0

    @property
    def notional(self) -> float:
        return self.price * self.quantity

    @property
    def bucket(self) -> str:
        return "maker" if self.is_maker else "taker"


class MarkoutTimeline:
    """Chronological mid-price samples with O(log n) lookups.

    Samples must arrive in non-decreasing ``ts_ns`` order; a backwards sample is
    a caller bug (usually two clocks or two streams mixed), so it raises instead
    of producing a silently wrong look-up. A repeated timestamp overwrites the
    previous sample, which keeps a snapshot-then-delta pair from inflating the
    sample count.
    """

    __slots__ = ("symbol", "_ts", "_mid", "_spread", "malformed", "ignored")

    def __init__(self, symbol: Optional[str] = None) -> None:
        self.symbol = symbol
        self._ts: List[int] = []
        self._mid: List[float] = []
        self._spread: List[float] = []
        self.malformed = 0
        self.ignored = 0

    # ------------------------------------------------------------- building
    def add(self, ts_ns: int, mid: float, spread_bps: float = 0.0) -> bool:
        """Appends one sample; ``False`` when the mid is unusable."""
        stamp = int(ts_ns)
        value = float(mid)
        if not math.isfinite(value) or value <= 0.0:
            return False
        if self._ts:
            previous = self._ts[-1]
            if stamp < previous:
                raise ValueError(
                    f"timeline samples must be chronological: {stamp} ns after "
                    f"{previous} ns (are two clocks or two symbols mixed?)")
            if stamp == previous:
                self._mid[-1] = value
                self._spread[-1] = float(spread_bps)
                return True
        self._ts.append(stamp)
        self._mid.append(value)
        self._spread.append(float(spread_bps))
        return True

    # -------------------------------------------------------------- reading
    def __len__(self) -> int:
        return len(self._ts)

    @property
    def is_empty(self) -> bool:
        return not self._ts

    @property
    def first_ts_ns(self) -> Optional[int]:
        return self._ts[0] if self._ts else None

    @property
    def last_ts_ns(self) -> Optional[int]:
        return self._ts[-1] if self._ts else None

    @property
    def span_s(self) -> float:
        if len(self._ts) < 2:
            return 0.0
        return (self._ts[-1] - self._ts[0]) / NANOS_PER_S

    @property
    def max_gap_s(self) -> float:
        """Largest silence between consecutive samples (feed health)."""
        if len(self._ts) < 2:
            return 0.0
        return max(
            (right - left) for left, right in zip(self._ts, self._ts[1:])
        ) / NANOS_PER_S

    def at_or_before(self, ts_ns: int) -> Optional[Tuple[int, float, float]]:
        """Last sample at or before ``ts_ns`` as ``(ts, mid, spread_bps)``."""
        index = bisect_right(self._ts, int(ts_ns)) - 1
        if index < 0:
            return None
        return self._ts[index], self._mid[index], self._spread[index]

    def at_or_after(self, ts_ns: int) -> Optional[Tuple[int, float, float]]:
        """First sample at or after ``ts_ns`` as ``(ts, mid, spread_bps)``."""
        index = bisect_left(self._ts, int(ts_ns))
        if index >= len(self._ts):
            return None
        return self._ts[index], self._mid[index], self._spread[index]

    def samples(self) -> Iterable[Tuple[int, float, float]]:
        """Yields ``(ts_ns, mid, spread_bps)`` in order (used to merge captures)."""
        return zip(self._ts, self._mid, self._spread)


class TopOfBook:
    """Analysis-grade ladder built from depth deltas (float, not fixed point).

    This is deliberately a plain dictionary: it runs offline, once, over a
    recording, where clarity beats the nanosecond budget the C++ book exists for.
    """

    __slots__ = ("_bids", "_asks")

    def __init__(self) -> None:
        self._bids: Dict[float, float] = {}
        self._asks: Dict[float, float] = {}

    def apply(self, side: str, levels: Iterable[Sequence[Any]]) -> int:
        """Applies level deltas (``[price, quantity]``, quantity 0 removes)."""
        book = self._bids if side_sign(side) > 0 else self._asks
        applied = 0
        for level in levels or ():
            try:
                price = float(level[0])
                quantity = float(level[1])
            except (TypeError, ValueError, IndexError):
                continue
            if not math.isfinite(price) or not math.isfinite(quantity) or price <= 0.0:
                continue
            if quantity <= 0.0:
                book.pop(price, None)
            else:
                book[price] = quantity
            applied += 1
        return applied

    def set(self, bids: Iterable[Sequence[Any]], asks: Iterable[Sequence[Any]]) -> None:
        """Replaces the ladder with an absolute snapshot."""
        self._bids.clear()
        self._asks.clear()
        self.apply("BUY", bids)
        self.apply("SELL", asks)

    @property
    def best_bid(self) -> Optional[float]:
        return max(self._bids) if self._bids else None

    @property
    def best_ask(self) -> Optional[float]:
        return min(self._asks) if self._asks else None

    @property
    def mid(self) -> Optional[float]:
        bid, ask = self.best_bid, self.best_ask
        if bid is None or ask is None or ask < bid:
            return None
        return (bid + ask) / 2.0

    @property
    def spread_bps(self) -> float:
        bid, ask = self.best_bid, self.best_ask
        mid = self.mid
        if bid is None or ask is None or not mid:
            return 0.0
        return (ask - bid) / mid * 10_000.0


def _message_ts_ns(message: Mapping[str, Any]) -> Optional[int]:
    """Extracts a venue timestamp from a raw feed message, in nanoseconds.

    ``E``/``T`` are Binance's millisecond fields; ``ts_ns``/``timestamp_ns`` are
    explicit nanoseconds. Anything unparseable returns ``None`` so the caller can
    count the frame as unsampled rather than inventing a time.
    """
    for key in ("ts_ns", "timestamp_ns", "E", "T", "timestamp", "time"):
        if key not in message:
            continue
        raw = message[key]
        if raw in (None, ""):
            continue
        try:
            if key.endswith("_ns"):
                exact = _integer(raw)
                return None if exact < 0 else exact
            return to_nanos(raw)
        except (TypeError, ValueError):
            return None
    return None


def iter_capture_lines(path: Path) -> Iterable[str]:
    """Yields lines from a plain or gzip-compressed NDJSON capture."""
    if str(path).endswith(".gz"):
        import gzip

        with gzip.open(path, "rt", encoding="utf-8") as handle:
            for line in handle:
                yield line
        return
    with Path(path).open("r", encoding="utf-8") as handle:
        for line in handle:
            yield line


def build_timeline(lines: Iterable[str], *,
                   symbol: Optional[str] = None) -> MarkoutTimeline:
    """Builds a mid-price timeline from depth / best-bid-ask frames.

    Recognised shapes, all taken from what ``scripts/ingest_l2.py`` archives:

    * ``depthUpdate`` — ``b``/``a`` level deltas applied to a running ladder;
    * a REST snapshot carrying ``lastUpdateId`` plus absolute ``bids``/``asks``;
    * ``bookTicker`` — scalar ``b``/``a`` best bid and ask (fallback mid only).

    Trades carry no mid and are ignored. Frames for a symbol other than the
    first one seen (or the caller's ``symbol``) are counted in
    :attr:`MarkoutTimeline.ignored` and never blended in: averaging BTC and ETH
    mids would produce a number that looks like evidence and is not.
    """
    timeline = MarkoutTimeline(symbol=symbol)
    book = TopOfBook()
    ticker: Optional[Tuple[float, float]] = None
    want = symbol.upper() if symbol else None

    for raw in lines:
        text = raw.strip() if isinstance(raw, str) else str(raw).strip()
        if not text:
            continue
        try:
            message = json.loads(text)
        except (TypeError, ValueError):
            timeline.malformed += 1
            continue
        if not isinstance(message, Mapping):
            timeline.malformed += 1
            continue

        frame_symbol = message.get("s") or message.get("symbol")
        if frame_symbol:
            name = str(frame_symbol).upper()
            if want is None:
                want = name
                timeline.symbol = name
            elif name != want:
                timeline.ignored += 1
                continue

        kind = str(message.get("e") or message.get("type") or "")
        ts_ns = _message_ts_ns(message)

        if kind == "depthUpdate" or ("b" in message and "a" in message
                                     and isinstance(message.get("b"), list)):
            book.apply("BUY", message.get("b") or ())
            book.apply("SELL", message.get("a") or ())
            mid = book.mid
            if mid is None and ticker is not None:
                mid = (ticker[0] + ticker[1]) / 2.0
            if ts_ns is None:
                timeline.ignored += 1
                continue
            if not timeline.add(ts_ns, mid if mid is not None else 0.0,
                                book.spread_bps):
                timeline.ignored += 1
            continue

        if "lastUpdateId" in message:
            book.set(message.get("bids") or (), message.get("asks") or ())
            if ts_ns is None:
                timeline.ignored += 1
                continue
            if not timeline.add(ts_ns, book.mid or 0.0, book.spread_bps):
                timeline.ignored += 1
            continue

        if kind == "bookTicker" or (not kind
                                    and isinstance(message.get("b"), (str, float, int))
                                    and isinstance(message.get("a"), (str, float, int))):
            # The type-based branch must not run for a known kind: a spot `trade`
            # frame carries buyer/seller *order ids* in `b`/`a`, and reading those
            # as a bid/ask would fabricate a mid.
            try:
                bid = float(message["b"])
                ask = float(message["a"])
            except (TypeError, ValueError, KeyError):
                timeline.malformed += 1
                continue
            if not (math.isfinite(bid) and math.isfinite(ask)) or ask < bid or bid <= 0.0:
                timeline.malformed += 1
                continue
            ticker = (bid, ask)
            mid = book.mid or (bid + ask) / 2.0
            spread = book.spread_bps or (ask - bid) / mid * 10_000.0
            if ts_ns is None or not timeline.add(ts_ns, mid, spread):
                timeline.ignored += 1
            continue

        timeline.ignored += 1

    return timeline


def build_timeline_from_files(paths: Iterable[Any], *,
                              symbol: Optional[str] = None) -> MarkoutTimeline:
    """Concatenates capture files (plain or ``.gz``) into one timeline."""
    timeline: Optional[MarkoutTimeline] = None
    for raw_path in paths:
        path = Path(raw_path)
        if not path.exists():
            raise FileNotFoundError(f"capture not found: {path}")
        chunk = build_timeline(iter_capture_lines(path), symbol=symbol)
        if timeline is None:
            timeline = chunk
            continue
        if chunk.symbol and timeline.symbol and chunk.symbol != timeline.symbol:
            raise ValueError(
                f"{path} carries {chunk.symbol} but the timeline is "
                f"{timeline.symbol}; pass --symbol to analyse one market")
        for ts_ns, mid, spread in chunk.samples():
            timeline.add(ts_ns, mid, spread)
        timeline.malformed += chunk.malformed
        timeline.ignored += chunk.ignored
    return timeline if timeline is not None else MarkoutTimeline(symbol=symbol)


def _as_bool(value: Any, *, default: bool) -> bool:
    """Coerces the many spellings of "was this a maker fill?"."""
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y", "maker"}:
        return True
    if text in {"0", "false", "no", "n", "taker"}:
        return False
    return default


def _fill_ts_ns(row: Mapping[str, Any], clock: str) -> int:
    """Reads a fill's timestamp, honouring an explicit nanosecond field."""
    for key in ("ts_ns", "timestamp_ns"):
        if row.get(key) not in (None, ""):
            return _integer(row[key])
    for key in ("timestamp_ms", "time_ms"):
        if row.get(key) not in (None, ""):
            return to_nanos(row[key])
    for key in ("timestamp", "time", "T", "E"):
        if row.get(key) not in (None, ""):
            return _integer(row[key]) if clock == "ns" else to_nanos(row[key])
    raise ValueError(
        "fill is missing a timestamp; supply ts_ns/timestamp_ns, "
        "timestamp_ms, or timestamp (with clock='ms'/'ns')")


def parse_fill_payload(payload: Any, *, clock: str = "ms") -> List[MarkoutFill]:
    """Normalises a fill payload into :class:`MarkoutFill` objects.

    Accepts a bare list or a mapping with ``fills``/``rows`` (the shape
    ``ReplaySummary.as_dict()`` and any sane exporter produce). ``clock`` only
    governs the unqualified ``timestamp`` field: ``'ms'`` for venue stamps,
    ``'ns'`` for a steady-clock log. A replay's steady-clock fills cannot be
    marked against a capture at all — pass them through
    :class:`MarkoutReplayer` instead, which never leaves the engine's clock.
    """
    if isinstance(payload, Mapping):
        rows = payload.get("fills")
        if rows is None:
            rows = payload.get("rows", [])
    else:
        rows = payload
    if not isinstance(rows, Sequence) or isinstance(rows, (str, bytes)):
        raise ValueError("fill payload must be a list of fill objects")

    fills: List[MarkoutFill] = []
    for index, row in enumerate(rows):
        if not isinstance(row, Mapping):
            raise ValueError(f"fill {index} is not an object")
        try:
            fills.append(
                MarkoutFill(
                    ts_ns=_fill_ts_ns(row, clock),
                    side=row.get("side", "BUY"),
                    price=float(row["price"]),
                    quantity=float(row["quantity"]),
                    is_maker=_as_bool(row.get("is_maker"), default=True),
                    symbol=row.get("symbol") or row.get("s"),
                )
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError(f"fill {index} is unusable: {exc}") from exc
    return fills


def load_fills(path: Any, *, clock: str = "ms") -> List[MarkoutFill]:
    """Loads a JSON fill payload from disk."""
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    return parse_fill_payload(payload, clock=clock)


@dataclass(frozen=True)
class MarkoutRow:
    """One fill marked at every configured horizon."""

    fill: MarkoutFill
    entry_ts_ns: Optional[int]
    entry_mid: Optional[float]
    entry_spread_bps: Optional[float]
    entry_offset_bps: Optional[float]
    markouts_bps: Mapping[str, Optional[float]]
    staleness_ms: Mapping[str, Optional[float]]
    truncated: Tuple[str, ...]

    @property
    def matched(self) -> bool:
        """``False`` when no sample was close enough to even establish a mid."""
        return self.entry_mid is not None

    @property
    def notional(self) -> float:
        return self.fill.notional

    def as_dict(self) -> Dict[str, Any]:
        """Flat, CSV-friendly view of the row."""
        payload: Dict[str, Any] = {
            "ts_ns": self.fill.ts_ns,
            "side": self.fill.side,
            "price": self.fill.price,
            "quantity": self.fill.quantity,
            "is_maker": self.fill.is_maker,
            "entry_mid": self.entry_mid,
            "entry_spread_bps": self.entry_spread_bps,
            "entry_offset_bps": self.entry_offset_bps,
            "truncated": ",".join(self.truncated),
        }
        for label, value in self.markouts_bps.items():
            payload[f"markout_{label}_bps"] = value
        for label, value in self.staleness_ms.items():
            payload[f"staleness_{label}_ms"] = value
        return payload


def resolve_markouts(fills: Sequence[MarkoutFill], timeline: MarkoutTimeline,
                     config: Optional[MarkoutConfig] = None) -> List[MarkoutRow]:
    """Marks every fill against ``timeline`` on the timeline's own clock.

    A horizon whose sample is missing, or further from the target time than
    ``config.tolerance_s``, is recorded as ``None`` and listed in
    :attr:`MarkoutRow.truncated`. It is never interpolated or carried forward:
    an invented price is indistinguishable from an edge once it reaches a report.
    """
    config = config or MarkoutConfig()
    tolerance_ns = int(config.tolerance_s * NANOS_PER_S)
    labels = [horizon_label(horizon) for horizon in config.horizons_s]
    rows: List[MarkoutRow] = []

    timeline_symbol = (timeline.symbol or "").upper()
    for fill in fills:
        if timeline_symbol and fill.symbol and fill.symbol != timeline_symbol:
            raise ValueError(
                f"fill at {fill.ts_ns} ns is {fill.symbol} but the timeline is "
                f"{timeline_symbol}; mark each market against its own capture")
        entry = timeline.at_or_before(fill.ts_ns)
        entry_gap = None if entry is None else fill.ts_ns - entry[0]
        if entry is None or entry_gap > tolerance_ns:
            rows.append(MarkoutRow(
                fill=fill,
                entry_ts_ns=None if entry is None else entry[0],
                entry_mid=None,
                entry_spread_bps=None,
                entry_offset_bps=None,
                markouts_bps={label: None for label in labels},
                staleness_ms={label: None for label in labels},
                truncated=tuple(labels),
            ))
            continue

        entry_mid = entry[1]
        markouts: Dict[str, Optional[float]] = {}
        staleness: Dict[str, Optional[float]] = {}
        truncated: List[str] = []
        for horizon, label in zip(config.horizons_s, labels):
            if horizon == 0.0:
                sample: Optional[Tuple[int, float, float]] = entry
                gap: Optional[int] = entry_gap
            else:
                target = fill.ts_ns + int(round(horizon * NANOS_PER_S))
                sample = timeline.at_or_after(target)
                gap = None if sample is None else sample[0] - target
            if sample is None or gap is None or gap > tolerance_ns:
                markouts[label] = None
                staleness[label] = None
                truncated.append(label)
                continue
            markouts[label] = fill.sign * (sample[1] - fill.price) / fill.price * 10_000.0
            staleness[label] = gap / NANOS_PER_MS

        rows.append(MarkoutRow(
            fill=fill,
            entry_ts_ns=entry[0],
            entry_mid=entry_mid,
            entry_spread_bps=entry[2],
            entry_offset_bps=fill.sign * (entry_mid - fill.price) / fill.price * 10_000.0,
            markouts_bps=markouts,
            staleness_ms=staleness,
            truncated=tuple(truncated),
        ))
    return rows


def _mean(values: Sequence[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def _median(values: Sequence[float]) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    middle = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[middle]
    return (ordered[middle - 1] + ordered[middle]) / 2.0


def _stdev(values: Sequence[float]) -> float:
    """Sample standard deviation (0.0 for fewer than two observations)."""
    if len(values) < 2:
        return 0.0
    mean = _mean(values)
    return math.sqrt(sum((value - mean) ** 2 for value in values) / (len(values) - 1))


def _t_stat(values: Sequence[float]) -> float:
    """Mean / standard error; 0.0 when there is nothing to test."""
    if len(values) < 2:
        return 0.0
    error = _stdev(values) / math.sqrt(len(values))
    if error <= 0.0:
        return 0.0
    return _mean(values) / error


def _horizon_stats(rows: Sequence[MarkoutRow], label: str) -> Dict[str, Any]:
    """Distribution of one horizon's markout across ``rows``."""
    samples: List[Tuple[float, float]] = []
    truncated = 0
    for row in rows:
        value = row.markouts_bps.get(label)
        if value is None:
            truncated += 1
            continue
        samples.append((float(value), row.notional))
    values = [value for value, _ in samples]
    notional = sum(weight for _, weight in samples)
    weighted = (
        sum(value * weight for value, weight in samples) / notional
        if notional > 0.0 else 0.0
    )
    mean = _mean(values)
    mean_notional = notional / len(samples) if samples else 0.0
    return {
        "samples": len(values),
        "truncated": truncated,
        "mean_bps": mean,
        "median_bps": _median(values),
        "stdev_bps": _stdev(values),
        "t_stat": _t_stat(values),
        "win_rate_pct": (
            100.0 * sum(1 for value in values if value > 0.0) / len(values)
            if values else 0.0
        ),
        "min_bps": min(values) if values else 0.0,
        "max_bps": max(values) if values else 0.0,
        "notional_weighted_bps": weighted,
        "usdt_per_fill": mean / 10_000.0 * mean_notional,
        "total_usdt": sum(value / 10_000.0 * weight for value, weight in samples),
    }


def _block(rows: Sequence[MarkoutRow], config: MarkoutConfig) -> Dict[str, Any]:
    """Per-horizon statistics for one bucket of fills."""
    spreads = [row.entry_spread_bps for row in rows if row.entry_spread_bps is not None]
    offsets = [row.entry_offset_bps for row in rows if row.entry_offset_bps is not None]
    return {
        "fills": len(rows),
        "notional": sum(row.notional for row in rows),
        "entry_spread_bps": _mean(spreads),
        "entry_offset_bps": _mean(offsets),
        "decay": _decay(rows, config),
        "horizons": {
            horizon_label(horizon): _horizon_stats(rows, horizon_label(horizon))
            for horizon in config.horizons_s
        },
    }


def summarize_markouts(rows: Sequence[MarkoutRow],
                       config: Optional[MarkoutConfig] = None) -> Dict[str, Any]:
    """Aggregates marked fills overall, by maker/taker and by maker side.

    Splitting maker buys from maker sells is not cosmetic: a one-sided book
    leaves the maker filled only on the side the market is about to run away
    from, so an aggregate that looks flat can hide a losing side and a winning
    one that never trades.
    """
    config = config or MarkoutConfig()
    matched = [row for row in rows if row.matched]
    maker = [row for row in matched if row.fill.is_maker]
    taker = [row for row in matched if not row.fill.is_maker]
    groups = {
        "maker_buy": [row for row in maker if row.fill.side == "BUY"],
        "maker_sell": [row for row in maker if row.fill.side == "SELL"],
        "taker_buy": [row for row in taker if row.fill.side == "BUY"],
        "taker_sell": [row for row in taker if row.fill.side == "SELL"],
    }
    labels = [horizon_label(horizon) for horizon in config.horizons_s]
    return {
        "config": config.as_dict(),
        "fills": {
            "total": len(rows),
            "matched": len(matched),
            "unmatched": len(rows) - len(matched),
            "maker": len(maker),
            "taker": len(taker),
            "truncated": {
                label: sum(1 for row in rows if row.markouts_bps.get(label) is None)
                for label in labels
            },
        },
        "overall": _block(matched, config),
        "maker": _block(maker, config),
        "taker": _block(taker, config),
        "buckets": {name: _block(group, config) for name, group in groups.items()},
    }


#: Two-sided 95% threshold for treating a markout mean as more than noise.
_SIGNIFICANT_T = 1.96


def _decay(rows: Sequence[MarkoutRow],
           config: MarkoutConfig) -> Optional[Dict[str, Any]]:
    """Markout drift between the first and last horizon, on the *same* fills.

    Comparing the 0 s mean (every fill) with the 60 s mean (only the fills old
    enough to have one) compares two populations, and the difference between them
    is not a drift. Only fills with a sample at both ends are used here, and that
    count is reported next to the number.
    """
    if len(config.horizons_s) < 2:
        return None
    first_label = horizon_label(config.horizons_s[0])
    last_label = horizon_label(config.horizons_s[-1])
    if first_label == last_label:
        return None
    pairs = [
        (row.markouts_bps[first_label], row.markouts_bps[last_label])
        for row in rows
        if row.markouts_bps.get(first_label) is not None
        and row.markouts_bps.get(last_label) is not None
    ]
    if not pairs:
        return None
    start = _mean([pair[0] for pair in pairs])
    end = _mean([pair[1] for pair in pairs])
    return {
        "bps": end - start,
        "from": first_label,
        "to": last_label,
        "from_bps": start,
        "to_bps": end,
        "samples": len(pairs),
    }


def markout_verdict(summary: Mapping[str, Any],
                    config: Optional[MarkoutConfig] = None) -> Dict[str, Any]:
    """Turns a summary into GO / MARGINAL / COSTLY / ADVERSE_SELECTION.

    The test is deliberately narrow: *did the maker's fills survive their own
    fee at the primary horizon, with enough observations to say so?* Fees are
    subtracted from the markout, so a strategy that captures 2 bps of spread and
    pays a 2 bps fee scores exactly zero and is refused — which is what the
    committed study's numbers imply.
    """
    config = config or MarkoutConfig()
    label = horizon_label(config.primary_horizon_s or 0.0)
    block = (summary.get("maker") or {})
    stats = ((block.get("horizons") or {}).get(label) or {})
    samples = int(stats.get("samples", 0) or 0)
    mean = float(stats.get("mean_bps", 0.0) or 0.0)
    t_stat = float(stats.get("t_stat", 0.0) or 0.0)
    fee = float(config.maker_bps)
    net = mean - fee
    reasons: List[str] = []

    if samples < config.min_samples:
        verdict = "INCONCLUSIVE"
        reasons.append(
            f"only {samples} maker fill(s) marked at {label}; "
            f"{config.min_samples} are required before a verdict means anything")
    elif mean <= 0.0 and t_stat <= -_SIGNIFICANT_T:
        verdict = "ADVERSE_SELECTION"
        reasons.append(
            f"maker markout at {label} is {mean:+.2f} bps (t={t_stat:+.2f}): every "
            f"fill is already under water before the {fee:.2f} bps fee is charged, "
            f"so resting adds a cost instead of capturing a spread")
    elif mean <= 0.0:
        verdict = "COSTLY"
        reasons.append(
            f"maker markout at {label} is {mean:+.2f} bps, so no spread is being "
            f"captured even before the {fee:.2f} bps fee (t={t_stat:+.2f})")
    elif net <= config.min_edge_bps:
        verdict = "COSTLY"
        reasons.append(
            f"markout of {mean:+.2f} bps at {label} does not clear the "
            f"{fee:.2f} bps maker fee (net {net:+.2f} bps)")
    elif t_stat < _SIGNIFICANT_T:
        verdict = "MARGINAL"
        reasons.append(
            f"markout of {mean:+.2f} bps leaves {net:+.2f} bps after fees, but "
            f"t={t_stat:+.2f} is below {_SIGNIFICANT_T}; not evidence yet")
    else:
        verdict = "GO"
        reasons.append(
            f"markout of {mean:+.2f} bps leaves {net:+.2f} bps after the "
            f"{fee:.2f} bps fee at t={t_stat:+.2f}")

    drift = block.get("decay")
    if drift:
        reasons.append(
            f"mean markout moves {float(drift['bps']):+.2f} bps between "
            f"{drift['from']} and {drift['to']} over the same "
            f"{int(drift['samples'])} fills (a negative drift is adverse "
            f"selection, not decay)")
    dispersion = float(stats.get("stdev_bps", 0.0) or 0.0)
    if samples and dispersion < 0.01:
        reasons.append(
            f"markouts are near-identical (σ={dispersion:.4f} bps): on a "
            f"deterministic replay the t-statistic is not informative, read the mean")
    reasons.append(
        "a GO verdict is a precondition, not proof: inventory risk, queue "
        "position and funding are not in this number")

    return {
        "verdict": verdict,
        "primary_horizon": label,
        "samples": samples,
        "mean_markout_bps": mean,
        "fee_bps": fee,
        "net_edge_bps": net,
        "t_stat": t_stat,
        "win_rate_pct": float(stats.get("win_rate_pct", 0.0) or 0.0),
        "usdt_per_fill": float(stats.get("usdt_per_fill", 0.0) or 0.0),
        "decay_bps": None if not drift else float(drift["bps"]),
        "decay_samples": None if not drift else int(drift["samples"]),
        "reasons": reasons,
    }


def _format_horizon_row(label: str, stats: Mapping[str, Any], fee_bps: float) -> str:
    samples = int(stats.get("samples", 0) or 0)
    if not samples:
        return f"| {label} | 0 | — | — | — | — | — | — |"
    mean = float(stats.get("mean_bps", 0.0) or 0.0)
    return (
        f"| {label} | {samples} | {mean:+.2f} | "
        f"{float(stats.get('median_bps', 0.0) or 0.0):+.2f} | "
        f"{float(stats.get('t_stat', 0.0) or 0.0):+.2f} | "
        f"{float(stats.get('win_rate_pct', 0.0) or 0.0):.1f}% | "
        f"{mean - fee_bps:+.2f} | "
        f"{float(stats.get('usdt_per_fill', 0.0) or 0.0):+.6f} |"
    )


def render_markdown(summary: Mapping[str, Any],
                    verdict: Mapping[str, Any]) -> List[str]:
    """Renders a study-grade markdown report (no terminal-width assumptions)."""
    known = MarkoutConfig.__dataclass_fields__
    config = MarkoutConfig(**{
        key: value for key, value in (summary.get("config") or {}).items()
        if key in known and key != "horizons_s"
    }, horizons_s=tuple((summary.get("config") or {}).get("horizons_s") or HORIZONS_S))
    fee = float(config.maker_bps)
    fills = summary.get("fills") or {}
    lines: List[str] = [
        "# Markout report",
        "",
        f"Fills: {fills.get('total', 0)} total, {fills.get('maker', 0)} maker, "
        f"{fills.get('taker', 0)} taker, {fills.get('unmatched', 0)} without a mid.",
        "",
        f"Horizons: {', '.join(horizon_label(h) for h in config.horizons_s)} "
        f"(tolerance {config.tolerance_s:g} s, maker fee {fee:.2f} bps).",
        "",
        "## Verdict",
        "",
        f"**{verdict['verdict']}** at {verdict['primary_horizon']} "
        f"({verdict['samples']} maker fills, t={verdict['t_stat']:+.2f}).",
        "",
    ]
    lines.extend(f"- {reason}" for reason in verdict.get("reasons", []))
    lines.extend([
        "",
        "## Maker fills",
        "",
        "| horizon | fills | mean bps | median bps | t | win % | net of fee bps | USDT/fill |",
        "| --- | --- | --- | --- | --- | --- | --- | --- |",
    ])
    maker = summary.get("maker") or {}
    for horizon in config.horizons_s:
        label = horizon_label(horizon)
        stats = (maker.get("horizons") or {}).get(label) or {}
        lines.append(_format_horizon_row(label, stats, fee))
    lines.extend([
        "",
        "## Taker fills",
        "",
        "| horizon | fills | mean bps | median bps | t | win % | net of fee bps | USDT/fill |",
        "| --- | --- | --- | --- | --- | --- | --- | --- |",
    ])
    taker = summary.get("taker") or {}
    for horizon in config.horizons_s:
        label = horizon_label(horizon)
        stats = (taker.get("horizons") or {}).get(label) or {}
        lines.append(_format_horizon_row(label, stats, float(config.taker_bps)))
    lines.extend([
        "",
        "## Maker by side",
        "",
        "| bucket | horizon | fills | mean bps | t | win % |",
        "| --- | --- | --- | --- | --- | --- |",
    ])
    buckets = summary.get("buckets") or {}
    primary = horizon_label(config.primary_horizon_s or 0.0)
    for name in ("maker_buy", "maker_sell"):
        block = buckets.get(name) or {}
        stats = (block.get("horizons") or {}).get(primary) or {}
        lines.append(
            f"| {name} | {primary} | {int(stats.get('samples', 0) or 0)} | "
            f"{float(stats.get('mean_bps', 0.0) or 0.0):+.2f} | "
            f"{float(stats.get('t_stat', 0.0) or 0.0):+.2f} | "
            f"{float(stats.get('win_rate_pct', 0.0) or 0.0):.1f}% |")
    truncated = ", ".join(
        f"{label}={count}" for label, count
        in (fills.get("truncated") or {}).items())
    lines.extend([
        "",
        "## Data quality",
        "",
        f"Horizons with no sample inside the {config.tolerance_s:g} s tolerance: "
        f"{truncated or 'none'}.",
        "",
        "## What this does not prove",
        "",
        "- Markout is a diagnostic over recorded data, not a forecast.",
        "- Fees are the only cost subtracted; funding, queue position and",
        "  inventory risk are outside this number.",
        "- A quote that never filled cannot be marked, so the fills that did are",
        "  a selected sample: the skipped ones are not in this report.",
        "",
    ])
    return lines


class MarkoutReplayer(BulkReplayer):
    """A :class:`BulkReplayer` that marks its own fills on the engine's clock.

    Simulated fills are stamped with ``steady_clock`` inside the C++ engine, so
    they cannot be joined to an archive's exchange timestamps after the fact.
    Measuring them *inside* the replay removes the join entirely: the mid behind
    ``h = 0`` is the book that produced the fill, and each later horizon is a
    later frame of the same feed.

    ``frames_per_hook`` bounds the resolution of a markout: with a batch of ``n``
    frames, a fill and its mid are both attributed to the hook that ended the
    batch, so horizons shorter than ``n`` frames are not measurable.
    """

    def __init__(self, *, config: Optional[MarkoutConfig] = None, **kwargs: Any) -> None:
        self._markout_config = config or MarkoutConfig()
        self._timeline = MarkoutTimeline()
        self._fill_log: List[MarkoutFill] = []
        self._unpriced_frames = 0
        super().__init__(**kwargs)

    @property
    def markout_config(self) -> MarkoutConfig:
        return self._markout_config

    @property
    def timeline(self) -> MarkoutTimeline:
        return self._timeline

    @property
    def fill_log(self) -> List[MarkoutFill]:
        return list(self._fill_log)

    def reset(self) -> None:
        super().reset()
        self._timeline = MarkoutTimeline()
        self._fill_log = []
        self._unpriced_frames = 0

    def on_frame(self, event_ns: int, new_fills: Sequence[Any]) -> None:
        """Samples the mid and records any fill discovered in this frame."""
        bbo = self.engine.bbo()
        exchange_ts = int(bbo.exchange_ts)
        # Two clocks reach this hook. Bbo.exchange_ts carries the venue's raw
        # `E` in milliseconds; _next_synth_ns counts nanoseconds from zero for an
        # archive without timestamps. Which one applies is decided by the frame,
        # not by the magnitude of the value: the synthetic counter passes 1e11
        # (100 s) after 1000 frames and 1.7e12 after ~17,000, so a magnitude rule
        # would silently switch units mid-replay and truncate every horizon.
        frame_ts_ns = exchange_ts * NANOS_PER_MS if exchange_ts > 0 else int(event_ns)
        bid = float(bbo.bid_price)
        ask = float(bbo.ask_price)
        if bid > 0.0 and ask >= bid:
            mid = (bid + ask) / 2.0
            spread = (ask - bid) / mid * 10_000.0 if mid > 0.0 else 0.0
            if not self._timeline.add(frame_ts_ns, mid, spread):
                self._unpriced_frames += 1
        else:
            self._unpriced_frames += 1
        for fill in new_fills:
            self._fill_log.append(MarkoutFill(
                ts_ns=frame_ts_ns,
                side=str(fill.side),
                price=float(fill.price),
                quantity=float(fill.quantity),
                is_maker=bool(fill.is_maker),
            ))

    def markout_rows(self) -> List[MarkoutRow]:
        return resolve_markouts(self._fill_log, self._timeline, self._markout_config)

    def markout_summary(self) -> Dict[str, Any]:
        return summarize_markouts(self.markout_rows(), self._markout_config)

    def diagnostics(self) -> Dict[str, Any]:
        """Coverage of the measurement itself (a thin timeline invalidates it)."""
        return {
            "timeline_samples": len(self._timeline),
            "timeline_span_s": self._timeline.span_s,
            "timeline_max_gap_s": self._timeline.max_gap_s,
            "frames_without_mid": self._unpriced_frames,
            "fills": len(self._fill_log),
            "frames_per_hook": self._frames_per_hook,
        }
