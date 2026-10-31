"""Edge curve: what a resting quote earns as a function of how far it rests.

A markout study answers "did the fills we got pay for themselves". It cannot
answer the question that decides whether a market maker is viable on an
instrument at all: **at what distance from the touch does a quote earn more than
the fee it pays, and how often does the tape reach that distance?**

That question has nothing to do with the strategy's parameters. For every print
in a capture, an order resting ``d`` basis points on the passive side would have
been filled if the print traded at or through its price. Marking that hypothetical
fill from its own resting price gives the *edge curve*::

    edge(d, h) = sign * (mid(t + h) - quote(d, t)) / quote(d, t) * 10_000

with the fee subtracted on top. Two properties make it usable as a feasibility
test:

* **It is monotone in the right direction.** A larger ``d`` means a lower
  resting price for a buy (higher for a sell), so the set of prints that reach
  it shrinks. The curve therefore reports the trade-off honestly: the distance
  where the edge clears the fee, and the fill rate that is left there.
* **It cannot be tuned.** There is no strategy in it — no inventory, no skew, no
  re-anchoring. Only the tape, a distance, and the fee.

The number that matters on a retail crypto schedule is stark: on BTCUSDT
perpetuals a tick of ``0.10`` at ``81_000`` is ``0.012 bps``, so a ``2 bps``
maker fee is **~160 ticks**. A passive maker on that instrument has to rest a
fiftieth of a percent away from the mid to be paid for the fill, and this module
measures whether the tape ever goes there.

What this does not model
------------------------
Queue position, our own size against the print's size, latency, and the effect
our resting order has on the book are all outside this number. None of them is
neutral. A quote re-priced from a *stale* mid, which is what a real maker posts
while it is waiting for its next update, can be reached by a print the idealised
quote would have moved away from, so latency can raise the fill count as well as
lower it, and the fills it changes are not the same fills: their markout moves
too. The number here is therefore conditional on this fill rule — the price on
our side being the mid before the print — and is not an execution estimate. A
distance that loses on this curve has no measured edge under the idealised
rule; whether a different execution model would find one is a separate
measurement, not a deduction from this one.

Scale
-----
The scan keeps one small record per print, which is a few tens of megabytes per
hour of BTCUSDT tape -- fine for a study, and the reason a day-long capture
should be measured roll by roll (or as a `--source` list, which this module
treats as one chronological tape). The timeline is the larger of the two and is
bounded by the depth frame rate, not by the print rate.
"""
from __future__ import annotations

import json
import math
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from ..costs import DEFAULT_MAKER_BPS
from .markouts import (
    NANOS_PER_S,
    MarkoutTimeline,
    horizon_label,
    message_ts_ns,
)

__all__ = [
    "DEFAULT_DISTANCES_BPS",
    "EdgeCurveConfig",
    "TapePrint",
    "TapeScan",
    "edge_curve_verdict",
    "measure_edge_curve",
    "quote_price",
    "render_markdown",
    "scan_tape",
]

#: Resting distances, in basis points, the curve is sampled at. The grid spans a
#: couple of ticks to eight bps so one run covers "inside the spread" through
#: "outside any fee": at ``81_000`` on a ``0.10`` tick a step is ``0.012 bps``, so
#: ``0.025`` is two ticks and ``2.0`` is what a retail maker fee costs.
DEFAULT_DISTANCES_BPS: Tuple[float, ...] = (
    0.025, 0.05, 0.1, 0.2, 0.5, 1.0, 2.0, 4.0, 8.0,
)

#: Horizons, in seconds, at which a hypothetical fill is marked. ``0`` measures
#: the offset the quote was posted at, which is not an edge: the mid has not had
#: time to move. The positive horizons are what the fee has to be paid out of.
DEFAULT_HORIZONS_S: Tuple[float, ...] = (0.0, 1.0, 10.0)


@dataclass(frozen=True)
class TapePrint:
    """One aggressor print from a capture, in venue order.

    ``aggressor`` is the *taker* side: it is the side that removed liquidity, so
    it is also the side a resting order must be on to be filled by it.
    """

    ts_ns: int
    price: float
    quantity: float
    aggressor: str

    def __post_init__(self) -> None:
        price = float(self.price)
        quantity = float(self.quantity)
        if not math.isfinite(price) or price <= 0.0:
            raise ValueError(f"print price must be > 0, got {self.price!r}")
        if not math.isfinite(quantity) or quantity < 0.0:
            raise ValueError(f"print quantity must be >= 0, got {self.quantity!r}")
        object.__setattr__(self, "price", price)
        object.__setattr__(self, "quantity", quantity)
        object.__setattr__(self, "ts_ns", int(self.ts_ns))
        side = str(self.aggressor).upper()
        if "BUY" not in side and "SELL" not in side:
            raise ValueError(f"unrecognised aggressor {self.aggressor!r}")
        object.__setattr__(self, "aggressor", "BUY" if "BUY" in side else "SELL")


@dataclass
class TapeScan:
    """Result of one pass over a capture's lines."""

    prints: List[TapePrint]
    malformed: int = 0
    ignored: int = 0
    symbol: Optional[str] = None

    @property
    def first_ts_ns(self) -> Optional[int]:
        return self.prints[0].ts_ns if self.prints else None

    @property
    def last_ts_ns(self) -> Optional[int]:
        return self.prints[-1].ts_ns if self.prints else None

    @property
    def span_s(self) -> float:
        if len(self.prints) < 2:
            return 0.0
        return (self.prints[-1].ts_ns - self.prints[0].ts_ns) / NANOS_PER_S


def _aggressor(message: Mapping[str, Any]) -> Optional[str]:
    """The taker side of a print, from the venue's buyer-is-maker flag.

    Binance's ``m`` means "the buyer was the maker", so ``m = true`` is a sell
    aggressor. A frame without a boolean flag is refused rather than guessed: the
    wrong sign would flip every edge on the curve.
    """
    for key in ("m", "is_buyer_maker", "buyer_is_maker"):
        if key not in message:
            continue
        maker = message[key]
        if isinstance(maker, bool):
            return "SELL" if maker else "BUY"
        return None
    return None


def scan_tape(lines: Iterable[str], *, symbol: Optional[str] = None) -> TapeScan:
    """Collects aggressive prints from capture lines (plain or gzipped text).

    Depth frames, snapshots and any other message are skipped: the curve needs
    the prints and the mid timeline built separately from the same capture.
    Prints for another symbol are counted in ``ignored`` instead of blended in.
    """
    want = str(symbol).upper() if symbol else None
    # A caller that named the market already knows it, and the first trade is
    # not necessarily that market: assigning ``scan.symbol`` from the first frame
    # would label an ignored ETH print as the population of a BTCUSDT study.
    scan = TapeScan(prints=[], symbol=want)
    for raw in lines:
        text = raw.strip() if isinstance(raw, str) else str(raw).strip()
        if not text:
            continue
        try:
            message = json.loads(text)
        except (TypeError, ValueError):
            scan.malformed += 1
            continue
        if not isinstance(message, Mapping):
            scan.malformed += 1
            continue
        kind = str(message.get("e") or message.get("type") or "")
        if kind not in ("trade", "aggTrade"):
            continue
        frame_symbol = message.get("s") or message.get("symbol")
        if frame_symbol:
            name = str(frame_symbol).upper()
            if scan.symbol is None:
                scan.symbol = name
                want = want or name
            if want and name != want:
                scan.ignored += 1
                continue
        price = message.get("p", message.get("price"))
        quantity = message.get("q", message.get("quantity"))
        ts_ns = message_ts_ns(message)
        aggressor = _aggressor(message)
        if price in (None, "") or quantity in (None, "") or ts_ns is None or aggressor is None:
            scan.malformed += 1
            continue
        try:
            scan.prints.append(TapePrint(ts_ns=ts_ns, price=float(price),
                                         quantity=float(quantity),
                                         aggressor=aggressor))
        except (TypeError, ValueError):
            scan.malformed += 1
    return scan


@dataclass(frozen=True)
class EdgeCurveConfig:
    """What distances to test, how to mark them, and what counts as clearing.

    ``tick_size`` only quantises the hypothetical quote. It is applied in the
    direction that *widens* (a bid rounds down, an ask up), so a missing or
    coarse grid can never flatter an edge. ``0`` disables quantisation, which is
    the right choice for a study that compares venues, and the wrong one for a
    study that claims a venue can be traded.
    """

    distances_bps: Tuple[float, ...] = DEFAULT_DISTANCES_BPS
    horizons_s: Tuple[float, ...] = DEFAULT_HORIZONS_S
    tolerance_s: float = 2.0
    maker_bps: float = DEFAULT_MAKER_BPS
    tick_size: float = 0.0
    min_samples: int = 30
    min_coverage: float = 0.25
    primary_horizon_s: Optional[float] = None

    def __post_init__(self) -> None:
        distances = tuple(sorted({float(value) for value in self.distances_bps}))
        if not distances:
            raise ValueError("distances_bps must contain at least one distance")
        if distances[0] <= 0.0:
            raise ValueError("distances_bps must be > 0; a quote at the mid is not resting")
        horizons = tuple(sorted({float(value) for value in self.horizons_s}))
        if not horizons or horizons[0] < 0.0:
            raise ValueError("horizons_s must be non-negative")
        # Two horizons that print as the same label would pool their markouts
        # under one report key, so fifteen fills could satisfy a 30-sample bar
        # and "clear" a distance on half the evidence it claims.
        labels = [horizon_label(value) for value in horizons]
        if len(set(labels)) != len(labels):
            raise ValueError(
                f"horizons_s has horizons that share a label: {labels}; "
                "their markouts would be pooled")
        if float(self.tolerance_s) <= 0.0:
            raise ValueError("tolerance_s must be > 0")
        if float(self.tick_size) < 0.0:
            raise ValueError("tick_size must be >= 0")
        if int(self.min_samples) < 1:
            raise ValueError("min_samples must be >= 1")
        if not 0.0 <= float(self.min_coverage) <= 1.0:
            raise ValueError("min_coverage must be between 0 and 1")
        object.__setattr__(self, "distances_bps", distances)
        object.__setattr__(self, "horizons_s", horizons)
        object.__setattr__(self, "tolerance_s", float(self.tolerance_s))
        object.__setattr__(self, "maker_bps", float(self.maker_bps))
        object.__setattr__(self, "tick_size", float(self.tick_size))
        object.__setattr__(self, "min_samples", int(self.min_samples))
        object.__setattr__(self, "min_coverage", float(self.min_coverage))

        primary = self.primary_horizon_s
        if primary is None:
            positive = [value for value in horizons if value > 0.0]
            if not positive:
                raise ValueError(
                    "horizons_s needs at least one positive horizon; h=0 is the "
                    "quote offset, not an edge")
            primary = positive[0]
        elif float(primary) not in horizons:
            raise ValueError(
                f"primary_horizon_s {primary!r} is not one of the measured "
                f"horizons {list(horizons)}")
        if float(primary) <= 0.0:
            # The verdict subtracts the fee from the primary horizon's mean. At
            # h=0 that mean is the distance the quote was posted at, so any
            # grid distance above the fee would "clear" it and the runner would
            # exit 0 on a GO that measures nothing. The column stays in the
            # table; it is only refused as the horizon that decides.
            raise ValueError(
                "primary_horizon_s must be positive; h=0 measures the quote "
                "offset, not an edge")
        object.__setattr__(self, "primary_horizon_s", float(primary))

    def as_dict(self) -> Dict[str, Any]:
        return {
            "distances_bps": list(self.distances_bps),
            "horizons_s": list(self.horizons_s),
            "tolerance_s": self.tolerance_s,
            "maker_bps": self.maker_bps,
            "tick_size": self.tick_size,
            "min_samples": self.min_samples,
            "min_coverage": self.min_coverage,
            "primary_horizon_s": float(self.primary_horizon_s or 0.0),
        }


def quote_price(mid: float, distance_bps: float, *, buy: bool,
                tick_size: float = 0.0) -> Optional[float]:
    """Resting price ``distance_bps`` away from ``mid``, snapped to ``tick_size``.

    Snapping widens: a bid floors and an ask ceilings, so a price the venue
    cannot represent becomes one slightly further from the mid. Returns ``None``
    for a non-positive result (a distance larger than the price itself).
    """
    factor = 1.0 - float(distance_bps) / 10_000.0 if buy else 1.0 + float(distance_bps) / 10_000.0
    price = float(mid) * factor
    if not math.isfinite(price) or price <= 0.0:
        return None
    tick = float(tick_size)
    if tick > 0.0:
        steps = price / tick
        price = (math.floor(steps) if buy else math.ceil(steps)) * tick
        if not math.isfinite(price) or price <= 0.0:
            return None
    return price


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
    if len(values) < 2:
        return 0.0
    mean = _mean(values)
    return math.sqrt(sum((value - mean) ** 2 for value in values) / (len(values) - 1))


def _t_stat(values: Sequence[float]) -> float:
    if len(values) < 2:
        return 0.0
    error = _stdev(values) / math.sqrt(len(values))
    if error <= 0.0:
        return 0.0
    return _mean(values) / error


def _block(values: Sequence[float], fee_bps: float,
           attempts: Optional[int] = None) -> Dict[str, Any]:
    """Distribution of one horizon's edge, plus how many fills it could not mark.

    ``attempts`` is the number of fills that reached this distance. A fill whose
    target time lands in a hole in the timeline has no markout and is not
    interpolated, so the difference is reported rather than silently shrinking
    the sample: on a real capture those holes are not random — they cluster
    around reconnects and violent moves, which is exactly when the fills that are
    dropped happened.
    """
    mean = _mean(values)
    dropped = 0 if attempts is None else max(int(attempts) - len(values), 0)
    return {
        "samples": len(values),
        "truncated": dropped,
        "mean_bps": mean,
        "median_bps": _median(values),
        "stdev_bps": _stdev(values),
        "t_stat": _t_stat(values),
        "win_rate_pct": (
            100.0 * sum(1 for value in values if value > 0.0) / len(values)
            if values else 0.0
        ),
        "net_bps": mean - fee_bps,
    }


def measure_edge_curve(scan: TapeScan, timeline: MarkoutTimeline,
                       config: Optional[EdgeCurveConfig] = None) -> Dict[str, Any]:
    """Sweeps every print against every distance and aggregates by distance.

    A print fills a resting buy when it trades at or below the quote price, and a
    resting sell when it trades at or above it. Each fill is marked from the
    quote's own price — the price the order would have been filled at — never
    from the print's price: a print that trades through us fills us at our limit,
    and marking from the print would book a few tenths of a bp of edge that the
    order never captured.

    Prints with no mid strictly before them are counted in ``unmatched`` and
    dropped: the quote could not have been placed, so there is nothing to mark.
    """
    config = config or EdgeCurveConfig()
    labels = [horizon_label(horizon) for horizon in config.horizons_s]
    tolerance_ns = int(config.tolerance_s * NANOS_PER_S)
    samples: Dict[float, Dict[str, List[float]]] = {
        distance: {label: [] for label in labels} for distance in config.distances_bps
    }
    fills: Dict[float, int] = {distance: 0 for distance in config.distances_bps}
    matched = 0
    unmatched = 0

    for print in scan.prints:
        entry = timeline.strictly_before(print.ts_ns)
        if entry is None or not entry[1]:
            unmatched += 1
            continue
        matched += 1
        mid = float(entry[1])
        buy = print.aggressor == "SELL"
        sign = 1.0 if buy else -1.0
        for distance in config.distances_bps:
            price = quote_price(mid, distance, buy=buy, tick_size=config.tick_size)
            if price is None:
                continue
            reached = print.price <= price if buy else print.price >= price
            if not reached:
                continue
            fills[distance] += 1
            for horizon, label in zip(config.horizons_s, labels):
                if horizon == 0.0:
                    sample: Optional[Tuple[int, float, float]] = entry
                    gap: Optional[int] = 0
                else:
                    target = print.ts_ns + int(round(horizon * NANOS_PER_S))
                    sample = timeline.at_or_after(target)
                    gap = None if sample is None else sample[0] - target
                if sample is None or gap is None or gap > tolerance_ns:
                    continue
                samples[distance][label].append(sign * (sample[1] - price) / price * 10_000.0)

    span_s = scan.span_s
    minutes = span_s / 60.0 if span_s > 0.0 else 0.0
    # A distance in bps only becomes a whole number of ticks once a price is
    # named, so the conversion uses the tape's own median mid. Reporting it
    # against anything else (a configured centre, one frame's touch) would quote
    # a tick count that never existed on this book.
    mids = [sample[1] for sample in timeline.samples() if sample[1]]
    median_mid = _median(mids)
    rows: List[Dict[str, Any]] = []
    primary_label = horizon_label(config.primary_horizon_s or 0.0)
    for distance in config.distances_bps:
        count = fills[distance]
        horizons = {
            label: _block(samples[distance][label], config.maker_bps, count)
            for label in labels
        }
        # A round trip is two fills, so it pays the fee twice and earns the
        # markout twice: 2*mean - 2*fee. `breakeven_maker_bps` is the per-side
        # fee at which that expression is zero, which is simply the per-fill
        # markout -- the number a fee tier has to beat for the configuration to
        # be worth anything at all. Both sides are assumed to fill at the same
        # distance, which is the standard maker decomposition and not a promise
        # that the exit arrives on schedule.
        primary_mean = float(horizons[primary_label]["mean_bps"])
        primary_samples = int(horizons[primary_label]["samples"])
        round_trip = {
            "horizon": primary_label,
            "gross_bps": 2.0 * primary_mean,
            "fees_bps": 2.0 * float(config.maker_bps),
            "net_bps": 2.0 * primary_mean - 2.0 * float(config.maker_bps),
            "breakeven_maker_bps": primary_mean,
            "samples": primary_samples,
            # What fraction of this distance's fills could be marked at all. A
            # cell computed on a small share of its fills is a statement about
            # the fills that happened to be markable, and far-distance fills
            # cluster in the volatile stretches where the timeline goes blind.
            "coverage_pct": (
                100.0 * primary_samples / count if count else 0.0
            ),
        }
        rows.append({
            "distance_bps": distance,
            "distance_ticks": (
                distance / 10_000.0 * median_mid / config.tick_size
                if config.tick_size > 0.0 and median_mid > 0.0 else None
            ),
            "fills": count,
            "reach_pct": 100.0 * count / matched if matched else 0.0,
            "fills_per_min": count / minutes if minutes > 0.0 else 0.0,
            "horizons": horizons,
            "round_trip": round_trip,
        })

    return {
        "config": config.as_dict(),
        "tape": {
            "symbol": scan.symbol,
            "prints": len(scan.prints),
            "matched": matched,
            "unmatched": unmatched,
            "malformed": scan.malformed,
            "ignored": scan.ignored,
            "span_s": span_s,
            "median_mid": median_mid,
            "timeline_samples": len(timeline),
            "timeline_max_gap_s": timeline.max_gap_s,
            # The health of the book the mids came from, not just how many there
            # are: coverage is what makes a far row decidable, and a seeded book
            # whose handoff the stream could not continue is the one way a mid can
            # be present and still wrong.
            "timeline_stale": timeline.stale,
            "timeline_handoff_gaps": timeline.handoff_gaps,
            "timeline_seed_backdated": timeline.seed_backdated,
            "timeline_seed_unstamped": timeline.seed_unstamped,
        },
        "rows": rows,
    }


def _net_at(row: Mapping[str, Any], label: str) -> float:
    return float(((row.get("horizons") or {}).get(label) or {}).get("net_bps", 0.0) or 0.0)


def _samples_at(row: Mapping[str, Any], label: str) -> int:
    return int(((row.get("horizons") or {}).get(label) or {}).get("samples", 0) or 0)


def edge_curve_verdict(summary: Mapping[str, Any],
                       config: Optional[EdgeCurveConfig] = None) -> Dict[str, Any]:
    """Names the first distance that clears the fee, and what it would fill.

    The verdict is deliberately about *feasibility*, not about a strategy: a
    distance that clears the fee with the required sample count is the only kind
    of distance worth designing a maker around. When no distance clears, the
    curve's own numbers say why — either the edge never gets there, or the tape
    never reaches it.
    """
    config = config or EdgeCurveConfig()
    label = horizon_label(config.primary_horizon_s or 0.0)
    tape = summary.get("tape") or {}
    rows = list(summary.get("rows") or [])
    reasons: List[str] = []

    if not rows or not int(tape.get("matched", 0) or 0):
        return {
            "verdict": "NO_DATA",
            "primary_horizon": label,
            "distance_bps": None,
            "fee_bps": float(config.maker_bps),
            "net_bps": None,
            "fills": 0,
            "fills_per_min": 0.0,
            "round_trip": {
                "distance_bps": None,
                "net_bps": None,
                "breakeven_maker_bps": None,
                "breakeven_distance_bps": None,
            },
            "reasons": [
                "no print had a mid before it, so no hypothetical quote could be "
                "priced; check that the capture carries depth frames as well as trades",
            ],
        }

    eligible = [row for row in rows if _samples_at(row, label) >= config.min_samples]
    under_covered = [
        row for row in eligible
        if float(((row.get("round_trip") or {}).get("coverage_pct", 100.0)) or 0.0)
        < 100.0 * config.min_coverage
    ]
    # A row that could only be marked for a small share of its fills is not
    # evidence about the distance, however many samples it has: the unmarkable
    # fills are the ones that happened while the timeline was blind, which is
    # where the violent moves -- and the far fills -- live.
    eligible = [row for row in eligible if row not in under_covered]
    clearing = [row for row in eligible if _net_at(row, label) > 0.0]
    if clearing:
        best = min(clearing, key=lambda row: float(row["distance_bps"]))
        net = _net_at(best, label)
        verdict = "CLEARS"
        reasons.append(
            f"a quote resting {float(best['distance_bps']):g} bps "
            f"({float(best['distance_ticks'] or 0.0):.0f} ticks) from the mid cleared "
            f"the {config.maker_bps:g} bps fee at {label}: net {net:+.2f} bps on "
            f"{_samples_at(best, label)} fill(s)")
        reasons.append(
            f"that distance was reached by {best['fills']} of {tape.get('matched')} "
            f"print(s) ({float(best['reach_pct']):.2f}%), "
            f"{float(best['fills_per_min']):.2f} fills/min on this tape")
    else:
        verdict = "NO_CLEARING_DISTANCE"
        if eligible:
            best = max(eligible, key=lambda row: _net_at(row, label))
            # Every claim below is about the rows that passed *both* gates. A row
            # excluded for coverage is described separately, so "no distance
            # cleared" is never read as "every measured cell was negative".
            reasons.append(
                f"among the {len(eligible)} covered distance(s) with at least "
                f"{config.min_samples} samples, none cleared the "
                f"{config.maker_bps:g} bps fee at {label}; the best was "
                f"{float(best['distance_bps']):g} bps "
                f"({float(best['distance_ticks'] or 0.0):.0f} ticks) with net "
                f"{_net_at(best, label):+.2f} bps on {_samples_at(best, label)} fill(s)")
        else:
            reachable = [row for row in rows if _samples_at(row, label) > 0]
            enough_samples = [
                row for row in reachable
                if _samples_at(row, label) >= config.min_samples
            ]
            if enough_samples:
                reasons.append(
                    f"no distance could decide: {len(enough_samples)} had the "
                    f"{config.min_samples} samples a verdict needs but not the "
                    f"{config.min_coverage:.0%} coverage, and the rest had too few "
                    f"samples at {label}")
            elif reachable:
                furthest = max(reachable, key=lambda row: float(row["distance_bps"]))
                reasons.append(
                    f"the tape reached {float(furthest['distance_bps']):g} bps "
                    f"({_samples_at(furthest, label)} fill(s), "
                    f"{float(furthest['fills_per_min']):.2f}/min) but no distance "
                    f"produced the {config.min_samples} fills a verdict needs")
            else:
                reasons.append(
                    f"the tape never reached any distance in the grid at {label}: "
                    f"the mid moved less than "
                    f"{min(float(row['distance_bps']) for row in rows):g} bps "
                    f"while a quote was resting")
    reasons.append(
        "queue position, latency and our own size are not modelled: the fill "
        "count is not an execution estimate, and those effects change both how "
        "often a quote fills and what the fills that remain are worth, so read "
        "the edge as conditional on the idealised fill rule")

    # One fill is not a business: a round trip pays the fee twice and earns the
    # markout twice, so the fee ceiling is the per-fill markout itself. This is
    # the number a fee tier has to beat, and it is the one an operator can act
    # on -- a venue schedule is negotiable in a way that adverse selection is
    # not.
    if under_covered:
        worst = min(
            under_covered,
            key=lambda row: float(row["round_trip"]["coverage_pct"]),
        )
        reasons.append(
            f"{len(under_covered)} distance(s) were excluded for coverage, not for "
            f"edge: at {float(worst['distance_bps']):g} bps only "
            f"{float(worst['round_trip']['coverage_pct']):.1f}% of "
            f"{int(worst['fills'])} fills could be marked at {label}, so a mean over "
            f"what was left is a statement about a selected subset and cannot "
            f"carry a verdict")
    round_trip_rows = [
        row for row in rows
        if int((row.get("round_trip") or {}).get("samples", 0) or 0) >= config.min_samples
        and row not in under_covered
    ]
    best_round_trip = max(
        round_trip_rows,
        key=lambda row: float(row["round_trip"]["net_bps"]),
        default=None,
    )
    best_ceiling = max(
        round_trip_rows,
        key=lambda row: float(row["round_trip"]["breakeven_maker_bps"]),
        default=None,
    )
    round_trip: Dict[str, Any] = {
        "distance_bps": (
            None if best_round_trip is None
            else float(best_round_trip["distance_bps"])
        ),
        "net_bps": (
            None if best_round_trip is None
            else float(best_round_trip["round_trip"]["net_bps"])
        ),
        "breakeven_maker_bps": (
            None if best_ceiling is None
            else float(best_ceiling["round_trip"]["breakeven_maker_bps"])
        ),
        "breakeven_distance_bps": (
            None if best_ceiling is None
            else float(best_ceiling["distance_bps"])
        ),
    }
    if best_round_trip is not None:
        net = round_trip["net_bps"] or 0.0
        if net > 0.0:
            reasons.append(
                f"a round trip at {round_trip['distance_bps']:g} bps nets "
                f"{net:+.2f} bps after two {config.maker_bps:g} bps maker fees "
                f"at {label} (covered rows only)")
        else:
            reasons.append(
                f"no round trip among the {len(round_trip_rows)} covered distance(s) "
                f"clears two {config.maker_bps:g} bps maker fees: the best is "
                f"{net:+.2f} bps at {round_trip['distance_bps']:g} bps")
    if best_ceiling is not None:
        reasons.append(
            f"the fee ceiling for a covered round trip is "
            f"{round_trip['breakeven_maker_bps']:+.3f} bps per side at "
            f"{round_trip['breakeven_distance_bps']:g} bps of quote offset "
            f"({label}, idealised fill rule); a maker schedule above that cannot "
            f"pay for a round trip on this tape")

    return {
        "verdict": verdict,
        "primary_horizon": label,
        "distance_bps": (
            float(clearing[0]["distance_bps"]) if clearing else None
        ),
        "fee_bps": float(config.maker_bps),
        "net_bps": _net_at(clearing[0], label) if clearing else None,
        "fills": int(clearing[0]["fills"]) if clearing else 0,
        "fills_per_min": float(clearing[0]["fills_per_min"]) if clearing else 0.0,
        "round_trip": round_trip,
        "reasons": reasons,
    }


def render_markdown(summary: Mapping[str, Any], verdict: Mapping[str, Any]) -> List[str]:
    """Renders the curve as a markdown report."""
    known = EdgeCurveConfig.__dataclass_fields__
    config = EdgeCurveConfig(**{
        key: value for key, value in (summary.get("config") or {}).items()
        if key in known and key not in ("distances_bps", "horizons_s")
    }, distances_bps=tuple((summary.get("config") or {}).get("distances_bps")
                           or DEFAULT_DISTANCES_BPS),
        horizons_s=tuple((summary.get("config") or {}).get("horizons_s")
                         or DEFAULT_HORIZONS_S))
    tape = summary.get("tape") or {}
    label = verdict["primary_horizon"]
    lines: List[str] = [
        "# Edge curve report",
        "",
        f"Tape: {tape.get('prints', 0)} print(s) over {float(tape.get('span_s', 0.0)):.1f} s, "
        f"{tape.get('matched', 0)} priced against a mid, "
        f"{tape.get('unmatched', 0)} dropped for having none.",
        "",
        f"Fee: {config.maker_bps:.2f} bps maker. Distances are basis points "
        "away from the mid; `fills` counts the prints that reached the quote.",
        "",
        "## Verdict",
        "",
        f"**{verdict['verdict']}** at {label}.",
        "",
    ]
    lines.extend(f"- {reason}" for reason in verdict.get("reasons", []))
    lines.extend([
        "",
        "## Curve",
        "",
        f"| distance bps | ticks | fills | reach % | fills/min | {label} n | "
        f"dropped | {label} mean | median | t | win % | net of fee |",
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
    ])
    for row in summary.get("rows") or []:
        block = (row.get("horizons") or {}).get(label) or {}
        ticks = row.get("distance_ticks")
        lines.append(
            f"| {float(row['distance_bps']):g} | "
            f"{'—' if ticks is None else f'{float(ticks):.0f}'} | "
            f"{int(row['fills'])} | {float(row['reach_pct']):.2f}% | "
            f"{float(row['fills_per_min']):.2f} | "
            f"{int(block.get('samples', 0) or 0)} | "
            f"{int(block.get('truncated', 0) or 0)} | "
            f"{float(block.get('mean_bps', 0.0) or 0.0):+.2f} | "
            f"{float(block.get('median_bps', 0.0) or 0.0):+.2f} | "
            f"{float(block.get('t_stat', 0.0) or 0.0):+.2f} | "
            f"{float(block.get('win_rate_pct', 0.0) or 0.0):.1f}% | "
            f"{float(block.get('net_bps', 0.0) or 0.0):+.2f} |")
    lines.extend([
        "",
        "## Round trip (two fills, two fees)",
        "",
        f"| distance bps | n | coverage | eligible | {label} mean | gross (2x) | "
        "fees (2x) | net | fee ceiling |",
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- |",
    ])
    for row in summary.get("rows") or []:
        trip = row.get("round_trip") or {}
        mean = float((row.get("horizons") or {}).get(label, {}).get("mean_bps", 0.0) or 0.0)
        coverage = float(trip.get("coverage_pct", 0.0) or 0.0)
        # The same two gates the verdict applies, so a table reader can see which
        # rows the conclusion speaks for instead of inferring it from the mean.
        eligible = (
            int(trip.get("samples", 0) or 0) >= config.min_samples
            and coverage >= 100.0 * config.min_coverage
        )
        lines.append(
            f"| {float(row['distance_bps']):g} | "
            f"{int(trip.get('samples', 0) or 0)} | {coverage:.1f}% | "
            f"{'yes' if eligible else 'no'} | {mean:+.3f} | "
            f"{float(trip.get('gross_bps', 0.0) or 0.0):+.2f} | "
            f"{float(trip.get('fees_bps', 0.0) or 0.0):.2f} | "
            f"{float(trip.get('net_bps', 0.0) or 0.0):+.2f} | "
            f"{float(trip.get('breakeven_maker_bps', 0.0) or 0.0):+.3f} |")
    lines.extend([
        "",
        "A round trip pays the maker fee twice and earns the markout twice, so",
        "`2 * mean - 2 * fee` is the only per-round-trip number that means anything.",
        "The fee ceiling is the per-fill markout: a maker schedule above it loses on",
        "this tape at that distance, whatever the fill count says. `eligible` applies",
        f"the verdict's own gates ({config.min_samples} samples and "
        f"{config.min_coverage:.0%} coverage); a `no` row is reported and not",
        "decided.",
        "",
        "## Data quality",
        "",
        "`dropped` above counts the fills whose target time landed in a hole in the",
        "timeline. They are excluded, never interpolated, and on a real capture they",
        "are not a random subset: they cluster around reconnects and violent moves.",
        "Read a row's `n` before its mean.",
        "",
        "## What this does not prove",
        "",
        "- Queue position, latency and our own size are not modelled. None is",
        "  neutral: a quote re-priced from a stale mid can be reached by a print",
        "  the idealised quote would have moved away from, so the fill count here",
        "  is not an execution estimate and the surviving fills' markout moves",
        "  with the execution model.",
        "- A distance whose quote would sit inside the touch would compete for the",
        "  front of the queue first; being filled there is not guaranteed.",
        "- One tape is one regime: the curve says what was reachable while this",
        "  capture ran, not what will be reachable tomorrow.",
        "",
    ])
    return lines
