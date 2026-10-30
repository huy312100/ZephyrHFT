"""Unit tests for the markout analyzer.

Each test pins one of the claims the report makes: the sign convention, the
refusal to extrapolate into a data hole, the split between spread capture and
adverse selection, and the fact that a simulated fill can only be marked on the
engine's own clock.
"""
from __future__ import annotations

import importlib.util
import json
import math
from pathlib import Path

import pytest

from python_analytics.backtesting.markouts import (
    MarkoutConfig,
    MarkoutFill,
    MarkoutReplayer,
    MarkoutTimeline,
    TopOfBook,
    build_timeline,
    build_timeline_from_files,
    horizon_label,
    load_fills,
    markout_verdict,
    parse_fill_payload,
    render_markdown,
    resolve_markouts,
    side_name,
    side_sign,
    summarize_markouts,
    to_nanos,
)
from python_analytics.backtesting.replay import BulkReplayer
from python_analytics.backtesting.walk_forward import LockedParameters
from python_analytics.simulator.paper_engine import synthetic_depth_messages

SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "run_markout.py"

SECOND = 1_000_000_000
BASE_MS = 1_700_000_000_000
BASE_NS = BASE_MS * 1_000_000


def _timeline(samples, symbol: str = "BTCUSDT") -> MarkoutTimeline:
    """Timeline from ``(seconds_after_base, mid)`` pairs."""
    timeline = MarkoutTimeline(symbol=symbol)
    for offset_s, mid in samples:
        timeline.add(BASE_NS + int(offset_s * SECOND), mid, spread_bps=1.0)
    return timeline


def _fill(offset_s: float, side: str, price: float, quantity: float = 1.0,
          is_maker: bool = True) -> MarkoutFill:
    return MarkoutFill(ts_ns=BASE_NS + int(offset_s * SECOND), side=side,
                       price=price, quantity=quantity, is_maker=is_maker)


def _flat_rows(values, *, side: str = "BUY", price: float = 100.0,
               is_maker: bool = True):
    """Rows whose primary-horizon markout is exactly ``values``."""
    config = MarkoutConfig(horizons_s=(0.0, 1.0), min_samples=2)
    rows = []
    for index, value in enumerate(values):
        target = price * (1.0 + value / 10_000.0)
        mid = target if side == "BUY" else price * (1.0 - value / 10_000.0)
        timeline = _timeline([(0.0, price), (1.0, mid)])
        rows.extend(resolve_markouts([_fill(0.0, side, price, is_maker=is_maker)],
                                     timeline, config))
    return rows, config


# --------------------------------------------------------------------------- #
# Clocks
# --------------------------------------------------------------------------- #
def test_to_nanos_distinguishes_milliseconds_from_nanoseconds() -> None:
    assert to_nanos(BASE_MS) == BASE_NS
    # Exact: 1.7e18 is outside float64's integer range, so this must not round.
    assert to_nanos(BASE_NS + 10) == BASE_NS + 10
    assert to_nanos(str(BASE_NS + 10)) == BASE_NS + 10
    assert horizon_label(1.0) == "1s"
    assert horizon_label(0.0) == "0s"
    with pytest.raises(ValueError):
        to_nanos(-5)


def test_side_helpers_accept_native_spellings_and_refuse_guesses() -> None:
    assert side_sign("Side.BUY") == 1.0
    assert side_sign("sell") == -1.0
    assert side_name("Side.SELL") == "SELL"
    with pytest.raises(ValueError):
        side_sign("long")
    # A bare 1 is ambiguous between "buy" and "side enum 1"; refuse it.
    with pytest.raises(ValueError):
        side_sign(1)


# --------------------------------------------------------------------------- #
# Timeline and ladder
# --------------------------------------------------------------------------- #
def test_timeline_lookups_are_inclusive_and_bounded() -> None:
    timeline = _timeline([(0.0, 100.0), (1.0, 101.0), (2.0, 102.0)])

    assert timeline.at_or_before(BASE_NS) == (BASE_NS, 100.0, 1.0)
    assert timeline.at_or_before(BASE_NS + SECOND // 2)[1] == 100.0
    assert timeline.at_or_after(BASE_NS + SECOND // 2)[1] == 101.0
    assert timeline.at_or_before(BASE_NS - 1) is None
    assert timeline.at_or_after(BASE_NS + 5 * SECOND) is None
    assert len(timeline) == 3
    assert timeline.first_ts_ns == BASE_NS
    assert timeline.span_s == pytest.approx(2.0)
    assert timeline.max_gap_s == pytest.approx(1.0)
    assert not timeline.is_empty


def test_timeline_rejects_out_of_order_samples_and_collapses_duplicates() -> None:
    timeline = _timeline([(0.0, 100.0)])
    timeline.add(BASE_NS, 100.5)
    assert len(timeline) == 1 and timeline.at_or_before(BASE_NS)[1] == 100.5

    with pytest.raises(ValueError, match="chronological"):
        timeline.add(BASE_NS - 1, 99.0)

    assert timeline.add(BASE_NS + SECOND, 0.0) is False  # unusable mid ignored


def test_top_of_book_applies_deltas_and_zero_removes_a_level() -> None:
    book = TopOfBook()
    book.apply("BUY", [["100.0", "1"], ["99.5", "2"]])
    book.apply("SELL", [["101.0", "1"], ["101.5", "2"]])
    assert (book.best_bid, book.best_ask) == (100.0, 101.0)
    assert book.mid == pytest.approx(100.5)
    assert book.spread_bps == pytest.approx(1.0 / 100.5 * 10_000.0)

    book.apply("BUY", [["100.0", "0"]])
    assert book.best_bid == 99.5

    book.set([["90.0", "1"]], [["91.0", "1"]])
    assert book.mid == pytest.approx(90.5)


def test_build_timeline_follows_depth_deltas_like_the_engine_does() -> None:
    lines = [
        json.dumps({"e": "depthUpdate", "E": BASE_MS, "s": "BTCUSDT",
                    "b": [["100.0", "1"]], "a": [["101.0", "1"]]}),
        json.dumps({"e": "depthUpdate", "E": BASE_MS + 100, "s": "BTCUSDT",
                    "b": [["100.5", "1"], ["100.0", "0"]],
                    "a": [["100.8", "1"], ["101.0", "0"]]}),
    ]
    timeline = build_timeline(lines)

    assert len(timeline) == 2
    assert timeline.at_or_before(BASE_NS)[1] == pytest.approx(100.5)
    assert timeline.at_or_before(BASE_NS + 100 * 1_000_000)[1] == pytest.approx(100.65)
    assert timeline.symbol == "BTCUSDT"


def test_build_timeline_handles_snapshots_and_counts_junk() -> None:
    lines = [
        "{not json",
        json.dumps({"e": "trade", "E": BASE_MS, "s": "BTCUSDT", "p": "100"}),
        json.dumps({"lastUpdateId": 5, "E": BASE_MS + 1,
                    "bids": [["200.0", "1"]], "asks": [["202.0", "1"]]}),
        json.dumps({"e": "bookTicker", "E": BASE_MS + 2, "s": "BTCUSDT",
                    "b": "199.0", "a": "201.0"}),
    ]
    timeline = build_timeline(lines)

    assert timeline.malformed == 1     # the broken line
    assert timeline.ignored == 1       # the trade, which carries no mid
    assert timeline.at_or_before(BASE_NS + 1_000_000)[1] == pytest.approx(201.0)
    # The ladder is authoritative once it exists, so the ticker cannot drag it.
    assert timeline.at_or_before(BASE_NS + 2_000_000)[1] == pytest.approx(201.0)


def test_build_timeline_uses_a_book_ticker_when_there_is_no_ladder() -> None:
    lines = [json.dumps({"e": "bookTicker", "E": BASE_MS, "s": "BTCUSDT",
                         "b": "199.0", "a": "201.0"})]
    timeline = build_timeline(lines)

    assert len(timeline) == 1
    assert timeline.at_or_before(BASE_NS)[1] == pytest.approx(200.0)
    assert timeline.at_or_before(BASE_NS)[2] == pytest.approx(100.0)


def test_build_timeline_never_blends_two_symbols() -> None:
    lines = [
        json.dumps({"e": "depthUpdate", "E": BASE_MS, "s": "BTCUSDT",
                    "b": [["100.0", "1"]], "a": [["101.0", "1"]]}),
        json.dumps({"e": "depthUpdate", "E": BASE_MS + 1, "s": "ETHUSDT",
                    "b": [["10.0", "1"]], "a": [["11.0", "1"]]}),
    ]
    timeline = build_timeline(lines)

    assert len(timeline) == 1
    assert timeline.ignored == 1
    assert timeline.symbol == "BTCUSDT"


def test_a_typed_trade_frame_is_not_mistaken_for_a_quote() -> None:
    # A spot trade carries buyer/seller *order ids* in `b`/`a`; reading those as
    # a bid/ask would fabricate a mid far away from the price.
    lines = [
        json.dumps({"e": "trade", "E": BASE_MS, "s": "BTCUSDT", "p": "100.0",
                    "q": "1", "b": 123456789, "a": 987654321}),
    ]
    timeline = build_timeline(lines)

    assert len(timeline) == 0
    assert timeline.malformed == 0
    assert timeline.ignored == 1


def test_explicit_nanosecond_timestamps_survive_build_timeline_exactly() -> None:
    lines = [json.dumps({"e": "depthUpdate", "ts_ns": BASE_NS + 7, "s": "BTCUSDT",
                         "b": [["100.0", "1"]], "a": [["101.0", "1"]]})]

    timeline = build_timeline(lines)

    assert timeline.first_ts_ns == BASE_NS + 7


def test_build_timeline_from_files_merges_captures_and_checks_the_market(
        tmp_path: Path) -> None:
    first = tmp_path / "one.ndjson"
    second = tmp_path / "two.gz"
    first.write_text(json.dumps({"e": "depthUpdate", "E": BASE_MS, "s": "BTCUSDT",
                                 "b": [["100.0", "1"]], "a": [["101.0", "1"]]}) + "\n",
                     encoding="utf-8")
    import gzip
    with gzip.open(second, "wt", encoding="utf-8") as handle:
        handle.write(json.dumps({"e": "depthUpdate", "E": BASE_MS + 10,
                                 "s": "BTCUSDT", "b": [["100.2", "1"]],
                                 "a": [["100.9", "1"]]}) + "\n")

    timeline = build_timeline_from_files([first, second])

    assert len(timeline) == 2
    assert timeline.at_or_before(BASE_NS + 10 * 1_000_000)[1] == pytest.approx(100.55)

    other = tmp_path / "three.ndjson"
    other.write_text(json.dumps({"e": "depthUpdate", "E": BASE_MS + 20, "s": "ETHUSDT",
                                 "b": [["10.0", "1"]], "a": [["11.0", "1"]]}) + "\n",
                     encoding="utf-8")
    with pytest.raises(ValueError, match="ETHUSDT"):
        build_timeline_from_files([first, other])


def test_build_timeline_from_files_carries_the_ladder_across_the_boundary(
        tmp_path: Path) -> None:
    first = tmp_path / "before.ndjson"
    second = tmp_path / "after.ndjson"
    first.write_text(json.dumps({"e": "depthUpdate", "E": BASE_MS, "s": "BTCUSDT",
                                 "b": [["100.0", "1"]], "a": [["101.0", "1"]]}) + "\n",
                     encoding="utf-8")
    # Rotation splits a capture by size, not by market event: this delta only
    # removes a level created in the first file. A fresh ladder per file would
    # keep the delete as a no-op, leave no ask, and drop the sample entirely.
    second.write_text(json.dumps({"e": "depthUpdate", "E": BASE_MS + 1_000,
                                  "s": "BTCUSDT", "b": [["100.0", "0"], ["99.8", "1"]],
                                  "a": []}) + "\n", encoding="utf-8")

    timeline = build_timeline_from_files([first, second])

    assert len(timeline) == 2
    assert timeline.ignored == 0
    assert timeline.at_or_before(BASE_NS + 1_000 * 1_000_000)[1] == pytest.approx(100.4)


# --------------------------------------------------------------------------- #
# Resolution
# --------------------------------------------------------------------------- #
def test_markout_is_signed_so_that_both_sides_earn_a_positive_number() -> None:
    config = MarkoutConfig(horizons_s=(0.0, 1.0))
    timeline = _timeline([(0.0, 101.0), (1.0, 101.0)])

    buy = resolve_markouts([_fill(0.0, "BUY", 100.0)], timeline, config)[0]
    sell = resolve_markouts([_fill(0.0, "SELL", 102.0)], timeline, config)[0]

    assert buy.markouts_bps["1s"] == pytest.approx(100.0)   # mid 1% above the fill
    # The sell is measured against its own fill price, so 101 vs 102 is 98 bps.
    assert sell.markouts_bps["1s"] == pytest.approx(98.0392156862745)
    assert buy.entry_offset_bps == pytest.approx(buy.markouts_bps["0s"])
    assert sell.entry_offset_bps == pytest.approx(sell.markouts_bps["0s"])


def test_markout_punishes_the_side_the_market_runs_away_from() -> None:
    config = MarkoutConfig(horizons_s=(0.0, 1.0))
    timeline = _timeline([(0.0, 100.0), (1.0, 99.0)])

    buy = resolve_markouts([_fill(0.0, "BUY", 100.0)], timeline, config)[0]
    sell = resolve_markouts([_fill(0.0, "SELL", 100.0)], timeline, config)[0]

    assert buy.markouts_bps["1s"] == pytest.approx(-100.0)
    assert sell.markouts_bps["1s"] == pytest.approx(100.0)


def test_horizon_uses_the_first_sample_at_or_after_the_target() -> None:
    config = MarkoutConfig(horizons_s=(0.0, 1.0), tolerance_s=2.0)
    timeline = _timeline([(0.0, 100.0), (1.5, 101.0)])

    row = resolve_markouts([_fill(0.0, "BUY", 100.0)], timeline, config)[0]

    assert row.markouts_bps["1s"] == pytest.approx(100.0)
    assert row.staleness_ms["1s"] == pytest.approx(500.0)
    assert row.truncated == ()


def test_a_horizon_past_the_tolerance_is_truncated_not_extrapolated() -> None:
    config = MarkoutConfig(horizons_s=(0.0, 1.0), tolerance_s=0.5)
    timeline = _timeline([(0.0, 100.0), (5.0, 101.0)])

    row = resolve_markouts([_fill(0.0, "BUY", 100.0)], timeline, config)[0]

    # 4 s of missing data is a hole, not a 1 s markout.
    assert row.markouts_bps["1s"] is None
    assert row.truncated == ("1s",)
    assert row.markouts_bps["0s"] == pytest.approx(0.0)


def test_a_fill_outside_the_timeline_is_unmatched_rather_than_zero() -> None:
    config = MarkoutConfig(horizons_s=(0.0, 1.0), tolerance_s=1.0)
    timeline = _timeline([(10.0, 100.0), (19.4, 100.5), (19.9, 101.0)])

    early = resolve_markouts([_fill(0.0, "BUY", 100.0)], timeline, config)[0]
    late = resolve_markouts([_fill(19.5, "BUY", 100.0)], timeline, config)[0]

    assert early.matched is False and early.truncated == ("0s", "1s")
    # The fill is inside the timeline, but the 1 s target runs past its end.
    assert late.matched is True
    assert late.markouts_bps["0s"] == pytest.approx(50.0)
    assert late.markouts_bps["1s"] is None and late.truncated == ("1s",)


def test_row_as_dict_flattens_every_horizon() -> None:
    config = MarkoutConfig(horizons_s=(0.0, 1.0))
    timeline = _timeline([(0.0, 100.0), (1.0, 100.5)])
    row = resolve_markouts([_fill(0.0, "BUY", 100.0, quantity=2.0)], timeline, config)[0]

    payload = row.as_dict()

    assert payload["markout_0s_bps"] == pytest.approx(0.0)
    assert payload["markout_1s_bps"] == pytest.approx(50.0)
    assert payload["truncated"] == ""
    assert payload["is_maker"] is True
    json.dumps(payload)  # must stay JSON-serialisable


# --------------------------------------------------------------------------- #
# Aggregation
# --------------------------------------------------------------------------- #
def test_summary_splits_maker_taker_and_side() -> None:
    config = MarkoutConfig(horizons_s=(0.0, 1.0), min_samples=1)
    rows: list = []
    for side in ("BUY", "SELL"):
        rows.extend(_flat_rows([3.0, 3.0], side=side, is_maker=True)[0])
    rows.extend(_flat_rows([-1.0], side="BUY", is_maker=False)[0])
    rows.append(resolve_markouts([_fill(0.0, "BUY", 100.0)],
                                 _timeline([(90.0, 100.0)]), config)[0])

    summary = summarize_markouts(rows, config)

    assert summary["fills"] == {"total": 6, "matched": 5, "unmatched": 1,
                                "maker": 4, "taker": 1,
                                "truncated": {"0s": 1, "1s": 1}}
    assert summary["maker"]["fills"] == 4
    assert summary["buckets"]["maker_buy"]["fills"] == 2
    assert summary["buckets"]["maker_sell"]["fills"] == 2
    assert summary["buckets"]["taker_buy"]["fills"] == 1
    assert summary["maker"]["horizons"]["1s"]["samples"] == 4


def test_summary_reports_mean_median_dispersion_and_t_statistic() -> None:
    config = MarkoutConfig(horizons_s=(0.0, 1.0), min_samples=1)
    rows, _ = _flat_rows([1.0, 2.0, 3.0, 4.0])

    stats = summarize_markouts(rows, config)["maker"]["horizons"]["1s"]

    assert stats["samples"] == 4
    assert stats["mean_bps"] == pytest.approx(2.5)
    assert stats["median_bps"] == pytest.approx(2.5)
    assert stats["stdev_bps"] == pytest.approx(math.sqrt(5.0 / 3.0))
    assert stats["t_stat"] == pytest.approx(2.5 / (math.sqrt(5.0 / 3.0) / 2.0))
    assert stats["win_rate_pct"] == pytest.approx(100.0)
    assert stats["min_bps"] == pytest.approx(1.0)
    assert stats["max_bps"] == pytest.approx(4.0)


def test_summary_weights_by_notional_and_translates_to_usdt() -> None:
    config = MarkoutConfig(horizons_s=(0.0, 1.0), min_samples=1)
    rows: list = []
    for value, quantity in ((0.0, 1.0), (4.0, 3.0)):
        timeline = _timeline([(0.0, 100.0), (1.0, 100.0 * (1.0 + value / 10_000.0))])
        rows.extend(resolve_markouts([_fill(0.0, "BUY", 100.0, quantity)],
                                     timeline, config))

    stats = summarize_markouts(rows, config)["maker"]["horizons"]["1s"]

    assert stats["mean_bps"] == pytest.approx(2.0)
    assert stats["notional_weighted_bps"] == pytest.approx(3.0)
    assert stats["usdt_per_fill"] == pytest.approx(2.0 / 10_000.0 * 200.0)
    assert stats["total_usdt"] == pytest.approx(4.0 * 300.0 / 10_000.0)


# --------------------------------------------------------------------------- #
# Verdict
# --------------------------------------------------------------------------- #
def _summary(values, *, fee_bps: float = 2.0, min_samples: int = 3) -> tuple:
    config = MarkoutConfig(horizons_s=(0.0, 1.0), min_samples=min_samples,
                           maker_bps=fee_bps)
    rows, _ = _flat_rows(values)
    return summarize_markouts(rows, config), config


def test_verdict_go_when_the_edge_survives_the_fee_with_evidence() -> None:
    summary, config = _summary([2.5, 3.5, 3.0, 3.2, 2.8])

    verdict = markout_verdict(summary, config)

    assert verdict["verdict"] == "GO"
    assert verdict["net_edge_bps"] == pytest.approx(1.0)
    assert verdict["samples"] == 5
    # The 0 s sample sits at the mid, so the drift is the whole captured 3 bps.
    assert verdict["decay_bps"] == pytest.approx(3.0)


def test_verdict_costs_the_fee_against_the_markout() -> None:
    summary, config = _summary([0.8, 1.2, 1.0, 1.1, 0.9])

    verdict = markout_verdict(summary, config)

    assert verdict["verdict"] == "COSTLY"
    assert verdict["net_edge_bps"] == pytest.approx(-1.0)
    assert "maker fee" in verdict["reasons"][0]


def test_verdict_names_adverse_selection_when_fills_are_under_water() -> None:
    summary, config = _summary([-1.5, -2.5, -2.0, -2.2, -1.8])

    verdict = markout_verdict(summary, config)

    assert verdict["verdict"] == "ADVERSE_SELECTION"
    assert verdict["mean_markout_bps"] == pytest.approx(-2.0)
    assert verdict["t_stat"] < -1.96


def test_verdict_is_inconclusive_below_the_sample_floor() -> None:
    summary, config = _summary([5.0, 5.5], min_samples=30)

    verdict = markout_verdict(summary, config)

    assert verdict["verdict"] == "INCONCLUSIVE"
    assert "30 are required" in verdict["reasons"][0]


def test_verdict_is_marginal_when_positive_but_noisy() -> None:
    summary, config = _summary([0.0, 5.0, 0.0, 5.0], min_samples=3)

    verdict = markout_verdict(summary, config)

    assert verdict["verdict"] == "MARGINAL"
    assert verdict["net_edge_bps"] > 0.0
    assert abs(verdict["t_stat"]) < 1.96


def test_verdict_reports_the_decay_between_horizons() -> None:
    config = MarkoutConfig(horizons_s=(0.0, 1.0), min_samples=1, maker_bps=0.0)
    rows = []
    for _ in range(3):
        timeline = _timeline([(0.0, 100.05), (1.0, 99.95)])
        rows.extend(resolve_markouts([_fill(0.0, "BUY", 100.0)], timeline, config))

    verdict = markout_verdict(summarize_markouts(rows, config), config)

    assert verdict["decay_bps"] == pytest.approx(-10.0)
    assert verdict["decay_samples"] == 3
    assert any("moves" in reason for reason in verdict["reasons"])


def test_decay_compares_the_same_fills_not_two_populations() -> None:
    config = MarkoutConfig(horizons_s=(0.0, 1.0), min_samples=1, maker_bps=0.0)
    # One fill has a 1 s sample (+5 bps -> -5 bps); the other is truncated there
    # and sits exactly at the mid. Averaging each horizon separately would give
    # ((5 + 0) / 2) - (-5) = -7.5 bps, which no fill experienced.
    rows = resolve_markouts([_fill(0.0, "BUY", 100.0)],
                            _timeline([(0.0, 100.05), (1.0, 99.95)]), config)
    rows.extend(resolve_markouts([_fill(0.0, "BUY", 100.0)],
                                 _timeline([(0.0, 100.0)]), config))
    summary = summarize_markouts(rows, config)

    assert summary["maker"]["horizons"]["1s"]["samples"] == 1
    assert summary["maker"]["decay"]["samples"] == 1
    assert summary["maker"]["decay"]["bps"] == pytest.approx(-10.0)
    verdict = markout_verdict(summary, config)
    assert verdict["decay_bps"] == pytest.approx(-10.0)
    assert verdict["decay_samples"] == 1


def test_a_fill_from_another_market_is_refused() -> None:
    timeline = _timeline([(0.0, 100.0)], symbol="BTCUSDT")
    fill = MarkoutFill(ts_ns=BASE_NS, side="BUY", price=100.0, quantity=1.0,
                       symbol="ETHUSDT")

    with pytest.raises(ValueError, match="ETHUSDT"):
        resolve_markouts([fill], timeline, MarkoutConfig())


def test_parse_fill_payload_keeps_the_venue_symbol() -> None:
    fills = parse_fill_payload([{"symbol": "btcusdt", "timestamp": BASE_MS,
                                 "side": "buy", "price": 1.0, "quantity": 1.0}])

    assert fills[0].symbol == "BTCUSDT"


def test_verdict_flags_a_degenerate_sample_instead_of_trusting_the_t_stat() -> None:
    summary, config = _summary([3.0, 3.0, 3.0, 3.0])

    verdict = markout_verdict(summary, config)

    assert any("σ=" in reason for reason in verdict["reasons"])
    assert verdict["verdict"] == "INCONCLUSIVE" or verdict["verdict"] == "MARGINAL"


# --------------------------------------------------------------------------- #
# Fill payloads
# --------------------------------------------------------------------------- #
def test_parse_fill_payload_accepts_a_summary_shaped_mapping() -> None:
    payload = {"fills": [
        {"timestamp": BASE_MS, "side": "Side.BUY", "price": "30000.5",
         "quantity": "0.01", "is_maker": 1},
        {"timestamp_ms": BASE_MS + 5, "side": "sell", "price": 30001.0,
         "quantity": 0.02, "is_maker": False},
        {"ts_ns": BASE_NS + 10, "side": "BUY", "price": 30000.0, "quantity": 1},
    ]}

    fills = parse_fill_payload(payload)

    assert [fill.side for fill in fills] == ["BUY", "SELL", "BUY"]
    assert [fill.is_maker for fill in fills] == [True, False, True]
    assert fills[0].ts_ns == BASE_NS
    assert fills[2].ts_ns == BASE_NS + 10
    assert fills[1].quantity == pytest.approx(0.02)


def test_parse_fill_payload_refuses_rows_it_cannot_sign_or_time() -> None:
    with pytest.raises(ValueError, match="timestamp"):
        parse_fill_payload([{"side": "BUY", "price": 1.0, "quantity": 1.0}])
    with pytest.raises(ValueError, match="price"):
        parse_fill_payload([{"ts_ns": BASE_NS, "side": "BUY", "price": 0.0,
                             "quantity": 1.0}])
    with pytest.raises(ValueError, match="side"):
        parse_fill_payload([{"ts_ns": BASE_NS, "side": "long", "price": 1.0,
                             "quantity": 1.0}])
    # A missing side must be refused, never defaulted to BUY: the default would
    # report a profitable buy markout for what was actually a sell.
    with pytest.raises(ValueError, match="side"):
        parse_fill_payload([{"timestamp": BASE_MS, "price": 1.0, "quantity": 1.0}])
    with pytest.raises(ValueError, match="list"):
        parse_fill_payload({"fills": "nope"})


def test_parse_fill_payload_reads_the_venue_qty_and_maker_aliases() -> None:
    # Binance's myTrades export names the size ``qty`` and the passive flag
    # ``maker``; reading only the snake_case names would misclassify the fill.
    fills = parse_fill_payload([
        {"time": BASE_MS, "side": "SELL", "price": "100.0", "qty": "0.25",
         "maker": False},
        {"time": BASE_MS + 5, "side": "BUY", "price": "100.0", "qty": 0.5,
         "maker": True},
    ])

    assert [fill.quantity for fill in fills] == [pytest.approx(0.25), pytest.approx(0.5)]
    assert [fill.is_maker for fill in fills] == [False, True]
    assert fills[0].ts_ns == BASE_NS


def test_load_fills_reads_a_file_with_an_explicit_nanosecond_clock(
        tmp_path: Path) -> None:
    path = tmp_path / "fills.json"
    path.write_text(json.dumps([
        {"timestamp": BASE_NS, "side": "SELL", "price": 100.0, "quantity": 1.0},
    ]), encoding="utf-8")

    fills = load_fills(path, clock="ns")

    assert len(fills) == 1
    assert fills[0].ts_ns == BASE_NS
    assert fills[0].is_maker is True  # default: an unmarked fill is assumed passive


# --------------------------------------------------------------------------- #
# In-loop replay (the only place a simulated fill can be marked)
# --------------------------------------------------------------------------- #
def _synthetic_lines(count: int = 400, seed: int = 7) -> list:
    generator = synthetic_depth_messages(mid=100.0, tick=0.5, seed=seed)
    lines = []
    for index in range(count):
        message = json.loads(next(generator))
        message["E"] = BASE_MS + index * 250
        lines.append(json.dumps(message))
    return lines


def _parameters() -> LockedParameters:
    return LockedParameters(gamma=0.1, sigma=0.02, k=5.0, time_horizon=60.0,
                            inventory_limit=5.0, ofi_sensitivity=0.5, ofi_decay=0.95,
                            ofi_cap=3.0, use_time_spread=True, tick_size="0.5",
                            center="100", market_type="CRYPTO", maker_bps=2.0,
                            taker_bps=5.0, initial_cash=100_000.0,
                            order_quantity="0.01")


def _markout_replayer(**overrides) -> MarkoutReplayer:
    settings = dict(config=MarkoutConfig(), tick_size="0.5", center="100",
                    maker_bps=2.0, taker_bps=5.0, initial_cash=100_000.0,
                    order_quantity="0.01")
    settings.update(overrides)
    return MarkoutReplayer(**settings)


def test_markout_replayer_marks_every_fill_it_produces() -> None:
    replayer = _markout_replayer()
    summary = replayer.replay_lines(_synthetic_lines(), _parameters().build_strategy())

    diagnostics = replayer.diagnostics()
    rows = replayer.markout_rows()

    assert summary.fills, "the fixture must produce fills or this test proves nothing"
    assert diagnostics["fills"] == len(summary.fills) == len(rows)
    assert diagnostics["timeline_samples"] == 400
    assert diagnostics["timeline_span_s"] == pytest.approx(99.75)
    assert diagnostics["frames_per_hook"] == 1
    assert all(row.matched for row in rows)
    assert all(row.markouts_bps["0s"] is not None for row in rows)
    # The engine stamps in milliseconds; the rows must be nanoseconds.
    assert rows[0].fill.ts_ns >= BASE_NS
    assert rows[0].fill.ts_ns % 1_000_000 == 0


def test_markout_replayer_prices_the_last_fills_as_truncated() -> None:
    replayer = _markout_replayer()
    replayer.replay_lines(_synthetic_lines(), _parameters().build_strategy())

    summary = replayer.markout_summary()

    assert summary["fills"]["total"] == summary["fills"]["matched"]
    # A 60 s horizon cannot exist for a fill in the final minute of the capture.
    assert summary["fills"]["truncated"]["60s"] > 0
    assert summary["fills"]["truncated"]["0s"] == 0


def test_markout_replayer_handles_a_synthetic_clock_past_1000_frames() -> None:
    # A timestamp-less archive advances a nanosecond counter that passes 1e11
    # after 1000 frames. Any magnitude-based unit guess would flip to
    # "milliseconds" there and rescale the rest of the replay by 1e6.
    generator = synthetic_depth_messages(mid=100.0, tick=0.5, seed=3)
    lines = []
    for _ in range(1200):
        message = json.loads(next(generator))
        message.pop("E", None)
        lines.append(json.dumps(message))

    replayer = _markout_replayer()
    replayer.replay_lines(lines, _parameters().build_strategy())

    diagnostics = replayer.diagnostics()
    rows = replayer.markout_rows()
    measured = [row for row in rows if row.markouts_bps["1s"] is not None]

    assert diagnostics["timeline_samples"] == 1200
    assert diagnostics["timeline_span_s"] == pytest.approx(119.9)
    assert rows, "the fixture must fill something"
    assert measured, "the synthetic clock was rescaled mid-replay"
    stamps = [row.fill.ts_ns for row in rows]
    assert stamps == sorted(stamps)


def test_markout_replayer_reset_forgets_the_previous_window() -> None:
    replayer = _markout_replayer()
    replayer.replay_lines(_synthetic_lines(), _parameters().build_strategy())
    assert replayer.fill_log

    replayer.reset()

    assert replayer.fill_log == []
    assert len(replayer.timeline) == 0
    assert replayer.diagnostics()["timeline_samples"] == 0


def test_plain_bulk_replayer_still_replays_with_the_new_hook() -> None:
    replayer = BulkReplayer(tick_size="0.5", center="100", maker_bps=2.0,
                            taker_bps=5.0, initial_cash=100_000.0,
                            order_quantity="0.01")

    summary = replayer.replay_lines(_synthetic_lines(), _parameters().build_strategy())

    assert replayer.on_frame(0, []) is None
    assert summary.fills
    assert summary.stats["fills"] == len(summary.fills)


class _StampedFill:
    """The fields ``on_frame`` reads from a native fill."""

    def __init__(self, event_ts: int, price: float = 100.0) -> None:
        self.event_ts = event_ts
        self.timestamp = 0  # local record time: not on the feed's clock
        self.side = "BUY"
        self.price = price
        self.quantity = 0.1
        self.is_maker = 1


def test_a_tape_fill_is_marked_from_the_print_not_the_depth_frame() -> None:
    """The mid behind a tape fill is the one at the print, not the last frame.

    Under the tape model a fill happens *between* depth frames, and the engine
    stamps it with the print's own event time (``match_trade``). Marking it on
    the frame clock moves the fill -- and every horizon measured from it -- onto
    a mid the market had already left.
    """
    replayer = _markout_replayer()
    event_ms = 1_700_000_000_000
    line = ('{"e":"depthUpdate","E":%d,"U":1,"u":1,"s":"BTCUSDT",'
            '"b":[["100.0","1"]],"a":[["100.5","1"]]}' % event_ms)
    replayer.replay_lines([line], None)
    assert int(replayer.engine.bbo().exchange_ts) == event_ms

    # The print that filled us arrived 37 ms before that depth frame.
    print_ms = event_ms - 37
    replayer.on_frame(print_ms * 1_000_000, [_StampedFill(print_ms)])
    assert replayer.fill_log[-1].ts_ns == print_ms * 1_000_000

    # An unstamped fill (no venue clock on this archive) keeps the frame clock.
    replayer.on_frame(0, [_StampedFill(0)])
    assert replayer.fill_log[-1].ts_ns == event_ms * 1_000_000


def test_render_markdown_states_the_verdict_and_its_limits() -> None:
    config = MarkoutConfig(horizons_s=(0.0, 1.0), min_samples=1)
    rows, _ = _flat_rows([3.0, 3.5, 2.5])
    summary = summarize_markouts(rows, config)
    verdict = markout_verdict(summary, config)

    markdown = "\n".join(render_markdown(summary, verdict))

    assert "# Markout report" in markdown
    assert verdict["verdict"] in markdown
    assert "What this does not prove" in markdown
    assert "| 1s | 3 |" in markdown
    # No model was named, so the report does not invent one.
    assert "Fill model" not in markdown


def test_render_markdown_names_the_fill_model_it_was_given() -> None:
    config = MarkoutConfig(horizons_s=(0.0, 1.0), min_samples=1)
    rows, _ = _flat_rows([3.0, 3.5, 2.5])
    summary = summarize_markouts(rows, config)
    verdict = markout_verdict(summary, config)

    markdown = "\n".join(render_markdown(summary, verdict, fill_model="queue"))

    # The verdict is model-dependent, so the report has to say which model it is.
    assert "Fill model: `queue`" in markdown


# --------------------------------------------------------------------------- #
# Config validation
# --------------------------------------------------------------------------- #
def test_config_defaults_the_primary_horizon_to_the_first_positive_one() -> None:
    assert MarkoutConfig().primary_horizon_s == 1.0
    assert MarkoutConfig(horizons_s=(0.0,)).primary_horizon_s == 0.0


def test_config_refuses_settings_that_would_quietly_change_the_answer() -> None:
    with pytest.raises(ValueError, match="at least one horizon"):
        MarkoutConfig(horizons_s=())
    with pytest.raises(ValueError, match="non-negative"):
        MarkoutConfig(horizons_s=(-1.0,))
    with pytest.raises(ValueError, match="tolerance_s"):
        MarkoutConfig(tolerance_s=0.0)
    with pytest.raises(ValueError, match="min_samples"):
        MarkoutConfig(min_samples=0)
    with pytest.raises(ValueError, match="primary_horizon_s"):
        MarkoutConfig(horizons_s=(0.0, 1.0), primary_horizon_s=5.0)


def test_config_output_is_json_safe() -> None:
    payload = MarkoutConfig(horizons_s=(0.0, 1.0)).as_dict()
    json.dumps(payload)
    assert payload["horizons_s"] == [0.0, 1.0]
    assert payload["primary_horizon_s"] == 1.0


# --------------------------------------------------------------------------- #
# CLI argument handling
# --------------------------------------------------------------------------- #
def _load_cli():
    spec = importlib.util.spec_from_file_location("run_markout_cli", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_cli_horizons_are_parsed_and_bad_values_are_usage_errors() -> None:
    cli = _load_cli()

    assert cli.parse_horizons("0, 1,60") == (0.0, 1.0, 60.0)
    for bad in ("inf", "nan", "-1", "x", ""):
        with pytest.raises(ValueError):
            cli.parse_horizons(bad)


def test_cli_rejects_horizons_before_any_replay_runs() -> None:
    cli = _load_cli()

    assert cli.main(["--horizons", "inf", "--no-report"]) == cli.EXIT_USAGE
    assert cli.main(["--horizons", "-1", "--no-report"]) == cli.EXIT_USAGE


def test_the_markout_artefact_records_the_fill_model() -> None:
    cli = _load_cli()
    args = cli.parse_args(["--fill-model", "queue", "--no-report"])

    payload = cli.build_payload(args, _parameters(), MarkoutConfig(horizons_s=(0.0, 1.0)),
                                {}, {}, [])

    # Two runs over the same archive with different models must be tellable apart.
    assert payload["inputs"]["fill_model"] == "queue"


def test_fills_supplied_from_outside_record_no_fill_model(tmp_path) -> None:
    """No simulator ran, so naming one would credit it with the edge."""
    cli = _load_cli()
    fills = tmp_path / "fills.json"
    fills.write_text("[]", encoding="utf-8")
    args = cli.parse_args(["--fills", str(fills), "--no-report"])

    payload = cli.build_payload(args, _parameters(), MarkoutConfig(horizons_s=(0.0, 1.0)),
                                {}, {}, [])

    assert payload["inputs"]["mode"] == "fills"
    assert payload["inputs"]["fill_model"] is None
