"""Unit tests for the edge curve.

The curve exists to answer one question — at what distance from the mid does a
resting quote clear its fee, and how often does the tape reach there — so each
test pins one of the ways that answer could be wrong: a fill booked where the
print never reached, an edge measured from the print instead of the quote, a mid
read from a frame that arrived after the print, an aggressor side read backwards,
or a verdict that clears a distance the sample count cannot support.
"""
from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pytest

from python_analytics.backtesting.edge_curve import (
    EdgeCurveConfig,
    TapePrint,
    edge_curve_verdict,
    measure_edge_curve,
    quote_price,
    render_markdown,
    scan_tape,
)
from python_analytics.backtesting.markouts import MarkoutTimeline, build_timeline

BASE_MS = 1_700_000_000_000
ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts" / "run_edge_curve.py"
SAMPLE = ROOT / "data" / "sample_events.ndjson"


def _load_script():
    """Imports the runner so its exit codes can be asserted in-process."""
    spec = importlib.util.spec_from_file_location("run_edge_curve", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def _snapshot(ts_ms: int, bid: float, ask: float,
              symbol: str = "BTCUSDT") -> str:
    """An absolute book snapshot, the form the timeline builder reads directly."""
    return json.dumps({
        "lastUpdateId": ts_ms,
        "s": symbol,
        "bids": [[f"{bid:.2f}", "1.0"]],
        "asks": [[f"{ask:.2f}", "1.0"]],
        "E": BASE_MS + ts_ms,
    })


def _print(ts_ms: int, price: float, quantity: float = 0.1,
           buyer_is_maker: bool = True, symbol: str = "BTCUSDT") -> str:
    """A trade frame. ``buyer_is_maker`` is the venue's ``m`` flag."""
    return json.dumps({
        "e": "trade",
        "E": BASE_MS + ts_ms,
        "T": BASE_MS + ts_ms,
        "s": symbol,
        "p": f"{price:.3f}",
        "q": f"{quantity:.3f}",
        "m": buyer_is_maker,
    })


def _config(**overrides) -> EdgeCurveConfig:
    defaults = dict(
        distances_bps=(0.5, 1.0, 2.0),
        horizons_s=(0.0, 1.0),
        tolerance_s=2.0,
        maker_bps=2.0,
        tick_size=0.01,
        min_samples=1,
    )
    defaults.update(overrides)
    return EdgeCurveConfig(**defaults)


def _curve(lines, config=None):
    timeline = build_timeline(lines)
    scan = scan_tape(lines)
    config = config or _config()
    return measure_edge_curve(scan, timeline, config), config


def test_a_quote_is_only_filled_by_a_print_that_reaches_it() -> None:
    """Mid 100, one print at 99.99: it reaches 1 bps (99.99), not 2 bps (99.98).

    This is the property the whole curve rests on. If a print filled every
    distance, the curve would report an infinite fill rate and a flat edge at
    every offset, and the fee comparison would be meaningless.
    """
    lines = [
        _snapshot(1_000, 99.99, 100.01),
        _print(1_100, 99.99),
        _snapshot(2_500, 99.99, 100.01),
    ]
    summary, _ = _curve(lines)
    fills = {row["distance_bps"]: row["fills"] for row in summary["rows"]}
    assert fills == {0.5: 1, 1.0: 1, 2.0: 0}


def test_the_markout_is_measured_from_the_quote_and_not_from_the_print() -> None:
    """A print that trades through us fills us at our limit, not at its price.

    The print is at 99.985 and the resting bid at 99.99, so booking the edge from
    the print would claim 0.5 bps the order never captured — the difference
    between "the market traded below me" and "I bought there".
    """
    lines = [
        _snapshot(1_000, 99.99, 100.01),
        _print(1_100, 99.985),
        _snapshot(2_200, 100.01, 100.03),
    ]
    summary, _ = _curve(lines, _config(distances_bps=(1.0,)))
    row = summary["rows"][0]
    expected = (100.02 - 99.99) / 99.99 * 10_000.0
    assert row["horizons"]["1s"]["mean_bps"] == pytest.approx(expected, rel=0, abs=1e-9)
    assert row["horizons"]["1s"]["mean_bps"] != pytest.approx(3.5, rel=0, abs=1e-3)


def test_a_print_is_priced_against_a_mid_that_arrived_before_it() -> None:
    """A depth frame stamped at the print's own millisecond is not the entry mid.

    Both frames carry the same venue millisecond and only their arrival order
    separates them. Pricing the quote from that frame would place it at 89.99
    around a mid of 90, and the print at 99.9 could not reach it — so the fill
    count, not just the markout, is what proves which mid was read.
    """
    lines = [
        _snapshot(1_000, 99.99, 100.01),
        _print(2_000, 99.90),
        _snapshot(2_000, 89.99, 90.01),
    ]
    summary, _ = _curve(lines, _config(distances_bps=(1.0,)))
    assert summary["rows"][0]["fills"] == 1


def test_the_agreeing_side_is_the_one_that_rests() -> None:
    """``m = true`` is a sell aggressor, so a bid is the order that fills.

    Reading the flag backwards would rest the wrong side and invert every edge
    on the curve, so the two prints below must fill disjoint distances.
    """
    down = scan_tape([_print(1_100, 99.99, buyer_is_maker=True)])
    up = scan_tape([_print(1_100, 100.01, buyer_is_maker=False)])
    assert down.prints[0].aggressor == "SELL"
    assert up.prints[0].aggressor == "BUY"

    assert quote_price(100.0, 1.0, buy=True, tick_size=0.01) == pytest.approx(
        99.99, rel=0, abs=1e-9)
    assert quote_price(100.0, 1.0, buy=False, tick_size=0.01) == pytest.approx(
        100.01, rel=0, abs=1e-9)

    lines = [
        _snapshot(1_000, 99.99, 100.01),
        _print(1_100, 99.99, buyer_is_maker=True),
        _print(1_200, 100.01, buyer_is_maker=False),
        _snapshot(2_500, 99.99, 100.01),
    ]
    summary, _ = _curve(lines, _config(distances_bps=(1.0,), horizons_s=(0.0, 1.0)))
    # One print fills the bid at 99.99, the other fills the ask at 100.01.
    assert summary["rows"][0]["fills"] == 2


def test_a_buy_quote_snaps_down_and_a_sell_quote_snaps_up() -> None:
    """Quantisation can only widen: a coarse grid never improves an edge."""
    # ``9999 * 0.01`` is not exactly 99.99 in binary, so the snapped price is
    # compared at an absolute tolerance far below a tick.
    assert quote_price(100.0, 0.05, buy=True, tick_size=0.01) == pytest.approx(
        99.99, rel=0, abs=1e-9)
    assert quote_price(100.0, 0.05, buy=False, tick_size=0.01) == pytest.approx(
        100.01, rel=0, abs=1e-9)
    # Without a grid the exact distance is used.
    assert quote_price(100.0, 1.0, buy=True) == pytest.approx(99.99, rel=0, abs=1e-12)


def test_the_verdict_clears_the_first_distance_the_tape_pays_for() -> None:
    """A mid that runs away from the quote clears the fee at every reached distance."""
    lines = [
        _snapshot(1_000, 99.99, 100.01),
        _print(1_100, 99.99),
        _snapshot(2_200, 101.00, 101.02),
    ]
    summary, config = _curve(lines, _config(min_samples=1))
    verdict = edge_curve_verdict(summary, config)
    assert verdict["verdict"] == "CLEARS"
    assert verdict["distance_bps"] == 0.5
    assert verdict["net_bps"] > 0.0

    markdown = "\n".join(render_markdown(summary, verdict))
    assert "**CLEARS**" in markdown
    assert "What this does not prove" in markdown


def test_the_verdict_refuses_a_distance_the_fee_eats() -> None:
    """A mid that drifts against the quote leaves nothing to pay the fee with."""
    lines = [
        _snapshot(1_000, 99.99, 100.01),
        _print(1_100, 99.99),
        _snapshot(2_200, 99.50, 99.52),
    ]
    summary, config = _curve(lines, _config(min_samples=1))
    verdict = edge_curve_verdict(summary, config)
    assert verdict["verdict"] == "NO_CLEARING_DISTANCE"
    assert verdict["distance_bps"] is None
    assert any("none cleared" in reason and "covered distance" in reason
               for reason in verdict["reasons"])


def test_a_distance_needs_its_sample_count_before_it_can_clear() -> None:
    """One lucky fill is not a distance that clears: the bar is a sample count."""
    lines = [
        _snapshot(1_000, 99.99, 100.01),
        _print(1_100, 99.99),
        _snapshot(2_200, 101.00, 101.02),
    ]
    summary, config = _curve(lines, _config(min_samples=30))
    verdict = edge_curve_verdict(summary, config)
    assert verdict["verdict"] == "NO_CLEARING_DISTANCE"
    assert any("produced the 30 fills" in reason for reason in verdict["reasons"])


def test_fill_rate_is_reported_per_minute_of_tape() -> None:
    """Two fills 60 s apart on a two-minute tape is one fill per minute."""
    lines = [
        _snapshot(1_000, 99.99, 100.01),
        _print(1_100, 99.99),
        _print(61_100, 99.99),
        _snapshot(121_100, 99.99, 100.01),
    ]
    summary, _ = _curve(lines, _config(distances_bps=(1.0,), horizons_s=(0.0, 1.0)))
    row = summary["rows"][0]
    assert row["fills"] == 2
    assert row["fills_per_min"] == pytest.approx(2.0, rel=1e-6)


def test_prints_without_a_side_flag_or_a_usable_price_are_refused() -> None:
    """A frame without ``m`` cannot name an aggressor, so it is counted, not guessed."""
    scan = scan_tape([
        json.dumps({"e": "trade", "E": BASE_MS, "T": BASE_MS, "s": "BTCUSDT",
                    "p": "100.0", "q": "1.0"}),
        json.dumps({"e": "trade", "E": BASE_MS, "T": BASE_MS, "s": "BTCUSDT",
                    "p": "0", "q": "1.0", "m": True}),
        json.dumps({"e": "aggTrade", "E": BASE_MS, "T": BASE_MS, "s": "BTCUSDT",
                    "p": "100.0", "q": "1.0", "m": False}),
        "not json",
    ])
    assert [print_.aggressor for print_ in scan.prints] == ["BUY"]
    assert scan.malformed == 3


def test_prints_for_another_market_are_never_blended_in() -> None:
    """An ETH print marked against a BTC mid is a fabricated number."""
    scan = scan_tape([
        _print(1_100, 99.99),
        json.dumps({"e": "trade", "E": BASE_MS + 1_200, "T": BASE_MS + 1_200,
                    "s": "ETHUSDT", "p": "3000.0", "q": "1.0", "m": True}),
    ])
    assert len(scan.prints) == 1
    assert scan.ignored == 1


def test_a_quote_that_cannot_be_priced_is_dropped_not_guessed() -> None:
    """A distance wider than the mid would give a negative price; that is ``None``."""
    assert quote_price(0.5, 20_000.0, buy=True) is None
    with pytest.raises(ValueError):
        EdgeCurveConfig(distances_bps=(0.0,))
    with pytest.raises(ValueError):
        EdgeCurveConfig(primary_horizon_s=42.0)


def test_the_verdict_is_never_decided_at_the_quote_offset() -> None:
    """``h = 0`` earns the distance it was posted at, so it cannot be an edge.

    A primary horizon of ``0`` makes net edge ``distance - fee``, so every grid
    distance above the fee would clear and the runner would exit 0 on a GO that
    measures the grid rather than the market. Both routes to it are refused: a
    horizon list with nothing positive, and an explicit ``0``.
    """
    with pytest.raises(ValueError):
        EdgeCurveConfig(horizons_s=(0.0,))
    with pytest.raises(ValueError):
        EdgeCurveConfig(horizons_s=(0.0, 1.0), primary_horizon_s=0.0)


def test_an_empty_tape_is_reported_as_no_data() -> None:
    """No priced print is not a verdict about the fee, and must not read as one."""
    timeline = MarkoutTimeline()
    summary = measure_edge_curve(scan_tape([]), timeline, _config())
    verdict = edge_curve_verdict(summary, _config())
    assert verdict["verdict"] == "NO_DATA"
    assert verdict["distance_bps"] is None


def test_a_fill_with_no_mid_after_it_is_dropped_and_counted() -> None:
    """A hole in the timeline must shrink the sample visibly, never silently.

    On a real capture the holes are not random — they cluster around reconnects
    and violent moves, so a row that quietly reported the surviving fills would
    report the calm ones and hide the episode the maker would actually have been
    filled in.
    """
    lines = [
        _snapshot(1_000, 99.99, 100.01),
        _print(1_100, 99.99),
        # Nothing for the next five seconds: the 1 s target has no sample
        # inside the 2 s tolerance.
        _snapshot(6_100, 99.99, 100.01),
    ]
    summary, _ = _curve(lines, _config(distances_bps=(0.5,), horizons_s=(0.0, 1.0)))
    row = summary["rows"][0]
    assert row["fills"] == 1
    assert row["horizons"]["0s"]["samples"] == 1
    assert row["horizons"]["1s"]["samples"] == 0
    assert row["horizons"]["1s"]["truncated"] == 1

    markdown = "\n".join(render_markdown(summary, edge_curve_verdict(summary, _config(
        distances_bps=(0.5,), horizons_s=(0.0, 1.0)))))
    assert "dropped" in markdown


def test_two_horizons_that_share_a_report_key_are_refused() -> None:
    """``1`` and ``1.0000001`` both print as ``1s``, so their marks would pool.

    Pooled samples would let fifteen fills satisfy a 30-fill bar and clear a
    distance on half the evidence the report claims, so the pairing is refused
    before anything is measured.
    """
    with pytest.raises(ValueError):
        EdgeCurveConfig(horizons_s=(1.0, 1.0000001))
    with pytest.raises(ValueError):
        # ``%g`` keeps six significant digits, so these two also print as ``2s``.
        EdgeCurveConfig(horizons_s=(0.0, 2.0, 2.0000001))


def test_the_runner_refuses_an_unparseable_list() -> None:
    """``--horizons foo`` is a usage error, not a traceback and not a "no"."""
    script = _load_script()
    assert script.main(["--source", str(SAMPLE), "--horizons", "foo",
                        "--no-report"]) == 2
    assert script.main(["--source", str(SAMPLE), "--distances-bps", "1,,x",
                        "--no-report"]) == 2


def test_the_runner_refuses_a_corrupt_capture(tmp_path) -> None:
    """A truncated or corrupt roll is an input defect, not a measured "no".

    ``EOFError``, ``zlib.error`` and the ``OSError`` family all mean the same
    thing to an operator: the tape could not be read. Python's default exit for
    an uncaught one is ``1`` — this runner's "no distance clears" — so all of
    them are mapped to the usage code.
    """
    import gzip

    script = _load_script()

    truncated = tmp_path / "truncated.ndjson.gz"
    with gzip.open(truncated, "wt", encoding="utf-8") as handle:
        handle.write(_snapshot(5_000, 99.99, 100.01) + "\n")
    body = truncated.read_bytes()
    truncated.write_bytes(body[: len(body) - 8])
    assert script.main(["--source", str(truncated), "--no-report"]) == 2

    corrupt = tmp_path / "corrupt.ndjson.gz"
    corrupt.write_bytes(b"not a gzip file at all")
    assert script.main(["--source", str(corrupt), "--no-report"]) == 2


def test_the_scan_names_the_market_it_was_asked_for() -> None:
    """A leading print from another market must not label the study.

    ``scan_tape`` used to take ``symbol`` from the first trade it read, so asking
    a BTCUSDT capture for BTCUSDT while an ETHUSDT print came first reported
    ETHUSDT as the population of a BTCUSDT study.
    """
    scan = scan_tape([
        _print(1_100, 3_000.0, symbol="ETHUSDT"),
        _print(1_200, 99.99),
    ], symbol="BTCUSDT")
    assert scan.symbol == "BTCUSDT"
    assert len(scan.prints) == 1
    assert scan.ignored == 1


def test_the_runner_prices_prints_against_their_own_market(tmp_path) -> None:
    """The scan must follow the timeline's market, not the first print's.

    A capture whose first depth frame is BTCUSDT and whose first print is
    ETHUSDT would otherwise fill every ETH sell against a BTC bid, and each of
    those fills would be marked at the distance it was posted at — a fabricated
    edge in exactly the units the report compares against the fee.
    """
    capture = tmp_path / "mixed.ndjson"
    capture.write_text("\n".join([
        _snapshot(1_000, 99.99, 100.01, symbol="BTCUSDT"),
        _print(1_100, 3_000.0, symbol="ETHUSDT"),
        _print(1_200, 99.99, symbol="BTCUSDT"),
        _snapshot(2_200, 101.00, 101.02, symbol="BTCUSDT"),
    ]) + "\n", encoding="utf-8")
    script = _load_script()
    assert script.main([
        "--source", str(capture), "--tick-size", "0.01",
        "--distances-bps", "0.5", "--min-samples", "1",
        "--report-dir", str(tmp_path), "--stem", "mixed",
    ]) == 0
    payload = json.loads((tmp_path / "mixed.json").read_text(encoding="utf-8"))
    tape = payload["summary"]["tape"]
    assert tape["symbol"] == "BTCUSDT"
    assert tape["prints"] == 1
    assert tape["ignored"] == 1


def test_a_round_trip_pays_the_fee_twice_and_earns_the_markout_twice() -> None:
    """One fill is not a business: the round trip is `2 * mean - 2 * fee`.

    With a +3 bps per-fill markout and a 2 bps maker fee the round trip nets
    +2 bps, and the fee ceiling -- the per-side fee at which it is exactly zero
    -- is the markout itself. A row whose per-fill net looks positive can still
    lose as a round trip, which is the mistake this pins.
    """
    lines = [
        _snapshot(1_000, 99.99, 100.01),
        _print(1_100, 99.99),
        _snapshot(2_200, 100.01, 100.03),
    ]
    summary, _ = _curve(lines, _config(distances_bps=(1.0,), horizons_s=(0.0, 1.0)))
    trip = summary["rows"][0]["round_trip"]
    mean = summary["rows"][0]["horizons"]["1s"]["mean_bps"]
    assert mean == pytest.approx(3.0, rel=0, abs=2e-3)
    assert trip["gross_bps"] == pytest.approx(2.0 * mean, rel=0, abs=1e-9)
    assert trip["fees_bps"] == pytest.approx(4.0, rel=0, abs=1e-9)
    assert trip["net_bps"] == pytest.approx(2.0 * mean - 4.0, rel=0, abs=1e-9)
    assert trip["breakeven_maker_bps"] == pytest.approx(mean, rel=0, abs=1e-9)


def test_a_distance_marked_for_a_fraction_of_its_fills_cannot_decide() -> None:
    """Ten fills with one markout is a selected subset, not a distance.

    The far fills are the ones that happen while the timeline is blind -- a
    reconnect, a burst -- so a coverage rule is what stops the cell that looks
    best (and is measured on 7 % of its fills) from carrying the verdict.
    """
    lines = [_snapshot(1_000, 99.99, 100.01)]
    # Nineteen prints whose 1 s target lands in a hole, then one that can be
    # marked: 5 % coverage, and a mean that would otherwise clear easily.
    for index in range(19):
        lines.append(_print(1_100 + index, 99.99))
    lines.append(_print(5_000, 99.99))
    lines.append(_snapshot(6_600, 101.00, 101.02))
    strict, strict_config = _curve(lines, _config(
        distances_bps=(1.0,), horizons_s=(0.0, 1.0), min_samples=1, min_coverage=0.9))
    verdict = edge_curve_verdict(strict, strict_config)
    assert verdict["verdict"] == "NO_CLEARING_DISTANCE"
    assert any("excluded for coverage" in reason for reason in verdict["reasons"])

    lenient, lenient_config = _curve(lines, _config(
        distances_bps=(1.0,), horizons_s=(0.0, 1.0), min_samples=1, min_coverage=0.0))
    assert edge_curve_verdict(lenient, lenient_config)["verdict"] == "CLEARS"


def test_the_runner_refuses_a_primary_horizon_it_did_not_measure() -> None:
    """A verdict horizon has to be one of the measured horizons."""
    script = _load_script()
    assert script.main([
        "--source", str(SAMPLE), "--horizons", "0,1",
        "--primary-horizon", "5", "--no-report",
    ]) == 2


def test_a_print_constructor_refuses_a_missing_side() -> None:
    """A defaulted side would silently invert the sign of an edge."""
    with pytest.raises(ValueError):
        TapePrint(ts_ns=1, price=100.0, quantity=1.0, aggressor="sideways")


def test_the_runner_exits_three_on_a_capture_with_no_prints() -> None:
    """A depth-only capture prices nothing, and must not report "no" as a verdict."""
    script = _load_script()
    assert script.main(["--source", str(SAMPLE), "--no-report"]) == 3


def test_the_runner_exits_zero_when_a_distance_clears_the_fee(tmp_path) -> None:
    """A mid that runs away from the quote is a measured GO, and writes the bundle."""
    capture = tmp_path / "runaway.ndjson"
    capture.write_text("\n".join([
        _snapshot(1_000, 99.99, 100.01),
        _print(1_100, 99.99),
        _snapshot(2_200, 101.00, 101.02),
    ]) + "\n", encoding="utf-8")
    script = _load_script()
    code = script.main([
        "--source", str(capture), "--tick-size", "0.01",
        "--distances-bps", "0.5,1", "--min-samples", "1",
        "--report-dir", str(tmp_path), "--stem", "edge",
    ])
    assert code == 0
    for suffix in (".json", ".md", ".csv"):
        assert (tmp_path / f"edge{suffix}").exists()


def test_the_runner_exits_one_when_no_distance_clears(tmp_path) -> None:
    """A mid that drifts against the quote is a measured NO, not an error."""
    capture = tmp_path / "adverse.ndjson"
    capture.write_text("\n".join([
        _snapshot(1_000, 99.99, 100.01),
        _print(1_100, 99.99),
        _snapshot(2_200, 99.50, 99.52),
    ]) + "\n", encoding="utf-8")
    script = _load_script()
    assert script.main([
        "--source", str(capture), "--tick-size", "0.01",
        "--distances-bps", "0.5,1", "--min-samples", "1", "--no-report",
    ]) == 1


def test_the_runner_refuses_a_missing_capture() -> None:
    """A path that does not exist is a usage error, never an empty study."""
    script = _load_script()
    assert script.main(["--source", "does-not-exist.ndjson.gz", "--no-report"]) == 2


def test_the_runner_refuses_roll_files_handed_over_out_of_order(tmp_path) -> None:
    """A backwards timeline is an input defect, and must not read as a verdict.

    Python's traceback would exit ``1``, which this runner reserves for "no
    distance clears". Feeding yesterday's roll after today's is a usage error.
    """
    later = tmp_path / "later.ndjson"
    later.write_text(_snapshot(5_000, 99.99, 100.01) + "\n", encoding="utf-8")
    earlier = tmp_path / "earlier.ndjson"
    earlier.write_text(_snapshot(1_000, 99.99, 100.01) + "\n", encoding="utf-8")
    script = _load_script()
    assert script.main([
        "--source", str(later), str(earlier), "--no-report",
    ]) == 2


def test_the_runner_refuses_an_unwritable_report_directory(tmp_path) -> None:
    """A report that cannot be written is an environment error, not a NO."""
    capture = tmp_path / "tape.ndjson"
    capture.write_text("\n".join([
        _snapshot(1_000, 99.99, 100.01),
        _print(1_100, 99.99),
        _snapshot(2_200, 101.00, 101.02),
    ]) + "\n", encoding="utf-8")
    blocker = tmp_path / "blocker"
    blocker.write_text("not a directory", encoding="utf-8")
    script = _load_script()
    assert script.main([
        "--source", str(capture), "--tick-size", "0.01",
        "--distances-bps", "0.5", "--min-samples", "1",
        "--report-dir", str(blocker / "reports"),
    ]) == 2
