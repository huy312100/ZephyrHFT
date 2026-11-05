"""Unit tests for the Phase 5 production-readiness gate.

The gate is only worth trusting if it can say *no*: these tests build a green
report, then break one input at a time and assert the matching check turns red.
The evidence book is kept small (400 frames) so the file runs in seconds.
"""
from __future__ import annotations

import importlib.util
import json
import sys
from dataclasses import replace
from decimal import Decimal
from pathlib import Path

import pytest

from python_analytics.backtesting.walk_forward import write_lock_file
from python_analytics.simulator.paper_engine import synthetic_depth_messages

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts" / "generate_readiness_report.py"

PHASE1 = {"phase1.configuration", "phase1.archive_integrity"}
PHASE2 = {"phase2.engine_latency_p99", "phase2.engine_latency_p999",
          "phase2.reanchor_latency_p99", "phase2.replay_throughput",
          "phase2.ledger_residual", "phase2.order_rejections",
          "phase2.fill_activity"}
PHASE3 = {"phase3.disconnect_storm", "phase3.latency_injection",
          "phase3.deadman_switch"}
PHASE4 = {"phase4.parameter_lock", "phase4.no_look_ahead",
          "phase4.ledger_residual", "phase4.window_coverage"}
GATED = PHASE1 | PHASE2 | PHASE3 | PHASE4


def _load_module():
    spec = importlib.util.spec_from_file_location("readiness_report_cli", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module  # dataclasses need a registered module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def gate():
    return _load_module()


def _archive(path: Path, count: int = 400, start_ms: int = 1_700_000_000_000) -> Path:
    """A Phase 1 shaped archive: chronological depth frames on a 250 ms grid."""
    generator = synthetic_depth_messages(mid=100.0, tick=0.5, seed=5)
    with path.open("w", encoding="utf-8") as handle:
        for index in range(count):
            message = json.loads(next(generator))
            message["E"] = start_ms + index * 250
            handle.write(json.dumps(message) + "\n")
    return path


@pytest.fixture(scope="module")
def source(tmp_path_factory) -> Path:
    return _archive(tmp_path_factory.mktemp("archive") / "events.ndjson")


def test_a_seeded_archive_still_passes_the_integrity_check(gate, tmp_path) -> None:
    """The capture seeds each connection with a REST book snapshot; the gate reads it.

    ``phase1.archive_integrity`` blocks the build by default, and the seed line
    carries no event type of its own -- so either the classifier fails to see it
    (every seeded capture turns NO-GO) or the ingest counters fail to add it up
    (the gate raises before it can report anything). Both are covered here,
    because the archive that triggers them is the normal case now, not the edge.
    """
    archive = _archive(tmp_path / "seeded.ndjson", count=64)
    # Exactly what the capture archives: the venue's body plus its own marker.
    seed = json.dumps({"e": "zephyr.bookSnapshot", "lastUpdateId": 5,
                       "E": 1_700_000_000_000 - 1000,
                       "bids": [["100.00", "1.0"]], "asks": [["100.50", "1.0"]]})
    archive.write_text(seed + "\n" + archive.read_text(encoding="utf-8"),
                       encoding="utf-8")

    checks, _evidence = gate.measure_ingestion(
        archive, tick_size="0.5", center="100.00")
    integrity = next(check for check in checks
                     if check.id == "phase1.archive_integrity")
    assert integrity.passed, integrity.measured
    assert "0 malformed" in integrity.measured
    assert "1 book seed(s) accepted" in integrity.measured


@pytest.fixture(scope="module")
def green(gate, source, tmp_path_factory):
    """The report the other tests perturb: small book, recomputed windows."""
    out = tmp_path_factory.mktemp("report")
    args = gate.parse_args([
        "--source", str(source),
        "--evidence-events", "400",
        "--chaos-frames", "100",
        "--recompute-walk-forward",
        "--walk-forward-json", str(out / "missing.json"),
    ])
    return args, gate.build_report(args)


# --------------------------------------------------------------------------- #
# The happy path
# --------------------------------------------------------------------------- #
def test_green_run_is_go_and_gates_every_phase(green) -> None:
    report = green[1]
    assert report.verdict == "GO", report.summary_lines()
    assert {check.id for check in report.gated} == GATED
    # Alpha rides alongside the four build phases but is never part of ``gated``.
    assert {check.phase for check in report.checks} == {"phase1", "phase2",
                                                        "phase3", "phase4", "alpha"}


def test_green_run_reports_measurements_not_booleans(green) -> None:
    engine = green[1].evidence["engine"]
    assert engine["events"] == 400
    assert engine["orders_submitted"] > 0
    assert engine["fills"] > 0
    assert engine["latency_us"]["p99"] > 0.0
    assert abs(engine["ledger_residual"]) <= 0.01
    walk = green[1].evidence["walk_forward"]
    assert walk["windows"] >= 2
    assert walk["lock_fingerprint"]
    assert walk["validation"]["parameter_lock"]["checks"] >= 1
    assert walk["validation"]["look_ahead"]["chronology_verified"] is True


def test_deadman_switch_proves_trip_and_recovery(green) -> None:
    evidence = green[1].evidence["deadman"]
    assert evidence["orders_allowed_before"] is True
    assert evidence["poll_before_timeout_tripped"] is False
    assert evidence["orders_blocked_while_halted"] is True
    # The monitor reset alone must not clear a trading halt.
    assert evidence["blocked_until_explicit_reset"] is True
    assert evidence["orders_allowed_after_reset"] is True
    assert evidence["retripped_after_recovery"] is True
    assert evidence["trip_reason"].startswith("heartbeat_timeout")


def test_reanchor_check_measures_the_path_it_names(gate) -> None:
    """The re-anchor p99 must come from frames that actually moved the window."""
    parameters = gate.LockedParameters()
    checks, evidence = gate.measure_reanchor(parameters, frames=50, max_p99_us=1e6)
    assert evidence["frames"] == 50
    assert evidence["recenters"] > 0
    assert evidence["latency_us"]["p99"] > 0.0
    assert checks[0].passed


def test_reanchor_check_fails_when_the_window_never_moves(gate) -> None:
    """A comfortable p99 for a path the run never took is not evidence."""
    # One frame next to the seeded centre proves nothing about the anchor.
    parameters = gate.LockedParameters()
    checks, evidence = gate.measure_reanchor(parameters, frames=1, max_p99_us=1e6)
    assert evidence["recenters"] == 0
    assert not checks[0].passed


def test_the_reanchor_ceiling_is_its_own_budget(gate, green) -> None:
    """The 5 us production tier gates seeded frames, not window moves."""
    args = gate.parse_args(["--max-engine-p99-us", "5.0"])
    assert args.max_reanchor_p99_us == 200.0
    assert green[1].thresholds["max_reanchor_p99_us"] == 200.0

    # The ceiling handed in is the one enforced, not the engine budget.
    parameters = gate.LockedParameters()
    checks, _ = gate.measure_reanchor(parameters, frames=20, max_p99_us=0.0)
    assert checks[0].threshold == "<= 0 us"
    assert not checks[0].passed


def test_the_reanchor_ceiling_cannot_be_disabled(gate) -> None:
    """`inf` would make every finite p99 pass, so the budget is refused, not trusted."""
    for unbounded in ("inf", "-inf", "nan"):
        with pytest.raises(SystemExit) as raised:
            gate.parse_args(["--max-reanchor-p99-us", unbounded])
        # argparse's usage refusal is the same code the script uses for config.
        assert raised.value.code == gate.EXIT_USAGE
    assert gate.parse_args(["--max-reanchor-p99-us", "0.5"]).max_reanchor_p99_us == 0.5


def test_chaos_profiles_are_attributed(green) -> None:
    chaos = green[1].evidence["chaos"]
    storm = chaos["disconnect_storm"]
    assert storm["outages"] >= 1
    assert storm["reconnects"] == storm["outages"]
    assert storm["frames_seen"] == storm["frames_forwarded"] + storm["frames_dropped"]
    injection = chaos["latency_injection"]
    assert injection["latency_events"] >= 1
    assert injection["max_latency_ms"] <= injection["latency_injected_ms"] + 1.0


# --------------------------------------------------------------------------- #
# It must be able to say NO
# --------------------------------------------------------------------------- #
def test_corrupt_archive_blocks_the_release(gate, tmp_path, source) -> None:
    broken = tmp_path / "events.ndjson"
    broken.write_text(source.read_text(encoding="utf-8") + "not-a-frame\n",
                      encoding="utf-8")
    args = gate.parse_args([
        "--source", str(broken),
        "--evidence-events", "200",
        "--chaos-frames", "50",
        "--recompute-walk-forward",
        "--walk-forward-json", str(tmp_path / "missing.json"),
        "--no-report",
    ])
    report = gate.build_report(args)
    assert report.verdict == "NO-GO"
    blocking = [check.id for check in report.failures]
    assert blocking == ["phase1.archive_integrity"], blocking
    integrity = next(c for c in report.checks if c.id == "phase1.archive_integrity")
    assert "1 malformed" in integrity.measured


def test_profitability_gate_is_opt_in(gate, green) -> None:
    assert "phase4.oos_sharpe" not in {check.id for check in green[1].checks}
    args, _ = green
    greedy = gate.parse_args([
        "--source", args.source,
        "--evidence-events", "400",
        "--chaos-frames", "100",
        "--recompute-walk-forward",
        "--walk-forward-json", args.walk_forward_json,
        "--min-oos-sharpe", "1e6",
    ])
    report = gate.build_report(greedy)
    assert report.verdict == "NO-GO"
    assert [check.id for check in report.failures] == ["phase4.oos_sharpe"]


def test_missing_source_is_a_usage_error(gate, tmp_path) -> None:
    missing = str(tmp_path / "nope.ndjson")
    with pytest.raises(FileNotFoundError):
        gate.build_report(gate.parse_args(["--source", missing, "--no-report"]))
    assert gate.main(["--source", missing, "--no-report"]) == gate.EXIT_USAGE


def test_invalid_market_type_is_rejected_before_any_work(gate) -> None:
    with pytest.raises(SystemExit):
        gate.parse_args(["--market-type", "EQUITY"])
    args = gate.parse_args(["--market-type", "crypto"])
    assert gate.build_parameters(args, gate.load_settings()).market_type == "CRYPTO"


# --------------------------------------------------------------------------- #
# Artefacts
# --------------------------------------------------------------------------- #
def test_written_artefacts_round_trip(gate, green, tmp_path) -> None:
    report = green[1]
    json_path, md_path = report.write(tmp_path, stem="readiness")
    payload = json.loads(json_path.read_text(encoding="utf-8"))
    assert payload["verdict"] == "GO"
    assert payload["summary"]["failed"] == 0
    assert len(payload["checks"]) == len(report.checks)
    assert payload["evidence"]["engine"]["events"] == 400
    text = md_path.read_text(encoding="utf-8")
    assert "## Build verdict: **GO**" in text
    assert f"## Alpha verdict: **{report.alpha_verdict}**" in text
    for phase in report.PHASES:
        assert f"## {phase[1]}" in text


def test_existing_walk_forward_artefact_is_reused(gate, green, tmp_path) -> None:
    args, recomputed = green
    assert recomputed.inputs["walk_forward_origin"].startswith("computed")
    locked = gate.build_parameters(args, gate.load_settings())
    artefact = tmp_path / "walk_forward.json"
    artefact.write_text(json.dumps({
        "report": "phase4_walk_forward",
        "generated_at": "2026-01-01T00:00:00+00:00",
        "inputs": {"window_events": 100},
        "aggregate": {
            "windows": 4, "events": 400, "initial_cash": 100000.0,
            "final_equity": 100010.0, "net_pnl": 10.0, "sharpe_ratio": 0.75,
            "sortino_ratio": 1.0, "max_drawdown_pct": 0.01,
            "fill_ratio_pct": 12.0, "fills": 40, "positive_windows": 3,
            "negative_windows": 1, "realized_pnl": 9.5,
            "boundary_settled_pnl": -0.1, "unrealized_pnl": 0.5,
            "ledger_residual": 1e-9, "window_hit_rate_pct": 75.0,
        },
        "validation": {
            "parameter_lock": {"fingerprint": "deadbeef", "checks": 4,
                               "parameters": locked.as_dict()},
            "look_ahead": {"verified": True, "chronology_verified": True,
                           "chronology_checks": 3, "windows_checked": 4},
            "ledger": {"residual": 1e-9, "tolerance": 0.02},
        },
    }), encoding="utf-8")
    reused = gate.parse_args([
        "--source", args.source,
        "--evidence-events", "400",
        "--chaos-frames", "100",
        "--walk-forward-json", str(artefact),
    ])
    report = gate.build_report(reused)
    assert report.inputs["walk_forward_origin"].startswith("artifact")
    walk = report.evidence["walk_forward"]
    assert walk["windows"] == 4
    assert walk["sharpe_ratio"] == pytest.approx(0.75)
    assert walk["fill_ratio_pct"] == pytest.approx(12.0)
    assert walk["windows_positive"] == 3
    assert walk["lock_fingerprint"] == "deadbeef"


def test_artefact_with_a_different_fee_assumption_is_recomputed(
    gate, green, tmp_path
) -> None:
    """An old study's PnL must never be reported under today's fee labels."""

    args, _ = green
    locked = gate.build_parameters(args, gate.load_settings()).as_dict()
    locked["costs"]["maker_bps"] = -1.0
    artefact = tmp_path / "walk_forward_rebate.json"
    artefact.write_text(json.dumps({
        "report": "phase4_walk_forward",
        "aggregate": {"windows": 2, "events": 800, "net_pnl": 4.1251},
        "validation": {"parameter_lock": {"checks": 4,
                                          "parameters": locked}},
    }), encoding="utf-8")
    reused = gate.parse_args([
        "--source", args.source,
        "--evidence-events", "400",
        "--chaos-frames", "100",
        "--walk-forward-json", str(artefact),
        "--no-report",
    ])
    report = gate.build_report(reused)
    origin = report.inputs["walk_forward_origin"]
    assert origin.startswith("computed")
    assert "ignored" in origin
    assert report.evidence["walk_forward"]["windows"] == 4  # freshly studied
    assert any("costs.maker_bps" in item
               for item in report.inputs["walk_forward_drift"])


def test_locked_parameter_drift_compares_grids_numerically(gate, green) -> None:
    """``"0.5"`` and ``"0.50"`` are the same grid, but a fee change is drift."""

    args, _ = green
    parameters = gate.build_parameters(args, gate.load_settings())
    block = parameters.as_dict()
    block["venue"]["tick_size"] = "0.5"
    payload = {"validation": {"parameter_lock": {"parameters": block}}}
    assert gate.locked_parameter_drift(payload, parameters) == []
    block["costs"]["maker_bps"] = -1.0
    drift = gate.locked_parameter_drift(payload, parameters)
    assert len(drift) == 1 and "costs.maker_bps" in drift[0]


def test_no_artefacts_without_consent(gate, green, tmp_path) -> None:
    args, _ = green
    assert gate.main([
        "--source", args.source, "--evidence-events", "400",
        "--chaos-frames", "100", "--recompute-walk-forward",
        "--walk-forward-json", args.walk_forward_json,
        "--no-report", "--report-dir", str(tmp_path),
    ]) == gate.EXIT_GO
    assert list(tmp_path.iterdir()) == []


# --------------------------------------------------------------------------- #
# Alpha: profitability is measured separately and never flatters the build
# --------------------------------------------------------------------------- #
ALPHA = {"alpha.net_pnl", "alpha.fee_assumption", "alpha.fee_wall"}


def test_alpha_is_always_measured_but_never_gated(gate, green) -> None:
    report = green[1]
    assert {check.id for check in report.checks if check.phase == gate.ALPHA_PHASE} == ALPHA
    # The build verdict must not inherit a losing strategy: a GO build with a
    # NO-GO alpha is the honest, most useful combination.
    assert report.verdict == "GO"
    assert ALPHA.isdisjoint({check.id for check in report.failures})
    assert gate.ALPHA_PHASE in {check.phase for check in report.checks}
    assert "ALPHA" in "\n".join(report.summary_lines())


def test_alpha_verdict_matches_its_own_checks(gate, green) -> None:
    report = green[1]
    assert [check.id for check in report.alpha_failures] == [
        check.id for check in report.checks
        if check.phase == gate.ALPHA_PHASE and not check.passed]
    assert report.alpha_verdict == ("NO-GO" if report.alpha_failures else "GO")


def test_alpha_gate_only_changes_the_exit_code(gate, green) -> None:
    args, report = green
    argv = [
        "--source", args.source, "--evidence-events", "400",
        "--chaos-frames", "100", "--recompute-walk-forward",
        "--walk-forward-json", args.walk_forward_json, "--no-report",
    ]
    # Default: alpha is reported but never masks a build regression in CI.
    assert gate.main(argv) == gate.EXIT_GO
    graded = gate.main(argv + ["--require-positive-alpha"])
    expected = gate.EXIT_ALPHA_NO_GO if report.alpha_failures else gate.EXIT_GO
    assert graded == expected


def test_measure_alpha_backs_fees_out_of_net_pnl(gate) -> None:
    """Fees sit inside realized/unrealized PnL, so the pre-fee number adds them back."""

    payload = {"aggregate": {"net_pnl": -16.4743, "net_pnl_pct": -0.0165,
                             "gross_pnl": -16.4743, "fees_paid": 13.733,
                             "fills": 105}}
    checks, evidence = gate.measure_alpha(payload, maker_bps=2.0, taker_bps=5.0)
    assert evidence["pnl_before_fees"] == pytest.approx(-2.7413, abs=1e-4)
    assert evidence["pre_fee_profitable"] is False
    assert evidence["round_trip_cost_bps"] == pytest.approx(4.0)
    failing = next(check for check in checks if check.id == "alpha.net_pnl")
    assert failing.passed is False
    assert "-2.7413 before +13.7330 of fees" in failing.measured


def test_measure_alpha_rejects_a_rebate_assumption(gate) -> None:
    """A negative maker fee turns every passive fill into free money; refuse it."""

    payload = {"aggregate": {"net_pnl": 4.0, "net_pnl_pct": 0.004,
                             "gross_pnl": 4.0, "fees_paid": -6.5, "fills": 105}}
    checks, evidence = gate.measure_alpha(payload, maker_bps=-1.0, taker_bps=5.0)
    assert [check.id for check in checks if not check.passed] == ["alpha.fee_assumption"]
    assert evidence["rebate_assumed"] is True
    assert evidence["pnl_before_fees"] == pytest.approx(-2.5)
    assert evidence["pre_fee_profitable"] is False


def test_measure_alpha_passes_a_genuinely_profitable_run(gate) -> None:
    payload = {"aggregate": {"net_pnl": 12.5, "net_pnl_pct": 0.0125,
                             "gross_pnl": 12.5, "fees_paid": 8.0, "fills": 60}}
    checks, evidence = gate.measure_alpha(payload, maker_bps=2.0, taker_bps=5.0)
    assert all(check.passed for check in checks), [c.measured for c in checks]
    assert evidence["pre_fee_profitable"] is True


def test_measure_alpha_names_the_break_even_maker_fee(gate) -> None:
    """Net PnL says the study loses; the wall says which fee would stop it."""

    # 0.5 USDT earned before fees on 60,000 USDT of notional is 0.0833 bps, so a
    # 2 bps maker fee is ~24x what the strategy can pay.
    payload = {"aggregate": {"net_pnl": -11.5, "net_pnl_pct": -0.0115,
                             "gross_pnl": -11.5, "fees_paid": 12.0,
                             "traded_notional": 60_000.0, "fills": 120}}
    checks, evidence = gate.measure_alpha(payload, maker_bps=2.0, taker_bps=5.0)
    wall = next(check for check in checks if check.id == "alpha.fee_wall")
    assert wall.passed is False
    assert "break-even maker fee +0.083 bps" in wall.measured
    assert "2 bps maker fee" in wall.measured
    assert evidence["breakeven_maker_bps"] == pytest.approx(0.0833, abs=1e-4)
    assert evidence["fee_wall_viable"] is False
    assert evidence["fee_wall_multiple"] == pytest.approx(24.0, abs=1e-2)


def test_measure_alpha_omits_the_fee_wall_when_nothing_was_traded(gate) -> None:
    """A run with no fills has no notional, so there is no break-even fee to state."""

    payload = {"aggregate": {"net_pnl": 0.0, "net_pnl_pct": 0.0,
                             "gross_pnl": 0.0, "fees_paid": 0.0, "fills": 0}}
    checks, evidence = gate.measure_alpha(payload, maker_bps=2.0, taker_bps=5.0)
    assert "alpha.fee_wall" not in {check.id for check in checks}
    assert "breakeven_maker_bps" not in evidence


# --------------------------------------------------------------------------- #
# The evidence book has to be a market the engine can trade on ANY venue grid
# --------------------------------------------------------------------------- #
def _measure_engine(gate, tick: str, center: str, events: int = 2000):
    """Phase 2 measured on the evidence world of one venue profile."""
    args = gate.parse_args(["--tick-size", tick, "--center", center])
    parameters = gate.build_parameters(args, gate.load_settings())
    lines = gate.synthetic_evidence(events, seed=7, mid=float(center),
                                    tick=float(tick))
    return gate.measure_engine(parameters, lines, max_p99_us=50.0,
                               max_p999_us=500.0, min_throughput=1000,
                               ledger_tolerance=0.01)


def test_a_fine_venue_tick_does_not_read_as_a_dead_fill_path(gate) -> None:
    """A 0.1 tick on an 80k mid is a real perp grid, and it has to trade.

    The strategy's quote distance is absolute (price units, so ~0.2 USDT at any
    venue) while the generator walked in *venue ticks*: at a 0.1 tick the 3-tick
    move could not reach a quote resting across the book's own spread, so
    ``phase2.fill_activity`` failed a build that was fine and the gate could not
    certify a real venue at all -- which is how a red study stayed measured on
    synthetic data.
    """
    for tick, center in (("0.1", "80000"), ("0.01", "77624.90")):
        checks, evidence = _measure_engine(gate, tick, center)
        activity = next(check for check in checks
                        if check.id == "phase2.fill_activity")
        assert evidence["fills"] > 0, (tick, center, evidence)
        assert activity.passed, (tick, center, activity.measured)


def test_generated_prices_stay_on_the_venue_grid(gate) -> None:
    """The amplitude is scaled, the *grid* is not: every price stays quotable.

    A generated price the ladder cannot represent is reported as an unpriced
    frame rather than guessed around, so a quantum that was not a whole number of
    venue ticks would turn the evidence book into the blind ladder this exists to
    prevent.
    """
    lines = gate.synthetic_evidence(200, seed=7, mid=80000.0, tick=0.1)
    venue_tick = Decimal("0.1")
    for line in lines[1:]:
        frame = json.loads(line)
        for level in list(frame["b"]) + list(frame["a"]):
            price = Decimal(level[0])
            assert price % venue_tick == 0, level[0]


def test_the_default_evidence_world_is_untouched(gate) -> None:
    """Scale-invariance must not re-price the synthetic baseline it came from.

    0.5 bps of a 30,000 mid across the generator's 3 steps is exactly one 0.5
    tick, so the committed profile keeps the book the reported numbers were
    measured on; only profiles that could never trade change.
    """
    line = json.loads(gate.synthetic_evidence(4, seed=7, mid=30000.0,
                                              tick=0.5)[1])
    bid, ask = Decimal(line["b"][0][0]), Decimal(line["a"][0][0])
    assert (ask - bid) == Decimal("2.0")  # 4 ticks of 0.5, as committed
    assert bid % Decimal("0.5") == 0


def test_the_report_names_the_world_it_measured(gate, green) -> None:
    """An auditor must be able to see the evidence book was the venue's own."""
    evidence = green[1].inputs["evidence"]
    assert "mid" in evidence and "tick" in evidence


# --------------------------------------------------------------------------- #
# Adopting a persisted lock, so a venue is certified rather than restated
# --------------------------------------------------------------------------- #
def test_from_lock_adopts_the_venue_verbatim(gate) -> None:
    """The venue lock is the study's own; the CLI must not re-describe it."""
    lock = ROOT / "config" / "venue_binance_futures_btcusdt.json"
    args = gate.parse_args(["--from-lock", "--lock-file", str(lock)])
    parameters = gate.build_parameters(args, gate.load_settings())
    assert parameters.fingerprint == json.loads(
        lock.read_text(encoding="utf-8"))["fingerprint"]
    assert str(parameters.tick_size) == "0.1"
    assert str(parameters.center) == "80000"


def test_a_lock_file_the_cli_disagrees_with_is_refused(gate) -> None:
    """Without ``--from-lock`` the lock only grades the command line."""
    lock = ROOT / "config" / "venue_binance_futures_btcusdt.json"
    args = gate.parse_args(["--lock-file", str(lock), "--maker-bps", "0.0"])
    with pytest.raises(gate.ParameterLockError):
        gate.build_parameters(args, gate.load_settings())


def test_from_lock_without_a_lock_file_is_a_usage_error(gate, source) -> None:
    assert gate.main(["--source", str(source), "--from-lock",
                      "--no-report"]) == gate.EXIT_USAGE


def test_a_missing_lock_file_is_refused_before_it_fails_the_build(
    gate, source, tmp_path
) -> None:
    """A typo in ``--lock-file`` is a configuration refusal, not a NO-GO release.

    ``read_lock_file`` reports a lost input as a ``ParameterLockError``, and the
    gate's documented exit contract reserves ``1`` for a genuine parameter
    mismatch. Both loading branches have to refuse the missing path as usage (2),
    or a wrong path reads to a release engineer like a build that failed review.
    """
    missing = str(tmp_path / "no-such-lock.json")
    with pytest.raises(FileNotFoundError):
        gate.build_parameters(gate.parse_args(["--lock-file", missing]),
                              gate.load_settings())
    for extra in ([], ["--from-lock"]):
        assert gate.main(["--source", str(source), "--lock-file", missing,
                          *extra, "--no-report"]) == gate.EXIT_USAGE


def test_alpha_is_priced_at_the_fees_the_lock_adopted(gate, source, tmp_path) -> None:
    """``--from-lock`` carries a fee tier, so the alpha check reads *that* rate.

    The engine replayed at the locked fees while ``measure_alpha``, the thresholds
    and the report labels used the CLI defaults: a lock holding a maker rebate was
    graded as if it paid retail, and ``alpha.fee_assumption`` -- the check whose
    whole job is to refuse a rebate assumption -- stayed green. A gate that
    prices its money at one rate and labels it at another is not a gate.
    """
    retail = gate.build_parameters(gate.parse_args(["--source", str(source)]),
                                   gate.load_settings())
    rebate = replace(retail, maker_bps=-0.5, taker_bps=2.5)
    lock = write_lock_file(rebate, tmp_path / "rebate.json")
    args = gate.parse_args([
        "--source", str(source), "--from-lock", "--lock-file", str(lock),
        "--evidence-events", "400", "--chaos-frames", "100",
        "--recompute-walk-forward",
        "--walk-forward-json", str(tmp_path / "missing.json"),
    ])
    report = gate.build_report(args)

    assert report.thresholds["maker_bps"] == -0.5
    assert report.thresholds["taker_bps"] == 2.5
    assert report.inputs["costs"] == {"maker_bps": -0.5, "taker_bps": 2.5}
    fee_check = next(check for check in report.checks
                     if check.id == "alpha.fee_assumption")
    assert fee_check.passed is False, fee_check.measured
    assert "-0.5 bps maker" in "\n".join(report.summary_lines())
    assert "-0.5 bps maker" in report.to_markdown_text()
