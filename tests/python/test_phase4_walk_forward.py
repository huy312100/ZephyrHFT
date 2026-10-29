"""Unit tests for the Phase 4 walk-forward OOS validation module.

Every test attacks one of the three invariants a walk-forward study has to
prove: the parameter lock never drifts, a window never sees another window's
events, and the chained ledger closes to zero.
"""
from __future__ import annotations

import importlib.util
import json
import sys
from dataclasses import replace
from pathlib import Path

import pytest

from python_analytics.backtesting.walk_forward import (
    LockedParameters,
    LookAheadError,
    ParameterLockError,
    WalkForwardError,
    WalkForwardRunner,
    read_lock_file,
    write_lock_file,
)
from python_analytics.simulator.paper_engine import synthetic_depth_messages


def _lines(count: int = 300, seed: int = 11, start_ms: int = 1_700_000_000_000) -> list:
    generator = synthetic_depth_messages(mid=100.0, tick=0.5, seed=seed)
    lines = []
    for index in range(count):
        message = json.loads(next(generator))
        message["E"] = start_ms + index * 250
        lines.append(json.dumps(message))
    return lines


def _write(path: Path, lines: list) -> Path:
    path.write_text("".join(f"{line}\n" for line in lines), encoding="utf-8")
    return path


def _parameters(**overrides) -> LockedParameters:
    base = dict(gamma=0.1, sigma=0.02, k=5.0, time_horizon=60.0,
                inventory_limit=5.0, ofi_sensitivity=0.5, ofi_decay=0.95,
                ofi_cap=3.0, use_time_spread=True, tick_size="0.5", center="100",
                market_type="CRYPTO", maker_bps=2.0, taker_bps=5.0,
                initial_cash=100_000.0, order_quantity="0.01")
    base.update(overrides)
    return LockedParameters(**base)


def _runner(parameters: LockedParameters, window_events: int = 100) -> WalkForwardRunner:
    return WalkForwardRunner(parameters, window_events=window_events,
                             ledger_tolerance=1e-2)


# --------------------------------------------------------------------------- #
# Parameter lock
# --------------------------------------------------------------------------- #
def test_locked_parameters_fingerprint_is_stable_and_complete() -> None:
    parameters = _parameters()
    assert parameters.fingerprint == _parameters().fingerprint
    assert len(parameters.fingerprint) == 16

    payload = parameters.as_dict()
    assert set(payload) == {"strategy", "venue", "costs", "sizing"}
    assert LockedParameters.from_dict(payload).fingerprint == parameters.fingerprint
    assert parameters.diff(_parameters()) == []


def test_locked_parameters_detect_drift() -> None:
    baseline = _parameters()
    drifted = _parameters(k=1.5, ofi_sensitivity=0.0)

    diff = baseline.diff(drifted)
    assert len(diff) == 2
    assert diff[0].startswith("strategy.k: 5.0 != 1.5")
    assert diff[1].startswith("strategy.ofi_sensitivity: 0.5 != 0.0")

    with pytest.raises(ParameterLockError):
        baseline.assert_matches(drifted, context="unit-test")


def test_lock_file_round_trip(tmp_path: Path) -> None:
    parameters = _parameters()
    target = tmp_path / "lock.json"

    write_lock_file(parameters, target, metadata={"note": "frozen on friday"})
    reloaded = read_lock_file(target)

    assert reloaded.fingerprint == parameters.fingerprint
    assert reloaded.diff(parameters) == []
    payload = json.loads(target.read_text(encoding="utf-8"))
    assert payload["kind"] == "walk_forward_parameter_lock"
    assert payload["metadata"] == {"note": "frozen on friday"}


def test_locked_parameters_reject_invalid_costs() -> None:
    with pytest.raises(ValueError):
        _parameters(taker_bps=-1.0)
    with pytest.raises(ValueError):
        _parameters(maker_bps=-500.0)


def test_locked_parameters_reject_an_unknown_venue_class() -> None:
    # `MarketType` only ships adapters it can actually route; a typo must fail
    # at construction instead of after the first window has traded.
    with pytest.raises(ValueError, match="unknown market_type 'EQUITY'"):
        _parameters(market_type="EQUITY")
    assert _parameters(market_type="crypto").market_type == "CRYPTO"


# --------------------------------------------------------------------------- #
# Windowing / OOS ledger
# --------------------------------------------------------------------------- #
def test_runner_slices_a_stream_into_contiguous_windows(tmp_path: Path) -> None:
    source = _write(tmp_path / "archive.ndjson", _lines(300))
    report = _runner(_parameters()).run([source])

    assert len(report.windows) == 3
    assert [window.offset for window in report.windows] == [0, 100, 200]
    assert [window.events for window in report.windows] == [100, 100, 100]
    assert sum(window.events for window in report.windows) == 300

    # Windows are chained: each one opens on the previous window's close.
    for previous, current in zip(report.windows, report.windows[1:]):
        assert current.start_equity == pytest.approx(previous.end_equity, abs=1e-9)
        assert current.first_event_ms > previous.last_event_ms

    aggregate = report.aggregate
    assert aggregate["windows"] == 3
    assert aggregate["events"] == 300
    assert aggregate["final_equity"] == pytest.approx(report.windows[-1].end_equity)
    assert abs(aggregate["ledger_residual"]) <= 1e-2 * 3

    # Two lock reads (pre/post) per window, one chronology check per boundary.
    assert report.lock_checks == 6
    assert report.chronology_checks == 2
    assert report.chronology_verified is True


def test_aggregate_ledger_carries_boundary_settlements_once(tmp_path: Path) -> None:
    source = _write(tmp_path / "archive.ndjson", _lines(300))
    report = _runner(_parameters()).run([source])
    aggregate = report.aggregate

    accounted = (aggregate["realized_pnl"] + aggregate["unrealized_pnl"])
    assert accounted == pytest.approx(aggregate["net_pnl"], abs=1e-4)
    # Boundary marks come from every window but the last.
    assert aggregate["boundary_settled_pnl"] == pytest.approx(
        sum(window.stats.get("unrealized_pnl", 0.0) for window in report.windows[:-1]),
        abs=1e-6)


def test_runner_treats_each_archive_as_one_window(tmp_path: Path) -> None:
    first = _write(tmp_path / "a.ndjson", _lines(120, seed=1))
    second = _write(tmp_path / "b.ndjson", _lines(80, seed=2, start_ms=1_700_100_000_000))
    report = _runner(_parameters(), window_events=0).run([first, second])

    assert [window.events for window in report.windows] == [120, 80]
    assert report.window_events == 0
    assert len(report.sources) == 2


def test_runner_expands_globs_in_sorted_order(tmp_path: Path) -> None:
    for index in range(2):
        _write(tmp_path / f"part{index}.ndjson",
               _lines(60, seed=index + 3, start_ms=1_700_000_000_000 + index * 10_000_000))
    report = _runner(_parameters(), window_events=0).run([str(tmp_path / "part*.ndjson")])

    assert [Path(window.source).name for window in report.windows] == [
        "part0.ndjson", "part1.ndjson"]


# --------------------------------------------------------------------------- #
# Invariant violations must abort, never average out
# --------------------------------------------------------------------------- #
def test_runner_aborts_when_a_window_silently_retunes(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A strategy that re-tunes itself between windows cannot pass as OOS."""
    source = _write(tmp_path / "archive.ndjson", _lines(300))
    original = LockedParameters.build_strategy
    builds = {"count": 0}

    def drifting(self: LockedParameters):
        builds["count"] += 1
        if builds["count"] >= 2:
            return original(replace(self, k=2.5))
        return original(self)

    monkeypatch.setattr(LockedParameters, "build_strategy", drifting)

    with pytest.raises(ParameterLockError) as excinfo:
        _runner(_parameters()).run([source])
    message = str(excinfo.value)
    assert "pre-window" in message
    assert "k: 5.0 != 2.5" in message


def test_runner_aborts_on_time_travel_between_archives(tmp_path: Path) -> None:
    first = _write(tmp_path / "a.ndjson", _lines(80, seed=4))
    # Second archive replays events that predate the first one's close.
    second = _write(tmp_path / "b.ndjson", _lines(80, seed=5, start_ms=1_600_000_000_000))

    with pytest.raises(LookAheadError) as excinfo:
        _runner(_parameters(), window_events=0).run([first, second])
    assert "before the previous window closed" in str(excinfo.value)


def test_runner_rejects_empty_input(tmp_path: Path) -> None:
    empty = _write(tmp_path / "empty.ndjson", [])
    with pytest.raises(WalkForwardError):
        _runner(_parameters()).run([empty])


def test_runner_rejects_missing_sources() -> None:
    with pytest.raises(FileNotFoundError):
        _runner(_parameters()).run(["/definitely/not/here.ndjson"])


# --------------------------------------------------------------------------- #
# Artefacts
# --------------------------------------------------------------------------- #
def test_report_serialises_json_and_markdown(tmp_path: Path) -> None:
    source = _write(tmp_path / "archive.ndjson", _lines(200))
    report = _runner(_parameters()).run([source])

    json_path, md_path = report.write(tmp_path / "reports")
    assert json_path.exists() and md_path.exists()

    payload = json.loads(json_path.read_text(encoding="utf-8"))
    assert payload["report"] == "phase4_walk_forward"
    assert payload["validation"]["parameter_lock"]["verified"] is True
    assert payload["validation"]["parameter_lock"]["fingerprint"] == report.lock_fingerprint
    assert payload["validation"]["look_ahead"]["chronology_verified"] is True
    assert payload["validation"]["ledger"]["residual"] == pytest.approx(
        payload["aggregate"]["ledger_residual"])
    assert len(payload["windows"]) == 2
    assert payload["aggregate"]["windows"] == 2

    markdown = md_path.read_text(encoding="utf-8")
    assert markdown.startswith("# Walk-forward out-of-sample validation")
    assert report.lock_fingerprint in markdown
    assert "## Aggregate OOS performance" in markdown
    assert "## Window detail" in markdown or "| 0 |" in markdown


def test_aggregate_reports_the_break_even_maker_fee(tmp_path: Path) -> None:
    """Gross edge per unit of notional is the fee a deployment has to clear.

    The number is derived, never stored twice: it must equal the aggregate's own
    pre-fee PnL over its own traded notional, and it must be absent (``None``)
    when nothing traded rather than a misleading zero.
    """
    source = _write(tmp_path / "archive.ndjson", _lines(300))
    report = _runner(_parameters()).run([source])
    aggregate = report.aggregate
    notional = aggregate["traded_notional"]
    wall = report.fee_wall()
    fee_lines = [line for line in report.summary_lines() if line.startswith("fee wall:")]

    if notional > 0.0:
        expected = ((aggregate["net_pnl"] + aggregate["fees_paid"])
                    / notional * 10_000.0)
        assert aggregate["gross_edge_bps"] == pytest.approx(expected, abs=1e-5)
        assert wall is not None and wall.maker_bps == pytest.approx(2.0)
        assert len(fee_lines) == 1
        assert f"{expected:+.3f} bps" in fee_lines[0]
    else:
        assert aggregate["gross_edge_bps"] is None
        assert wall is None
        assert fee_lines == []


def test_report_numbers_are_finite_or_null(tmp_path: Path) -> None:
    """NaN/Inf must never leak into JSON (they are not valid JSON numbers)."""
    source = _write(tmp_path / "archive.ndjson", _lines(120))
    report = _runner(_parameters()).run([source])
    payload = report.as_dict()

    def walk(node) -> None:
        if isinstance(node, dict):
            for value in node.values():
                walk(value)
        elif isinstance(node, list):
            for value in node:
                walk(value)
        elif isinstance(node, float):
            assert node == node and abs(node) != float("inf")

    walk(payload)
    json.dumps(payload, allow_nan=False)


# --------------------------------------------------------------------------- #
# CLI: replaying a committed lock without restating it
# --------------------------------------------------------------------------- #
ROOT = Path(__file__).resolve().parents[2]
CLI = ROOT / "scripts" / "run_walk_forward.py"


def _load_cli():
    spec = importlib.util.spec_from_file_location("walk_forward_cli", CLI)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_cli_from_lock_replays_the_committed_parameters(tmp_path: Path, capsys):
    """``--from-lock`` must run the lock verbatim, even if the CLI disagrees."""
    cli = _load_cli()
    parameters = _parameters()
    lock = tmp_path / "lock.json"
    write_lock_file(parameters, lock, metadata={"source": ["fixture"]})
    archive = _write(tmp_path / "archive.ndjson", _lines(300))

    code = cli.main(["--source", str(archive), "--window-events", "100",
                     "--from-lock", "--lock-file", str(lock),
                     "--no-report", "--log-level", "ERROR"])

    assert code == cli.EXIT_OK
    out = capsys.readouterr().out
    assert parameters.fingerprint in out
    assert str(lock) in out


def test_cli_from_lock_ignores_conflicting_flags(tmp_path: Path, capsys):
    """A stray ``--k`` on the command line cannot re-tune a locked study."""
    cli = _load_cli()
    parameters = _parameters()
    lock = tmp_path / "lock.json"
    write_lock_file(parameters, lock)
    archive = _write(tmp_path / "archive.ndjson", _lines(300))

    code = cli.main(["--source", str(archive), "--window-events", "100",
                     "--from-lock", "--lock-file", str(lock), "--k", "0.5",
                     "--no-report", "--log-level", "ERROR"])

    assert code == cli.EXIT_OK
    assert parameters.fingerprint in capsys.readouterr().out


def test_cli_from_lock_without_a_lock_file_is_a_usage_error(tmp_path: Path):
    cli = _load_cli()
    archive = _write(tmp_path / "archive.ndjson", _lines(50))
    code = cli.main(["--source", str(archive), "--from-lock", "--no-report"])
    assert code == cli.EXIT_USAGE
