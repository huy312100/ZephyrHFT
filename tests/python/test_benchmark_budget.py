"""Tests for the latency-budget mode of ``scripts/run_benchmark.py``.

CI turns the benchmark into a gate (``--max-p99-us`` / ``--max-p999-us``), so
the budget logic needs to be provably able to fail. The ceilings used here are
deliberately explicit (``0.001`` us can never pass, ``1e6`` us always does)
rather than derived from timings, so the tests cannot flake on a slow runner.
"""
from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts" / "run_benchmark.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("benchmark_cli", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def bench():
    return _load_module()


def test_report_is_internally_ordered(bench):
    """p50 <= p99 <= p99.9 <= max, and the stress loop never gets rejected."""
    report = bench.run_benchmark(ticks=200, seed=3)
    assert report["ticks"] == 200
    assert report["seed"] == 3
    total = report["total_us"]
    assert 0.0 <= total["p50"] <= total["p99"] <= total["p999"] <= total["max"]
    assert report["risk_rejections"] == 0


def test_json_artefact_round_trip(bench, tmp_path: Path, capsys):
    """``--json`` writes the raw report to a directory that does not exist yet."""
    target = tmp_path / "nested" / "bench.json"
    code = bench.main(["--ticks", "200", "--json", str(target),
                       "--log-level", "ERROR"])
    assert code == 0
    payload = json.loads(target.read_text())
    assert set(payload) >= {"ticks", "seed", "total_us", "apply_us", "risk_us"}
    assert "json report" in capsys.readouterr().out


def test_impossible_budget_fails_the_build(bench):
    """A p99 of 0.001 us cannot be met, so the gate must exit 1."""
    code = bench.main(["--ticks", "200", "--max-p99-us", "0.001",
                       "--log-level", "ERROR"])
    assert code == 1


def test_generous_budget_passes(bench):
    code = bench.main(["--ticks", "200", "--max-p99-us", "1e6",
                       "--max-p999-us", "1e6", "--log-level", "ERROR"])
    assert code == 0


def test_ticks_must_be_positive(bench):
    assert bench.main(["--ticks", "0"]) == 2


def test_budget_messages_name_the_measurement(bench):
    breaches = bench.check_budget(
        {"total_us": {"p99": 10.0, "p999": 90.0}}, 5.0, 50.0)
    assert len(breaches) == 2
    assert breaches[0].startswith("p99 ")
    assert breaches[1].startswith("p99.9 ")


def test_no_budget_means_no_breaches(bench):
    assert bench.check_budget({"total_us": {"p99": 1.0, "p999": 2.0}},
                              None, None) == []
