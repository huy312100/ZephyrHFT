"""Offline tests for the session harness's arming guardrails.

The supervisor is machinery; what these tests protect is the rule that
machinery does not earn money for us. A production session still needs the
alpha gate, ``--live`` still has to be said out loud, and the kill-on-stop path
must not depend on either.
"""
from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from python_analytics.live import AlphaGateError

from scripts import run_live_session as script


def _production() -> SimpleNamespace:
    return SimpleNamespace(is_testnet=False, environment="production")


def _testnet() -> SimpleNamespace:
    return SimpleNamespace(is_testnet=True, environment="testnet")


# --------------------------------------------------------------------------- #
# Environment
# --------------------------------------------------------------------------- #
def test_production_needs_live_to_be_said_out_loud() -> None:
    assert script._production_refused(_production(), SimpleNamespace(live=False))
    assert not script._production_refused(_production(), SimpleNamespace(live=True))
    assert not script._production_refused(_testnet(), SimpleNamespace(live=False))


def test_a_missing_environment_is_a_usage_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from python_analytics.exchange import binance_futures as bf

    def refuse(*_a: Any, **_k: Any) -> Any:
        raise bf.BinanceConfigError("BINANCE_API_KEY not set")

    monkeypatch.setattr(bf.BinanceFuturesConfig, "from_env", classmethod(
        lambda cls, *a, **k: refuse()))
    args = script.parse_args(["--store", str(tmp_path / "o.sqlite3")])
    assert script.run(args) == script.EXIT_USAGE


# --------------------------------------------------------------------------- #
# The alpha gate
# --------------------------------------------------------------------------- #
def test_production_without_a_green_alpha_gate_is_refused(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from python_analytics.exchange import binance_futures as bf

    monkeypatch.setattr(bf.BinanceFuturesConfig, "from_env", classmethod(
        lambda cls, *a, **k: _production()))

    def refuse(*_a: Any, **_k: Any) -> str:
        raise AlphaGateError("alpha checks failed: alpha.net_pnl, alpha.fee_wall")

    monkeypatch.setattr(script, "require_alpha_go", refuse)
    args = script.parse_args(["--live", "--store", str(tmp_path / "o.sqlite3")])
    assert script.run(args) == script.EXIT_ALPHA_NO_GO


def test_a_testnet_session_runs_without_the_gate_but_reports_it(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Rehearsal is the point of testnet; the verdict is logged, not required."""
    from python_analytics.exchange import binance_futures as bf

    monkeypatch.setattr(bf.BinanceFuturesConfig, "from_env", classmethod(
        lambda cls, *a, **k: _testnet()))
    report = tmp_path / "report.json"
    report.write_text(json.dumps({"alpha_verdict": "NO-GO", "checks": []}),
                      encoding="utf-8")

    reached = {}

    def stop_after_the_gate(*_a: Any, **_k: Any) -> None:
        reached["past_gate"] = True
        raise RuntimeError("end of test")

    monkeypatch.setattr(script, "BinanceFuturesClient", stop_after_the_gate)
    args = script.parse_args(["--report", str(report),
                              "--store", str(tmp_path / "o.sqlite3")])
    with pytest.raises(RuntimeError):
        script.run(args)
    assert reached.get("past_gate"), "testnet must not be gated on alpha"


# --------------------------------------------------------------------------- #
# Defaults
# --------------------------------------------------------------------------- #
def test_the_session_is_bounded_by_default() -> None:
    """An unbounded loop on a laptop is not a service."""
    args = script.parse_args([])
    assert args.iterations > 0, "the default session must end on its own"


def test_documented_exit_codes_are_distinct() -> None:
    codes = [script.EXIT_CLEAN, script.EXIT_HALTED, script.EXIT_USAGE,
             script.EXIT_ALPHA_NO_GO, script.EXIT_INTERRUPTED]
    assert len(set(codes)) == len(codes), codes
    assert script.EXIT_ALPHA_NO_GO == 3, "the alpha refusal must be its own code"
