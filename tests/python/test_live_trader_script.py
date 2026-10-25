"""Offline tests for the single-shot live harness's CLI guardrails.

Nothing here opens a socket. The assertions are about the flags that decide
whether a real-money path may run, and about the exit code a submit result earns.
"""
from __future__ import annotations

from types import SimpleNamespace
from pathlib import Path
from typing import Any, List

import pytest

from python_analytics.exchange.binance_futures import BinanceTransportError
from python_analytics.live import OrderStore, SubmissionResult

from scripts import run_live_trader as script


# --------------------------------------------------------------------------- #
# Arguments and production arming
# --------------------------------------------------------------------------- #
def test_a_non_finite_report_age_is_refused() -> None:
    # nan/inf slip through a plain ``<= 0`` check; both must be usage errors.
    for value in ("nan", "inf", "-inf"):
        with pytest.raises(SystemExit):
            script.parse_args(["--max-report-age-hours", value])


def test_a_finite_report_age_is_accepted() -> None:
    args = script.parse_args(["--max-report-age-hours", "12"])
    assert args.max_report_age_hours == 12.0


def test_production_needs_live_except_for_the_kill_switch() -> None:
    production = SimpleNamespace(is_testnet=False, environment="production")

    assert script._production_refused(
        production, SimpleNamespace(live=False, cancel_all=False)
    )
    # Cancelling out of a position must not depend on an arming flag.
    assert not script._production_refused(
        production, SimpleNamespace(live=False, cancel_all=True)
    )
    assert not script._production_refused(
        production, SimpleNamespace(live=True, cancel_all=False)
    )
    testnet = SimpleNamespace(is_testnet=True, environment="testnet")
    assert not script._production_refused(
        testnet, SimpleNamespace(live=False, cancel_all=False)
    )


# --------------------------------------------------------------------------- #
# Submit outcome -> exit code
# --------------------------------------------------------------------------- #
def test_only_a_confirmed_post_only_crossing_counts_as_success() -> None:
    crossing = SubmissionResult(
        "cid",
        "REJECTED",
        code=script.POST_ONLY_REJECTION_CODE,
        note="venue rejected: would immediately trigger (code -2010)",
    )
    assert script._post_only_crossing_rejected(crossing)

    other_code = SubmissionResult(
        "cid", "REJECTED", code=-2019, note="venue rejected: margin is insufficient"
    )
    assert not script._post_only_crossing_rejected(other_code)

    # The same code with different text is not proof of a post-only crossing.
    same_code = SubmissionResult(
        "cid",
        "REJECTED",
        code=script.POST_ONLY_REJECTION_CODE,
        note="venue rejected: filter failure",
    )
    assert not script._post_only_crossing_rejected(same_code)

    unresolved = SubmissionResult("cid", "UNKNOWN", note="would immediately trigger")
    assert not script._post_only_crossing_rejected(unresolved)

    # USDⓈ-M futures refuse a crossing GTX order with -5022, not -2010. That code
    # exists only for a post-only order that could not rest as maker, so the
    # code alone is proof; the same code on a non-rejection still is not.
    futures = SubmissionResult(
        "cid",
        "REJECTED",
        code=-5022,
        note="venue rejected: the post only order will be rejected",
    )
    assert script._post_only_crossing_rejected(futures)
    unresolved_futures = SubmissionResult("cid", "UNKNOWN", code=-5022)
    assert not script._post_only_crossing_rejected(unresolved_futures)


# --------------------------------------------------------------------------- #
# --cancel-all: the kill path must run even when the clock cannot sync
# --------------------------------------------------------------------------- #
class _UnsyncedClient:
    time_synced = False

    def sync_time(self) -> int:
        raise BinanceTransportError("dns unreachable")


def _patch_executor(
    monkeypatch: pytest.MonkeyPatch, panic_errors: List[str]
) -> List[str]:
    """Replaces LiveExecutor with a recorder; returns the panic reasons."""

    seen: List[str] = []

    class _Executor:
        def __init__(self, client: Any, store: Any, symbol: str) -> None:
            self.symbol = symbol

        def panic(self, reason: str) -> List[str]:
            seen.append(reason)
            return list(panic_errors)

    monkeypatch.setattr(script, "LiveExecutor", _Executor)
    return seen


@pytest.mark.parametrize("panic_errors", [[], ["flatten: venue still down"]])
def test_cancel_all_runs_even_when_clock_sync_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, panic_errors: List[str]
) -> None:
    args = script.parse_args(["--cancel-all"])
    seen = _patch_executor(monkeypatch, panic_errors)
    with OrderStore(tmp_path / "orders.sqlite3") as store:
        code = script.run(
            SimpleNamespace(), args, client=_UnsyncedClient(), store=store
        )

    # A failed sync must not stop the kill path: panic still ran, and the sync
    # failure is preserved so the run cannot masquerade as a clean exit.
    assert seen == ["operator requested --cancel-all"]
    assert code == 1
