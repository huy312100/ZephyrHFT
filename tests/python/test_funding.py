"""Tests for the funding ledger."""
from __future__ import annotations

from datetime import datetime, timezone

from python_analytics.live.funding import FundingEvent, FundingLedger


def test_funding_ledger_record_and_total(tmp_path) -> None:
    db = tmp_path / "funding.sqlite3"
    ledger = FundingLedger(db)

    now = datetime.now(timezone.utc)
    ledger.record(
        FundingEvent(
            ts=now,
            symbol="BTCUSDT",
            funding_rate=0.0001,
            position_size=1.0,
            notional_usdt=80000.0,
            payment_usdt=8.0,
        )
    )
    ledger.record(
        FundingEvent(
            ts=now,
            symbol="BTCUSDT",
            funding_rate=-0.00005,
            position_size=1.0,
            notional_usdt=80000.0,
            payment_usdt=-4.0,
        )
    )

    assert ledger.total_paid_usdt("BTCUSDT") == -4.0  # +8 - 4 = +4 net received -> -4 paid
    assert ledger.total_received_usdt("BTCUSDT") == 4.0

    events = ledger.events("BTCUSDT")
    assert len(events) == 2
    assert events[0].symbol == "BTCUSDT"

    ledger.close()


def test_funding_ledger_unknown_symbol_returns_zero(tmp_path) -> None:
    db = tmp_path / "funding_empty.sqlite3"
    ledger = FundingLedger(db)
    assert ledger.total_paid_usdt("ETHUSDT") == 0.0
    assert ledger.total_received_usdt("ETHUSDT") == 0.0
    ledger.close()