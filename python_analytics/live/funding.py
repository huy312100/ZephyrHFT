"""Funding ledger for perpetual-swap sessions.

Tracks funding payments (the periodic peer-to-peer funding rate on perpetual
futures) so a live session can see whether apparent flatness is hiding a
funding drain. A session that looks flat can still be losing to funding:
this ledger makes it visible.

The ledger is append-only: each funding event is recorded with its timestamp,
rate, position size at the time, and the resulting payment. Nothing here
sends alerts -- that is :mod:`python_analytics.live.alerts`.
"""
from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional


@dataclass(frozen=True)
class FundingEvent:
    """One funding payment recorded at funding time."""

    ts: datetime
    symbol: str
    funding_rate: float  # e.g. 0.0001 = +0.01% (longs pay shorts)
    position_size: float  # base units, signed (positive = long)
    notional_usdt: float  # quote currency at funding time
    payment_usdt: float  # position_size * funding_rate * price (positive = received)


class FundingLedger:
    """Append-only SQLite ledger of funding payments.

    Writes to ``path`` (created on first use). All amounts are in quote
    currency (e.g. USDT). Positive payment means the account received funding;
    negative means it paid.
    """

    def __init__(self, path: str | Path = "state/funding.sqlite3") -> None:
        self._path = Path(path)
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(self._path))
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS funding_events (
                ts TEXT NOT NULL,
                symbol TEXT NOT NULL,
                funding_rate REAL NOT NULL,
                position_size REAL NOT NULL,
                notional_usdt REAL NOT NULL,
                payment_usdt REAL NOT NULL
            )
            """
        )
        self._conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_funding_ts ON funding_events(ts)"
        )
        self._conn.commit()

    def record(self, event: FundingEvent) -> None:
        """Append one funding payment to the ledger."""
        self._conn.execute(
            """
            INSERT INTO funding_events (ts, symbol, funding_rate, position_size,
                                        notional_usdt, payment_usdt)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                event.ts.isoformat(),
                event.symbol,
                event.funding_rate,
                event.position_size,
                event.notional_usdt,
                event.payment_usdt,
            ),
        )
        self._conn.commit()

    def total_paid_usdt(self, symbol: str | None = None,
                        since: Optional[datetime] = None) -> float:
        """Net funding paid (positive = paid out) for a symbol/time range."""
        where: list[str] = []
        params: list = []
        if symbol is not None:
            where.append("symbol = ?")
            params.append(symbol)
        if since is not None:
            where.append("ts >= ?")
            params.append(since.isoformat())
        sql = "SELECT COALESCE(SUM(-payment_usdt), 0.0) FROM funding_events"
        if where:
            sql += " WHERE " + " AND ".join(where)
        row = self._conn.execute(sql, params).fetchone()
        return float(row[0] if row else 0.0)

    def total_received_usdt(self, symbol: str | None = None,
                            since: Optional[datetime] = None) -> float:
        """Net funding received (positive = received) for a symbol/time range."""
        where: list[str] = []
        params: list = []
        if symbol is not None:
            where.append("symbol = ?")
            params.append(symbol)
        if since is not None:
            where.append("ts >= ?")
            params.append(since.isoformat())
        sql = "SELECT COALESCE(SUM(payment_usdt), 0.0) FROM funding_events"
        if where:
            sql += " WHERE " + " AND ".join(where)
        row = self._conn.execute(sql, params).fetchone()
        return float(row[0] if row else 0.0)

    def events(self, symbol: str | None = None,
               since: Optional[datetime] = None) -> list[FundingEvent]:
        """Return funding events, newest first."""
        where: list[str] = []
        params: list = []
        if symbol is not None:
            where.append("symbol = ?")
            params.append(symbol)
        if since is not None:
            where.append("ts >= ?")
            params.append(since.isoformat())
        sql = ("SELECT ts, symbol, funding_rate, position_size, notional_usdt, "
               "payment_usdt FROM funding_events")
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY ts DESC"
        rows = self._conn.execute(sql, params).fetchall()
        return [
            FundingEvent(
                ts=datetime.fromisoformat(r[0]),
                symbol=r[1],
                funding_rate=r[2],
                position_size=r[3],
                notional_usdt=r[4],
                payment_usdt=r[5],
            )
            for r in rows
        ]

    def close(self) -> None:
        self._conn.close()

    def __enter__(self) -> "FundingLedger":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()