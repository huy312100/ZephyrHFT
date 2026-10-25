"""Durable record of every order this process believes is live.

A dict is the wrong place for this state. The failure that costs money is not a
lost order, it is a process that restarts *believing* it is flat while a
post-only quote is still resting on the venue, or one that retries a submit the
venue already accepted. Every intent is therefore written before the request
leaves the process, keyed by the client order id the venue dedupes on, so a
restart always has something to reconcile against.

SQLite is the storage because it is in the standard library, survives
``kill -9``, and needs no server: the same file can be inspected by hand during
an incident.
"""
from __future__ import annotations

import sqlite3
import time
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

#: Statuses the venue may still know about; these are what reconciliation diffs.
ACTIVE_STATUSES = ("INTENT", "SUBMITTED", "UNKNOWN", "ADOPTED")

#: Statuses that end an order's life. Nothing may move back out of one of these:
#: a filled order that "becomes active again" is how a position gets doubled.
TERMINAL_STATUSES = ("FILLED", "CANCELLED", "EXPIRED", "REJECTED", "LOST")

KNOWN_STATUSES = ACTIVE_STATUSES + TERMINAL_STATUSES

#: Client order ids are echoed by Binance and capped at 36 characters.
MAX_CLIENT_ORDER_ID = 36

_SCHEMA = """
CREATE TABLE IF NOT EXISTS orders (
    client_order_id TEXT PRIMARY KEY,
    venue_order_id  INTEGER,
    symbol          TEXT NOT NULL,
    side            TEXT NOT NULL,
    price           TEXT NOT NULL,
    quantity        TEXT NOT NULL,
    status          TEXT NOT NULL,
    note            TEXT,
    created_at_ms   INTEGER NOT NULL,
    updated_at_ms   INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS orders_symbol_status ON orders (symbol, status);
CREATE TABLE IF NOT EXISTS events (
    id     INTEGER PRIMARY KEY AUTOINCREMENT,
    at_ms  INTEGER NOT NULL,
    kind   TEXT NOT NULL,
    detail TEXT
);
"""


class LiveStoreError(RuntimeError):
    """Raised when the store is asked to record something contradictory."""


class OrderStore:
    """Crash-safe order book of record, one row per client order id."""

    def __init__(
        self, path: Any, *, clock: Callable[[], float] = time.time
    ) -> None:
        self._path = Path(path)
        self._clock = clock
        if str(self._path.parent) not in ("", "."):
            self._path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(self._path))
        self._conn.row_factory = sqlite3.Row
        with self._conn:
            self._conn.executescript(_SCHEMA)

    # ------------------------------------------------------------- lifecycle
    @property
    def path(self) -> Path:
        return self._path

    def close(self) -> None:
        self._conn.close()

    def __enter__(self) -> "OrderStore":
        return self

    def __exit__(self, *exc_info: Any) -> None:
        self.close()

    def _now_ms(self) -> int:
        return int(self._clock() * 1000)

    # --------------------------------------------------------------- writing
    def record_intent(
        self,
        *,
        client_order_id: str,
        symbol: str,
        side: str,
        price: Any,
        quantity: Any,
        note: Optional[str] = None,
    ) -> None:
        """Writes the intent *before* the order is sent.

        A duplicate id is an error, not an update: reusing an id is exactly the
        signal that two code paths believe they own the same order.
        """

        cid = str(client_order_id or "")
        if not cid:
            raise LiveStoreError("client_order_id must not be empty")
        if len(cid) > MAX_CLIENT_ORDER_ID:
            raise LiveStoreError(
                f"client_order_id exceeds {MAX_CLIENT_ORDER_ID} characters: {cid!r}"
            )
        now = self._now_ms()
        try:
            with self._conn:
                self._conn.execute(
                    "INSERT INTO orders (client_order_id, venue_order_id, symbol, side,"
                    " price, quantity, status, note, created_at_ms, updated_at_ms)"
                    " VALUES (?, NULL, ?, ?, ?, ?, 'INTENT', ?, ?, ?)",
                    (
                        cid,
                        str(symbol).upper(),
                        str(side).upper(),
                        str(price),
                        str(quantity),
                        note,
                        now,
                        now,
                    ),
                )
        except sqlite3.IntegrityError as exc:
            raise LiveStoreError(f"client order id already recorded: {cid}") from exc

    def mark_status(
        self,
        client_order_id: str,
        status: str,
        *,
        venue_order_id: Optional[int] = None,
        note: Optional[str] = None,
    ) -> None:
        """Moves an order to ``status``, refusing to resurrect a dead order."""

        normalised = str(status or "").strip().upper()
        if normalised not in KNOWN_STATUSES:
            raise LiveStoreError(
                f"unknown order status {status!r}; expected one of "
                + ", ".join(KNOWN_STATUSES)
            )
        with self._conn:
            row = self._conn.execute(
                "SELECT status FROM orders WHERE client_order_id = ?",
                (str(client_order_id),),
            ).fetchone()
            if row is None:
                raise LiveStoreError(f"unknown client order id: {client_order_id}")
            current = str(row["status"])
            if current in TERMINAL_STATUSES and normalised in ACTIVE_STATUSES:
                raise LiveStoreError(
                    f"refusing to revive {client_order_id}: {current} -> {normalised}"
                )
            self._conn.execute(
                "UPDATE orders SET status = ?, note = COALESCE(?, note),"
                " venue_order_id = COALESCE(?, venue_order_id), updated_at_ms = ?"
                " WHERE client_order_id = ?",
                (
                    normalised,
                    note,
                    None if venue_order_id is None else int(venue_order_id),
                    self._now_ms(),
                    str(client_order_id),
                ),
            )

    def record_event(self, kind: str, detail: Optional[str] = None) -> None:
        with self._conn:
            self._conn.execute(
                "INSERT INTO events (at_ms, kind, detail) VALUES (?, ?, ?)",
                (self._now_ms(), str(kind), detail),
            )

    # --------------------------------------------------------------- reading
    def get(self, client_order_id: str) -> Optional[Dict[str, Any]]:
        row = self._conn.execute(
            "SELECT * FROM orders WHERE client_order_id = ?",
            (str(client_order_id),),
        ).fetchone()
        return dict(row) if row is not None else None

    def active_orders(self, symbol: Optional[str] = None) -> List[Dict[str, Any]]:
        query = "SELECT * FROM orders WHERE status IN ({})".format(
            ", ".join("?" for _ in ACTIVE_STATUSES)
        )
        params: List[Any] = list(ACTIVE_STATUSES)
        if symbol is not None:
            query += " AND symbol = ?"
            params.append(str(symbol).upper())
        rows = self._conn.execute(
            query + " ORDER BY created_at_ms", params
        ).fetchall()
        return [dict(row) for row in rows]

    def orders(
        self, symbol: Optional[str] = None, limit: int = 50
    ) -> List[Dict[str, Any]]:
        query = "SELECT * FROM orders"
        params: List[Any] = []
        if symbol is not None:
            query += " WHERE symbol = ?"
            params.append(str(symbol).upper())
        query += " ORDER BY created_at_ms DESC LIMIT ?"
        params.append(int(limit))
        return [dict(row) for row in self._conn.execute(query, params).fetchall()]

    def events(self, limit: int = 50) -> List[Dict[str, Any]]:
        rows = self._conn.execute(
            "SELECT * FROM events ORDER BY id DESC LIMIT ?", (int(limit),)
        ).fetchall()
        return [dict(row) for row in rows]
