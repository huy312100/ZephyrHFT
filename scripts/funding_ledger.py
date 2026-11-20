#!/usr/bin/env python3
"""Query the funding ledger for a live session.

Shows total funding paid/received per symbol and lists recent events.
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from python_analytics.live.funding import FundingLedger


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Query the funding ledger for live sessions."
    )
    parser.add_argument(
        "--db", default="state/funding.sqlite3",
        help="Path to the funding ledger SQLite database.",
    )
    parser.add_argument(
        "--symbol", default=None,
        help="Filter by symbol (e.g. BTCUSDT).",
    )
    parser.add_argument(
        "--hours", type=int, default=24,
        help="Show events from the last N hours (default 24).",
    )
    args = parser.parse_args(argv)

    ledger = FundingLedger(args.db)
    try:
        since = datetime.now(timezone.utc) - timedelta(hours=args.hours)

        paid = ledger.total_paid_usdt(args.symbol, since)
        received = ledger.total_received_usdt(args.symbol, since)
        net = received - paid

        symbol_label = args.symbol or "all symbols"
        print(f"Funding ledger ({symbol_label}, last {args.hours}h):")
        print(f"  Total paid:    {paid:+.4f} USDT")
        print(f"  Total received:{received:+.4f} USDT")
        print(f"  Net:           {net:+.4f} USDT")

        events = ledger.events(args.symbol, since)
        if events:
            print(f"\n  Recent events ({len(events)}):")
            for ev in events[:10]:
                direction = "received" if ev.payment_usdt >= 0 else "paid"
                print(
                    f"    {ev.ts.isoformat()}  {ev.symbol}  "
                    f"rate={ev.funding_rate:+.6f}  "
                    f"pos={ev.position_size:+.4f}  "
                    f"{direction} {abs(ev.payment_usdt):.4f} USDT"
                )
        else:
            print("\n  No funding events in the selected window.")
    finally:
        ledger.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())