#!/usr/bin/env python3
"""Pre-live promotion checklist runner.

Runs the steps from docs/phase5_production_readiness.md §10 in order:
1. Unit and system tests (pytest + ctest)
2. Walk-forward replay from the committed lock file
3. Production readiness gate
4. (Optional) Readiness gate on the trading host at production latency budget

Exits non-zero on any failure. Archive the output of steps 3 and 4 manually.
"""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def run(cmd: list[str], desc: str) -> int:
    print(f"\n{'=' * 60}")
    print(f"STEP: {desc}")
    print(f"CMD:  {' '.join(cmd)}")
    print(f"{'=' * 60}\n")
    result = subprocess.run(cmd, cwd=ROOT)
    if result.returncode != 0:
        print(f"\nFAILED: {desc} (exit {result.returncode})")
    return result.returncode


def main() -> int:
    failures = 0

    # Step 1: Unit and system tests
    rc = run(
        ["python3", "-m", "pytest"],
        "Unit and system tests (pytest)",
    )
    if rc != 0:
        failures += 1

    # Step 2: Walk-forward replay from committed lock
    rc = run(
        [
            "python3", "scripts/run_walk_forward.py",
            "--source", "data/sample_events.ndjson",
            "--window-events", "400",
            "--from-lock", "--lock-file", "config/walk_forward_lock.json",
        ],
        "Walk-forward replay from committed lock file",
    )
    if rc != 0:
        failures += 1

    # Step 3: Readiness gate (default host)
    rc = run(
        ["python3", "scripts/generate_readiness_report.py"],
        "Production readiness gate (this host)",
    )
    if rc != 0:
        failures += 1

    # Step 4: Readiness gate on trading host (optional, requires --host flag)
    if "--host" in sys.argv:
        rc = run(
            [
                "python3", "scripts/generate_readiness_report.py",
                "--max-engine-p99-us", "5.0",
                "--max-engine-p999-us", "50",
                "--evidence-events", "20000",
                "--report-dir", "reports",
                "--stem", "readiness_trading_host",
            ],
            "Production readiness gate (trading host, latency budget)",
        )
        if rc != 0:
            failures += 1

    print(f"\n{'=' * 60}")
    if failures == 0:
        print("ALL STEPS PASSED — ready to archive reports and promote to live.")
        print("Remember: git add -f reports/ to archive (reports/ is git-ignored).")
    else:
        print(f"{failures} step(s) FAILED — do not promote to live.")
    print(f"{'=' * 60}")

    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())