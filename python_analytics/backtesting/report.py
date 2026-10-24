"""Backtest report export: JSON, CSV and optional PNG charts.

Charts use ``matplotlib`` when available and degrade silently to JSON/CSV-only
otherwise, so the export path never throws on a headless build.
"""
from __future__ import annotations

import csv
import json
import math
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence

__all__ = ["export_report", "write_csv", "write_equity_png", "write_json"]


def _sanitize(value: Any) -> Any:
    """Maps non-finite floats to ``None`` so the payload stays JSON-safe."""
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    if isinstance(value, dict):
        return {key: _sanitize(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_sanitize(item) for item in value]
    return value


def write_json(payload: Mapping[str, Any], path: Path) -> Path:
    """Serialises ``payload`` to a pretty-printed JSON file."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(_sanitize(dict(payload)), indent=2, sort_keys=True),
                    encoding="utf-8")
    return path


def write_csv(rows: Sequence[Mapping[str, Any]], path: Path) -> Path:
    """Writes a sequence of flat mappings as CSV (field order from first row)."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    materialized = [dict(row) for row in rows]
    if not materialized:
        path.write_text("", encoding="utf-8")
        return path
    fieldnames = list(materialized[0].keys())
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in materialized:
            writer.writerow({key: _sanitize(row.get(key)) for key in fieldnames})
    return path


def write_equity_png(equity_curve: Sequence[float], path: Path) -> bool:
    """Writes an equity curve + drawdown chart; ``False`` if matplotlib is absent."""
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        return False

    curve = [float(value) for value in equity_curve]
    if not curve:
        return False

    peaks: List[float] = []
    peak = -math.inf
    for value in curve:
        peak = max(peak, value)
        peaks.append(peak)

    fig, (ax_top, ax_bottom) = plt.subplots(2, 1, figsize=(10, 6), sharex=True)
    ax_top.plot(curve, label="equity", linewidth=1.0)
    ax_top.set_ylabel("equity (quote)")
    ax_top.grid(alpha=0.3)
    ax_top.legend(loc="best")

    underwater = [0.0 if peak <= 0 else (value - peak) / peak for value, peak
                  in zip(curve, peaks)]
    ax_bottom.fill_between(range(len(underwater)), underwater, 0.0,
                           color="tab:red", alpha=0.4)
    ax_bottom.set_ylabel("drawdown")
    ax_bottom.set_xlabel("frame")
    ax_bottom.grid(alpha=0.3)

    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.tight_layout()
    fig.savefig(path, dpi=120)
    plt.close(fig)
    return True


def export_report(directory: Path, *, summary: Mapping[str, Any],
                  fills: Sequence[Mapping[str, Any]],
                  equity_curve: Sequence[float],
                  grid: Optional[Sequence[Mapping[str, Any]]] = None,
                  plot: bool = False) -> Dict[str, str]:
    """Writes ``summary.json``, ``fills.csv``, optional ``grid.csv`` and PNGs.

    Returns a mapping of artefact name → absolute path written.
    """
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    written: Dict[str, str] = {}

    written["summary"] = str(write_json(dict(summary), directory / "summary.json"))
    written["fills"] = str(write_csv(fills, directory / "fills.csv"))
    if grid:
        written["grid"] = str(write_csv(grid, directory / "grid.csv"))
    if plot:
        png = directory / "equity.png"
        if write_equity_png(equity_curve, png):
            written["equity_png"] = str(png)
    return written