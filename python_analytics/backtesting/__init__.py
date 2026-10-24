"""Backtesting primitives for the native engine."""
from .metrics import (
    fill_rows,
    max_drawdown,
    quant_summary,
    round_trips,
    sharpe_ratio,
    sortino_ratio,
    summarize,
)
from .replay import (
    BacktestEngine,
    BacktestResult,
    BulkReplayer,
    ReplaySummary,
    iter_jsonl,
    replay_archive,
    replay_archives,
    replay_file,
    replay_lines,
    seed_from_snapshot,
)
from .report import export_report, write_csv, write_equity_png, write_json

__all__ = [
    "BacktestEngine",
    "BacktestResult",
    "BulkReplayer",
    "ReplaySummary",
    "export_report",
    "fill_rows",
    "iter_jsonl",
    "max_drawdown",
    "quant_summary",
    "replay_archive",
    "replay_archives",
    "replay_file",
    "replay_lines",
    "round_trips",
    "seed_from_snapshot",
    "sharpe_ratio",
    "sortino_ratio",
    "summarize",
    "write_csv",
    "write_equity_png",
    "write_json",
]
