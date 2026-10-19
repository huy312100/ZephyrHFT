"""Backtesting primitives for the native engine."""
from .metrics import fill_rows, max_drawdown, sharpe_ratio, summarize
from .replay import BacktestResult, iter_jsonl, replay_file, replay_lines

__all__ = [
    "BacktestResult",
    "fill_rows",
    "iter_jsonl",
    "max_drawdown",
    "replay_file",
    "replay_lines",
    "sharpe_ratio",
    "summarize",
]
