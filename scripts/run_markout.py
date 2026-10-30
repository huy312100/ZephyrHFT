#!/usr/bin/env python3
"""Phase 5 markout runner: is the maker quote earning the spread or being run over?

A walk-forward reports what a strategy earned. This reports *why*: every fill is
marked against the mid that followed it, so spread capture and adverse selection
stop hiding behind a single PnL number. A study whose fees explain the whole loss
and one whose fills are picked off both end in red, and they require opposite
fixes.

Two modes:

* **replay** (default) — replays the same archives, the same frozen parameter
  lock and the same window slicing as ``run_walk_forward.py`` and marks the
  fills it produces. Simulated fills carry a ``steady_clock`` stamp, so they can
  only be measured inside the replay; there is no post-hoc join to fake.
* **--fills FILE** — marks venue-timestamped fills (an export from a real
  session) against ``--source`` captures. Both sides must share the venue clock.

Exit codes: ``0`` verdict GO, ``1`` COSTLY / MARGINAL / ADVERSE_SELECTION,
``2`` usage or missing input, ``3`` INCONCLUSIVE (too few marked fills).
"""
from __future__ import annotations

import argparse
import json
import math
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from python_analytics.backtesting.markouts import (  # noqa: E402
    HORIZONS_S,
    MarkoutConfig,
    MarkoutReplayer,
    build_timeline_from_files,
    horizon_label,
    load_fills,
    markout_verdict,
    render_markdown,
    resolve_markouts,
    summarize_markouts,
)
from python_analytics.backtesting.report import write_csv, write_json  # noqa: E402
from python_analytics.backtesting.walk_forward import (  # noqa: E402
    LockedParameters,
    WalkForwardRunner,
    read_lock_file,
    resolve_sources,
)
from python_analytics.utils import configure_logging  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SOURCE = ROOT / "data" / "sample_events.ndjson"
DEFAULT_LOCK = ROOT / "config" / "walk_forward_lock.json"
DEFAULT_REPORT_DIR = ROOT / "reports"

EXIT_GO = 0
EXIT_FAILED = 1
EXIT_USAGE = 2
EXIT_INCONCLUSIVE = 3

#: A verdict maps to an exit code: only GO is allowed to look like success.
VERDICT_EXIT = {
    "GO": EXIT_GO,
    "COSTLY": EXIT_FAILED,
    "MARGINAL": EXIT_FAILED,
    "ADVERSE_SELECTION": EXIT_FAILED,
    "INCONCLUSIVE": EXIT_INCONCLUSIVE,
}


def parse_horizons(text: str) -> Tuple[float, ...]:
    """Parses ``"0,1,10,60"`` into a tuple of seconds."""
    horizons: List[float] = []
    for chunk in str(text).replace(" ", "").split(","):
        if not chunk:
            continue
        try:
            value = float(chunk)
        except ValueError as exc:
            raise ValueError(
                f"bad horizon {chunk!r}; expected seconds like 1 or 0.5") from exc
        if not math.isfinite(value):
            raise ValueError(f"horizon {chunk!r} is not a finite number")
        if value < 0.0:
            raise ValueError(f"horizon {value} is negative")
        horizons.append(value)
    if not horizons:
        raise ValueError("--horizons needs at least one value")
    return tuple(horizons)


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--source", nargs="+", default=[str(DEFAULT_SOURCE)],
                        help="captures in chronological order; globs expand in sorted "
                             "order (default: data/sample_events.ndjson)")
    parser.add_argument("--lock-file", default=str(DEFAULT_LOCK),
                        help="frozen parameter lock the fills are produced under "
                             "(default: config/walk_forward_lock.json)")
    parser.add_argument("--window-events", type=int, default=0,
                        help="events per window; 0 = one window per archive. Match the "
                             "study to mark exactly the study's fills")
    parser.add_argument("--frames-per-hook", type=int, default=1,
                        help="frames per strategy hook; bounds markout resolution "
                             "(default 1 = every frame)")
    parser.add_argument("--fill-model", default="touch",
                        choices=("touch", "queue", "trade"),
                        help="fill model used to reproduce the fills; match the "
                             "study's --fill-model (default: touch)")
    parser.add_argument("--fills", default=None,
                        help="JSON of venue-timestamped fills; skips the replay and marks "
                             "these against --source instead")
    parser.add_argument("--clock", default="ms", choices=("ms", "ns"),
                        help="clock of an unqualified 'timestamp' field in --fills")
    parser.add_argument("--symbol", default=None,
                        help="market to build the timeline for (default: first seen)")
    parser.add_argument("--horizons", default=",".join(f"{value:g}" for value in HORIZONS_S),
                        help="markout horizons in seconds (default: 0,1,10,60)")
    parser.add_argument("--tolerance-ms", type=float, default=2000.0,
                        help="max distance between a horizon target and the sample used; "
                             "wider gaps are reported as truncated, never interpolated")
    parser.add_argument("--min-samples", type=int, default=30,
                        help="marked maker fills required before a verdict (default 30)")
    parser.add_argument("--min-edge-bps", type=float, default=0.0,
                        help="net edge after fees required for GO (default 0.0)")
    parser.add_argument("--maker-bps", type=float, default=None,
                        help="maker fee charged against markout (default: the lock's)")
    parser.add_argument("--taker-bps", type=float, default=None,
                        help="taker fee reported for taker fills (default: the lock's)")
    parser.add_argument("--report-dir", default=str(DEFAULT_REPORT_DIR))
    parser.add_argument("--stem", default="markout")
    parser.add_argument("--no-report", action="store_true")
    parser.add_argument("--log-level", default="WARNING")
    return parser.parse_args(argv)


def build_config(args: argparse.Namespace,
                 parameters: LockedParameters) -> MarkoutConfig:
    """Markout settings, defaulting the fee to the one the study was priced at."""
    maker_bps = parameters.maker_bps if args.maker_bps is None else args.maker_bps
    taker_bps = parameters.taker_bps if args.taker_bps is None else args.taker_bps
    return MarkoutConfig(
        horizons_s=parse_horizons(args.horizons),
        tolerance_s=max(1e-9, float(args.tolerance_ms) / 1000.0),
        maker_bps=float(maker_bps),
        taker_bps=float(taker_bps),
        min_samples=int(args.min_samples),
        min_edge_bps=float(args.min_edge_bps),
    )


def markouts_from_replay(args: argparse.Namespace, parameters: LockedParameters,
                         config: MarkoutConfig) -> Tuple[List[Any], List[Dict[str, Any]]]:
    """Reproduces the study's fills and marks each one inside its own window.

    The slicing is the walk-forward's own, so a run with the same
    ``--window-events`` marks exactly the fills the study traded. Every window
    gets a fresh replayer (and therefore a fresh timeline), which also makes a
    window boundary impossible to mistake for a price move.
    """
    paths = resolve_sources(args.source)
    runner = WalkForwardRunner(parameters, window_events=args.window_events,
                               fill_model=args.fill_model)
    rows: List[Any] = []
    windows: List[Dict[str, Any]] = []
    for path, offset, lines in runner.iter_slices(paths):
        replayer = MarkoutReplayer(
            config=config,
            tick_size=parameters.tick_size,
            center=parameters.center,
            maker_bps=parameters.maker_bps,
            taker_bps=parameters.taker_bps,
            initial_cash=parameters.initial_cash,
            frames_per_hook=args.frames_per_hook,
            order_quantity=parameters.order_quantity,
            fill_model=args.fill_model,
        )
        replayer.replay_lines(lines, parameters.build_strategy())
        window_rows = replayer.markout_rows()
        rows.extend(window_rows)
        info: Dict[str, Any] = {
            "label": f"{path.name}#{offset}",
            "events": len(lines),
            **replayer.diagnostics(),
            "marked": sum(1 for row in window_rows if row.matched),
            "unmatched": sum(1 for row in window_rows if not row.matched),
        }
        windows.append(info)
    return rows, windows


def markouts_from_files(args: argparse.Namespace,
                        config: MarkoutConfig) -> Tuple[List[Any], List[Dict[str, Any]]]:
    """Marks venue-timestamped fills against the capture on the venue clock.

    A fill export may cover several markets; only the fills whose symbol matches
    the timeline's are marked, and the number dropped is reported. Marking a BTC
    fill against an ETH mid would produce a confident, meaningless number.
    """
    timeline = build_timeline_from_files(resolve_sources(args.source),
                                         symbol=args.symbol)
    loaded = load_fills(args.fills, clock=args.clock)
    market = (timeline.symbol or "").upper()
    fills = loaded
    dropped = 0
    if market:
        fills = [fill for fill in loaded
                 if fill.symbol is None or fill.symbol == market]
        dropped = len(loaded) - len(fills)
    rows = resolve_markouts(fills, timeline, config)
    info = {
        "label": str(args.fills),
        "events": 0,
        "timeline_symbol": timeline.symbol,
        "timeline_samples": len(timeline),
        "timeline_span_s": timeline.span_s,
        "timeline_max_gap_s": timeline.max_gap_s,
        "timeline_first_ts_ns": timeline.first_ts_ns,
        "timeline_last_ts_ns": timeline.last_ts_ns,
        "timeline_ignored_frames": timeline.ignored,
        "fills_loaded": len(loaded),
        "fills_dropped_other_symbol": dropped,
        "fills": len(fills),
        "marked": sum(1 for row in rows if row.matched),
        "unmatched": sum(1 for row in rows if not row.matched),
        "frames_per_hook": None,
    }
    return rows, [info]


def build_payload(args: argparse.Namespace, parameters: LockedParameters,
                  config: MarkoutConfig, summary: Dict[str, Any],
                  verdict: Dict[str, Any],
                  windows: Sequence[Mapping[str, Any]]) -> Dict[str, Any]:
    """The JSON artefact: inputs, settings, evidence and the verdict."""
    return {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "inputs": {
            "mode": "fills" if args.fills else "replay",
            "fill_model": str(args.fill_model),
            "sources": [str(path) for path in resolve_sources(args.source)],
            "lock_file": str(args.lock_file),
            "lock_fingerprint": parameters.fingerprint,
            "window_events": int(args.window_events),
            "frames_per_hook": int(args.frames_per_hook),
            "fills_file": None if not args.fills else str(args.fills),
        },
        "config": config.as_dict(),
        "summary": summary,
        "verdict": verdict,
        "windows": [dict(window) for window in windows],
    }


def _print_table(title: str, block: Mapping[str, Any],
                 config: MarkoutConfig, fee_bps: float) -> None:
    print(f"  {title}")
    print(f"    {'horizon':>7} {'fills':>6} {'mean bps':>9} {'median':>9} "
          f"{'sd':>8} {'t':>9} {'win %':>7} {'net bps':>9} {'USDT/fill':>11}")
    horizons = block.get("horizons") or {}
    for horizon in config.horizons_s:
        label = horizon_label(horizon)
        stats = horizons.get(label) or {}
        samples = int(stats.get("samples", 0) or 0)
        mean = float(stats.get("mean_bps", 0.0) or 0.0)
        print(f"    {label:>7} {samples:>6} {mean:>+9.3f} "
              f"{float(stats.get('median_bps', 0.0) or 0.0):>+9.3f} "
              f"{float(stats.get('stdev_bps', 0.0) or 0.0):>8.3f} "
              f"{float(stats.get('t_stat', 0.0) or 0.0):>+9.2f} "
              f"{float(stats.get('win_rate_pct', 0.0) or 0.0):>7.1f} "
              f"{mean - fee_bps:>+9.3f} "
              f"{float(stats.get('usdt_per_fill', 0.0) or 0.0):>+11.6f}")


def run(args: argparse.Namespace) -> int:
    parameters = read_lock_file(args.lock_file)
    config = build_config(args, parameters)

    print("=" * 100)
    print(f"  markout mode  : {'fills file' if args.fills else 'replay'}"
          f"  (lock {parameters.fingerprint} from {args.lock_file})")
    print(f"  horizons      : {', '.join(horizon_label(h) for h in config.horizons_s)}"
          f"  tolerance {config.tolerance_s:g} s  maker fee {config.maker_bps:g} bps")
    if args.fills:
        print(f"  fills         : {args.fills} (clock={args.clock})")
    else:
        print(f"  replay        : {args.fill_model} fill model"
              f"  window_events {args.window_events or 'per-archive'}"
              f"  frames_per_hook {args.frames_per_hook}")
    print("-" * 100)

    if args.fills:
        rows, windows = markouts_from_files(args, config)
    else:
        rows, windows = markouts_from_replay(args, parameters, config)

    summary = summarize_markouts(rows, config)
    verdict = markout_verdict(summary, config)
    fills = summary["fills"]
    if args.fills:
        dropped = int(windows[0].get("fills_dropped_other_symbol", 0) or 0)
        if dropped:
            print(f"  dropped       : {dropped} fill(s) for a market other than "
                  f"{windows[0].get('timeline_symbol')}")
    print(f"  fills         : {fills['total']} total, {fills['maker']} maker, "
          f"{fills['taker']} taker, {fills['unmatched']} without a mid")
    print(f"  entry         : maker spread {summary['maker']['entry_spread_bps']:.3f} bps, "
          f"offset {summary['maker']['entry_offset_bps']:+.3f} bps "
          f"(negative = filled through the mid)")
    print("  truncated     : " + ", ".join(
        f"{label}={count}" for label, count in fills["truncated"].items()))
    print("-" * 100)
    _print_table("maker", summary["maker"], config, config.maker_bps)
    if fills["taker"]:
        _print_table("taker", summary["taker"], config, config.taker_bps)
    print("-" * 100)
    print(f"  verdict       : {verdict['verdict']} at {verdict['primary_horizon']}"
          f"  (n={verdict['samples']}, mean {verdict['mean_markout_bps']:+.3f} bps,"
          f" net {verdict['net_edge_bps']:+.3f} bps, t={verdict['t_stat']:+.2f})")
    for reason in verdict["reasons"]:
        print(f"    - {reason}")

    if not args.no_report:
        report_dir = Path(args.report_dir)
        payload = build_payload(args, parameters, config, summary, verdict, windows)
        json_path = write_json(payload, report_dir / f"{args.stem}.json")
        csv_path = write_csv([row.as_dict() for row in rows],
                             report_dir / f"{args.stem}_fills.csv")
        markdown_path = report_dir / f"{args.stem}.md"
        markdown_path.parent.mkdir(parents=True, exist_ok=True)
        markdown_path.write_text(
            "\n".join(render_markdown(summary, verdict,
                                      fill_model=None if args.fills else args.fill_model))
            + "\n",
            encoding="utf-8")
        print("-" * 100)
        print(f"  wrote: {json_path}")
        print(f"  wrote: {markdown_path}")
        print(f"  wrote: {csv_path}")
    print("=" * 100)
    return VERDICT_EXIT.get(str(verdict["verdict"]), EXIT_FAILED)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    configure_logging(args.log_level)
    try:
        return run(args)
    except (FileNotFoundError, ValueError, json.JSONDecodeError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return EXIT_USAGE


if __name__ == "__main__":
    raise SystemExit(main())
