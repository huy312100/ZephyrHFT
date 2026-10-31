#!/usr/bin/env python3
"""Phase 5 edge-curve runner: what does a resting quote earn at each distance?

A walk-forward says what the strategy earned; a markout says whether its fills
paid for themselves. Neither answers the question that decides whether a maker
is viable on an instrument at all: **how far from the mid must a quote rest to
clear the fee, and how often does the tape reach that distance?**

This runner answers it directly from a capture, with no strategy in the loop. For
every print, an order resting ``d`` basis points on the passive side would have
filled if the print traded at or through it; the fill is marked from the quote's
own price at each horizon, and the fee is subtracted. The output is a curve of
net edge against distance, and the fill rate still available out there.

Exit codes: ``0`` a distance clears the fee, ``1`` none does, ``2`` usage or
missing input, ``3`` no print could be priced (insufficient data for a verdict).
"""
from __future__ import annotations

import argparse
import sys
import time
import zlib
from pathlib import Path
from typing import List, Optional, Sequence

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from python_analytics.backtesting.edge_curve import (  # noqa: E402
    DEFAULT_DISTANCES_BPS,
    DEFAULT_HORIZONS_S,
    EdgeCurveConfig,
    edge_curve_verdict,
    measure_edge_curve,
    render_markdown,
    scan_tape,
)
from python_analytics.backtesting.markouts import (  # noqa: E402
    build_timeline_from_files,
    iter_capture_lines,
)
from python_analytics.backtesting.report import write_csv, write_json  # noqa: E402
from python_analytics.backtesting.walk_forward import resolve_sources  # noqa: E402
from python_analytics.costs import DEFAULT_MAKER_BPS  # noqa: E402
from python_analytics.utils import configure_logging  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SOURCE = ROOT / "data" / "sample_events.ndjson"
DEFAULT_REPORT_DIR = ROOT / "reports"

EXIT_CLEARS = 0
EXIT_NO_CLEARING_DISTANCE = 1
EXIT_USAGE = 2
EXIT_NO_DATA = 3


def parse_float_list(text: str) -> List[float]:
    """Comma-separated floats, refusing an empty or unparseable list."""
    values: List[float] = []
    for chunk in str(text).split(","):
        chunk = chunk.strip()
        if not chunk:
            continue
        try:
            values.append(float(chunk))
        except ValueError as exc:
            raise argparse.ArgumentTypeError(f"{chunk!r} is not a number") from exc
    if not values:
        raise argparse.ArgumentTypeError("expected at least one number")
    return values


#: Everything a capture read can throw at us while *reading* it. A truncated
#: ``.gz`` roll raises ``EOFError``, a bad header or CRC raises
#: ``gzip.BadGzipFile`` (an ``OSError``), a corrupt deflate body raises
#: ``zlib.error`` (which is **not** an ``OSError``), and a vanished or unreadable
#: file raises ``FileNotFoundError`` / ``PermissionError``. All of them are input
#: defects, so none may escape as a traceback and exit ``1`` -- the code this
#: runner reserves for "no distance clears".
READ_ERRORS = (OSError, EOFError, ValueError, zlib.error)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__.splitlines()[0],
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--source", nargs="+", default=[str(DEFAULT_SOURCE)],
                        help="capture file(s) carrying depth and trade frames; "
                             "several files are read in the order given, as "
                             "consecutive roll segments")
    parser.add_argument("--symbol", default=None,
                        help="only price prints for this market (default: the "
                             "first symbol the capture names)")
    parser.add_argument("--distances-bps",
                        default=",".join(f"{value:g}" for value in DEFAULT_DISTANCES_BPS),
                        help="resting distances from the mid, in bps")
    parser.add_argument("--horizons",
                        default=",".join(f"{value:g}" for value in DEFAULT_HORIZONS_S),
                        help="markout horizons in seconds")
    parser.add_argument("--tolerance-ms", type=float, default=2000.0,
                        help="max distance between a horizon target and the "
                             "sample used; past it the horizon is dropped")
    parser.add_argument("--maker-bps", type=float, default=None,
                        help="maker fee the curve is charged against "
                             "(default: python_analytics.costs)")
    parser.add_argument("--tick-size", type=float, default=0.0,
                        help="venue price increment for the hypothetical quote; "
                             "quantisation only ever widens, so 0 is the "
                             "permissive choice and a venue claim needs the real "
                             "grid")
    parser.add_argument("--min-samples", type=int, default=30,
                        help="fills a distance needs before it can clear the fee")
    parser.add_argument("--report-dir", default=str(DEFAULT_REPORT_DIR))
    parser.add_argument("--stem", default="edge_curve")
    parser.add_argument("--no-report", action="store_true",
                        help="print the verdict only, write no artefacts")
    parser.add_argument("--log-level", default="WARNING")
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    configure_logging(level=args.log_level)

    try:
        distances = parse_float_list(args.distances_bps)
        horizons = parse_float_list(args.horizons)
        config = EdgeCurveConfig(
            distances_bps=tuple(distances),
            horizons_s=tuple(horizons),
            tolerance_s=float(args.tolerance_ms) / 1000.0,
            maker_bps=(float(args.maker_bps) if args.maker_bps is not None
                       else DEFAULT_MAKER_BPS),
            tick_size=float(args.tick_size),
            min_samples=int(args.min_samples),
        )
    except (ValueError, argparse.ArgumentTypeError) as exc:
        # ``ArgumentTypeError`` is not a ``ValueError``, and a bare traceback
        # would exit 1 -- the code this runner reserves for "no distance clears".
        print(f"usage: {exc}", file=sys.stderr)
        return EXIT_USAGE
    try:
        sources = resolve_sources(args.source)
    except (FileNotFoundError, ValueError) as exc:
        print(f"usage: {exc}", file=sys.stderr)
        return EXIT_USAGE

    # Building the timeline is part of reading the input, not of the study: a
    # capture that mixes symbols, or roll files handed over out of order, raises
    # ``ValueError`` here, and a corrupt roll raises the gzip/zlib errors above.
    try:
        timeline = build_timeline_from_files(sources, symbol=args.symbol)
    except READ_ERRORS as exc:
        print(f"usage: {exc}", file=sys.stderr)
        return EXIT_USAGE
    # The scan reads the same captures a second time, so it gets the same guard:
    # a roll rotated away or corrupted between the two passes is an input defect,
    # not a measured verdict.
    try:
        scan = scan_tape(
            (line for path in sources for line in iter_capture_lines(path)),
            symbol=args.symbol,
        )
    except READ_ERRORS as exc:
        print(f"usage: {exc}", file=sys.stderr)
        return EXIT_USAGE
    # Concatenating rolls is only sound when they are chronological; a capture
    # list given out of order would price prints against a future mid.
    backwards = [
        (left.ts_ns, right.ts_ns)
        for left, right in zip(scan.prints, scan.prints[1:])
        if right.ts_ns < left.ts_ns
    ]
    if backwards:
        print(
            f"usage: prints are not chronological ({backwards[0][0]} ns then "
            f"{backwards[0][1]} ns); pass the roll files in time order",
            file=sys.stderr)
        return EXIT_USAGE

    summary = measure_edge_curve(scan, timeline, config)
    verdict = edge_curve_verdict(summary, config)

    print(f"tape: {len(scan.prints)} print(s), {summary['tape']['matched']} priced, "
          f"{summary['tape']['unmatched']} without a mid, "
          f"{summary['tape']['span_s']:.1f} s")
    print(f"fee: {config.maker_bps:.2f} bps maker | grid: "
          f"{', '.join(f'{value:g}' for value in config.distances_bps)} bps"
          + (f" | tick {config.tick_size:g}" if config.tick_size > 0 else ""))
    label = verdict["primary_horizon"]
    print(f"| distance bps | fills | fills/min | {label} n | {label} mean | net of fee |")
    print("| --- | --- | --- | --- | --- | --- |")
    for row in summary["rows"]:
        block = row["horizons"][label]
        print(f"| {row['distance_bps']:g} | {row['fills']} | "
              f"{row['fills_per_min']:.2f} | {block['samples']} | "
              f"{block['mean_bps']:+.2f} | {block['net_bps']:+.2f} |")
    dropped = sum(row["horizons"][label]["truncated"] for row in summary["rows"])
    if dropped:
        print(f"note: {dropped} fill(s) had no mid inside the "
              f"{config.tolerance_s:g} s tolerance and are excluded, not interpolated")
    print(f"verdict: {verdict['verdict']}")
    for reason in verdict["reasons"]:
        print(f"  - {reason}")

    if not args.no_report:
        directory = Path(args.report_dir)
        payload = {
            "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "inputs": {"sources": [str(path) for path in sources]},
            "summary": summary,
            "verdict": verdict,
        }
        try:
            write_json(payload, directory / f"{args.stem}.json")
            write_csv(
                [{
                    "distance_bps": row["distance_bps"],
                    "distance_ticks": row["distance_ticks"],
                    "fills": row["fills"],
                    "reach_pct": row["reach_pct"],
                    "fills_per_min": row["fills_per_min"],
                    **{
                        f"net_{label2}_bps": block["net_bps"]
                        for label2, block in row["horizons"].items()
                    },
                    **{
                        f"mean_{label2}_bps": block["mean_bps"]
                        for label2, block in row["horizons"].items()
                    },
                } for row in summary["rows"]],
                directory / f"{args.stem}.csv",
            )
            markdown = render_markdown(summary, verdict)
            (directory / f"{args.stem}.md").write_text(
                "\n".join(markdown) + "\n", encoding="utf-8")
        except OSError as exc:
            # An unwritable report directory is an environment problem, never a
            # measured verdict, so it must not exit with the "no" code.
            print(f"usage: cannot write the report to {directory}: {exc}",
                  file=sys.stderr)
            return EXIT_USAGE

    if verdict["verdict"] == "CLEARS":
        return EXIT_CLEARS
    if verdict["verdict"] == "NO_DATA":
        return EXIT_NO_DATA
    return EXIT_NO_CLEARING_DISTANCE


if __name__ == "__main__":
    sys.exit(main())
