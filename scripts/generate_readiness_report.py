#!/usr/bin/env python3
"""Phase 5 production-readiness gate.

Runs every invariant the previous phases promised, measures it, and prints two
*separate* verdicts against explicit thresholds:

======================  =====================================================
Phase 1 (ingestion)     settings parse, archive integrity (0 malformed, 0 gaps)
Phase 2 (execution)     engine p99 < 50 us, p999 < 500 us, throughput,
                        ledger residual, rejected orders, fill activity
Phase 3 (resilience)    disconnect storm, latency injection, dead-man's
                        switch tripping and releasing the trading halt
Phase 4 (validation)    parameter lock, look-ahead, ledger, OOS window count,
                        optional OOS Sharpe gate
Alpha                   net OOS PnL after modelled fees, and whether the fee
                        assumption is one a retail account can actually reach
======================  =====================================================

Engine/chaos/walk-forward evidence is produced from a deterministic synthetic
book (``--evidence-events``, ``--seed``), so the verdict is reproducible on any
machine. Phase 1 integrity is measured on the real archive (``--source``).
When ``reports/walk_forward.json`` already exists it is reused instead of being
recomputed; pass ``--recompute-walk-forward`` to force a fresh run.

Build readiness and profitability are reported side by side and never merged:
the machinery can be production-ready while a given alpha is not, and
conflating the two hides which one is broken. The alpha checks always print;
``--require-positive-alpha`` additionally turns a negative alpha into exit ``3``
so the promotion checklist cannot ship a losing parameter set.

Exit codes: ``0`` GO, ``1`` build NO-GO, ``2`` usage error, ``3`` alpha NO-GO
(only with ``--require-positive-alpha``).
"""
from __future__ import annotations

import argparse
import json
import sys
import tempfile
import time
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from python_analytics.backtesting.replay import BulkReplayer  # noqa: E402
from python_analytics.backtesting.walk_forward import (  # noqa: E402
    LockedParameters,
    LookAheadError,
    ParameterLockError,
    WalkForwardError,
    WalkForwardRunner,
    utc_timestamp,
)
from python_analytics.bridge import build_paper_engine, ingest_batch  # noqa: E402
from python_analytics.chaos import (  # noqa: E402
    ChaosConfig,
    ChaosMode,
    ChaosNetworkInjector,
)
from python_analytics.costs import (  # noqa: E402
    DEFAULT_MAKER_BPS,
    DEFAULT_TAKER_BPS,
    round_trip_cost_bps,
)
from python_analytics.native import hft_native  # noqa: E402
from python_analytics.simulator.paper_engine import synthetic_depth_messages  # noqa: E402
from python_analytics.utils import configure_logging, load_settings  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SOURCE = ROOT / "data" / "sample_events.ndjson"
DEFAULT_REPORT_DIR = ROOT / "reports"
DEFAULT_STEM = "production_readiness_report"

EXIT_GO = 0
EXIT_NO_GO = 1
EXIT_USAGE = 2
#: Build is ready but the strategy does not make money at the modelled fees.
#: Only returned under ``--require-positive-alpha``; the shipped default reports
#: the alpha verdict without letting it mask a build regression in CI.
EXIT_ALPHA_NO_GO = 3

#: Phase id for the profitability checks, which are gated separately from build.
ALPHA_PHASE = "alpha"

# Evidence book: small, deterministic and liquid enough to fill.
EVIDENCE_MID = 100.0
EVIDENCE_TICK = 0.5
EVIDENCE_EPOCH_MS = 1_700_000_000_000


@dataclass
class Check:
    """One measured gate."""

    id: str
    phase: str
    title: str
    measured: Any
    threshold: str
    passed: bool
    gated: bool = True
    note: str = ""

    def as_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "phase": self.phase,
            "title": self.title,
            "measured": self.measured,
            "threshold": self.threshold,
            "passed": self.passed,
            "gated": self.gated,
            "note": self.note,
        }


@dataclass
class ReadinessReport:
    """The verdict plus every measurement that produced it.

    Two verdicts are reported and never merged:

    ``verdict``
        Build readiness -- does the machinery do what the phases promised?
    ``alpha_verdict``
        Profitability -- did the frozen parameters make money on the OOS book
        *at the modelled fees*? A build can be production-ready while its alpha
        loses money; merging the two hides which one is broken.
    """

    checks: List[Check]
    evidence: Dict[str, Any]
    thresholds: Dict[str, Any]
    inputs: Dict[str, Any]
    generated_at: str = field(default_factory=utc_timestamp)

    @property
    def build_checks(self) -> List[Check]:
        return [check for check in self.checks if check.phase != ALPHA_PHASE]

    @property
    def alpha_checks(self) -> List[Check]:
        return [check for check in self.checks
                if check.phase == ALPHA_PHASE and check.gated]

    @property
    def gated(self) -> List[Check]:
        return [check for check in self.build_checks if check.gated]

    @property
    def failures(self) -> List[Check]:
        return [check for check in self.gated if not check.passed]

    @property
    def alpha_failures(self) -> List[Check]:
        return [check for check in self.alpha_checks if not check.passed]

    @property
    def verdict(self) -> str:
        return "GO" if not self.failures else "NO-GO"

    @property
    def alpha_verdict(self) -> str:
        checks = self.alpha_checks
        if not checks:
            return "N/A"
        return "GO" if not self.alpha_failures else "NO-GO"

    def as_dict(self) -> Dict[str, Any]:
        gated = self.gated
        alpha = self.alpha_checks
        return {
            "report": "production_readiness",
            "generated_at": self.generated_at,
            "verdict": self.verdict,
            "alpha_verdict": self.alpha_verdict,
            "summary": {
                "checks": len(self.checks),
                "gated": len(gated),
                "passed": sum(1 for check in gated if check.passed),
                "failed": len(self.failures),
                "blocking": [check.id for check in self.failures],
                "alpha_checks": len(alpha),
                "alpha_failed": len(self.alpha_failures),
                "alpha_blocking": [check.id for check in self.alpha_failures],
            },
            "thresholds": self.thresholds,
            "inputs": self.inputs,
            "checks": [check.as_dict() for check in self.checks],
            "evidence": self.evidence,
        }

    # ------------------------------------------------------------- artefacts
    def to_json(self, path: Any) -> Path:
        target = Path(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(json.dumps(self.as_dict(), indent=2, sort_keys=True) + "\n",
                          encoding="utf-8")
        return target

    def to_markdown(self, path: Any) -> Path:
        target = Path(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(self.to_markdown_text(), encoding="utf-8")
        return target

    def write(self, directory: Any, stem: str = DEFAULT_STEM):
        root = Path(directory)
        return self.to_json(root / f"{stem}.json"), self.to_markdown(root / f"{stem}.md")

    # ------------------------------------------------------------- narrative
    PHASES = (
        ("phase1", "Phase 1 - data ingestion"),
        ("phase2", "Phase 2 - paper execution"),
        ("phase3", "Phase 3 - resilience"),
        ("phase4", "Phase 4 - out-of-sample validation"),
        (ALPHA_PHASE, "Alpha - profitability at modelled fees"),
    )

    def summary_lines(self) -> List[str]:
        gated = self.gated
        alpha = self.alpha_checks
        lines = [
            f"BUILD: {self.verdict} "
            f"({sum(1 for c in gated if c.passed)}/{len(gated)} gated checks passed)",
            f"ALPHA: {self.alpha_verdict} "
            f"({len(alpha) - len(self.alpha_failures)}/{len(alpha)} profitability "
            f"checks passed at {self.thresholds.get('maker_bps')} / "
            f"{self.thresholds.get('taker_bps')} bps maker/taker)",
        ]
        for check in self.failures:
            lines.append(f"  BLOCKING {check.id}: {check.measured} (required {check.threshold})")
        for check in self.alpha_failures:
            lines.append(f"  ALPHA FAIL {check.id}: {check.measured} "
                         f"(required {check.threshold})")
        for phase, title in self.PHASES:
            subset = [check for check in self.checks if check.phase == phase]
            if subset:
                ok = sum(1 for check in subset if check.passed)
                lines.append(f"  {title:<38} {ok}/{len(subset)} checks")
        return lines

    def to_markdown_text(self) -> str:
        gated = self.gated
        alpha = self.alpha_checks
        lines: List[str] = [
            "# Production readiness report",
            "",
            f"## Build verdict: **{self.verdict}**",
            "",
            f"## Alpha verdict: **{self.alpha_verdict}**",
            "",
            f"- Generated: `{self.generated_at}`",
            f"- Gated checks: {sum(1 for c in gated if c.passed)}/{len(gated)} passed",
            f"- Profitability checks: "
            f"{len(alpha) - len(self.alpha_failures)}/{len(alpha)} passed",
            f"- Fee assumption: {self.thresholds.get('maker_bps')} bps maker / "
            f"{self.thresholds.get('taker_bps')} bps taker "
            f"({self.thresholds.get('round_trip_cost_bps')} bps round trip)",
            f"- Evidence source: `{self.inputs.get('evidence', 'synthetic')}`",
            f"- Walk-forward evidence: {self.inputs.get('walk_forward_origin', 'n/a')}",
            "",
        ]
        if self.failures:
            lines += ["### Blocking failures", "",
                      "| Check | Measured | Required |", "| --- | --- | --- |"]
            for check in self.failures:
                lines.append(f"| `{check.id}` | {check.measured} | {check.threshold} |")
            lines.append("")
        else:
            lines += ["No blocking failures.", ""]

        lines += [
            "> Build readiness and profitability are reported separately: a green "
            "build does not mean the strategy earns, and a negative alpha does not "
            "mean the machinery is broken.",
            "",
        ]

        for phase, title in self.PHASES:
            subset = [check for check in self.checks if check.phase == phase]
            if not subset:
                continue
            lines += [f"## {title}", "",
                      "| Check | Measured | Threshold | Status |", "| --- | --- | --- | --- |"]
            for check in subset:
                status = "PASS" if check.passed else "FAIL"
                if not check.gated:
                    status += " (info)"
                lines.append(f"| `{check.id}` {check.title} | {check.measured} | "
                             f"{check.threshold} | {status} |")
            lines.append("")

        lines += ["## Thresholds", "", "| Threshold | Value |", "| --- | --- |"]
        for name, value in self.thresholds.items():
            lines.append(f"| `{name}` | `{value}` |")

        lines += ["", "## Evidence", "", "```json",
                  json.dumps(self.evidence, indent=2, sort_keys=True, default=str),
                  "```", ""]
        return "\n".join(lines)


# --------------------------------------------------------------------------- #
# Evidence generation
# --------------------------------------------------------------------------- #
def synthetic_evidence(events: int, *, seed: int = 7,
                       mid: float = EVIDENCE_MID,
                       tick: float = EVIDENCE_TICK) -> List[str]:
    """Deterministic, strictly time-ordered depth frames.

    ``synthetic_depth_messages`` stamps every frame with the wall clock, which
    makes walk-forward windows look simultaneous. The frames are re-stamped here
    with a fixed 250 ms grid so chronology checks are meaningful and the whole
    evidence set is bit-reproducible.
    """
    frames = synthetic_depth_messages(mid=mid, tick=tick, spread_ticks=2, seed=seed)
    lines: List[str] = []
    for index, line in enumerate(frames):
        if index >= events:
            break
        payload = json.loads(line)
        payload["E"] = EVIDENCE_EPOCH_MS + index * 250
        lines.append(json.dumps(payload, separators=(",", ":")))
    return lines


def _read_source(path: Path):
    from python_analytics.bridge import iter_gzip_ndjson, iter_ndjson

    if str(path).endswith(".gz"):
        return iter_gzip_ndjson(path)
    return iter_ndjson(path)


def measure_ingestion(source: Path, *, tick_size: str, center: str,
                      batch_size: int = 512) -> tuple[List[Check], Dict[str, Any]]:
    """Phase 1: does the real archive ingest cleanly and at what rate?"""
    engine = build_paper_engine(tick_size, center)
    totals = {"lines": 0, "depth": 0, "trade": 0, "malformed": 0, "gaps": 0}
    batch: List[str] = []
    started = time.monotonic()
    for line in _read_source(source):
        batch.append(line)
        if len(batch) >= batch_size:
            for key, value in ingest_batch(engine, batch).items():
                totals[key] += value
            batch = []
    if batch:
        for key, value in ingest_batch(engine, batch).items():
            totals[key] += value
    elapsed = max(time.monotonic() - started, 1e-9)
    rate = totals["lines"] / elapsed

    checks = [
        Check(
            id="phase1.archive_integrity",
            phase="phase1",
            title=f"{source.name}",
            measured=f"{totals['malformed']} malformed, {totals['gaps']} gaps "
                     f"in {totals['lines']:,} lines",
            threshold="0 malformed, 0 gaps",
            passed=totals["malformed"] == 0 and totals["gaps"] == 0
            and totals["lines"] > 0,
            note="a gap means the venue sequence skipped; that is a data defect, "
                 "not a replay decision",
        ),
        Check(
            id="phase1.ingest_throughput",
            phase="phase1",
            title="archive ingest rate",
            measured=f"{rate:,.0f} lines/s",
            threshold="info",
            passed=True,
            gated=False,
        ),
    ]
    evidence = {"source": str(source), "totals": totals,
                "lines_per_s": round(rate, 1), "elapsed_s": round(elapsed, 4)}
    return checks, evidence


def measure_engine(parameters: LockedParameters, lines: List[str], *,
                   max_p99_us: float, max_p999_us: float,
                   min_throughput: int, ledger_tolerance: float,
                   ) -> tuple[List[Check], Dict[str, Any]]:
    """Phase 2: latency, throughput, ledger coherence and order flow."""
    strategy = parameters.build_strategy()
    replayer = BulkReplayer(
        tick_size=parameters.tick_size,
        center=parameters.center,
        maker_bps=parameters.maker_bps,
        taker_bps=parameters.taker_bps,
        initial_cash=parameters.initial_cash,
        batch_size=64,
        frames_per_hook=1,
        order_quantity=parameters.order_quantity,
    )
    summary = replayer.replay_lines(lines, strategy)
    stats = summary.stats
    latency = stats["latency_ns"]
    p50_us = latency["engine_p50_ns"] / 1000.0
    p99_us = latency["engine_p99_ns"] / 1000.0
    p999_us = latency["engine_p999_ns"] / 1000.0
    residual = (float(summary.equity_curve[-1]) - float(parameters.initial_cash)
                - float(stats["realized_pnl"]) - float(stats["unrealized_pnl"]))
    submitted = int(stats["orders_submitted"])
    rejected = int(stats["orders_rejected"])
    fill_rate = float(stats["fill_rate_pct"])
    throughput = float(stats["throughput_fps"])

    checks = [
        Check("phase2.engine_latency_p99", "phase2", "engine p99",
              f"{p99_us:.1f} us (p50 {p50_us:.1f} us)",
              f"<= {max_p99_us:g} us", p99_us <= max_p99_us),
        Check("phase2.engine_latency_p999", "phase2", "engine p999",
              f"{p999_us:.1f} us", f"<= {max_p999_us:g} us",
              p999_us <= max_p999_us,
              note="tail includes cache-cold first touches of the book ladder"),
        Check("phase2.replay_throughput", "phase2", "replay throughput",
              f"{throughput:,.0f} depth events/s",
              f">= {min_throughput:,}", throughput >= min_throughput),
        Check("phase2.ledger_residual", "phase2", "ledger coherence",
              f"{residual:+.8f}",
              f"|residual| <= {ledger_tolerance:g}",
              abs(residual) <= ledger_tolerance,
              note="equity change must equal realized + unrealized mark-to-market"),
        Check("phase2.order_rejections", "phase2", "rejected orders",
              f"{rejected} of {submitted} submitted",
              "0 rejected", rejected == 0,
              note="risk/violation rejects: a strategy quoting outside its own "
                   "limits is a configuration error, not bad luck"),
        Check("phase2.fill_activity", "phase2", "fill activity",
              f"{fill_rate:.1f}% of submitted quantity",
              "> 0%", fill_rate > 0.0,
              note="proves the evidence book actually trades"),
    ]
    evidence = {
        "events": int(stats["lines"]),
        "depth_events": int(stats["depth_events"]),
        "fills": int(stats["fills"]),
        "round_trips": int(stats.get("round_trips", 0) or 0),
        "orders_submitted": submitted,
        "orders_rejected": rejected,
        "fill_rate_pct": round(fill_rate, 4),
        "latency_us": {"p50": round(p50_us, 3), "p99": round(p99_us, 3),
                       "p999": round(p999_us, 3)},
        "throughput_fps": round(throughput, 2),
        "ledger_residual": round(residual, 8),
        "net_pnl": round(float(summary.equity_curve[-1])
                         - float(parameters.initial_cash), 8),
        "strategy_fingerprint": parameters.fingerprint,
    }
    return checks, evidence


class _FakeClock:
    """Monotonic clock that only advances when ``sleep`` is called."""

    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.now += max(0.0, float(seconds))

    def advance(self, seconds: float) -> None:
        self.now += max(0.0, float(seconds))


def measure_chaos(frames: int = 200, *, frame_interval_s: float = 0.001,
                  ) -> tuple[List[Check], Dict[str, Any]]:
    """Phase 3: transport disruptions must be observable and self-healing."""
    clock = _FakeClock()
    # Scale the schedule to the run length: a burst of 50 frames with a fixed
    # 50 ms interval would end before the first silence window even opens, and
    # a chaos profile that never fires proves nothing.
    span_s = frames * frame_interval_s
    interval_s = max(frame_interval_s * 5.0, span_s / 3.0)
    storm = ChaosNetworkInjector(
        ChaosConfig(mode=ChaosMode.DISCONNECT_STORM, interval_s=interval_s,
                    disconnect_s=interval_s * 0.4, duration_s=span_s, seed=7),
        clock=clock, sleep=clock.sleep,
    )
    forwarded = dropped = 0
    for sequence in range(frames):
        if storm.admit(sequence):
            forwarded += 1
        else:
            dropped += 1
        clock.advance(frame_interval_s)
    storm_stats = storm.stats
    accounted = (storm_stats.frames_forwarded + storm_stats.frames_dropped
                 == storm_stats.frames_seen)

    latency_clock = _FakeClock()
    spikes = ChaosNetworkInjector(
        ChaosConfig(mode=ChaosMode.LATENCY_INJECTION, latency_min_ms=1.0,
                    latency_max_ms=20.0, latency_probability=0.25, seed=11),
        clock=latency_clock, sleep=latency_clock.sleep,
    )
    for sequence in range(120):
        spikes.admit(sequence)
        latency_clock.advance(frame_interval_s)
    spike_stats = spikes.stats
    spike_accounted = (spike_stats.frames_forwarded + spike_stats.frames_dropped
                       == spike_stats.frames_seen)

    checks = [
        Check("phase3.disconnect_storm", "phase3", "disconnect storm",
              f"{storm_stats.outages} outage(s), {storm_stats.reconnects} "
              f"reconnect(s), {storm_stats.frames_dropped} frame(s) dropped",
              ">= 1 outage, reconnects == outages, every frame accounted for",
              storm_stats.outages >= 1
              and storm_stats.reconnects == storm_stats.outages
              and accounted and forwarded > 0 and dropped > 0,
              note="the feed must resume automatically after each silence window"),
        Check("phase3.latency_injection", "phase3", "latency injection",
              f"{spike_stats.latency_events} spike(s), max "
              f"{spike_stats.max_latency_ms:.1f} ms",
              ">= 1 spike, max <= configured ceiling",
              spike_stats.latency_events > 0
              and spike_stats.max_latency_ms <= 20.0 + 1e-6 and spike_accounted),
    ]
    evidence = {
        "disconnect_storm": storm_stats.as_dict(),
        "latency_injection": spike_stats.as_dict(),
        "frames_issued": frames,
        "storm_schedule": {
            "span_s": round(span_s, 6),
            "interval_s": round(interval_s, 6),
            "disconnect_s": round(interval_s * 0.4, 6),
        },
    }
    return checks, evidence


def measure_deadman_switch() -> tuple[List[Check], Dict[str, Any]]:
    """Phase 3: feed silence must halt order flow until an explicit reset."""
    config = hft_native.HeartbeatConfig()
    config.timeout_ns = 500_000_000
    monitor = hft_native.HeartbeatMonitor(config)
    switch = hft_native.KillSwitch()
    monitor.arm(switch)

    monitor.beat(1_000_000_000)
    allowed_before = bool(switch.check_order_allowed())
    silent_poll = bool(monitor.poll(1_400_000_000))
    tripped = bool(monitor.poll(1_600_000_000))
    blocked = not bool(switch.check_order_allowed())
    reason = str(switch.get_stats().last_trip_reason)
    # Recovery is an operator action: the monitor restarts its window, the
    # switch must be cleared explicitly. Both are required to resume.
    monitor.reset(2_000_000_000)
    halted_until_reset = not bool(switch.check_order_allowed())
    switch.reset()
    released = bool(switch.check_order_allowed())
    # A recovered feed must be able to trip again, otherwise the guard is dead.
    monitor.beat(2_100_000_000)
    retripped = bool(monitor.poll(2_800_000_000))
    switch.reset()

    passed = (allowed_before and not silent_poll and tripped and blocked
              and halted_until_reset and released and retripped
              and reason.startswith("heartbeat_timeout"))
    checks = [
        Check("phase3.deadman_switch", "phase3", "heartbeat dead-man's switch",
              f"tripped on {reason!r}, orders {'blocked' if blocked else 'ALLOWED'} "
              f"while halted, released only on explicit reset, re-armed",
              "halts on timeout, blocks orders, releases on reset, can trip again",
              passed,
              note="silence alone must trip the halt; no component trips it directly"),
    ]
    evidence = {
        "timeout_ms": config.timeout_ns / 1_000_000,
        "trip_reason": reason,
        "trips": int(switch.get_stats().trip_count),
        "resets": int(switch.get_stats().reset_count),
        "orders_allowed_before": allowed_before,
        "poll_before_timeout_tripped": silent_poll,
        "orders_blocked_while_halted": blocked,
        "blocked_until_explicit_reset": halted_until_reset,
        "orders_allowed_after_reset": released,
        "retripped_after_recovery": retripped,
    }
    return checks, evidence


# --------------------------------------------------------------------------- #
# Phase 1 - configuration
# --------------------------------------------------------------------------- #
def measure_settings(settings: Any) -> tuple[List[Check], Dict[str, Any]]:
    """Phase 1: the shipped configuration must be self-consistent."""
    engine = settings.engine
    pretrade = engine.pretrade
    kill = settings.kill_switch
    reasons: List[str] = []
    try:
        tick = float(engine.tick_size)
        center = float(engine.center)
    except (TypeError, ValueError):
        tick = center = 0.0
        reasons.append("tick_size/center are not numeric")
    if tick <= 0.0:
        reasons.append("tick_size must be positive")
    if center <= 0.0:
        reasons.append("center must be positive")
    if not kill.enabled:
        reasons.append("kill switch is disabled")
    # Pre-trade guards are deliberately off for deterministic replay, so their
    # state is reported but not gated; if they are on, the limits must be sane.
    if pretrade.enabled and float(pretrade.max_position) <= 0.0:
        reasons.append("max_position must be positive when pretrade is enabled")

    passed = not reasons
    check = Check(
        "phase1.configuration", "phase1", "settings",
        f"tick {tick:g} / center {center:g} / pretrade "
        f"{'on' if pretrade.enabled else 'off (replay profile)'} / kill switch "
        f"{'on' if kill.enabled else 'OFF'}",
        "positive tick+center, kill switch enabled, sane limits when armed",
        passed, note="; ".join(reasons),
    )
    evidence = {
        "tick_size": engine.tick_size,
        "center": engine.center,
        "pretrade_enabled": bool(pretrade.enabled),
        "max_position": pretrade.max_position,
        "kill_switch_enabled": bool(kill.enabled),
        "max_drawdown_limit": kill.max_drawdown_limit,
    }
    return [check], evidence


# --------------------------------------------------------------------------- #
# Report assembly
# --------------------------------------------------------------------------- #
def build_parameters(args: argparse.Namespace, settings: Any) -> LockedParameters:
    """One frozen parameter set drives every measurement."""
    return LockedParameters(
        gamma=args.gamma,
        sigma=args.sigma,
        k=args.k,
        time_horizon=args.time_horizon,
        inventory_limit=args.inventory_limit,
        ofi_sensitivity=args.ofi_sensitivity,
        ofi_decay=args.ofi_decay,
        ofi_cap=args.ofi_cap,
        use_time_spread=True,
        tick_size=args.tick_size or settings.engine.tick_size,
        center=args.center or settings.engine.center,
        market_type=args.market_type.upper(),
        maker_bps=args.maker_bps,
        taker_bps=args.taker_bps,
        initial_cash=args.initial_cash,
        order_quantity=args.order_quantity,
    )


#: Locked fields stored as decimal *strings* so a grid is never silently
#: re-rounded. ``LockedParameters.diff`` compares them as text (by design), so
#: the artefact check below normalises them numerically instead: ``"0.5"`` and
#: ``"0.50"`` describe the same tick grid and must not force a recompute.
_DECIMAL_FIELDS = frozenset({
    ("venue", "tick_size"),
    ("venue", "center"),
    ("sizing", "order_quantity"),
})


def _decimal_key(value: Any) -> str:
    """Canonical text for a decimal-string lock field (``"0.50"`` -> ``"0.5"``)."""
    try:
        return format(Decimal(str(value)).normalize(), "f")
    except (InvalidOperation, TypeError, ValueError):
        return str(value)


def locked_parameter_drift(payload: Mapping[str, Any],
                           parameters: LockedParameters) -> List[str]:
    """Mismatches between a reused study's frozen lock and the current settings.

    A committed ``walk_forward.json`` carries the parameters that produced its
    PnL. Reusing it while the gate runs with different fees (or a different
    strategy/venue set) would report an old study's net PnL under today's labels,
    which is exactly the dishonesty this gate exists to prevent -- so any drift
    means the artefact cannot be reused for alpha and must be recomputed.

    An artefact that records no parameters is treated as drift too: an
    unverifiable fee assumption is not evidence.
    """
    lock = payload.get("validation")
    block = lock.get("parameter_lock") if isinstance(lock, Mapping) else None
    recorded = block.get("parameters") if isinstance(block, Mapping) else None
    if not isinstance(recorded, Mapping):
        return ["validation.parameter_lock.parameters is missing"]
    try:
        locked = LockedParameters.from_dict(recorded)
    except (ParameterLockError, TypeError, ValueError) as exc:
        return [f"validation.parameter_lock.parameters is unreadable: {exc}"]

    mine, theirs = parameters.as_dict(), locked.as_dict()
    drift: List[str] = []
    for section, fields in mine.items():
        for name, value in fields.items():
            other = theirs.get(section, {}).get(name)
            if (section, name) in _DECIMAL_FIELDS:
                same = _decimal_key(value) == _decimal_key(other)
            else:
                same = value == other
            if not same:
                drift.append(f"{section}.{name}: {value!r} != {other!r}")
    return drift


def measure_alpha(payload: Dict[str, Any], *, maker_bps: float,
                  taker_bps: float) -> tuple[List[Check], Dict[str, Any]]:
    """Profitability: does the frozen parameter set earn at honest costs?

    These checks are *never* allowed to hide inside the build verdict. They are
    computed on every run so the number is always on the page, and only
    ``--require-positive-alpha`` turns them into a non-zero exit code.
    """
    aggregate = payload.get("aggregate", {})
    net = float(aggregate.get("net_pnl", 0.0) or 0.0)
    net_pct = float(aggregate.get("net_pnl_pct", 0.0) or 0.0)
    gross = float(aggregate.get("gross_pnl", 0.0) or 0.0)
    fees = float(aggregate.get("fees_paid", 0.0) or 0.0)
    trips = aggregate.get("fills")
    round_trip = round_trip_cost_bps(maker_bps, both_sides_maker=True)
    # Fees are already inside realized/unrealized PnL (both come from the cash
    # ledger), so adding them back is what the study would have earned for free.
    before_fees = net + fees

    checks = [
        Check(
            "alpha.net_pnl", "alpha", "net PnL after fees",
            f"{net:+,.4f} USDT ({net_pct:+.4f}%); {before_fees:+,.4f} before "
            f"{fees:+,.4f} of fees",
            ">= 0 after modelled fees", net >= 0.0,
            note="a negative number means a live deployment at these rates also "
                 "loses money, regardless of how healthy the build is"),
        Check(
            "alpha.fee_assumption", "alpha", "fee assumption is reachable",
            f"maker {maker_bps:g} bps / taker {taker_bps:g} bps "
            f"({round_trip:g} bps round trip), {trips} fill(s)",
            "maker_bps >= 0 (no rebate assumed)", maker_bps >= 0.0,
            note="a negative maker_bps is a rebate tier retail VIP 0 does not "
                 "have; it flatters every passive fill"),
    ]

    evidence = {
        "net_pnl": net,
        "net_pnl_pct": net_pct,
        "gross_pnl": gross,
        "pnl_before_fees": before_fees,
        "pre_fee_profitable": before_fees > 0.0,
        "fees_paid": fees,
        "fills": trips,
        "maker_bps": maker_bps,
        "taker_bps": taker_bps,
        "round_trip_cost_bps": round_trip,
        "rebate_assumed": maker_bps < 0.0,
    }
    return checks, evidence


def build_report(args: argparse.Namespace) -> ReadinessReport:
    settings = load_settings()
    parameters = build_parameters(args, settings)
    checks: List[Check] = []
    evidence: Dict[str, Any] = {}
    inputs: Dict[str, Any] = {
        "source": args.source,
        "evidence": f"synthetic book, {args.evidence_events} frames, seed {args.seed}",
        "parameters": parameters.fingerprint,
        "walk_forward_origin": "n/a",
    }

    found, measured = measure_settings(settings)
    checks += found
    evidence["configuration"] = measured

    source = Path(args.source)
    if not source.exists():
        raise FileNotFoundError(f"source archive not found: {source}")
    found, measured = measure_ingestion(source, tick_size=parameters.tick_size,
                                        center=parameters.center)
    checks += found
    evidence["ingestion"] = measured

    lines = synthetic_evidence(args.evidence_events, seed=args.seed,
                               mid=float(parameters.center),
                               tick=float(parameters.tick_size))
    found, measured = measure_engine(
        parameters, lines,
        max_p99_us=args.max_engine_p99_us,
        max_p999_us=args.max_engine_p999_us,
        min_throughput=args.min_throughput_fps,
        ledger_tolerance=args.ledger_tolerance)
    checks += found
    evidence["engine"] = measured

    found, measured = measure_chaos(args.chaos_frames)
    checks += found
    evidence["chaos"] = measured
    found, measured = measure_deadman_switch()
    checks += found
    evidence["deadman"] = measured

    artifact = Path(args.walk_forward_json)
    payload: Dict[str, Any] = {}
    drift: List[str] = []
    reused = artifact.is_file() and not args.recompute_walk_forward
    if reused:
        candidate = json.loads(artifact.read_text(encoding="utf-8"))
        # Only a study run at *these* parameters can be priced at these fees.
        drift = locked_parameter_drift(candidate, parameters)
        reused = not drift
        if reused:
            payload = candidate
    if reused:
        origin = f"artifact {artifact}"
    else:
        window_events = args.window_events or max(1, args.evidence_events // 4)
        with tempfile.TemporaryDirectory(prefix="zhf_readiness_") as workdir:
            payload = run_walk_forward(parameters, lines,
                                       window_events=window_events,
                                       workdir=Path(workdir))
        origin = f"computed in-process ({window_events} events/window)"
        if drift:
            # Name the rejection: grading a study priced at another fee tier and
            # labelling it with today's would be exactly the dishonesty the
            # alpha checks exist to prevent.
            origin += f"; ignored {artifact}: {len(drift)} locked field(s) differ"
            inputs["walk_forward_drift"] = drift
    inputs["walk_forward_origin"] = origin
    found, measured = measure_validation(
        payload, origin, min_windows=args.min_windows,
        min_oos_sharpe=args.min_oos_sharpe)
    checks += found
    evidence["walk_forward"] = measured

    found, measured = measure_alpha(payload, maker_bps=args.maker_bps,
                                    taker_bps=args.taker_bps)
    checks += found
    evidence["alpha"] = measured

    thresholds = {
        "max_engine_p99_us": args.max_engine_p99_us,
        "max_engine_p999_us": args.max_engine_p999_us,
        "min_throughput_fps": args.min_throughput_fps,
        "ledger_tolerance": args.ledger_tolerance,
        "max_archive_malformed": 0,
        "max_archive_gaps": 0,
        "min_chaos_outages": 1,
        "min_oos_windows": args.min_windows,
        "min_oos_sharpe": args.min_oos_sharpe,
        "maker_bps": args.maker_bps,
        "taker_bps": args.taker_bps,
        "round_trip_cost_bps": round_trip_cost_bps(args.maker_bps),
        "require_positive_alpha": bool(args.require_positive_alpha),
    }
    inputs["costs"] = {"maker_bps": args.maker_bps, "taker_bps": args.taker_bps}
    return ReadinessReport(checks=checks, evidence=evidence,
                           thresholds=thresholds, inputs=inputs)


# --------------------------------------------------------------------------- #
# Phase 4 - out-of-sample evidence
# --------------------------------------------------------------------------- #
def run_walk_forward(parameters: LockedParameters, lines: List[str], *,
                     window_events: int, workdir: Path) -> Dict[str, Any]:
    """Runs a fresh walk-forward over the evidence book and returns its JSON."""
    archive = Path(workdir) / "evidence.ndjson"
    archive.write_text("\n".join(lines) + "\n", encoding="utf-8")
    runner = WalkForwardRunner(parameters, window_events=window_events,
                               lock_source="readiness-report")
    return runner.run([archive]).as_dict()


def measure_validation(payload: Dict[str, Any], origin: str, *,
                       min_windows: int,
                       min_oos_sharpe: Optional[float],
                       ) -> tuple[List[Check], Dict[str, Any]]:
    """Phase 4: are the OOS numbers actually admissible?"""
    validation = payload.get("validation", {})
    lock = validation.get("parameter_lock", {})
    look_ahead = validation.get("look_ahead", {})
    ledger = validation.get("ledger", {})
    aggregate = payload.get("aggregate", {})

    residual = ledger.get("residual")
    tolerance = float(ledger.get("tolerance", 0.0) or 0.0)
    residual_ok = residual is not None and abs(float(residual)) <= max(tolerance, 1e-9)
    windows = int(aggregate.get("windows", 0) or 0)
    events = int(aggregate.get("events", 0) or 0)
    sharpe = aggregate.get("sharpe_ratio")
    sharpe_value = float(sharpe) if isinstance(sharpe, (int, float)) else 0.0

    checks = [
        Check("phase4.parameter_lock", "phase4", "parameter lock",
              f"{lock.get('checks', 0)} check(s) on "
              f"{str(lock.get('fingerprint', 'n/a'))[:12]}...",
              ">= 1 pre/post check, verified",
              bool(lock.get("verified")) and int(lock.get("checks", 0)) > 0,
              note="a fresh strategy per window must be read back identical"),
        Check("phase4.no_look_ahead", "phase4", "look-ahead guard",
              f"{look_ahead.get('chronology_checks', 0)} chronology check(s) "
              f"across {look_ahead.get('windows_checked', 0)} window(s)",
              "chronology verified on >= 1 boundary",
              bool(look_ahead.get("verified"))
              and bool(look_ahead.get("chronology_verified"))
              and int(look_ahead.get("chronology_checks", 0)) > 0),
        Check("phase4.ledger_residual", "phase4", "aggregate ledger",
              f"{residual:+.8f}" if isinstance(residual, (int, float)) else "n/a",
              f"|residual| <= {tolerance:g}", residual_ok,
              note="includes boundary mark-to-market settlements"),
        Check("phase4.window_coverage", "phase4", "OOS coverage",
              f"{windows} window(s), {events:,} events",
              f">= {min_windows} window(s), events > 0",
              windows >= min_windows and events > 0),
    ]
    if min_oos_sharpe is not None:
        checks.append(Check(
            "phase4.oos_sharpe", "phase4", "OOS Sharpe",
            f"{sharpe_value:.3f}", f">= {min_oos_sharpe:g}",
            sharpe_value >= min_oos_sharpe,
            note="opt-in profitability gate; machinery gates are always enforced"))

    checks.append(Check(
        "phase4.oos_sharpe_info", "phase4", "OOS Sharpe (reported)",
        f"{sharpe_value:.3f} on net PnL {float(aggregate.get('net_pnl', 0.0)):+,.4f}",
        "info", True, gated=False,
        note="the evidence book is a zero-drift random walk; expect ~0 Sharpe "
             "with a fee drag, and use a real archive to judge an alpha"))

    evidence = {
        "origin": origin,
        "generated_at": payload.get("generated_at"),
        "windows": windows,
        "events": events,
        "window_events": payload.get("inputs", {}).get("window_events"),
        "initial_cash": aggregate.get("initial_cash"),
        "final_equity": aggregate.get("final_equity"),
        "net_pnl": aggregate.get("net_pnl"),
        "sharpe_ratio": sharpe,
        "sortino_ratio": aggregate.get("sortino_ratio"),
        "max_drawdown_pct": aggregate.get("max_drawdown_pct"),
        "fill_ratio_pct": aggregate.get("fill_ratio_pct"),
        "fills": aggregate.get("fills"),
        "window_hit_rate_pct": aggregate.get("window_hit_rate_pct"),
        "windows_positive": aggregate.get("positive_windows"),
        "windows_negative": aggregate.get("negative_windows"),
        "realized_pnl": aggregate.get("realized_pnl"),
        "boundary_settled_pnl": aggregate.get("boundary_settled_pnl"),
        "unrealized_pnl": aggregate.get("unrealized_pnl"),
        "ledger_residual": residual,
        "ledger_tolerance": tolerance,
        "lock_fingerprint": lock.get("fingerprint"),
        "validation": validation,
    }
    return checks, evidence


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #
def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--source", default=str(DEFAULT_SOURCE),
                        help="Phase 1 archive whose integrity is verified")
    parser.add_argument("--evidence-events", type=int, default=2000,
                        help="synthetic depth frames used for engine/chaos/OOS "
                             "evidence (default: 2000)")
    parser.add_argument("--seed", type=int, default=7,
                        help="seed of the evidence book (default: 7)")
    parser.add_argument("--chaos-frames", type=int, default=200,
                        help="frames pushed through each chaos profile")
    parser.add_argument("--window-events", type=int, default=0,
                        help="walk-forward window size for recomputed evidence; "
                             "0 = evidence-events/4")
    parser.add_argument("--min-windows", type=int, default=2,
                        help="minimum number of OOS windows (default: 2)")
    # Frozen strategy parameters, shared with scripts/run_walk_forward.py.
    parser.add_argument("--gamma", type=float, default=0.1)
    parser.add_argument("--sigma", type=float, default=0.02)
    parser.add_argument("--k", type=float, default=5.0)
    parser.add_argument("--time-horizon", type=float, default=60.0)
    parser.add_argument("--inventory-limit", type=float, default=5.0)
    parser.add_argument("--ofi-sensitivity", type=float, default=0.0)
    parser.add_argument("--ofi-decay", type=float, default=0.95)
    parser.add_argument("--ofi-cap", type=float, default=3.0)
    parser.add_argument("--market-type", default="CRYPTO",
                        choices=("CRYPTO", "STOCK", "COMMODITIES",
                                 "crypto", "stock", "commodities"))
    parser.add_argument("--tick-size", default=None,
                        help="defaults to the value in config/settings.yaml")
    parser.add_argument("--center", default=None,
                        help="defaults to the value in config/settings.yaml")
    parser.add_argument("--maker-bps", type=float, default=DEFAULT_MAKER_BPS,
                        help="maker fee in fractional basis points "
                             "(default: 2.0 = retail VIP 0; negative = rebate)")
    parser.add_argument("--taker-bps", type=float, default=DEFAULT_TAKER_BPS,
                        help="taker fee in fractional basis points "
                             "(default: 5.0 = retail VIP 0)")
    parser.add_argument("--initial-cash", type=float, default=100000.0)
    parser.add_argument("--order-quantity", default="0.01")
    # Gates.
    parser.add_argument("--max-engine-p99-us", type=float, default=50.0)
    parser.add_argument("--max-engine-p999-us", type=float, default=500.0)
    parser.add_argument("--min-throughput-fps", type=int, default=1000)
    parser.add_argument("--ledger-tolerance", type=float, default=1e-2)
    parser.add_argument("--min-oos-sharpe", type=float, default=None,
                        help="opt-in threshold gate applied to the OOS Sharpe "
                             "(machinery gates always apply)")
    parser.add_argument("--require-positive-alpha", action="store_true",
                        help="exit 3 when the alpha verdict is NO-GO (the study "
                             "loses money at the modelled fees); required by the "
                             "promotion checklist, off by default so CI gates on "
                             "build readiness alone")
    # Walk-forward evidence.
    parser.add_argument("--walk-forward-json", default=str(
        DEFAULT_REPORT_DIR / "walk_forward.json"),
        help="reuse this Phase 4 artefact instead of recomputing")
    parser.add_argument("--recompute-walk-forward", action="store_true",
                        help="ignore the artefact and run the walk-forward now")
    # Output.
    parser.add_argument("--report-dir", default=str(DEFAULT_REPORT_DIR))
    parser.add_argument("--stem", default=DEFAULT_STEM)
    parser.add_argument("--no-report", action="store_true",
                        help="console verdict only (no artefacts written)")
    parser.add_argument("--log-level", default="ERROR",
                        help="chaos events are recorded in the report, so the "
                             "console stays quiet by default")
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    configure_logging(args.log_level)
    try:
        report = build_report(args)
    except FileNotFoundError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return EXIT_USAGE
    except (ParameterLockError, LookAheadError, WalkForwardError) as exc:
        print(f"validation failure: {exc}", file=sys.stderr)
        return EXIT_NO_GO
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return EXIT_USAGE

    print("=" * 78)
    print(f"  BUILD READINESS:     {report.verdict}")
    print(f"  ALPHA (PROFITABILITY): {report.alpha_verdict}")
    print(f"  generated {report.generated_at}")
    print("-" * 78)
    for line in report.summary_lines():
        print(f"  {line}")
    print("-" * 78)
    if not args.no_report:
        json_path, md_path = report.write(args.report_dir, args.stem)
        print(f"  wrote: {json_path}")
        print(f"  wrote: {md_path}")
    print("=" * 78)
    if report.failures:
        print("  NO-GO: fix the blocking build checks above before promoting "
              "this build.", file=sys.stderr)
        return EXIT_NO_GO
    if report.alpha_failures:
        print("  ALPHA NO-GO: the machinery is ready but this parameter set does "
              "not make money at the modelled fees; do not trade it live.",
              file=sys.stderr)
        if args.require_positive_alpha:
            return EXIT_ALPHA_NO_GO
    return EXIT_GO


if __name__ == "__main__":
    raise SystemExit(main())
