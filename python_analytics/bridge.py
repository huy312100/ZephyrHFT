"""Phase 2 bridge: stream Phase 1 gzip NDJSON into the C++ ``paper_engine``.

The per-level parsing and book update happen inside ``hft_native`` (allocation
free); Python only reads compressed lines and hands them over in bulk batches,
so no per-level Python object is ever constructed.
"""
from __future__ import annotations

import gzip
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Dict, Iterable, Iterator, List, Optional

from .native import hft_native
from .utils.config import require_aligned_center

logger = logging.getLogger(__name__)


def iter_gzip_ndjson(path: Path) -> Iterator[str]:
    with gzip.open(path, "rt", encoding="utf-8") as handle:
        for line in handle:
            stripped = line.strip()
            if stripped:
                yield stripped


def iter_ndjson(path: Path) -> Iterator[str]:
    with Path(path).open("r", encoding="utf-8") as handle:
        for line in handle:
            stripped = line.strip()
            if stripped:
                yield stripped


def build_paper_engine(
    tick_size: str,
    center: str,
    maker_bps: float = 0.0,
    taker_bps: float = 5.0,
    initial_cash: float = 0.0,
) -> "hft_native.PaperEngine":
    """Creates the C++ paper engine.

    ``maker_bps`` / ``taker_bps`` are fractional basis points (e.g. ``0.5``);
    the ledger stores them as micro-bps so no precision is lost here. The
    centre/tick pair is validated first: an off-grid centre rejects every price
    the ladder is offered, which looks like a quiet market rather than a bug.
    """
    require_aligned_center(center, tick_size)
    return hft_native.PaperEngine(
        hft_native.Price.from_string(tick_size),
        hft_native.Price.from_string(center),
        float(maker_bps),
        float(taker_bps),
        hft_native.Notional.from_string(f"{initial_cash:.8f}"),
    )


def ingest_batch(engine: "hft_native.PaperEngine", lines: List[str]) -> Dict[str, int]:
    stats = hft_native.ingest_lines(engine, lines)
    return {
        "lines": stats.lines,
        "depth": stats.depth_events,
        "trade": stats.trade_events,
        "malformed": stats.malformed,
        "gaps": stats.gaps,
    }


def feed_archive(
    engine: "hft_native.PaperEngine",
    source: Path,
    *,
    batch_size: int = 512,
    on_batch: Optional[Callable[[Dict[str, int]], None]] = None,
    frame_gate: Optional[Callable[[int], bool]] = None,
    should_stop: Optional[Callable[[], bool]] = None,
) -> Dict[str, int]:
    """Streams an archive into the C++ engine.

    ``frame_gate`` is called once per line with a 1-based sequence number and
    must return ``True`` to forward the frame. That is the seam the chaos
    injector uses to stall or drop frames without touching the engine; a gate
    that raises is logged and the frame is forwarded (fail open) so a broken
    injector can never take the feed down.

    ``should_stop`` is polled between frames and lets a SIGINT handler end the
    run at a frame boundary instead of unwinding through the C++ engine.
    """
    iterator = iter_gzip_ndjson(source) if str(source).endswith(".gz") else iter_ndjson(source)
    totals = {
        "lines": 0,
        "depth": 0,
        "trade": 0,
        "malformed": 0,
        "gaps": 0,
        "batches": 0,
        # Frames refused by the gate (chaos), gate failures (fail-open) and
        # whether the loop ended through `should_stop`.
        "dropped": 0,
        "gate_errors": 0,
        "stopped": 0,
    }
    batch: List[str] = []
    sequence = 0
    for line in iterator:
        if should_stop is not None and should_stop():
            totals["stopped"] = 1
            break
        if frame_gate is not None:
            sequence += 1
            try:
                forward = bool(frame_gate(sequence))
            except Exception:  # noqa: BLE001 - an injector bug must not stop the feed
                logger.exception("frame gate raised on frame %d; forwarding it", sequence)
                totals["gate_errors"] += 1
                forward = True
            if not forward:
                totals["dropped"] += 1
                continue
        batch.append(line)
        if len(batch) >= batch_size:
            _drain(engine, batch, totals, on_batch)
            batch = []
    if batch:
        _drain(engine, batch, totals, on_batch)
    return totals


def _drain(
    engine: "hft_native.PaperEngine",
    batch: List[str],
    totals: Dict[str, int],
    on_batch: Optional[Callable[[Dict[str, int]], None]],
) -> None:
    stats = hft_native.ingest_lines(engine, batch)
    totals["lines"] += stats.lines
    totals["depth"] += stats.depth_events
    totals["trade"] += stats.trade_events
    totals["malformed"] += stats.malformed
    totals["gaps"] = stats.gaps
    totals["batches"] += 1
    if on_batch is not None:
        on_batch(
            {
                "lines": stats.lines,
                "depth": stats.depth_events,
                "trade": stats.trade_events,
                "malformed": stats.malformed,
                "gaps": stats.gaps,
            }
        )


@dataclass
class PaperSession:
    """Owns a C++ ``paper_engine`` plus Python-side fill buffers.

    ``kill_switch`` is optional: when present every order is checked against it
    (one acquire load in C++) and :meth:`bind_kill_switch` registers hooks that
    stage ``cancel_all`` / ``flatten`` for :meth:`service_remediation`, which is
    what makes the dead-man's switch actually flatten the book during a chaos
    run.
    """

    engine: "hft_native.PaperEngine"
    fills: List[Dict[str, float]] = None  # type: ignore[assignment]
    maker_bps: float = 0.0
    taker_bps: float = 5.0
    kill_switch: Optional["hft_native.KillSwitch"] = None

    def __post_init__(self) -> None:
        if self.fills is None:
            self.fills = []
        # Staged by the KillSwitch hooks (possibly from a supervision thread)
        # and executed by `service_remediation()` on the engine thread.
        self._pending_cancel = False
        self._pending_flatten = False
        if self.kill_switch is not None:
            self.bind_kill_switch(self.kill_switch)

    @classmethod
    def create(
        cls,
        tick_size: str,
        center: str,
        maker_bps: float = 0.0,
        taker_bps: float = 5.0,
        initial_cash: float = 0.0,
        kill_switch: Optional["hft_native.KillSwitch"] = None,
    ) -> "PaperSession":
        """Opens a paper session; fee rates are fractional basis points."""
        return cls(
            build_paper_engine(tick_size, center, maker_bps, taker_bps, initial_cash),
            maker_bps=maker_bps,
            taker_bps=taker_bps,
            kill_switch=kill_switch,
        )

    def feed(self, lines: Iterable[str]) -> Dict[str, int]:
        stats = ingest_batch(self.engine, list(lines))
        self.service_remediation()
        return stats

    def feed_file(
        self,
        path: Path,
        batch_size: int = 512,
        *,
        frame_gate: Optional[Callable[[int], bool]] = None,
        should_stop: Optional[Callable[[], bool]] = None,
    ) -> Dict[str, int]:
        totals = feed_archive(
            self.engine,
            path,
            batch_size=batch_size,
            frame_gate=frame_gate,
            should_stop=should_stop,
        )
        self.service_remediation()
        return totals

    # ----------------------------------------------------------- order path
    def order_state(self) -> Optional[str]:
        """Current kill-switch state, or ``None`` when no switch is attached."""
        if self.kill_switch is None:
            return None
        return hft_native.system_state_name(self.kill_switch.get_state())

    def market(self, side: "hft_native.Side", quantity: str) -> Dict[str, float]:
        """Submits a taker order, honouring the kill switch when attached."""
        if self.kill_switch is not None and not self.kill_switch.check_order_allowed():
            state = self.order_state()
            logger.warning("order blocked: kill_switch=%s side=%s quantity=%s",
                           state, side, quantity)
            return {
                "accepted": False,
                "filled": 0.0,
                "average_price": 0.0,
                "slippage": 0.0,
                "reason": f"kill_switch:{state}",
                "blocked": True,
            }
        return self._submit_market(side, quantity)

    def _submit_market(
        self,
        side: "hft_native.Side",
        quantity: str,
        *,
        record_streak: bool = True,
    ) -> Dict[str, float]:
        request = hft_native.OrderRequest()
        request.side = side
        request.type = hft_native.OrderType.MARKET
        request.quantity = hft_native.Quantity.from_string(quantity)
        ack = self.engine.submit(request)
        self._drain_fills()
        accepted = ack.accepted()
        # Only venue decisions feed the reject-streak guard; a kill-switch
        # block is not a rejection and remediation bypasses the streak.
        if self.kill_switch is not None and record_streak:
            if accepted:
                self.kill_switch.record_order_accepted()
            else:
                self.kill_switch.record_order_rejected()
        return {
            "accepted": accepted,
            "filled": float(ack.filled),
            "average_price": float(ack.average_price),
            "slippage": float(ack.slippage),
            "reason": str(ack.reason),
            "blocked": False,
        }

    # -------------------------------------------------------- kill-switch hooks
    def bind_kill_switch(self, kill_switch: "hft_native.KillSwitch") -> "PaperSession":
        """Attaches a kill switch and registers the remediation hooks.

        The hooks only *stage* work. ``paper_engine`` is single-threaded by
        contract, so cancelling/flattening directly from a supervision thread
        would race the ingest thread; :meth:`service_remediation` executes the
        staged actions on the thread that owns the engine. Blocking itself is
        still immediate: ``check_order_allowed()`` is one atomic load.

        Overwrites any hooks already registered on the switch.
        """
        self.kill_switch = kill_switch
        kill_switch.set_cancel_all_hook(self._stage_cancel)
        kill_switch.set_flatten_hook(self._stage_flatten)
        return self

    def _stage_cancel(self) -> None:
        self._pending_cancel = True

    def _stage_flatten(self) -> None:
        self._pending_flatten = True

    @property
    def remediation_pending(self) -> bool:
        return bool(self._pending_cancel or self._pending_flatten)

    def service_remediation(self) -> Dict[str, float]:
        """Runs staged cancel -> flatten actions on the engine thread.

        Safe to call on every batch: it is a no-op unless a hook fired, and it
        always runs on the caller's thread (the one that owns the engine).
        """
        if not self.remediation_pending:
            return {}
        outcome: Dict[str, float] = {"cancelled": 0.0, "flattened": 0.0}
        if self._pending_cancel:
            self._pending_cancel = False
            outcome["cancelled"] = float(self.cancel_all())
        if self._pending_flatten:
            self._pending_flatten = False
            outcome["flattened"] = float(self.flatten()["flattened"])
        return outcome

    def cancel_all(self) -> int:
        """Cancels every resting order (engine thread only)."""
        cancelled = int(self.engine.cancel_all())
        logger.warning("kill switch: cancelled %d resting order(s)", cancelled)
        return cancelled

    def flatten(self) -> Dict[str, float]:
        """Closes the net position with one market order.

        Deliberately bypasses the gate in :meth:`market`: the flatten hook runs
        *after* the switch has left ``NORMAL``, so going through the gate would
        make flattening impossible exactly when it is needed. The remediation
        order also bypasses the reject-streak guard, so a rejected flatten can
        never re-trip the switch from inside its own hook.
        """
        net = float(self.metrics()["net_quantity"])
        if abs(net) < 1e-12:
            logger.info("kill switch: flat, nothing to flatten")
            return {"accepted": True, "flattened": 0.0, "reason": "FLAT",
                    "side": "FLAT", "blocked": False}
        side = hft_native.Side.SELL if net > 0 else hft_native.Side.BUY
        result = self._submit_market(side, f"{abs(net):.8f}", record_streak=False)
        result["flattened"] = result["filled"] if result["accepted"] else 0.0
        result["side"] = "SELL" if net > 0 else "BUY"
        logger.warning("kill switch: flatten %s %.8f accepted=%s filled=%.8f",
                       result["side"], abs(net), result["accepted"],
                       result["flattened"])
        return result

    def _drain_fills(self) -> None:
        while True:
            fill = self.engine.drain_fill()
            if fill is None:
                return
            self.fills.append(
                {
                    "price": float(fill.price),
                    "quantity": float(fill.quantity),
                    "fee": float(fill.fee),
                    "side": "BUY" if fill.side == hft_native.Side.BUY else "SELL",
                    "is_maker": bool(fill.is_maker),
                }
            )

    def metrics(self) -> Dict[str, float]:
        metrics = self.engine.metrics()
        account = metrics.account
        return {
            "depth_events": metrics.depth_events,
            "gap_events": metrics.gap_events,
            "orders_submitted": metrics.orders_submitted,
            "orders_rejected": metrics.orders_rejected,
            "orders_cancelled": metrics.orders_cancelled,
            "open_orders": metrics.open_orders,
            "fills": metrics.fills,
            "filled_quantity": float(metrics.filled_quantity),
            "traded_notional": float(metrics.traded_notional),
            "slippage_total": float(metrics.slippage_total),
            "p50_ns": metrics.p50_ns,
            "p99_ns": metrics.p99_ns,
            "p999_ns": metrics.p999_ns,
            "last_frame_latency_ns": metrics.last_frame_latency_ns,
            "cash": float(account.cash),
            "fees_paid": float(account.fees_paid),
            "realized_pnl": float(account.realized_pnl),
            "unrealized_pnl": float(account.unrealized_pnl),
            "equity": float(account.equity),
            "net_quantity": float(account.net_quantity),
            "average_entry_price": float(account.average_entry_price),
            "maker_fills": account.maker_fills,
            "taker_fills": account.taker_fills,
            "fill_count": account.fill_count,
        }
