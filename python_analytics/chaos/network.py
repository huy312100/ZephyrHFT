"""Transport-level chaos for the market-data path.

``ChaosNetworkInjector`` sits between a frame source and the C++ engine. It
implements the same "gate" contract the bridge expects::

    forward = injector.admit(sequence)   # True -> hand the frame to the engine

so a run can be disrupted without touching the engine, the strategy or the
matching code. Everything the injector does is observable:

``disconnect_storm``
    blocks the feed for ``disconnect_s`` every ``interval_s``; exercises the
    silent socket, the ``RECOVERING`` state machine, the heartbeat dead-man's
    switch and the missed-update gaps a silenced feed produces.

``latency_injection``
    sleeps ``uniform(min, max)`` ms on ``latency_probability`` of frames;
    exercises stale-book quoting, order timeouts and the risk boundaries.

The clock, sleep function and RNG are injectable, so the schedule is unit
testable without wall-clock sleeps (pass a fake clock and a fake sleep that
advances it).

Threading: ``admit()`` is called from the ingestion thread and is the only
place the counters are mutated; ``request_stop()`` may be called from a signal
handler. The stop flag is a ``threading.Event``, so no lock is needed on the
data path.
"""
from __future__ import annotations

import logging
import random
import threading
import time
from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Protocol, runtime_checkable

logger = logging.getLogger("zephyr.chaos")

#: Longest single ``sleep`` slice. Keeps Ctrl+C responsive during an outage.
SLEEP_SLICE_S = 0.05
#: Events kept in memory for the post-run report (the callback sees them all).
MAX_RETAINED_EVENTS = 512


class ChaosMode(str, Enum):
    """The two disruption profiles the harness supports."""

    DISCONNECT_STORM = "disconnect_storm"
    LATENCY_INJECTION = "latency_injection"


class ChaosEventType(str, Enum):
    """Kinds of disruption recorded in the event log."""

    DISCONNECT = "disconnect"
    RECONNECT = "reconnect"
    LATENCY = "latency"
    DROP = "drop"
    STOP = "stop"


#: Visual markers prefixed to every log line so a terminal transcript is
#: scannable at a glance.
MARKERS: Dict[ChaosEventType, str] = {
    ChaosEventType.DISCONNECT: "!!",
    ChaosEventType.RECONNECT: "++",
    ChaosEventType.LATENCY: "~~",
    ChaosEventType.DROP: "--",
    ChaosEventType.STOP: "..",
}


@runtime_checkable
class HeartbeatSink(Protocol):
    """Anything with a ``beat()`` method (native ``HeartbeatMonitor``)."""

    def beat(self) -> None:  # pragma: no cover - structural typing only
        ...


@dataclass(frozen=True)
class ChaosConfig:
    """Immutable chaos schedule.

    ``duration_s`` bounds the chaos window (``0`` = for the whole run); outside
    the window frames pass through untouched, which makes the end of a run
    converge to a clean baseline. ``disconnect_s`` must be shorter than
    ``interval_s`` so outages never overlap.
    """

    mode: ChaosMode = ChaosMode.DISCONNECT_STORM
    interval_s: float = 2.0
    duration_s: float = 0.0
    disconnect_s: float = 1.0
    latency_min_ms: float = 200.0
    latency_max_ms: float = 1000.0
    latency_probability: float = 0.10
    seed: Optional[int] = None

    def __post_init__(self) -> None:
        if not isinstance(self.mode, ChaosMode):
            object.__setattr__(self, "mode", ChaosMode(str(self.mode)))
        if self.interval_s <= 0:
            raise ValueError("chaos interval_s must be > 0")
        if self.duration_s < 0:
            raise ValueError("chaos duration_s must be >= 0")
        if self.disconnect_s <= 0:
            raise ValueError("chaos disconnect_s must be > 0")
        # The ordering only matters for the storm: latency mode ignores
        # `disconnect_s`/`interval_s` entirely.
        if (self.mode is ChaosMode.DISCONNECT_STORM
                and self.disconnect_s >= self.interval_s):
            raise ValueError("chaos disconnect_s must be shorter than interval_s")
        if self.latency_min_ms <= 0 or self.latency_max_ms < self.latency_min_ms:
            raise ValueError(
                "chaos latency window must satisfy 0 < latency_min_ms <= latency_max_ms"
            )
        if not 0.0 <= self.latency_probability <= 1.0:
            raise ValueError("chaos latency_probability must be within [0, 1]")

    @property
    def expected_silence_ms(self) -> float:
        """Worst feed silence the profile can produce, for timeout sanity checks."""
        if self.mode is ChaosMode.LATENCY_INJECTION:
            return self.latency_max_ms
        return self.disconnect_s * 1000.0


@dataclass(frozen=True)
class ChaosEvent:
    """One disruption, as emitted to the log and the ``on_event`` callback."""

    kind: ChaosEventType
    at_s: float
    sequence: int
    detail: str = ""
    delay_ms: float = 0.0

    def as_dict(self) -> Dict[str, Any]:
        return {
            "kind": self.kind.value,
            "at_s": round(self.at_s, 6),
            "sequence": self.sequence,
            "detail": self.detail,
            "delay_ms": round(self.delay_ms, 3),
        }


@dataclass
class ChaosStats:
    """Counters accumulated for the lifetime of one injector."""

    frames_seen: int = 0
    frames_forwarded: int = 0
    frames_dropped: int = 0
    outages: int = 0
    reconnects: int = 0
    latency_events: int = 0
    latency_injected_ms: float = 0.0
    max_latency_ms: float = 0.0
    max_heartbeat_gap_s: float = 0.0

    def as_dict(self) -> Dict[str, Any]:
        return {
            "frames_seen": self.frames_seen,
            "frames_forwarded": self.frames_forwarded,
            "frames_dropped": self.frames_dropped,
            "outages": self.outages,
            "reconnects": self.reconnects,
            "latency_events": self.latency_events,
            "latency_injected_ms": round(self.latency_injected_ms, 3),
            "max_latency_ms": round(self.max_latency_ms, 3),
            "max_heartbeat_gap_s": round(self.max_heartbeat_gap_s, 6),
        }

    @property
    def drop_rate(self) -> float:
        if not self.frames_seen:
            return 0.0
        return self.frames_dropped / self.frames_seen

    @property
    def mean_latency_ms(self) -> float:
        if not self.latency_events:
            return 0.0
        return self.latency_injected_ms / self.latency_events


class ChaosNetworkInjector:
    """Frame gate that injects disconnects or latency spikes.

    Parameters
    ----------
    config:
        The schedule (see :class:`ChaosConfig`).
    kill_switch:
        Optional native ``KillSwitch``, exposed for callers that want to
        correlate blocked orders with chaos windows. The injector never trips it
        directly -- only the heartbeat path does, because a timeout is what a
        real outage looks like to the engine.
    heartbeat:
        Optional sink with ``beat()``; called once per *forwarded* frame, so a
        stalled feed stops beating and the watchdog can expire it.
    clock / sleep / rng:
        Injection points for deterministic tests. ``sleep`` must advance
        ``clock`` when a fake clock is supplied.
    on_event:
        Optional callback for every disruption, e.g. to stream them into a
        report. Exceptions from the callback are logged, never propagated into
        the feed loop.
    """

    def __init__(
        self,
        config: ChaosConfig,
        *,
        kill_switch: Optional[Any] = None,
        heartbeat: Optional[HeartbeatSink] = None,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
        rng: Optional[random.Random] = None,
        on_event: Optional[Callable[[ChaosEvent], None]] = None,
    ) -> None:
        self._config = config
        self._kill_switch = kill_switch
        self._heartbeat = heartbeat
        self._clock = clock
        self._sleep = sleep
        self._rng = rng if rng is not None else random.Random(config.seed)
        self._on_event = on_event
        self._stats = ChaosStats()
        self._events: List[ChaosEvent] = []
        self._stop = threading.Event()
        self._started_at = clock()
        self._chaos_until: Optional[float] = (
            self._started_at + config.duration_s if config.duration_s > 0 else None
        )
        # The first outage lands one full interval after the start, so every
        # run begins with a healthy feed (and a passing smoke test).
        self._next_outage_at = self._started_at + config.interval_s
        self._epoch = 0
        self._in_outage = False
        self._last_beat_at: Optional[float] = None
        logger.info(
            "chaos armed: mode=%s interval=%.2fs disconnect=%.2fs "
            "latency=%.0f-%.0fms p=%.2f seed=%s window=%s",
            config.mode.value, config.interval_s, config.disconnect_s,
            config.latency_min_ms, config.latency_max_ms,
            config.latency_probability, config.seed,
            f"{config.duration_s:.2f}s" if config.duration_s else "whole run",
        )

    # ----------------------------------------------------------- properties
    @property
    def config(self) -> ChaosConfig:
        return self._config

    @property
    def stats(self) -> ChaosStats:
        return self._stats

    @property
    def events(self) -> List[ChaosEvent]:
        return list(self._events)

    @property
    def epoch(self) -> int:
        """Connection epoch: incremented on every successful reconnect."""
        return self._epoch

    @property
    def in_outage(self) -> bool:
        return self._in_outage

    @property
    def stopped(self) -> bool:
        return self._stop.is_set()

    @property
    def kill_switch(self) -> Optional[Any]:
        return self._kill_switch

    # ------------------------------------------------------------- control
    def request_stop(self) -> None:
        """Stops forwarding frames (safe to call from a signal handler)."""
        if not self._stop.is_set():
            self._stop.set()
            self._emit(ChaosEventType.STOP, self._clock(), 0, "stop requested")

    # --------------------------------------------------------------- gate
    def admit(self, sequence: int = 0) -> bool:
        """Returns ``True`` when the frame should reach the engine."""
        if self._stop.is_set():
            return False

        now = self._clock()
        self._stats.frames_seen += 1

        if self._chaos_until is not None and now >= self._chaos_until:
            # The storm is over: forward cleanly so the tail of the run is a
            # usable baseline again.
            return self._forward(now, sequence)

        if self._config.mode is ChaosMode.DISCONNECT_STORM:
            if now >= self._next_outage_at:
                return self._run_outage(now, sequence)
        else:
            delay_ms = self._draw_latency_ms()
            if delay_ms > 0.0:
                self._stats.latency_events += 1
                self._stats.latency_injected_ms += delay_ms
                self._stats.max_latency_ms = max(self._stats.max_latency_ms, delay_ms)
                self._emit(ChaosEventType.LATENCY, now, sequence,
                           f"+{delay_ms:.0f}ms", delay_ms)
                self._sleep_for(delay_ms / 1000.0)
                now = self._clock()

        return self._forward(now, sequence)

    # ``feed_archive(frame_gate=...)`` calls the gate like a predicate.
    __call__ = admit

    # ------------------------------------------------------------ internals
    def _run_outage(self, now: float, sequence: int) -> bool:
        """Simulates a dead socket, then reconnects.

        The frame in hand is lost (``frames_dropped``): it arrived exactly as
        the socket went down. While the feed is blocked the source produces
        nothing, so no further frames are counted as dropped -- that is what a
        real reconnect looks like, and it is why the engine must rely on its own
        sequence-gap detection once the connection returns.
        """
        self._in_outage = True
        self._stats.outages += 1
        self._stats.frames_dropped += 1

        outage_s = self._config.disconnect_s
        if self._chaos_until is not None:
            outage_s = min(outage_s, max(0.0, self._chaos_until - now))
        self._emit(ChaosEventType.DISCONNECT, now, sequence,
                   f"epoch={self._epoch} outage={outage_s * 1000:.0f}ms")

        # No heartbeat is emitted while the socket is down: that silence is
        # precisely what the dead-man's switch is watching for.
        self._sleep_for(outage_s)

        self._stats.reconnects += 1
        self._in_outage = False
        # The epoch is the count of *successful* reconnects.
        self._epoch += 1
        self._emit(ChaosEventType.RECONNECT, self._clock(), sequence,
                   f"epoch={self._epoch}")

        # Schedule the next outage from the reconnect, never in a burst.
        self._next_outage_at += self._config.interval_s
        if self._next_outage_at <= self._clock():
            self._next_outage_at = self._clock() + self._config.interval_s
        return False

    def _forward(self, now: float, sequence: int) -> bool:
        self._stats.frames_forwarded += 1
        if self._last_beat_at is not None:
            gap = now - self._last_beat_at
            if gap > self._stats.max_heartbeat_gap_s:
                self._stats.max_heartbeat_gap_s = gap
        self._last_beat_at = now
        if self._heartbeat is not None:
            self._heartbeat.beat()
        return True

    def _draw_latency_ms(self) -> float:
        probability = self._config.latency_probability
        if probability <= 0.0:
            return 0.0
        if self._rng.random() >= probability:
            return 0.0
        return self._rng.uniform(self._config.latency_min_ms,
                                 self._config.latency_max_ms)

    def _sleep_for(self, seconds: float) -> None:
        """Sleeps in short slices so ``request_stop()`` is honoured promptly."""
        if seconds <= 0.0:
            return
        deadline = self._clock() + seconds
        while not self._stop.is_set():
            remaining = deadline - self._clock()
            if remaining <= 0.0:
                return
            self._sleep(min(remaining, SLEEP_SLICE_S))

    # ------------------------------------------------------------ reporting
    def _emit(self, kind: ChaosEventType, at_s: float, sequence: int,
              detail: str = "", delay_ms: float = 0.0) -> None:
        event = ChaosEvent(
            kind=kind,
            at_s=at_s - self._started_at,
            sequence=sequence,
            detail=detail,
            delay_ms=delay_ms,
        )
        if len(self._events) < MAX_RETAINED_EVENTS:
            self._events.append(event)

        marker = MARKERS.get(kind, "..")
        if kind is ChaosEventType.DISCONNECT:
            logger.warning("%s DISCONNECT  t=+%.2fs  seq=%d  %s",
                           marker, event.at_s, sequence, detail)
        elif kind is ChaosEventType.RECONNECT:
            logger.info("%s RECONNECT   t=+%.2fs  seq=%d  %s",
                        marker, event.at_s, sequence, detail)
        elif kind is ChaosEventType.LATENCY:
            logger.warning("%s LATENCY     t=+%.2fs  seq=%d  %s",
                           marker, event.at_s, sequence, detail)
        else:
            logger.info("%s STOP        t=+%.2fs  %s", marker, event.at_s, detail)

        if self._on_event is not None:
            try:
                self._on_event(event)
            except Exception:  # noqa: BLE001 - a report sink must not stop the feed
                logger.exception("chaos event callback failed; continuing")

    def snapshot(self) -> Dict[str, Any]:
        """Machine-readable state, suitable for a JSON report."""
        data = self._stats.as_dict()
        data.update(
            {
                "mode": self._config.mode.value,
                "epoch": self._epoch,
                "in_outage": self._in_outage,
                "stopped": self.stopped,
                "chaos_window_s": self._config.duration_s,
                "elapsed_s": round(self._clock() - self._started_at, 3),
                "drop_rate": round(self._stats.drop_rate, 4),
                "mean_latency_ms": round(self._stats.mean_latency_ms, 3),
                "events": [event.as_dict() for event in self._events],
            }
        )
        return data

    def summary(self) -> str:
        """Human-readable block printed at the end of a chaos run."""
        stats = self._stats
        lines = [
            f"mode            : {self._config.mode.value}",
            f"frames          : forwarded={stats.frames_forwarded} "
            f"dropped={stats.frames_dropped} seen={stats.frames_seen} "
            f"(drop rate {stats.drop_rate * 100:.1f}%)",
        ]
        if self._config.mode is ChaosMode.DISCONNECT_STORM:
            lines.append(
                f"outages         : {stats.outages} "
                f"(reconnects={stats.reconnects}, epoch={self._epoch})"
            )
        else:
            lines.append(
                f"latency spikes  : {stats.latency_events} "
                f"injected={stats.latency_injected_ms:.0f}ms "
                f"mean={stats.mean_latency_ms:.0f}ms "
                f"max={stats.max_latency_ms:.0f}ms"
            )
        lines.append(f"max feed gap    : {stats.max_heartbeat_gap_s:.3f}s")
        return "\n".join(lines)
