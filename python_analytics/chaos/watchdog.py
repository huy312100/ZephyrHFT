"""Supervision thread that turns feed silence into a trading halt.

The watchdog is deliberately the *only* component allowed to trip the kill
switch from an injected fault: a disconnect or a lag spike never calls
``trip()`` itself, it just stops the heartbeat. That keeps the failure path
honest -- the halt is caused by observed silence, exactly like production.
"""
from __future__ import annotations

import logging
import threading
import time
from typing import Any, Callable, Dict, Optional

logger = logging.getLogger("zephyr.chaos.heartbeat")


class HeartbeatWatchdog:
    """Polls a native ``HeartbeatMonitor`` on a daemon thread.

    Timeout sequence:

    1. ``HeartbeatMonitor.poll()`` trips the attached ``KillSwitch``;
    2. the watchdog moves the switch to ``RECOVERING`` so orders stay blocked
       while cancels/flattening are in flight;
    3. after ``recovery_delay_s`` it calls ``reset()`` when ``auto_recover`` is
       set, otherwise the halt stays in place for an operator to clear.

    ``interval_s`` is the supervision cadence, not the timeout: the timeout
    lives on the native monitor, so a slow watchdog can delay detection but can
    never change the threshold. The thread is a daemon so a crashed host exits
    cleanly; the CLI still joins it before printing the verdict.

    Automatic recovery waits for a frame *after* the trip
    (:meth:`await_beat`): clearing the halt while the socket is still down
    would simply re-trip on the next poll and quote into nothing.
    """

    def __init__(
        self,
        monitor: Any,
        *,
        interval_s: float = 0.05,
        kill_switch: Optional[Any] = None,
        auto_recover: bool = True,
        recovery_delay_s: float = 0.5,
        recovery_timeout_s: float = 5.0,
        on_timeout: Optional[Callable[[Any], None]] = None,
        on_recover: Optional[Callable[[Any], None]] = None,
        name: str = "chaos-heartbeat",
    ) -> None:
        if interval_s <= 0:
            raise ValueError("watchdog interval_s must be > 0")
        if recovery_delay_s < 0:
            raise ValueError("watchdog recovery_delay_s must be >= 0")
        if recovery_timeout_s <= 0:
            raise ValueError("watchdog recovery_timeout_s must be > 0")
        self._monitor = monitor
        self._interval_s = float(interval_s)
        self._kill_switch = kill_switch
        self._auto_recover = bool(auto_recover)
        self._recovery_delay_s = float(recovery_delay_s)
        self._recovery_timeout_s = float(recovery_timeout_s)
        self._on_timeout = on_timeout
        self._on_recover = on_recover
        self._name = name
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()
        self._timeouts = 0
        self._recoveries = 0
        self._trip_beat_ns = 0

    # ------------------------------------------------------------ lifecycle
    def start(self) -> "HeartbeatWatchdog":
        if self._thread is not None:
            return self
        thread = threading.Thread(target=self._run, name=self._name, daemon=True)
        self._thread = thread
        thread.start()
        logger.info("watchdog started: interval=%.0fms auto_recover=%s "
                    "recovery_delay=%.2fs",
                    self._interval_s * 1000, self._auto_recover,
                    self._recovery_delay_s)
        return self

    def stop(self) -> None:
        self._stop.set()

    def join(self, timeout: Optional[float] = None) -> None:
        thread = self._thread
        if thread is not None:
            thread.join(timeout)

    def __enter__(self) -> "HeartbeatWatchdog":
        return self.start()

    def __exit__(self, *_exc: object) -> None:
        self.stop()
        self.join(timeout=2.0)

    @property
    def running(self) -> bool:
        thread = self._thread
        return thread is not None and thread.is_alive()

    # ------------------------------------------------- deterministic surface
    def poll_once(self, now_ns: int = 0) -> bool:
        """Runs one supervision step; returns True when an expiry was handled."""
        if not self._monitor.poll(now_ns):
            return False
        self._handle_timeout(now_ns)
        return True

    def await_beat(self, timeout_s: Optional[float] = None) -> bool:
        """Waits for a frame that arrived *after* the last trip.

        Returns ``False`` when the watchdog is stopped or the feed stays silent
        for ``timeout_s`` (defaults to ``recovery_timeout_s``). This is what
        keeps auto-recovery from clearing a halt into a dead socket.
        """
        deadline = time.monotonic() + (
            self._recovery_timeout_s if timeout_s is None else max(0.0, timeout_s)
        )
        while not self._stop.is_set():
            if self._monitor.last_beat_ns() > self._trip_beat_ns:
                return True
            if time.monotonic() >= deadline:
                return False
            self._stop.wait(min(self._interval_s, 0.05))
        return False

    def recover(self) -> None:
        """Clears the halt and re-arms the heartbeat window."""
        if self._kill_switch is not None:
            self._kill_switch.reset()
        self._monitor.reset()
        with self._lock:
            self._recoveries += 1
        state = "n/a"
        if self._kill_switch is not None:
            state = str(self._kill_switch.get_state().name)
        logger.warning("+++ RECOVERED   state=%s  order flow restored", state)
        if self._on_recover is not None:
            self._safe_call(self._on_recover, None, "recover")

    def stats(self) -> Dict[str, Any]:
        with self._lock:
            timeouts = self._timeouts
            recoveries = self._recoveries
        native = self._monitor.stats()
        return {
            "timeouts": timeouts,
            "recoveries": recoveries,
            "interval_s": self._interval_s,
            "auto_recover": self._auto_recover,
            "recovery_delay_s": self._recovery_delay_s,
            "recovery_timeout_s": self._recovery_timeout_s,
            "monitor_beats": native.beats,
            "monitor_timeouts": native.timeouts,
            "monitor_silence_ms": native.silence_ns / 1_000_000.0,
            "monitor_max_silence_ms": native.max_silence_ns / 1_000_000.0,
            "monitor_expired": native.expired,
            "kill_switch_state": str(native.state.name),
        }

    # ------------------------------------------------------------ internals
    def _handle_timeout(self, now_ns: int = 0) -> None:
        native = self._monitor.stats(now_ns)
        self._trip_beat_ns = native.last_beat_ns
        with self._lock:
            self._timeouts += 1
        logger.critical(
            "!!! HEARTBEAT TIMEOUT  silence=%.0fms >= limit=%.0fms%s",
            native.silence_ns / 1_000_000.0, native.timeout_ns / 1_000_000.0,
            "  -> kill switch TRIGGERED" if native.armed else "",
        )
        if self._on_timeout is not None:
            self._safe_call(self._on_timeout, native, "timeout")
        if self._kill_switch is not None and self._kill_switch.begin_recovery():
            logger.warning("    state=RECOVERING (orders blocked, cancels in flight)")

    def _safe_call(self, callback: Callable[[Any], None], argument: Any,
                   label: str) -> None:
        try:
            callback(argument)
        except Exception:  # noqa: BLE001 - a report sink must not kill the watchdog
            logger.exception("%s callback failed; continuing", label)

    def _run(self) -> None:
        while not self._stop.is_set():
            if self._stop.wait(self._interval_s):
                return
            try:
                if not self.poll_once():
                    continue
                if not self._auto_recover:
                    continue
                if self._stop.wait(self._recovery_delay_s):
                    return
                # Do not clear a halt into a socket that is still silent.
                if not self.await_beat():
                    logger.error(
                        "feed still silent after %.1fs; halt stays in place "
                        "(operator reset required)", self._recovery_timeout_s)
                    continue
                self.recover()
            except Exception:  # noqa: BLE001 - keep supervising after a bad iteration
                logger.exception("watchdog iteration failed; continuing")

