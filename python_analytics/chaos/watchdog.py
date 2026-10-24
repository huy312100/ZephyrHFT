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

    Recovery is also scoped to the halt the heartbeat caused.
    ``KillSwitch::trip()`` converges on ``TRIGGERED`` but every call is counted
    and overwrites the recorded reason, so an expiry that lands while the
    operator, the drawdown guard or the reject-streak guard already holds the
    halt must not be moved into the remediation window -- and a second trip
    arriving during that window abandons the automatic reset instead of
    clearing a condition that is not ours.
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
    def poll_once(self, now_ns: int = 0, *, own_halt: bool = True) -> bool:
        """Runs one supervision step; returns True when an expiry was handled.

        ``own_halt`` records whether the switch was ``NORMAL`` *before* this
        poll, i.e. whether the expiry is the only thing halting trading. The
        timeout is reported either way, but a halt another guard already owns is
        neither moved to ``RECOVERING`` nor cleared later.
        """
        if not self._monitor.poll(now_ns):
            return False
        self._handle_timeout(now_ns, own_halt=own_halt)
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
    def _handle_timeout(self, now_ns: int = 0, *, own_halt: bool = True) -> None:
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
        if self._kill_switch is None:
            return
        if not own_halt:
            # The switch was already halting for another reason; anything this
            # thread does to it now -- RECOVERING, reset() -- would edit a halt
            # it does not understand.
            logger.error("    halt predates this expiry; "
                         "leaving it to its owner to clear")
            return
        if self._kill_switch.begin_recovery():
            logger.warning("    state=RECOVERING (orders blocked, cancels in flight)")

    def _state_name(self) -> str:
        """Current switch state by name, without importing the native enum."""
        if self._kill_switch is None:
            return "NORMAL"
        try:
            state = self._kill_switch.get_state()
        except Exception:  # noqa: BLE001 - telemetry must not kill the watchdog
            return "UNKNOWN"
        return str(getattr(state, "name", state))

    def _switch_is_normal(self) -> bool:
        """True when no other guard owns a halt on the switch."""
        return self._state_name() == "NORMAL"

    def _trip_count(self) -> int:
        """Cumulative trips, used to detect a second trigger during recovery."""
        if self._kill_switch is None:
            return 0
        try:
            return int(self._kill_switch.get_stats().trip_count)
        except Exception:  # noqa: BLE001 - a switch without stats still works
            return 0

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
                # Sample ownership *before* the poll: the poll trips the switch
                # and overwrites the recorded reason, so a halt that already
                # existed can only be recognised up front.
                owned = self._switch_is_normal()
                if not self.poll_once(own_halt=owned):
                    continue
                if not owned:
                    logger.error(
                        "feed went silent while the switch was already halted; "
                        "that halt stays with its owner (no auto-recovery)")
                    continue
                if not self._auto_recover:
                    continue
                trips = self._trip_count()
                if self._stop.wait(self._recovery_delay_s):
                    return
                # Do not clear a halt into a socket that is still silent.
                if not self.await_beat():
                    logger.error(
                        "feed still silent after %.1fs; halt stays in place "
                        "(operator reset required)", self._recovery_timeout_s)
                    continue
                current = self._trip_count()
                if current != trips:
                    # A drawdown breach, a reject streak or an operator trip
                    # landed during the window: clearing now would resume
                    # trading into a condition this watchdog did not observe.
                    logger.error(
                        "another trip landed during the recovery window "
                        "(trips %d -> %d); halt stays in place", trips, current)
                    continue
                self.recover()
            except Exception:  # noqa: BLE001 - keep supervising after a bad iteration
                logger.exception("watchdog iteration failed; continuing")
