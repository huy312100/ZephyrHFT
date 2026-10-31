#!/usr/bin/env python3
"""Phase 1: real-time L2 ingestion client (exchange depth stream -> gzip NDJSON).

The client subscribes to ``<endpoint>/<symbol>@<stream_type>``, stamps every
frame with ``time.time_ns()`` the instant it leaves the socket, derives the
network/feed latency from the venue event time (``E``, falling back to ``T``)
and appends the byte-exact payload to gzip-compressed NDJSON roll files under
``data_ingestion.storage_path`` (``data/raw_l2`` by default).

With ``data_ingestion.aux_streams`` set (``trade`` by default in
``config/settings.yaml``) the socket is the *combined* one, so trade prints are
archived interleaved with the depth frames they moved. That is what a
``--fill-model trade`` backtest needs: a depth snapshot alone can never show
that a print happened at our quote.

Design notes
------------
* **The receive loop never touches the filesystem.** Frames are appended to an
  in-memory ``bytearray``; a background task swaps that buffer and hands the
  detached chunk to a worker thread (``asyncio.to_thread``) which compresses and
  writes it. gzip and disk I/O therefore cost the event loop nothing, and a slow
  disk only grows the buffer (a high-water warning is logged, never a drop).
* **Reconnects are automatic** with capped exponential backoff plus jitter; the
  backoff resets after a healthy session.
* **Shutdown is graceful.** SIGINT/SIGTERM close the socket, drain the buffer,
  write the gzip trailer and close the roll file, so every archive is readable.
* **Everything received is archived**, including frames without a venue
  timestamp (subscription acks, venue errors); those are counted separately so
  a gap in the capture is always attributable.
* **Combined frames are unwrapped textually, not re-serialised.** A combined
  socket delivers ``{"stream": .., "data": ..}``; the envelope is sliced off so
  the archived line is the venue's own payload bytes, identical in shape to a
  single-stream capture (and to what the C++ parser expects).

Examples
--------
    python3 scripts/ingest_l2.py --duration 30
    python3 scripts/ingest_l2.py --dry-run --duration 10   # no files, no disk
    python3 scripts/ingest_l2.py --replay data/sample_events.ndjson
    python3 scripts/ingest_l2.py --depth-only --duration 30  # depth, no tape
"""
from __future__ import annotations

import argparse
import asyncio
import gzip
import hashlib
import json
import logging
import math
import os
import random
import signal
import sys
import time
import urllib.error
import urllib.request
from collections import deque
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import (Any, AsyncIterator, Callable, Deque, List, Mapping, Optional,
                    Sequence, Tuple)

# Make the repository root importable when this script is run directly.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from python_analytics.utils import configure_logging, load_settings
from python_analytics.utils.config import (
    COMPRESSION_CHOICES,
    ConfigError,
    DataIngestionSettings,
    check_snapshot_limit,
    depth_snapshot_url,
    parse_stream_names,
    silent_aux_streams,
)

logger = logging.getLogger("zephyr.ingest_l2")

NS_PER_MS = 1_000_000
NS_PER_S = 1_000_000_000

#: ``(receive_timestamp_ns, raw_frame_text)`` as yielded by a frame source.
Frame = Tuple[int, str]

#: Buffer size above which a WARNING is logged (nothing is ever dropped).
HIGH_WATER_BYTES = 64 * 1024 * 1024

#: Cap on the roll-file name prefix. ``NAME_MAX`` is 255 bytes on Linux and on
#: APFS; the stamp, sequence and suffix add ~30, so 120 leaves room for both
#: and for a directory the venue does not control.
MAX_SLUG_BYTES = 120

#: A connection that survives this long resets the reconnect backoff.
HEALTHY_SESSION_S = 5.0

#: Binance closes idle connections after ~3 min; our ping keeps it alive.
PING_INTERVAL_S = 20


# --------------------------------------------------------------------------- #
# Frame helpers
# --------------------------------------------------------------------------- #
def unwrap_payload(payload: Any) -> Any:
    """Unwraps a Binance combined-stream envelope (``{"stream":..,"data":..}``)."""
    if isinstance(payload, Mapping) and isinstance(payload.get("data"), Mapping):
        return payload["data"]
    return payload


_JSON_DECODER = json.JSONDecoder()


def _skip_ws(raw: str, index: int) -> int:
    """Advances past spaces, tabs and newlines."""
    limit = len(raw)
    while index < limit and raw[index] in " \t\r\n":
        index += 1
    return index


def _envelope_data_span(raw: str) -> Optional[Tuple[int, int]]:
    """Returns the text span of a combined envelope's ``data`` value, or ``None``.

    The members are walked one at a time with ``JSONDecoder.raw_decode``, which
    reports where each value ends, so the span is the value's own text: a brace
    inside a string, a nested object, or a key that *follows* ``data`` can
    neither shorten nor extend it. ``None`` unless the line is a single
    ``{"stream": ..., "data": ...}`` object, so a bare payload that happens to
    carry a ``data`` key is left alone.
    """
    index = _skip_ws(raw, 0)
    if index >= len(raw) or raw[index] != "{":
        return None
    index += 1
    has_stream = False
    span: Optional[Tuple[int, int]] = None
    while True:
        index = _skip_ws(raw, index)
        if index >= len(raw):
            return None
        if raw[index] == "}":
            break
        try:
            key, index = _JSON_DECODER.raw_decode(raw, index)
        except ValueError:
            return None
        if not isinstance(key, str):
            return None
        index = _skip_ws(raw, index)
        if index >= len(raw) or raw[index] != ":":
            return None
        index = _skip_ws(raw, index + 1)
        start = index
        try:
            _, index = _JSON_DECODER.raw_decode(raw, index)
        except ValueError:
            return None
        if key == "stream":
            has_stream = True
        elif key == "data":
            span = (start, index)
        index = _skip_ws(raw, index)
        if index >= len(raw):
            return None
        if raw[index] == ",":
            index += 1
            continue
        if raw[index] != "}":
            return None
        break
    if not has_stream or span is None:
        return None
    return span


def unwrap_frame_text(raw: str) -> str:
    """Returns the line to archive for one socket frame.

    A single-stream socket already delivers the bare payload, which is archived
    verbatim. A combined socket wraps it as
    ``{"stream": "<name>", "data": <payload>}``; the envelope is sliced off
    *textually* rather than round-tripped through ``json``, because re-serialising
    would reformat numbers and the archive would stop being ground truth. The
    ``stream`` name is pure redundancy: the payload carries ``s`` itself.
    """
    if '"stream"' not in raw:
        return raw
    span = _envelope_data_span(raw)
    if span is None:
        return raw
    return raw[span[0]:span[1]]


def event_kind(payload: Any) -> str:
    """Classifies a payload as ``depth``, ``trade`` or ``other``."""
    data = unwrap_payload(payload)
    if not isinstance(data, Mapping):
        return "other"
    event = data.get("e")
    if event in ("depthUpdate", "depth"):
        return "depth"
    if event in ("trade", "aggTrade"):
        return "trade"
    return "other"


def event_timestamp_ns(payload: Any) -> Optional[int]:
    """Returns the venue event time in nanoseconds (``E`` then ``T``), or ``None``.

    Binance depth payloads carry ``E`` (event time, ms) and, for futures,
    ``T`` (transaction time, ms). Binary floating point never touches this path:
    the value is converted with ``int()`` and scaled by ``1_000_000``.
    """
    data = unwrap_payload(payload)
    if not isinstance(data, Mapping):
        return None
    for key in ("E", "T"):
        raw = data.get(key)
        if raw is None or isinstance(raw, bool):
            continue
        try:
            value = int(raw)
        except (TypeError, ValueError):
            continue
        if value > 0:
            return value * NS_PER_MS
    return None


def parse_payload(raw: str) -> Optional[Any]:
    """Parses a frame, returning ``None`` when it is not valid JSON."""
    try:
        return json.loads(raw)
    except (TypeError, ValueError):
        return None


def fetch_snapshot_text(url: str, timeout_s: float) -> str:
    """Fetches an absolute depth snapshot and returns the venue's raw body.

    Runs on a worker thread (``asyncio.to_thread``), so the receive loop never
    blocks on HTTP. The body is returned **verbatim**: it is archived as a line
    of the roll file and re-serialising it would make the capture disagree with
    the venue about number formatting, which is the one thing the archive exists
    not to do. Raises on any failure; the caller decides what a failed seed means
    (the capture continues, it just records the hole).
    """
    request = urllib.request.Request(
        url, headers={"User-Agent": "ZephyrHFT-ingest/1.0", "Accept": "application/json"}
    )
    with urllib.request.urlopen(request, timeout=timeout_s) as response:
        return response.read().decode("utf-8")


def annotate_snapshot(text: str, now_ms: int, symbol: str) -> str:
    """Adds the capture metadata a REST book snapshot needs but does not carry.

    Two fields, both facts about this capture rather than claims about the
    market, and both inserted **textually** right after the opening brace so the
    venue's own bytes stay byte-for-byte:

    * ``E`` -- Binance's USDⓈ-M snapshot carries ``E``/``T``; the spot one
      carries neither, and a book with no time cannot be placed on the mid
      timeline, so it would be archived and then ignored.
    * ``s`` -- neither snapshot names its market, and the consumers select
      frames by symbol. An unlabelled seed is the one line in the archive that a
      mixed capture would accept from any market and use as its first mid.

    Bodies that already carry a field keep it, and anything that is not a JSON
    object is returned unchanged.
    """
    start = text.find("{")
    if start < 0:
        return text
    try:
        payload = json.loads(text)
    except (TypeError, ValueError):
        return text
    if not isinstance(payload, Mapping):
        return text
    parts: List[str] = []
    if payload.get("E") is None and payload.get("T") is None:
        parts.append(f'"E":{int(now_ms)}')
    if payload.get("s") is None and payload.get("symbol") is None:
        parts.append(f'"s":"{str(symbol).upper()}"')
    if not parts:
        return text
    return f'{text[:start + 1]}{",".join(parts)},{text[start + 1:]}'


def percentile(samples: Sequence[int], fraction: float) -> float:
    """Nearest-rank percentile over an ascending sequence of nanosecond samples."""
    if not samples:
        return 0.0
    rank = math.ceil(fraction * len(samples)) - 1
    return float(samples[max(0, min(len(samples) - 1, rank))])


@dataclass(frozen=True)
class LatencySummary:
    """Latency snapshot; min/max/counters are cumulative, percentiles rolling."""

    samples: int
    mean_ms: float
    p50_ms: float
    p95_ms: float
    p99_ms: float
    min_ms: float
    max_ms: float
    last_ms: float
    breaches: int
    negative: int


class LatencyTracker:
    """Rolling feed-latency statistics with a breach counter.

    Latency is ``receive_timestamp_ns - event_timestamp_ns``: the venue-to-us
    transit time (exchange egress + network + kernel/socket scheduling). A
    negative value means the venue clock is ahead of ours and is counted
    separately instead of silently skewing the percentiles.
    """

    def __init__(self, warn_ns: int, window: int = 20_000) -> None:
        self._warn_ns = int(warn_ns)
        self._samples: Deque[int] = deque(maxlen=max(1, int(window)))
        self._count = 0
        self._total_ns = 0
        self._min_ns = 0
        self._max_ns = 0
        self._last_ns = 0
        self._breaches = 0
        self._negative = 0

    @property
    def warn_ns(self) -> int:
        return self._warn_ns

    @property
    def breaches(self) -> int:
        return self._breaches

    @property
    def last_ns(self) -> int:
        return self._last_ns

    def record(self, latency_ns: int) -> bool:
        """Records one sample; returns ``True`` when it exceeds the threshold."""
        self._count += 1
        self._total_ns += latency_ns
        self._last_ns = latency_ns
        if latency_ns < 0:
            self._negative += 1
        if self._count == 1 or latency_ns < self._min_ns:
            self._min_ns = latency_ns
        if latency_ns > self._max_ns:
            self._max_ns = latency_ns
        self._samples.append(latency_ns)
        if latency_ns > self._warn_ns:
            self._breaches += 1
            return True
        return False

    def summary(self, *, reset_window: bool = False) -> LatencySummary:
        """Materialises the statistics; ``reset_window`` clears percentiles only."""
        ordered = sorted(self._samples)
        mean_ns = self._total_ns / self._count if self._count else 0.0
        summary = LatencySummary(
            samples=len(ordered),
            mean_ms=mean_ns / NS_PER_MS,
            p50_ms=percentile(ordered, 0.50) / NS_PER_MS,
            p95_ms=percentile(ordered, 0.95) / NS_PER_MS,
            p99_ms=percentile(ordered, 0.99) / NS_PER_MS,
            min_ms=self._min_ns / NS_PER_MS,
            max_ms=self._max_ns / NS_PER_MS,
            last_ms=self._last_ns / NS_PER_MS,
            breaches=self._breaches,
            negative=self._negative,
        )
        if reset_window:
            self._samples.clear()
        return summary


def reconnect_delay(
    attempt: int,
    *,
    base: float,
    cap: float,
    jitter: float = 0.25,
    rng: Optional[random.Random] = None,
) -> float:
    """Capped exponential backoff with symmetric jitter.

    ``attempt`` is zero-based, so the first retry waits ``base`` seconds, the
    second ``2*base``, and so on until ``cap``. Jitter de-synchronises
    reconnects of co-located clients; ``jitter=0`` makes it deterministic.
    """
    exponent = max(0, min(int(attempt), 32))
    delay = min(cap, base * (2.0 ** exponent))
    if jitter > 0.0:
        generator = rng or random
        delay *= 1.0 + jitter * (generator.random() * 2.0 - 1.0)
    return max(0.0, min(cap, delay))


# --------------------------------------------------------------------------- #
# Compressed roll-file writer
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class WriterStats:
    """Snapshot of the roll-file writer (counters are cumulative)."""

    files: int
    records: int
    buffered_records: int
    buffered_bytes: int
    uncompressed_bytes: int
    compressed_bytes: int
    current_path: Optional[str]
    write_errors: int


class RawL2Writer:
    """Append-only gzip NDJSON roll files drained off the event loop.

    Concurrency contract: ``append()`` and the buffer hand-off in ``flush_async``
    run on the event-loop thread, while ``_write`` runs in a worker thread via
    ``asyncio.to_thread``. Only the *detached* chunk reaches the worker, so the
    loop can keep appending to the fresh buffer meanwhile; at most one ``_write``
    is ever in flight (the writer task awaits it), and ``aclose()`` joins that
    task before the final synchronous flush, so the file handles are never
    touched by two threads at once.
    """

    def __init__(
        self,
        settings: DataIngestionSettings,
        *,
        directory: Optional[Path] = None,
        max_file_bytes: Optional[int] = None,
        rotate_seconds: Optional[int] = None,
        clock=time.time,
    ) -> None:
        self._settings = settings
        self._directory = Path(directory) if directory is not None else settings.storage_dir
        self._suffix = settings.file_suffix
        self._compress = settings.compression == "gzip"
        self._clock = clock
        self._max_bytes = (
            settings.max_file_mb * 1024 * 1024 if max_file_bytes is None else max_file_bytes
        )
        self._rotate_s = settings.rotate_seconds if rotate_seconds is None else rotate_seconds
        self._buffer = bytearray()
        self._buffer_records = 0
        self._handle: Any = None
        self._raw: Any = None
        self._path: Optional[Path] = None
        self._opened_at = 0.0
        self._live_bytes = 0
        self._file_bytes = 0
        self._total_records = 0
        self._total_uncompressed = 0
        self._closed_files = 0
        self._closed_bytes = 0
        self._write_errors = 0
        self._sequence = 0
        self._task: Optional[asyncio.Task] = None
        self._wake: Optional[asyncio.Event] = None
        self._closed = False
        self._high_water_logged = False
        # Create the capture directory eagerly: a missing path is never an error.
        self._directory.mkdir(parents=True, exist_ok=True)

    # -- hot path ---------------------------------------------------------- #
    def append(self, line: str) -> None:
        """Queues one frame (no syscall, no compression, no await)."""
        if self._closed:
            raise RuntimeError("cannot append to a closed RawL2Writer")
        self._buffer += line.encode("utf-8")
        self._buffer.append(0x0A)
        self._buffer_records += 1
        if self._wake is not None:
            self._wake.set()

    # -- lifecycle --------------------------------------------------------- #
    async def start(self) -> None:
        """Starts the background drain task (call from inside the event loop)."""
        if self._task is not None or self._closed:
            return
        self._wake = asyncio.Event()
        self._task = asyncio.create_task(self._writer_loop(), name="raw-l2-writer")

    async def aclose(self) -> None:
        """Flushes the tail, writes the gzip trailer and fsyncs the roll file."""
        if self._closed:
            return
        # Cooperative stop: the writer loop exits on its own after the current
        # write, so no task is ever cancelled mid-flight (which would leave a
        # dangling ``wait_for`` inner task behind).
        self._closed = True
        if self._wake is not None:
            self._wake.set()
        task, self._task = self._task, None
        if task is not None:
            try:
                await task
            except asyncio.CancelledError:
                pass
            except Exception:  # noqa: BLE001 - never mask shutdown on drain errors
                logger.exception("roll-file drain failed during shutdown")
        # Final drain happens on the caller thread: shutdown latency is accepted
        # in exchange for a guaranteed complete gzip stream.
        try:
            self.flush()
        finally:
            self._close_file()

    # -- flushing ---------------------------------------------------------- #
    def flush(self) -> None:
        """Synchronously detaches and writes the buffer (shutdown / tests)."""
        chunk = self._detach()
        self._write(chunk, self._clock())

    async def flush_async(self) -> None:
        """Detaches the buffer and compresses/writes it in a worker thread."""
        chunk = self._detach()
        stamp = self._clock()
        try:
            await asyncio.to_thread(self._write, chunk, stamp)
        except Exception:  # noqa: BLE001 - a disk error must not stop ingestion
            self._write_errors += 1
            logger.exception("roll-file write failed; %d bytes were in the chunk",
                             len(chunk))

    def _detach(self) -> bytes:
        buffer = self._buffer
        if not buffer:
            return b""
        self._buffer = bytearray()
        self._buffer_records = 0
        return bytes(buffer)

    async def _writer_loop(self) -> None:
        interval = max(0.001, self._settings.flush_interval_ms / 1000.0)
        wake = self._wake
        assert wake is not None
        while not self._closed:
            try:
                await asyncio.wait_for(wake.wait(), timeout=interval)
            except asyncio.TimeoutError:
                pass
            wake.clear()
            self._check_backpressure()
            await self.flush_async()

    def _check_backpressure(self) -> None:
        pending = len(self._buffer)
        if pending >= HIGH_WATER_BYTES and not self._high_water_logged:
            self._high_water_logged = True
            logger.warning(
                "write buffer at %.1f MiB: disk slower than the feed "
                "(records are retained, never dropped)", pending / (1024 * 1024)
            )
        elif pending < HIGH_WATER_BYTES // 2:
            self._high_water_logged = False

    # -- file handling (worker thread only, except stats()) ---------------- #
    def prepare(self) -> None:
        """Opens the first roll file eagerly, before the socket is connected.

        The first write otherwise happens in the worker thread, where a failing
        ``_open_file`` is logged, the chunk is discarded, and ``stats.stored``
        keeps counting frames -- a capture that looks healthy and archives
        nothing. Opening one file up front turns an unusable archive (missing
        permission, wrong filesystem) into a refusal to start.
        """
        if self._handle is None:
            self._open_file(self._clock())

    def _should_rotate(self, now: float, incoming: int) -> bool:
        # Size rotation only applies when there is data to write; otherwise a
        # final empty flush would create a spurious (empty) roll file.
        if incoming > 0 and self._max_bytes > 0 and self._file_bytes > 0:
            if self._file_bytes + incoming > self._max_bytes:
                return True
        if self._rotate_s > 0 and self._opened_at:
            return (now - self._opened_at) >= self._rotate_s
        return False

    def _open_file(self, now: float) -> None:
        self._directory.mkdir(parents=True, exist_ok=True)
        stamp = datetime.fromtimestamp(now, timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        slug = self._slug()
        while True:
            self._sequence += 1
            path = self._directory / f"{slug}_{stamp}_{self._sequence:05d}{self._suffix}"
            if not path.exists():
                break
        self._path = path
        self._raw = open(path, "wb")
        if self._compress:
            # mtime=0 pins the gzip header so identical input compresses to
            # identical bytes (reproducible fixtures); the roll name carries
            # the wall-clock stamp instead.
            self._handle = gzip.GzipFile(
                filename="",
                mode="wb",
                compresslevel=self._settings.compression_level,
                fileobj=self._raw,
                mtime=0,
            )
        else:
            self._handle = self._raw
        self._opened_at = now
        self._live_bytes = 0
        self._file_bytes = 0
        logger.info("roll file open: %s", path)

    def _slug(self) -> str:
        """Roll-file name prefix, bounded so the whole name fits a filesystem.

        ``data_ingestion.aux_streams`` accepts any number of distinct tags, and
        ten long ones would push the name past ``NAME_MAX`` (255 bytes). The
        prefix is therefore capped and disambiguated with a hash of the full
        set, so two long configurations still produce distinct, readable names.
        """
        slug = "_".join(
            [
                f"{self._settings.exchange}_{self._settings.symbol}",
                *(tag.replace("@", "_") for tag in self._settings.stream_tags),
            ]
        )
        encoded = slug.encode("utf-8")
        if len(encoded) <= MAX_SLUG_BYTES:
            return slug
        digest = hashlib.sha1(encoded).hexdigest()[:8]
        head = encoded[: MAX_SLUG_BYTES - len(digest) - 1].decode("utf-8", "ignore")
        return f"{head}_{digest}"

    def _close_file(self) -> None:
        handle, self._handle = self._handle, None
        if handle is not None and handle is not self._raw:
            try:
                handle.close()  # emits the gzip trailer (CRC32 + ISIZE)
            except Exception:  # noqa: BLE001 - keep the raw handle closable
                logger.exception("failed to finalise gzip stream")
        raw, self._raw = self._raw, None
        if raw is not None:
            try:
                raw.flush()
                os.fsync(raw.fileno())
            except (OSError, ValueError):
                pass
            finally:
                raw.close()
            if self._path is not None:
                try:
                    self._closed_bytes += self._path.stat().st_size
                except OSError:
                    pass
                self._closed_files += 1
                logger.info("roll file closed: %s (%.1f KiB uncompressed)",
                            self._path, self._file_bytes / 1024)
            self._live_bytes = 0

    def _write(self, chunk: bytes, now: float) -> None:
        """Compresses and appends ``chunk``; runs in a worker thread."""
        if not chunk:
            if self._handle is not None and self._should_rotate(now, 0):
                self._close_file()
                self._open_file(now)
            return
        if self._handle is None or self._should_rotate(now, len(chunk)):
            if self._handle is not None:
                self._close_file()
            self._open_file(now)
        handle = self._handle
        assert handle is not None
        handle.write(chunk)
        # Z_SYNC_FLUSH: a crash loses at most this chunk, and the archive stays
        # readable up to the last flush instead of ending in a partial block.
        handle.flush()
        self._file_bytes += len(chunk)
        self._total_uncompressed += len(chunk)
        records = chunk.count(b"\n")
        self._total_records += records
        if self._raw is not None:
            try:
                self._live_bytes = self._raw.tell()
            except (OSError, ValueError):
                pass

    def stats(self) -> WriterStats:
        """Loop-thread-safe snapshot (no file object is touched)."""
        return WriterStats(
            files=self._closed_files + (1 if self._handle is not None else 0),
            records=self._total_records,
            buffered_records=self._buffer_records,
            buffered_bytes=len(self._buffer),
            uncompressed_bytes=self._total_uncompressed + len(self._buffer),
            compressed_bytes=self._closed_bytes + self._live_bytes,
            current_path=(str(self._path) if self._path and self._handle else None),
            write_errors=self._write_errors,
        )


# --------------------------------------------------------------------------- #
# Ingestion client
# --------------------------------------------------------------------------- #
class FeedUnavailable(RuntimeError):
    """Raised when the live feed cannot start (e.g. ``websockets`` missing)."""


@dataclass
class IngestStats:
    """Cumulative counters for one ingestion session."""

    started_ns: int = 0
    frames: int = 0
    stored: int = 0
    depth_frames: int = 0
    trade_frames: int = 0
    no_timestamp: int = 0
    malformed: int = 0
    bytes_received: int = 0
    connections: int = 0
    disconnects: int = 0
    reconnects: int = 0
    connect_failures: int = 0
    snapshots: int = 0
    snapshot_failures: int = 0

    def elapsed_s(self, now_ns: int) -> float:
        if not self.started_ns:
            return 0.0
        return max(0.0, (now_ns - self.started_ns) / NS_PER_S)


def _import_websockets() -> Any:
    """Imports ``websockets`` lazily so offline/replay use needs no dependency."""
    try:
        import websockets
    except ImportError as exc:  # pragma: no cover - dependency guard
        raise FeedUnavailable(
            "the 'websockets' package is required for live ingestion "
            "(pip install 'websockets>=12,<16')"
        ) from exc
    return websockets


class _SocketFrames:
    """Async iterator over one WebSocket connection (explicit lifecycle).

    A class instead of an async generator: the socket must be closed by
    ``aclose()`` no matter how iteration ends, and a generator that is kept alive
    by an exception traceback would still be registered with the event loop when
    it shuts down.
    """

    def __init__(self, client: "L2IngestClient") -> None:
        self._client = client
        self._socket: Any = None
        self._iterator: Any = None
        self._closed = False

    def __aiter__(self) -> "_SocketFrames":
        return self

    async def __anext__(self) -> Frame:
        client = self._client
        if self._closed or client._stop_requested:
            raise StopAsyncIteration
        if self._iterator is None:
            await self._connect()
        try:
            message = await self._iterator.__anext__()
        except StopAsyncIteration:
            self._closed = True
            raise
        # Stamped as soon as the frame is handed to us: everything after this
        # line is our own processing cost.
        return client._clock(), client._decode(message)

    async def _connect(self) -> None:
        websockets = _import_websockets()
        client = self._client
        url = client.stream_url
        self._socket = await websockets.connect(
            url,
            ping_interval=PING_INTERVAL_S,
            ping_timeout=PING_INTERVAL_S,
            # Bounded because shutdown latency is bounded by it: we close the
            # socket to wake the reader, and the venue handshake completes in
            # one round trip (1 s is generous).
            close_timeout=1,
            open_timeout=10,
            max_queue=1024,
            # Binance already frames its own payloads; permessage-deflate would
            # only add CPU and jitter to a latency-sensitive path.
            compression=None,
        )
        self._iterator = self._socket.__aiter__()
        client._socket = self._socket
        client._stats.connections += 1
        logger.info("connected: %s", url)

    async def aclose(self) -> None:
        """Closes the connection; idempotent and never raises."""
        self._closed = True
        socket, self._socket = self._socket, None
        self._iterator = None
        if self._client._socket is socket:
            self._client._socket = None
        if socket is None:
            return
        try:
            await socket.close()
        except Exception:  # noqa: BLE001 - teardown must not raise
            logger.debug("socket close failed", exc_info=True)


class L2IngestClient:
    """Depth-stream capture client: connect, timestamp, archive, repeat.

    The receive path is deliberately synchronous: ``consume`` awaits only the
    socket iterator, and each frame costs one JSON parse plus an append to a
    ``bytearray``. Everything expensive (gzip, disk) happens on the writer task's
    worker thread.
    """

    def __init__(
        self,
        settings: DataIngestionSettings,
        *,
        directory: Optional[Path] = None,
        dry_run: bool = False,
        track_latency: bool = True,
        clock=time.time_ns,
        rng: Optional[random.Random] = None,
        max_file_bytes: Optional[int] = None,
        snapshot_fetcher: Optional[Callable[[str, float], str]] = None,
    ) -> None:
        self._settings = settings
        self._clock = clock
        self._rng = rng
        self._track_latency = track_latency
        self._snapshot_fetcher = snapshot_fetcher or fetch_snapshot_text
        self._snapshot_url: Optional[str] = None
        self._snapshot_url_logged = False
        self._writer = (
            None
            if dry_run
            else RawL2Writer(
                settings,
                directory=directory,
                max_file_bytes=max_file_bytes,
            )
        )
        self._latency = LatencyTracker(int(settings.latency_warn_ms * NS_PER_MS))
        self._stats = IngestStats()
        self._stop: Optional[asyncio.Event] = None
        self._stop_requested = False
        self._socket: Any = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._last_warn_ns = 0
        self._last_summary: Optional[LatencySummary] = None

    # -- introspection ----------------------------------------------------- #
    @property
    def stats(self) -> IngestStats:
        return self._stats

    @property
    def latency(self) -> LatencyTracker:
        return self._latency

    def latency_snapshot(self) -> LatencySummary:
        """Most recent latency report (percentiles are per telemetry interval)."""
        if self._last_summary is not None:
            return self._last_summary
        return self._latency.summary()

    @property
    def stream_url(self) -> str:
        return self._settings.stream_url

    @property
    def writer_stats(self) -> Optional[WriterStats]:
        """Roll-file counters, or ``None`` in dry-run mode."""
        return None if self._writer is None else self._writer.stats()

    def prepare_storage(self) -> None:
        """Opens the first roll file, so a bad archive path fails loudly here.

        No-op in dry-run mode. Raises ``OSError`` if the archive cannot be
        opened; the caller turns that into a refusal to start.
        """
        if self._writer is not None:
            self._writer.prepare()

    # -- shutdown ---------------------------------------------------------- #
    def _stop_event(self) -> asyncio.Event:
        """Lazily creates the stop event (asyncio.Event needs a running loop)."""
        if self._stop is None:
            self._stop = asyncio.Event()
            if self._stop_requested:
                self._stop.set()
        return self._stop

    def request_stop(self) -> None:
        """Requests a graceful stop; safe from a signal handler or another thread."""
        already_requested = self._stop_requested
        self._stop_requested = True
        if not already_requested:
            logger.info("shutdown requested; draining and closing the capture")
        if self._stop is not None:
            self._stop.set()
        socket, loop = self._socket, self._loop
        if socket is None or loop is None or loop.is_closed():
            return

        def _close() -> None:
            # Closing from the loop thread makes the `async for` terminate with
            # ConnectionClosed, which unwinds the client normally.
            asyncio.ensure_future(socket.close())

        try:
            loop.call_soon_threadsafe(_close)
        except RuntimeError:  # loop already shutting down
            pass

    async def _sleep_or_stop(self, delay: float) -> None:
        try:
            await asyncio.wait_for(self._stop_event().wait(), timeout=delay)
        except asyncio.TimeoutError:
            pass

    # -- frame sources ----------------------------------------------------- #
    async def _file_frames(self, path: Path) -> AsyncIterator[Frame]:
        """Replays a local NDJSON archive with synthetic receive timestamps."""
        with path.open("r", encoding="utf-8") as handle:
            for index, line in enumerate(handle):
                if self._stop_requested:
                    return
                text = line.strip()
                if not text:
                    continue
                yield self._clock(), text
                if index % 1000 == 0:
                    await asyncio.sleep(0)  # let the loop run telemetry / writer

    @staticmethod
    def _decode(message: Any) -> str:
        if isinstance(message, (bytes, bytearray, memoryview)):
            return bytes(message).decode("utf-8", "replace")
        return str(message)

    # -- hot path ---------------------------------------------------------- #
    def _handle_frame(self, recv_ns: int, raw: str,
                      measure_latency: bool = True) -> None:
        """Archives + measures one frame. No awaits, no copies beyond JSON."""
        stats = self._stats
        stats.frames += 1
        stats.bytes_received += len(raw)

        # Archive first: the capture stays byte-complete even if a payload is
        # unparseable, which is what makes the roll files usable as ground truth.
        if self._writer is not None:
            self._writer.append(unwrap_frame_text(raw))
            stats.stored += 1

        payload = parse_payload(raw)
        if payload is None:
            stats.malformed += 1
            if stats.malformed <= 5:
                logger.warning("malformed frame #%d: %.160s", stats.malformed, raw)
            return

        kind = event_kind(payload)
        if kind == "depth":
            stats.depth_frames += 1
        elif kind == "trade":
            stats.trade_frames += 1

        event_ns = event_timestamp_ns(payload)
        if event_ns is None:
            stats.no_timestamp += 1
            return
        if not measure_latency or not self._track_latency:
            return

        latency_ns = recv_ns - event_ns
        if self._latency.record(latency_ns):
            now = self._clock()
            if now - self._last_warn_ns >= NS_PER_S:  # at most one WARNING/second
                self._last_warn_ns = now
                logger.warning(
                    "feed latency %.1f ms > %.1f ms threshold",
                    latency_ns / NS_PER_MS, self._latency.warn_ns / NS_PER_MS,
                )

    async def consume(self, frames: AsyncIterator[Frame]) -> int:
        """Drains any frame source through ``_handle_frame``; returns the count.

        Public so tests (and other feeds) can drive the exact production path
        without a socket.
        """
        consumed = 0
        async for recv_ns, raw in frames:
            if self._stop_requested:
                break
            self._handle_frame(recv_ns, raw)
            consumed += 1
        return consumed

    # -- session drivers --------------------------------------------------- #
    async def _seed_snapshot(self) -> None:
        """Archives an absolute depth snapshot so the book has a mid at once.

        A depth subscription is a *diff* stream. A reader that starts empty --
        and every reader starts empty, and every reader is empty again after a
        reconnect until enough deltas arrive -- has no book, and a study that
        marks fills against a missing mid drops them. Fetching one snapshot per
        connection turns a 113 s blind window into a single line in the roll
        file that the analysis can rebuild the book from.

        Failing to seed is a recorded hole, not a capture failure: the venue's
        own stream is still archived byte-for-byte, so nothing here can corrupt
        the tape.
        """
        if not self._settings.snapshot_on_connect:
            return
        if self._snapshot_url is None:
            self._snapshot_url = depth_snapshot_url(
                self._settings.endpoint,
                self._settings.symbol,
                self._settings.snapshot_limit,
            )
            if self._snapshot_url is None and not self._snapshot_url_logged:
                self._snapshot_url_logged = True
                logger.warning(
                    "no depth snapshot endpoint is known for %s; the archive "
                    "will have no seeded book (see --no-snapshot to silence this)",
                    self._settings.endpoint,
                )
        if self._snapshot_url is None:
            return
        try:
            text = await asyncio.to_thread(
                self._snapshot_fetcher, self._snapshot_url,
                self._settings.snapshot_timeout_s,
            )
        except Exception as exc:  # noqa: BLE001 - a seed failure is never fatal
            self._stats.snapshot_failures += 1
            logger.warning("depth snapshot failed (%s: %s); continuing unseeded",
                           type(exc).__name__, exc)
            return
        self._stats.snapshots += 1
        received_ns = self._clock()
        # The venue's own fields win; the capture adds only what it knows and the
        # snapshot lacks -- its receive time and its market.
        seeded = annotate_snapshot(
            text, received_ns // NS_PER_MS, self._settings.symbol)
        logger.info("seeded the book from %s (%d bytes)", self._snapshot_url, len(seeded))
        self._handle_frame(received_ns, seeded, measure_latency=False)

    async def run(self) -> IngestStats:
        """Connects, ingests and reconnects until stopped."""
        if not self._settings.enabled:
            logger.warning("data_ingestion.enabled is false: exiting without capture")
            return self._stats
        self._loop = asyncio.get_running_loop()
        self._stats.started_ns = self._clock()
        if self._writer is not None:
            await self._writer.start()
        telemetry = asyncio.create_task(self._telemetry_loop(), name="l2-telemetry")
        try:
            await self._consume_with_reconnect()
        finally:
            telemetry.cancel()
            try:
                await telemetry
            except asyncio.CancelledError:
                pass
            if self._writer is not None:
                await self._writer.aclose()
            self._log_telemetry(final=True)
        return self._stats

    async def aclose(self) -> None:
        """Flushes and closes the roll file; safe to call more than once."""
        if self._writer is not None:
            await self._writer.aclose()

    async def replay(self, path: Path) -> IngestStats:
        """Feeds a local NDJSON archive through the same pipeline (offline check)."""
        self._loop = asyncio.get_running_loop()
        self._stats.started_ns = self._clock()
        if self._writer is not None:
            await self._writer.start()
        try:
            await self.consume(self._file_frames(Path(path)))
        finally:
            if self._writer is not None:
                await self._writer.aclose()
            self._log_telemetry(final=True)
        return self._stats

    async def _consume_with_reconnect(self) -> None:
        attempt = 0
        while not self._stop_requested:
            started = self._clock()
            frames = 0
            error: Optional[BaseException] = None
            source = _SocketFrames(self)
            try:
                # Seed **before** the socket opens. A snapshot fetched after
                # connecting carries a venue time later than the frames the
                # socket buffered while it was in flight, and the archive would
                # then hold a book that is timestamped after the deltas that
                # follow it -- which the analysis refuses, correctly, as a
                # non-monotonic timeline. What the seed cannot do is hand off
                # contiguously: whatever changed between its ``lastUpdateId`` and
                # the first frame was not on any socket yet, so it is absent from
                # the archive. The book is therefore correct *from the seed
                # forward*, up to one round trip of level changes; the analysis
                # discards any frame whose id the seed already contains rather
                # than applying it twice.
                await self._seed_snapshot()
                frames = await self.consume(source)
            except FeedUnavailable:
                raise
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - every feed error is retryable
                error = exc
                self._stats.connect_failures += 1
                logger.warning("feed error: %s: %s", type(exc).__name__, exc)
            finally:
                # Always unwind the source: it owns the socket, which must not
                # outlive this iteration even when ``consume`` raised.
                await source.aclose()
            if self._stop_requested:
                break
            if error is None:
                self._stats.disconnects += 1
                logger.warning("connection closed after %d frames", frames)
            if (self._clock() - started) / NS_PER_S >= HEALTHY_SESSION_S:
                attempt = 0  # the session was healthy: restart the backoff ladder

            limit = self._settings.max_reconnect_attempts
            if limit and attempt >= limit:
                logger.error("giving up after %d reconnect attempts", limit)
                break
            delay = reconnect_delay(
                attempt,
                base=self._settings.reconnect_base_delay_s,
                cap=self._settings.reconnect_max_delay_s,
                rng=self._rng,
            )
            attempt += 1
            self._stats.reconnects += 1
            logger.info("reconnecting in %.2fs (attempt %d)", delay, attempt)
            await self._sleep_or_stop(delay)

    # -- telemetry --------------------------------------------------------- #
    async def _telemetry_loop(self) -> None:
        while True:
            await asyncio.sleep(self._settings.telemetry_interval_s)
            self._log_telemetry()

    def _log_telemetry(self, *, final: bool = False) -> None:
        """Emits one structured telemetry line (periodic and on shutdown)."""
        stats = self._stats
        latency = self._latency.summary(reset_window=not final)
        if final and not latency.samples and self._last_summary is not None:
            # The last periodic report already cleared the window; reuse its
            # percentiles so the FINAL line still shows the recent latency.
            latency = self._last_summary
        elif not final:
            self._last_summary = latency
        elapsed = stats.elapsed_s(self._clock())
        rate = stats.frames / elapsed if elapsed > 0 else 0.0
        parts = [f"frames={stats.frames}", f"rate={rate:.1f}/s"]
        if stats.depth_frames or stats.trade_frames:
            parts.append(f"depth={stats.depth_frames} trade={stats.trade_frames}")
        if latency.samples:
            parts.append(
                f"lat_ms(p50={latency.p50_ms:.2f} p95={latency.p95_ms:.2f} "
                f"p99={latency.p99_ms:.2f} max={latency.max_ms:.2f})"
            )
        if latency.breaches:
            parts.append(f"lat_breaches={latency.breaches}")
        if latency.negative:
            parts.append(f"clock_skew={latency.negative}")
        if stats.no_timestamp:
            parts.append(f"no_timestamp={stats.no_timestamp}")
        if stats.malformed:
            parts.append(f"malformed={stats.malformed}")
        if stats.reconnects or stats.connect_failures:
            parts.append(f"reconnects={stats.reconnects} fail={stats.connect_failures}")
        if stats.snapshots or stats.snapshot_failures:
            parts.append(f"snapshots={stats.snapshots} snap_fail={stats.snapshot_failures}")
        if self._writer is not None:
            writer = self._writer.stats()
            ratio = (
                writer.compressed_bytes / writer.uncompressed_bytes
                if writer.uncompressed_bytes
                else 0.0
            )
            parts.append(
                f"files={writer.files} rec={writer.records} "
                f"buf={writer.buffered_records} "
                f"out={writer.compressed_bytes / (1024 * 1024):.2f}MiB "
                f"ratio={ratio:.2f}"
            )
            if writer.write_errors:
                parts.append(f"write_errors={writer.write_errors}")
        logger.info("%s %s", "FINAL" if final else "telemetry", " ".join(parts))


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #
def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--config", type=Path, default=None,
                        help="YAML settings file (default: config/settings.yaml)")
    parser.add_argument("--symbol", default=None, help="stream symbol, e.g. btcusdt")
    parser.add_argument("--stream", dest="stream_type", default=None,
                        help="stream type, e.g. depth@100ms")
    parser.add_argument("--aux-stream", dest="aux_streams", action="append",
                        default=None, metavar="TYPE",
                        help="extra stream on the same socket, e.g. trade "
                             "(repeatable; replaces data_ingestion.aux_streams)")
    parser.add_argument("--depth-only", action="store_true",
                        help="capture the depth stream only (drop the tape)")
    parser.add_argument("--url", dest="ws_base_url", default=None,
                        help="WebSocket base URL (overrides the exchange endpoint)")
    parser.add_argument("--output-dir", type=Path, default=None,
                        help="roll-file directory (overrides data_ingestion.storage_path)")
    parser.add_argument("--compression", choices=COMPRESSION_CHOICES, default=None,
                        help="gzip (default) or none")
    parser.add_argument("--duration", type=float, default=0.0,
                        help="stop after N seconds (0 = run until SIGINT/SIGTERM)")
    parser.add_argument("--max-file-mb", type=int, default=None,
                        help="rotate the roll file at this size (0 = never)")
    parser.add_argument("--rotate-seconds", type=int, default=None,
                        help="rotate the roll file after N seconds (0 = never)")
    parser.add_argument("--flush-interval-ms", type=int, default=None,
                        help="write-buffer flush cadence")
    parser.add_argument("--telemetry-interval", type=float, default=None,
                        help="latency/throughput summary cadence in seconds")
    parser.add_argument("--latency-warn-ms", type=float, default=None,
                        help="WARNING threshold for feed latency")
    parser.add_argument("--no-snapshot", dest="snapshot_on_connect",
                        action="store_false", default=None,
                        help="do not seed the book from a REST depth snapshot on "
                             "connect/reconnect (the archive then has no mid until "
                             "the venue's deltas rebuild one)")
    parser.add_argument("--snapshot-limit", type=int, default=None,
                        help="levels requested from the snapshot endpoint; the "
                             "venue's own set applies (USD-M: 5, 10, 20, 50, 100, "
                             "500, 1000; spot: any depth to 5000). Default 1000.")
    parser.add_argument("--dry-run", action="store_true",
                        help="connect and measure only; write no files")
    parser.add_argument("--replay", type=Path, default=None,
                        help="ingest a local NDJSON archive instead of the socket")
    parser.add_argument("--log-level", default="INFO")
    return parser.parse_args(argv)


def build_settings(args: argparse.Namespace) -> DataIngestionSettings:
    """Loads ``data_ingestion`` and applies the CLI overrides."""
    settings = load_settings(args.config).data_ingestion
    overrides = {
        "symbol": args.symbol,
        "stream_type": args.stream_type,
        "ws_base_url": args.ws_base_url,
        "compression": args.compression,
        "max_file_mb": args.max_file_mb,
        "rotate_seconds": args.rotate_seconds,
        "flush_interval_ms": args.flush_interval_ms,
        "telemetry_interval_s": args.telemetry_interval,
        "latency_warn_ms": args.latency_warn_ms,
        "snapshot_on_connect": args.snapshot_on_connect,
        "snapshot_limit": args.snapshot_limit,
    }
    # ``--depth-only`` wins over ``--aux-stream``: an explicit request to drop
    # the tape must not be undone by a flag earlier on the command line.
    if args.depth_only:
        overrides["aux_streams"] = ()
    elif args.aux_streams:
        overrides["aux_streams"] = parse_stream_names(args.aux_streams)
    overrides = {name: value for name, value in overrides.items() if value is not None}
    settings = replace(settings, **overrides) if overrides else settings
    # ``dataclasses.replace`` does not re-run the settings validation, so a CLI
    # value could otherwise reach the venue with a depth it answers with HTTP
    # 400 -- an unseeded capture that still exits clean. Checked against the
    # endpoint the merged settings will actually dial.
    check_snapshot_limit(
        settings.endpoint, settings.snapshot_limit, "--snapshot-limit")
    return settings


def install_signal_handlers(client: L2IngestClient) -> None:
    """Routes SIGINT/SIGTERM to ``client.request_stop`` for a clean shutdown."""
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, client.request_stop)
        except (NotImplementedError, RuntimeError):
            # Windows / non-main-thread fallback: the handler only flips flags,
            # and the client closes its own socket from the loop thread.
            try:
                signal.signal(sig, lambda *_: client.request_stop())
            except (ValueError, OSError):  # pragma: no cover - exotic platforms
                logger.debug("cannot install a handler for signal %s", sig)


async def _stop_after(client: L2IngestClient, seconds: float) -> None:
    await asyncio.sleep(seconds)
    logger.info("duration reached; shutting down")
    client.request_stop()


def _print_summary(
    client: L2IngestClient,
    settings: DataIngestionSettings,
    storage_dir: Path,
) -> None:
    stats = client.stats
    latency = client.latency_snapshot()
    writer = client.writer_stats
    elapsed = stats.elapsed_s(time.time_ns())
    rate = stats.frames / elapsed if elapsed > 0 else 0.0
    print("=" * 74)
    print(f"  Stream        : {settings.stream_url}")
    print(f"  Frames        : {stats.frames:,}  ({rate:.1f}/s over {elapsed:.1f}s)")
    if stats.depth_frames or stats.trade_frames:
        print(f"  Composition   : depth={stats.depth_frames:,}"
              f"  trade={stats.trade_frames:,}")
    if writer is None:
        print("  Archived      : dry-run (no storage)")
    else:
        print(f"  Archived      : {stats.stored:,} frames"
              f"  ({stats.bytes_received / (1024 * 1024):.2f} MiB received)")
    if writer is not None:
        print(f"  Roll files    : {writer.files}"
              f"  ({writer.records:,} records, "
              f"{writer.compressed_bytes / (1024 * 1024):.2f} MiB on disk)")
        print(f"  Storage       : {storage_dir}")
    if latency.samples:
        print(f"  Feed latency  : p50={latency.p50_ms:.2f} ms"
              f"  p95={latency.p95_ms:.2f} ms"
              f"  p99={latency.p99_ms:.2f} ms"
              f"  max={latency.max_ms:.2f} ms")
    if stats.reconnects:
        print(f"  Reconnects    : {stats.reconnects}"
              f"  (failures: {stats.connect_failures})")
    if stats.snapshots or stats.snapshot_failures:
        print(f"  Book seeds    : {stats.snapshots}"
              f"  (failed: {stats.snapshot_failures})")
    if stats.malformed or stats.no_timestamp:
        print(f"  Diagnostics   : malformed={stats.malformed}"
              f"  no_timestamp={stats.no_timestamp}")
    print("=" * 74)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    configure_logging(args.log_level)
    try:
        settings = build_settings(args)
    except ConfigError as exc:
        print(f"configuration error: {exc}", file=sys.stderr)
        return 2

    client = L2IngestClient(
        settings,
        directory=args.output_dir,
        dry_run=args.dry_run,
        # A replayed archive carries its original venue timestamps, so the
        # derived latency would be meaningless and is not tracked.
        track_latency=args.replay is None,
    )
    storage_dir = (
        Path(args.output_dir) if args.output_dir is not None else settings.storage_dir
    )
    # Fail before the socket is opened: an unwritable archive would otherwise be
    # discovered by the first flush, whose error is logged and the chunk
    # discarded while the run keeps counting archived frames.
    try:
        client.prepare_storage()
    except OSError as exc:
        print(f"cannot open the capture archive in {storage_dir}: {exc}", file=sys.stderr)
        return 1
    logger.info(
        "ingest %s -> %s (%s)",
        settings.stream_url,
        "dry-run (no files)" if args.dry_run else storage_dir,
        "no storage" if args.dry_run else settings.compression,
    )
    if not settings.aux_streams:
        logger.warning(
            "depth-only capture: no trade prints are archived, so "
            "'--fill-model trade' can never fill from this archive"
        )
    else:
        silent = silent_aux_streams(settings.aux_streams, settings.endpoint)
        if silent:
            logger.warning(
                "aux stream(s) %s were accepted and stayed silent on this "
                "endpoint; the tape needs 'trade' (docs/phase5 section 11.6.1)",
                ", ".join(f"{settings.symbol}@{tag}" for tag in silent),
            )

    async def _run() -> None:
        install_signal_handlers(client)
        timer: Optional[asyncio.Task] = None
        if args.duration > 0:
            timer = asyncio.create_task(_stop_after(client, args.duration))
        try:
            if args.replay is not None:
                await client.replay(args.replay)
            else:
                await client.run()
        finally:
            if timer is not None:
                timer.cancel()
                try:
                    await timer
                except asyncio.CancelledError:
                    pass

    try:
        asyncio.run(_run())
    except KeyboardInterrupt:  # raised before our handler took over
        logger.warning("interrupted; the roll file was flushed")
        return 130
    except FeedUnavailable as exc:
        logger.error("%s", exc)
        return 1
    _print_summary(client, settings, storage_dir)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
