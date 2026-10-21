"""Tests for the Phase 1 real-time L2 ingestion client (``scripts/ingest_l2.py``).

The capture path must archive every frame byte-for-byte, keep the receive loop
free of disk I/O, measure feed latency from the venue timestamp, and always
leave a readable gzip archive behind — including after an abrupt shutdown.
"""
from __future__ import annotations

import asyncio
import gzip
import json
import random
from dataclasses import replace
from pathlib import Path
from typing import Any, Dict, List, Sequence

import pytest
import yaml

from python_analytics.utils.config import (
    EXCHANGE_ENDPOINTS,
    ConfigError,
    DataIngestionSettings,
    as_dict,
    load_settings,
)
from scripts import ingest_l2 as ingest_module
from scripts.ingest_l2 import (  # noqa: E402 - sys.path bootstrap lives in the script
    Frame,
    LatencySummary,
    LatencyTracker,
    L2IngestClient,
    RawL2Writer,
    build_settings,
    event_timestamp_ns,
    parse_args,
    parse_payload,
    percentile,
    reconnect_delay,
)

NS_PER_MS = 1_000_000
REPO_ROOT = Path(__file__).resolve().parents[2]


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def _settings(**overrides: Any) -> DataIngestionSettings:
    return replace(DataIngestionSettings(), **overrides)


def _depth(event_ms: int = 1_700_000_000_000, **extra: Any) -> str:
    payload: Dict[str, Any] = {
        "e": "depthUpdate",
        "E": event_ms,
        "T": event_ms - 1,
        "s": "BTCUSDT",
        "U": 1,
        "u": 2,
        "b": [["30000.00", "1.5"]],
        "a": [["30001.00", "2.0"]],
    }
    payload.update(extra)
    return json.dumps(payload)


class _FrameSource:
    """Minimal async iterator standing in for a socket (no finalisation task)."""

    def __init__(self, items: Sequence[Frame]) -> None:
        self._items = list(items)
        self._index = 0

    def __aiter__(self) -> "_FrameSource":
        return self

    async def __anext__(self) -> Frame:
        if self._index >= len(self._items):
            raise StopAsyncIteration
        item = self._items[self._index]
        self._index += 1
        return item


def _read_roll(path: Path) -> List[str]:
    opener = gzip.open if path.suffix == ".gz" else open
    with opener(path, "rt", encoding="utf-8") as handle:  # type: ignore[operator]
        return [line.rstrip("\n") for line in handle if line.strip()]


def _roll_files(directory: Path) -> List[Path]:
    return sorted(p for p in directory.iterdir() if p.is_file())


class _Clock:
    """Deterministic ``time.time()`` stand-in for rotation tests."""

    def __init__(self, start: float = 1_700_000_000.0) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
def test_settings_load_from_repository_yaml() -> None:
    ingestion = load_settings().data_ingestion
    assert ingestion.enabled is True
    assert ingestion.exchange == "binance_futures"
    assert ingestion.symbol == "btcusdt"
    assert ingestion.stream_type == "depth@100ms"
    assert ingestion.storage_path == "data/raw_l2"
    assert ingestion.compression == "gzip"
    assert ingestion.compression_level == 1
    assert ingestion.flush_interval_ms == 250
    assert ingestion.max_file_mb == 256
    assert ingestion.rotate_seconds == 900
    assert ingestion.reconnect_base_delay_s == pytest.approx(0.5)
    assert ingestion.max_reconnect_attempts == 0


def test_stream_url_targets_the_futures_endpoint() -> None:
    settings = _settings()
    assert settings.endpoint == EXCHANGE_ENDPOINTS["binance_futures"]
    assert settings.stream_url == "wss://fstream.binance.com/ws/btcusdt@depth@100ms"


def test_storage_dir_is_absolute_and_repo_relative() -> None:
    assert _settings().storage_dir == REPO_ROOT / "data" / "raw_l2"
    assert _settings(storage_path="/tmp/l2").storage_dir == Path("/tmp/l2")


def test_file_suffix_tracks_compression() -> None:
    assert _settings().file_suffix == ".ndjson.gz"
    assert _settings(compression="none").file_suffix == ".ndjson"


def _write_settings(tmp_path: Path, block: Dict[str, Any]) -> Path:
    path = tmp_path / "settings.yaml"
    path.write_text(yaml.safe_dump({"data_ingestion": block}), encoding="utf-8")
    return path


def test_yaml_overrides_are_applied(tmp_path) -> None:
    path = _write_settings(tmp_path, {
        "exchange": "binance_spot",
        "symbol": "ethusdt",
        "stream_type": "depth20@100ms",
        "storage_path": "/var/tmp/l2",
        "compression": "none",
        "compression_level": 6,
        "flush_interval_ms": 50,
        "max_file_mb": 64,
        "rotate_seconds": 0,
        "telemetry_interval_s": 2.5,
        "latency_warn_ms": 100,
        "reconnect_base_delay_s": 0.25,
        "reconnect_max_delay_s": 5,
        "max_reconnect_attempts": 3,
        "ws_base_url": "wss://example.test/ws/",
    })
    settings = load_settings(path).data_ingestion
    assert settings.exchange == "binance_spot"
    assert settings.symbol == "ethusdt"
    assert settings.stream_type == "depth20@100ms"
    assert settings.storage_dir == Path("/var/tmp/l2")
    assert settings.compression == "none"
    assert settings.compression_level == 6
    assert settings.flush_interval_ms == 50
    assert settings.max_file_mb == 64
    assert settings.rotate_seconds == 0
    assert settings.telemetry_interval_s == pytest.approx(2.5)
    assert settings.latency_warn_ms == pytest.approx(100.0)
    assert settings.reconnect_base_delay_s == pytest.approx(0.25)
    assert settings.reconnect_max_delay_s == pytest.approx(5.0)
    assert settings.max_reconnect_attempts == 3
    # The override wins over the exchange default and loses its trailing slash.
    assert settings.stream_url == "wss://example.test/ws/ethusdt@depth20@100ms"


def test_missing_block_falls_back_to_defaults(tmp_path) -> None:
    path = tmp_path / "settings.yaml"
    path.write_text("engine:\n  symbol: BTCUSDT\n", encoding="utf-8")
    assert load_settings(path).data_ingestion == DataIngestionSettings()


def test_symbol_and_stream_are_normalised_to_lowercase(tmp_path) -> None:
    settings = load_settings(
        _write_settings(tmp_path, {"symbol": "ETHUSDT", "stream_type": "DEPTH@100MS"})
    ).data_ingestion
    assert settings.symbol == "ethusdt"
    assert settings.stream_url.endswith("/ethusdt@depth@100ms")


def test_spot_exchange_uses_the_spot_endpoint(tmp_path) -> None:
    settings = load_settings(_write_settings(tmp_path, {"exchange": "binance_spot"}))
    assert settings.data_ingestion.stream_url == (
        "wss://stream.binance.com:9443/ws/btcusdt@depth@100ms"
    )


@pytest.mark.parametrize(
    "block, section",
    [
        ({"compression": "lz4"}, "data_ingestion.compression"),
        ({"symbol": "b"}, "data_ingestion.symbol"),
        ({"symbol": "btc/usdt"}, "data_ingestion.symbol"),
        ({"stream_type": "depth @100ms"}, "data_ingestion.stream_type"),
        ({"exchange": "kraken"}, "data_ingestion.exchange"),
        ({"compression_level": 0}, "data_ingestion.compression_level"),
        ({"compression_level": 10}, "data_ingestion.compression_level"),
        ({"flush_interval_ms": 0}, "data_ingestion.flush_interval_ms"),
        ({"telemetry_interval_s": 0}, "data_ingestion.telemetry_interval_s"),
        ({"latency_warn_ms": 0}, "data_ingestion.latency_warn_ms"),
        ({"reconnect_base_delay_s": 0}, "data_ingestion.reconnect_base_delay_s"),
        ({"reconnect_max_delay_s": 0.1}, "data_ingestion.reconnect_max_delay_s"),
        ({"max_reconnect_attempts": -1}, "data_ingestion.max_reconnect_attempts"),
        ({"ws_base_url": "https://example.test"}, "data_ingestion.ws_base_url"),
        ({"storage_path": "  "}, "data_ingestion.storage_path"),
    ],
)
def test_invalid_values_are_rejected(tmp_path, block, section) -> None:
    with pytest.raises(ConfigError, match=section.replace(".", r"\.")):
        load_settings(_write_settings(tmp_path, block))


def test_non_mapping_block_is_rejected(tmp_path) -> None:
    path = tmp_path / "settings.yaml"
    path.write_text("data_ingestion:\n  - btcusdt\n", encoding="utf-8")
    with pytest.raises(ConfigError, match="data_ingestion"):
        load_settings(path)


def test_as_dict_exposes_the_resolved_stream_and_directory() -> None:
    entry = as_dict(load_settings())["data_ingestion"]
    assert entry["stream_url"] == "wss://fstream.binance.com/ws/btcusdt@depth@100ms"
    assert entry["storage_dir"] == str(REPO_ROOT / "data" / "raw_l2")
    assert entry["symbol"] == "btcusdt"


# --------------------------------------------------------------------------- #
# Latency extraction
# --------------------------------------------------------------------------- #
def test_event_timestamp_prefers_event_time() -> None:
    assert event_timestamp_ns(json.loads(_depth(1_700_000_000_123))) == (
        1_700_000_000_123 * NS_PER_MS
    )


def test_event_timestamp_falls_back_to_transaction_time() -> None:
    payload = {"e": "depthUpdate", "T": 1_700_000_000_000}
    assert event_timestamp_ns(payload) == 1_700_000_000_000 * NS_PER_MS


def test_event_timestamp_unwraps_combined_streams() -> None:
    payload = {"stream": "btcusdt@depth@100ms", "data": {"E": 1_700_000_000_000}}
    assert event_timestamp_ns(payload) == 1_700_000_000_000 * NS_PER_MS


@pytest.mark.parametrize(
    "payload",
    [
        {},
        {"e": "depthUpdate"},
        {"E": "not-a-number"},
        {"E": None},
        {"E": True},
        {"E": -5},
        {"E": 0},
        [["1", "2"]],
        "raw text",
        None,
    ],
)
def test_event_timestamp_rejects_unusable_payloads(payload: Any) -> None:
    assert event_timestamp_ns(payload) is None


def test_parse_payload_rejects_invalid_json() -> None:
    assert parse_payload("{not json") is None
    assert parse_payload(_depth())["e"] == "depthUpdate"


# --------------------------------------------------------------------------- #
# Latency statistics
# --------------------------------------------------------------------------- #
def test_latency_tracker_percentiles_breaches_and_extremes() -> None:
    tracker = LatencyTracker(warn_ns=90 * NS_PER_MS)
    breaches = 0
    for index in range(1, 101):  # 1 ms .. 100 ms
        breaches += tracker.record(index * NS_PER_MS)
    summary = tracker.summary()

    assert summary.samples == 100
    assert summary.p50_ms == pytest.approx(50.0)
    assert summary.p95_ms == pytest.approx(95.0)
    assert summary.p99_ms == pytest.approx(99.0)
    assert summary.max_ms == pytest.approx(100.0)
    assert summary.min_ms == pytest.approx(1.0)
    assert summary.mean_ms == pytest.approx(50.5)
    assert summary.last_ms == pytest.approx(100.0)
    assert summary.breaches == breaches == 10


def test_latency_tracker_counts_clock_skew_separately() -> None:
    tracker = LatencyTracker(warn_ns=5 * NS_PER_MS)
    assert tracker.record(-2 * NS_PER_MS) is False
    assert tracker.record(1 * NS_PER_MS) is False
    summary = tracker.summary()
    assert summary.negative == 1
    assert summary.breaches == 0
    assert summary.min_ms == pytest.approx(-2.0)


def test_latency_tracker_window_bounds_percentiles() -> None:
    tracker = LatencyTracker(warn_ns=1_000 * NS_PER_MS, window=10)
    for index in range(100):
        tracker.record((100 - index) * NS_PER_MS)  # descending: 100 ms .. 1 ms
    summary = tracker.summary()
    assert summary.samples == 10
    assert summary.max_ms == pytest.approx(100.0)  # cumulative
    assert summary.p99_ms == pytest.approx(10.0)  # rolling window only


def test_latency_summary_reset_keeps_cumulative_counters() -> None:
    tracker = LatencyTracker(warn_ns=100 * NS_PER_MS)
    tracker.record(150 * NS_PER_MS)
    first = tracker.summary(reset_window=True)
    assert first.samples == 1
    second = tracker.summary()
    assert second.samples == 0
    assert second.breaches == first.breaches == 1
    assert second.max_ms == pytest.approx(150.0)


@pytest.mark.parametrize(
    "fraction, expected",
    [(0.0, 1.0), (0.5, 2.0), (1.0, 3.0)],
)
def test_percentile_nearest_rank(fraction: float, expected: float) -> None:
    samples = [1 * NS_PER_MS, 2 * NS_PER_MS, 3 * NS_PER_MS]
    assert percentile(samples, fraction) / NS_PER_MS == pytest.approx(expected)


def test_percentile_of_empty_sequence_is_zero() -> None:
    assert percentile([], 0.99) == 0.0


# --------------------------------------------------------------------------- #
# Reconnect backoff
# --------------------------------------------------------------------------- #
def test_backoff_doubles_then_caps() -> None:
    delays = [
        reconnect_delay(attempt, base=0.5, cap=8.0, jitter=0.0)
        for attempt in range(8)
    ]
    assert delays == [0.5, 1.0, 2.0, 4.0, 8.0, 8.0, 8.0, 8.0]


def test_backoff_is_zero_for_the_first_attempt_when_base_is_tiny() -> None:
    assert reconnect_delay(0, base=0.0, cap=5.0, jitter=0.0) == 0.0


def test_backoff_jitter_stays_within_the_cap_and_around_the_ladder() -> None:
    rng = random.Random(7)
    for attempt in range(6):
        delay = reconnect_delay(attempt, base=1.0, cap=30.0, jitter=0.25, rng=rng)
        nominal = min(30.0, 1.0 * (2 ** attempt))
        assert nominal * 0.75 <= delay <= nominal * 1.25
        assert delay <= 30.0


def test_backoff_is_deterministic_with_a_seeded_generator() -> None:
    first = reconnect_delay(3, base=1.0, cap=60.0, rng=random.Random(42))
    second = reconnect_delay(3, base=1.0, cap=60.0, rng=random.Random(42))
    assert first == second


# --------------------------------------------------------------------------- #
# Compressed roll-file writer
# --------------------------------------------------------------------------- #
def test_writer_creates_the_directory_and_roundtrips_gzip(tmp_path) -> None:
    target = tmp_path / "raw_l2" / "nested"
    assert not target.exists()
    writer = RawL2Writer(_settings(), directory=target)
    lines = [_depth(event_ms=1_700_000_000_000 + index) for index in range(5)]
    for line in lines:
        writer.append(line)

    assert writer.stats().buffered_records == 5
    writer.flush()
    asyncio.run(writer.aclose())  # writes the gzip trailer

    files = _roll_files(target)
    assert len(files) == 1
    assert files[0].name.endswith(".ndjson.gz")
    assert _read_roll(files[0]) == lines


def test_writer_aclose_flushes_pending_records(tmp_path) -> None:
    writer = RawL2Writer(_settings(), directory=tmp_path)
    for index in range(3):
        writer.append(_depth(event_ms=1_700_000_000_000 + index))
    asyncio.run(writer.aclose())

    files = _roll_files(tmp_path)
    assert len(files) == 1
    assert len(_read_roll(files[0])) == 3
    stats = writer.stats()
    assert stats.records == 3
    assert stats.buffered_records == 0
    assert stats.buffered_bytes == 0
    assert stats.compressed_bytes > 0
    assert stats.current_path is None  # closed


def test_writer_rotates_by_size(tmp_path) -> None:
    writer = RawL2Writer(_settings(), directory=tmp_path, max_file_bytes=600)
    written = 0
    for _ in range(6):
        for index in range(4):
            writer.append(_depth(event_ms=1_700_000_000_000 + written + index))
            written += 1
        writer.flush()
    asyncio.run(writer.aclose())

    files = _roll_files(tmp_path)
    assert len(files) >= 3
    records = [record for path in files for record in _read_roll(path)]
    assert len(records) == written
    assert writer.stats().files == len(files)


def test_writer_rotates_by_time(tmp_path) -> None:
    clock = _Clock()
    writer = RawL2Writer(
        _settings(), directory=tmp_path, rotate_seconds=10, clock=clock
    )
    writer.append(_depth())
    writer.flush()
    clock.advance(5)
    writer.append(_depth())
    writer.flush()
    assert len(_roll_files(tmp_path)) == 1

    clock.advance(11)  # total 16 s > rotate_seconds
    writer.append(_depth())
    writer.flush()
    files = _roll_files(tmp_path)
    assert len(files) == 2
    asyncio.run(writer.aclose())
    assert [len(_read_roll(path)) for path in _roll_files(tmp_path)] == [2, 1]


def test_writer_can_store_plain_ndjson(tmp_path) -> None:
    writer = RawL2Writer(_settings(compression="none"), directory=tmp_path)
    writer.append(_depth())
    asyncio.run(writer.aclose())

    files = _roll_files(tmp_path)
    assert len(files) == 1
    assert files[0].suffix == ".ndjson"
    assert files[0].read_text(encoding="utf-8").strip().startswith('{"e": "depthUpdate"')
    assert len(_read_roll(files[0])) == 1


def test_writer_rejects_appends_after_close(tmp_path) -> None:
    writer = RawL2Writer(_settings(), directory=tmp_path)
    asyncio.run(writer.aclose())
    with pytest.raises(RuntimeError, match="closed"):
        writer.append(_depth())


def test_writer_fsyncs_and_leaves_a_valid_gzip_trailer(tmp_path) -> None:
    writer = RawL2Writer(_settings(), directory=tmp_path)
    payload = _depth()
    writer.append(payload)
    asyncio.run(writer.aclose())

    files = _roll_files(tmp_path)
    with gzip.open(files[0], "rb") as handle:
        assert handle.read() == (payload + "\n").encode("utf-8")


def test_writer_sequence_never_reuses_a_file_name(tmp_path) -> None:
    clock = _Clock()
    settings = _settings()
    first = RawL2Writer(settings, directory=tmp_path, max_file_bytes=1, clock=clock)
    first.append(_depth())
    first.flush()
    first.append(_depth())
    first.flush()
    asyncio.run(first.aclose())

    second = RawL2Writer(settings, directory=tmp_path, max_file_bytes=1, clock=clock)
    second.append(_depth())
    asyncio.run(second.aclose())

    names = [path.name for path in _roll_files(tmp_path)]
    assert len(names) == len(set(names)) == 3


# --------------------------------------------------------------------------- #
# Ingest client
# --------------------------------------------------------------------------- #
def _client(tmp_path: Path, **kwargs: Any) -> L2IngestClient:
    settings = kwargs.pop("settings", _settings())
    return L2IngestClient(settings, directory=tmp_path, **kwargs)


def _consume(client: L2IngestClient, items: Sequence[Frame]) -> int:
    """Drains ``items`` and closes the archive the way ``run()`` does."""

    async def _main() -> int:
        try:
            return await client.consume(_FrameSource(items))
        finally:
            await client.aclose()

    return asyncio.run(_main())


def test_client_consumes_frames_measures_latency_and_archives(tmp_path) -> None:
    client = _client(tmp_path)
    event_ms = 1_700_000_000_000
    items: List[Frame] = [
        (event_ms * NS_PER_MS + 12 * NS_PER_MS, _depth(event_ms)),
        (event_ms * NS_PER_MS + 140 * NS_PER_MS, _depth(event_ms + 100)),
    ]
    consumed = _consume(client, items)

    assert consumed == 2
    stats = client.stats
    assert stats.frames == stats.stored == 2
    assert stats.malformed == 0
    assert stats.no_timestamp == 0
    assert stats.bytes_received == sum(len(raw) for _, raw in items)

    latency = client.latency.summary()
    assert latency.samples == 2
    assert latency.p50_ms == pytest.approx(12.0)
    assert latency.max_ms == pytest.approx(40.0)

    records = [record for path in _roll_files(tmp_path) for record in _read_roll(path)]
    assert records == [raw for _, raw in items]


def test_client_counts_malformed_and_untimestamped_frames(tmp_path) -> None:
    client = _client(tmp_path)
    items: List[Frame] = [
        (1, "{not json"),
        (2, json.dumps({"result": None, "id": 1})),  # subscription ack
        (3, _depth()),
    ]
    _consume(client, items)

    assert client.stats.frames == 3
    assert client.stats.malformed == 1
    assert client.stats.no_timestamp == 1
    # Every frame is archived, including the unparseable one: the roll file is
    # the ground truth for the capture window.
    records = [record for path in _roll_files(tmp_path) for record in _read_roll(path)]
    assert records == [raw for _, raw in items]


def test_client_dry_run_writes_nothing(tmp_path) -> None:
    client = _client(tmp_path, dry_run=True)
    items = [(10 * NS_PER_MS, _depth(1_700_000_000_000)), (2 * NS_PER_MS, "junk")]
    _consume(client, items)

    assert client.writer_stats is None
    assert client.stats.frames == 2
    assert client.stats.stored == 0
    assert _roll_files(tmp_path) == []


def test_client_threshold_breach_is_counted(tmp_path) -> None:
    client = _client(tmp_path, settings=_settings(latency_warn_ms=5.0))
    event_ms = 1_700_000_000_000
    frame = (event_ms * NS_PER_MS + 50 * NS_PER_MS, _depth(event_ms))
    _consume(client, [frame])
    assert client.latency.breaches == 1


def test_client_stop_request_ends_consumption(tmp_path) -> None:
    client = _client(tmp_path)
    client.request_stop()
    consumed = _consume(client, [(1, _depth())])
    assert consumed == 0
    assert client.stats.frames == 0


def test_client_replay_archives_a_local_archive(tmp_path) -> None:
    source = tmp_path / "sample.ndjson"
    lines = [_depth(event_ms=1_700_000_000_000 + index) for index in range(4)]
    source.write_text("\n".join(lines) + "\n", encoding="utf-8")

    destination = tmp_path / "roll"
    client = _client(destination)
    stats = asyncio.run(client.replay(source))

    assert stats.frames == 4
    records = [record for path in _roll_files(destination) for record in _read_roll(path)]
    assert records == lines
    assert client.writer_stats is not None
    assert client.writer_stats.records == 4


def test_client_accepts_the_bundled_sample_archive(tmp_path) -> None:
    sample = REPO_ROOT / "data" / "sample_events.ndjson"
    client = _client(tmp_path)
    stats = asyncio.run(client.replay(sample))

    assert stats.frames > 0
    assert stats.stored == stats.frames
    assert stats.malformed == 0
    assert client.writer_stats is not None
    assert client.writer_stats.records == stats.frames


def test_client_handles_combined_stream_frames(tmp_path) -> None:
    client = _client(tmp_path)
    envelope = json.dumps({
        "stream": "btcusdt@depth@100ms",
        "data": json.loads(_depth(1_700_000_000_000)),
    })
    _consume(client, [(1_700_000_000_050 * NS_PER_MS, envelope)])
    assert client.stats.no_timestamp == 0
    assert client.latency.summary().last_ms == pytest.approx(50.0)


def test_client_decodes_binary_frames() -> None:
    assert L2IngestClient._decode(b'{"E": 1}') == '{"E": 1}'
    assert L2IngestClient._decode(memoryview(b"abc")) == "abc"
    assert L2IngestClient._decode("text") == "text"


def test_latency_snapshot_reports_the_tracked_window(tmp_path) -> None:
    client = _client(tmp_path)
    event_ms = 1_700_000_000_000
    _consume(client, [(event_ms * NS_PER_MS + 7 * NS_PER_MS, _depth(event_ms))])

    snapshot = client.latency_snapshot()
    assert isinstance(snapshot, LatencySummary)
    assert snapshot.samples == 1
    assert snapshot.p50_ms == pytest.approx(7.0)


def test_client_uses_the_configured_stream_url(tmp_path) -> None:
    settings = _settings(symbol="ethusdt", stream_type="depth@500ms")
    client = _client(tmp_path, settings=settings)
    assert client.stream_url == "wss://fstream.binance.com/ws/ethusdt@depth@500ms"


def test_disabled_ingestion_does_not_connect(tmp_path) -> None:
    client = _client(tmp_path, settings=_settings(enabled=False))
    stats = asyncio.run(client.run())
    assert stats.frames == 0
    assert stats.connections == 0


def test_client_reconnects_with_backoff_and_gives_up(tmp_path, monkeypatch) -> None:
    """Every disconnect must back off, count and (optionally) give up."""

    class _FailingSource:
        def __init__(self, client: L2IngestClient) -> None:
            self._client = client

        def __aiter__(self) -> "_FailingSource":
            return self

        async def __anext__(self) -> Frame:
            raise ConnectionError("socket closed")

        async def aclose(self) -> None:
            return None

    monkeypatch.setattr(ingest_module, "_SocketFrames", _FailingSource)
    settings = _settings(
        max_reconnect_attempts=2,
        reconnect_base_delay_s=0.001,
        reconnect_max_delay_s=0.002,
        telemetry_interval_s=0.5,
    )
    client = _client(tmp_path, settings=settings)
    stats = asyncio.run(client.run())

    assert stats.connections == 0
    assert stats.connect_failures == 3  # initial attempt + two retries
    assert stats.reconnects == 2
    assert client.writer_stats is not None
    assert client.writer_stats.files == 0


def test_missing_websockets_aborts_instead_of_reconnecting(
    tmp_path, monkeypatch
) -> None:
    def _unavailable() -> Any:
        raise ingest_module.FeedUnavailable("websockets is required")

    monkeypatch.setattr(ingest_module, "_import_websockets", _unavailable)
    client = _client(tmp_path)
    with pytest.raises(ingest_module.FeedUnavailable, match="websockets"):
        asyncio.run(client.run())
    assert client.stats.reconnects == 0


# --------------------------------------------------------------------------- #
# CLI plumbing
# --------------------------------------------------------------------------- #
def test_cli_overrides_are_applied_to_the_settings() -> None:
    args = parse_args([
        "--symbol", "ethusdt",
        "--stream", "depth@500ms",
        "--compression", "none",
        "--max-file-mb", "8",
        "--flush-interval-ms", "20",
        "--duration", "1.5",
    ])
    settings = build_settings(args)
    assert settings.symbol == "ethusdt"
    assert settings.stream_url == "wss://fstream.binance.com/ws/ethusdt@depth@500ms"
    assert settings.compression == "none"
    assert settings.max_file_mb == 8
    assert settings.flush_interval_ms == 20
    assert args.duration == pytest.approx(1.5)


def test_cli_defaults_keep_the_yaml_settings() -> None:
    args = parse_args(["--dry-run"])
    assert build_settings(args) == load_settings().data_ingestion
    assert args.dry_run is True


def test_cli_rejects_unknown_compression() -> None:
    with pytest.raises(SystemExit):
        parse_args(["--compression", "lz4"])
