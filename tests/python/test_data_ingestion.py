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
from typing import Any, Dict, List, Optional, Sequence

import pytest
import yaml

from python_analytics.native import hft_native
from python_analytics.utils.config import (
    EXCHANGE_ENDPOINTS,
    ConfigError,
    DataIngestionSettings,
    as_dict,
    check_snapshot_limit,
    depth_snapshot_url,
    load_settings,
    silent_aux_streams,
)
from scripts import ingest_l2 as ingest_module
from scripts.ingest_l2 import (  # noqa: E402 - sys.path bootstrap lives in the script
    Frame,
    LatencySummary,
    LatencyTracker,
    L2IngestClient,
    RawL2Writer,
    annotate_snapshot,
    build_settings,
    event_kind,
    event_timestamp_ns,
    parse_args,
    parse_payload,
    percentile,
    reconnect_delay,
    unwrap_frame_text,
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


def _agg_trade(event_ms: int = 1_700_000_000_000, **extra: Any) -> str:
    """One Binance USDⓈ-M ``aggTrade`` frame (the tape)."""
    payload: Dict[str, Any] = {
        "e": "aggTrade",
        "E": event_ms,
        "T": event_ms - 1,
        "s": "BTCUSDT",
        "a": 12345,
        "p": "30000.50",
        "q": "0.010",
        "f": 1,
        "l": 1,
        "m": False,
    }
    payload.update(extra)
    return json.dumps(payload)


def _combined(stream: str, payload: str) -> str:
    """Wraps a payload exactly the way a combined socket does (no whitespace)."""
    return '{"stream":"%s","data":%s}' % (stream, payload)


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
    # The tape rides along on the same socket: a depth-only archive cannot
    # distinguish a hit at our quote from the book crossing it.
    assert ingestion.aux_streams == ("trade",)
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


def test_aux_streams_build_a_combined_subscription() -> None:
    settings = _settings(aux_streams=("aggTrade",))
    assert settings.stream_tags == ("depth@100ms", "aggTrade")
    assert settings.combined_stream_url == (
        "wss://fstream.binance.com/stream"
        "?streams=btcusdt@depth@100ms/btcusdt@aggTrade"
    )
    assert settings.stream_url == settings.combined_stream_url


def test_a_single_stream_keeps_the_plain_url() -> None:
    settings = _settings()
    assert settings.combined_stream_url is None
    assert settings.stream_url.endswith("/ws/btcusdt@depth@100ms")


def test_aux_streams_this_venue_accepts_but_never_delivers_are_named() -> None:
    """``@aggTrade`` subscribes cleanly and then carries nothing here.

    Measured in docs/phase5 section 11.6.1: on the configured futures endpoint
    ``@aggTrade`` delivered 0 frames while ``@trade`` delivered the same prints
    at ~40/s. The capture therefore has to name the silent ones at startup
    instead of letting a tape study discover the empty archive later.
    """
    assert silent_aux_streams(("trade",), EXCHANGE_ENDPOINTS["binance_futures"]) == ()
    assert silent_aux_streams((), EXCHANGE_ENDPOINTS["binance_futures"]) == ()
    assert silent_aux_streams(("aggTrade", "trade"),
                              EXCHANGE_ENDPOINTS["binance_futures"]) == (
        "aggTrade",)
    assert silent_aux_streams(("aggTrade@100ms",),
                              EXCHANGE_ENDPOINTS["binance_futures"]) == (
        "aggTrade@100ms",)


def test_the_silent_stream_list_is_scoped_to_the_venue_that_was_measured() -> None:
    """A warning is a claim about an endpoint, so it may only cover that one.

    Spot names its aggregates stream ``aggTrade`` and delivers it, so the
    futures measurement says nothing about spot; the testnet endpoint is not
    measured at all. Warning there would be a statement the repository cannot
    support, and on spot it would be false in the operator's favour.
    """
    assert silent_aux_streams(("aggTrade",), EXCHANGE_ENDPOINTS["binance_spot"]) == ()
    assert silent_aux_streams(("aggTrade",),
                              EXCHANGE_ENDPOINTS["binance_futures_testnet"]) == ()


def test_the_silent_stream_warning_follows_the_endpoint_that_is_dialled() -> None:
    """A ``ws_base_url`` override moves the frames, so the claim moves with them.

    ``exchange`` only selects a default URL. Reading the warning's key from it
    would attach the futures measurement to whatever the override points at --
    silent where ``aggTrade`` does deliver, and quiet where it does not.
    """
    futures = EXCHANGE_ENDPOINTS["binance_futures"]
    # Configured as testnet, dialled at the endpoint that was measured.
    assert silent_aux_streams(("aggTrade",), futures) == ("aggTrade",)
    assert _settings(exchange="binance_futures_testnet",
                     ws_base_url=futures).endpoint == futures
    # Configured as futures, dialled at spot, where ``aggTrade`` delivers.
    assert silent_aux_streams(("aggTrade",),
                              EXCHANGE_ENDPOINTS["binance_spot"]) == ()
    # An endpoint nothing here has measured carries no claim in either direction.
    assert silent_aux_streams(("aggTrade",), "wss://example.invalid/ws") == ()
    # A trailing slash is not a different endpoint.
    assert silent_aux_streams(("aggTrade",), futures + "/") == ("aggTrade",)


def test_storage_dir_is_absolute_and_repo_relative() -> None:
    assert _settings().storage_dir == REPO_ROOT / "data" / "raw_l2"
    assert _settings(storage_path="/tmp/l2").storage_dir == Path("/tmp/l2")


def test_file_suffix_tracks_compression() -> None:
    assert _settings().file_suffix == ".ndjson.gz"
    assert _settings(compression="none").file_suffix == ".ndjson"


def _write_settings(tmp_path: Path, block: Dict[str, Any],
                    name: str = "settings.yaml") -> Path:
    path = tmp_path / name
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


def test_aux_stream_names_keep_their_case(tmp_path) -> None:
    """``aggTrade`` is case-sensitive on the venue; lower-casing it breaks it."""
    settings = load_settings(
        _write_settings(tmp_path, {"aux_streams": ["aggTrade", "markPrice@1s", "aggTrade"]})
    ).data_ingestion
    assert settings.aux_streams == ("aggTrade", "markPrice@1s")


def test_an_empty_aux_stream_list_is_a_depth_only_capture(tmp_path) -> None:
    settings = load_settings(_write_settings(tmp_path, {"aux_streams": []})).data_ingestion
    assert settings.aux_streams == ()
    assert settings.combined_stream_url is None


@pytest.mark.parametrize(
    "value",
    ["aggTrade", {"aggTrade": True}, ["agg Trade"], [""], ["a@b@c"], ["btcusdt@aggTrade/x"]],
)
def test_invalid_aux_streams_are_rejected(tmp_path, value) -> None:
    path = _write_settings(tmp_path, {"aux_streams": value})
    with pytest.raises(ConfigError, match="data_ingestion.aux_streams"):
        load_settings(path)


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
    # The repository config captures the tape, so the resolved URL is the
    # combined one; drop ``aux_streams`` and it collapses to ``/ws/<stream>``.
    assert entry["stream_url"] == (
        "wss://fstream.binance.com/stream"
        "?streams=btcusdt@depth@100ms/btcusdt@trade"
    )
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


def test_a_long_aux_list_still_fits_the_filesystem_limit(tmp_path) -> None:
    """``aux_streams`` accepts any number of tags; the name must still open."""
    tags = tuple(f"bookTicker{index:04d}@100ms" for index in range(12))
    writer = RawL2Writer(_settings(aux_streams=tags), directory=tmp_path)
    writer.append(_depth())
    asyncio.run(writer.aclose())

    files = _roll_files(tmp_path)
    assert len(files) == 1
    assert len(files[0].name.encode("utf-8")) <= 255
    # Truncated, not scrambled: the capture is still identifiable.
    assert files[0].name.startswith("binance_futures_btcusdt_depth_100ms_book")


def test_two_long_aux_lists_do_not_share_a_truncated_name(tmp_path) -> None:
    tags = [f"bookTicker{index:04d}@100ms" for index in range(12)]
    variants = (tuple(tags), tuple(tags[:-1] + ["aggTrade@100ms"]))
    names = []
    for index, variant in enumerate(variants):
        directory = tmp_path / str(index)
        writer = RawL2Writer(_settings(aux_streams=variant), directory=directory)
        writer.append(_depth())
        asyncio.run(writer.aclose())
        names.append(_roll_files(directory)[0].name)

    assert names[0] != names[1]


def test_prepare_opens_the_first_roll_file_before_any_frame(tmp_path) -> None:
    """An archive that cannot be opened must fail now, not on the first flush."""
    writer = RawL2Writer(_settings(), directory=tmp_path)
    assert _roll_files(tmp_path) == []

    writer.prepare()

    assert len(_roll_files(tmp_path)) == 1
    asyncio.run(writer.aclose())


def test_main_refuses_to_capture_into_an_unusable_archive(tmp_path, capsys,
                                                          monkeypatch) -> None:
    def _refuse(self, now):
        raise PermissionError("read-only archive")

    monkeypatch.setattr(ingest_module.RawL2Writer, "_open_file", _refuse)

    code = ingest_module.main(["--output-dir", str(tmp_path)])

    assert code == 1
    assert "cannot open the capture archive" in capsys.readouterr().err


# --------------------------------------------------------------------------- #
# Ingest client
# --------------------------------------------------------------------------- #
def _client(tmp_path: Path, **kwargs: Any) -> L2IngestClient:
    settings = kwargs.pop("settings", _settings())
    # Tests never reach the network: the snapshot fetcher is stubbed unless a
    # test is specifically exercising it. The stub is a valid book, so a test
    # that drives ``run()`` sees exactly one seeded record per attempt.
    kwargs.setdefault("snapshot_fetcher", lambda url, timeout: _snapshot())
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


def test_unwrap_frame_text_keeps_the_payload_bytes_verbatim() -> None:
    """The envelope is sliced off, not re-serialised: numbers must not move."""
    payload = _agg_trade()
    assert unwrap_frame_text(_combined("btcusdt@aggTrade", payload)) == payload
    # A single-stream frame is already the payload.
    assert unwrap_frame_text(payload) == payload
    # A bare payload that happens to contain a "data" key is untouched.
    bare = '{"e":"trade","data":{"x":1}}'
    assert unwrap_frame_text(bare) == bare


def test_unwrap_frame_text_extracts_the_whole_data_value() -> None:
    """The slice is the value's own span, not everything before the last brace."""
    payload = _agg_trade()
    # Keys after "data" would otherwise be swallowed into the archived line.
    data_first = '{"data":%s,"stream":"btcusdt@trade"}' % payload
    assert unwrap_frame_text(data_first) == payload
    # A brace inside a string is payload text, not the end of the envelope.
    braced = '{"stream":"btcusdt@trade","data":{"e":"trade","s":"A}B","p":"1"}}'
    assert unwrap_frame_text(braced) == '{"e":"trade","s":"A}B","p":"1"}'
    # An envelope whose members are separated by whitespace still unwraps.
    spaced = '{ "stream" : "btcusdt@trade" , "data" : %s }' % payload
    assert unwrap_frame_text(spaced) == payload
    # A truncated envelope is archived whole rather than half-sliced.
    truncated = '{"stream":"btcusdt@trade","data":{"e":"tra'
    assert unwrap_frame_text(truncated) == truncated


def test_a_mark_price_stream_is_not_ingestible() -> None:
    """Why settings.yaml's aux_streams example stops at ``trade``.

    The capture client would archive a mark-price frame happily; native ingestion
    cannot parse it, so ``ingest_json`` counts it malformed and
    ``phase1.archive_integrity`` (which requires zero) turns BUILD NO-GO.
    """
    from python_analytics.bridge import build_paper_engine, ingest_batch

    mark_price = json.dumps({"e": "markPriceUpdate", "E": 1_700_000_000_000,
                             "s": "BTCUSDT", "p": "30000.00", "i": "30001.00"})
    assert hft_native.classify_json(mark_price) == hft_native.FeedMessageType.UNKNOWN

    engine = build_paper_engine(tick_size="0.50", center="30000.00")
    stats = ingest_batch(engine, [_depth(), _agg_trade(), mark_price])
    assert (stats["depth"], stats["trade"], stats["malformed"]) == (1, 1, 1)


def test_a_seeded_capture_is_an_input_and_not_a_malformed_line() -> None:
    """Why the gate must survive the seed the capture now writes.

    ``phase1.archive_integrity`` requires zero malformed lines, and the archived
    REST book snapshot carries no lowercase ``e`` -- so a classifier that knows
    only stream events reads the seed as junk and turns every seeded capture into
    a BUILD NO-GO. It has to be a counted input that never reaches the delta path.
    """
    from python_analytics.bridge import build_paper_engine, ingest_batch

    # What the capture archives is the venue's body plus its own marker.
    seed = annotate_snapshot(_snapshot(event_ms=1_700_000_000_000), "btcusdt")
    assert (hft_native.classify_json(seed)
            == hft_native.FeedMessageType.BOOK_SNAPSHOT)

    engine = build_paper_engine(tick_size="0.50", center="30000.00")
    stats = ingest_batch(engine, [seed, _depth(), _agg_trade()])
    assert (stats["depth"], stats["trade"], stats["malformed"]) == (1, 1, 0)
    assert stats["snapshot"] == 1


def test_a_snapshot_shaped_stream_frame_is_never_a_seed() -> None:
    """Shape is not a marker, and confusing the two silences a broken capture.

    The spot ``depth20@100ms`` *stream* sends ``{lastUpdateId, bids, asks}`` with
    no event type -- the same shape an archived book seed has. Classify by shape
    and those frames are counted as seeds, never reach the book, and a capture of
    a stream the engine cannot use reports a clean archive with an empty ladder.
    The malformed count is what makes that loud, so it must stay malformed.
    """
    from python_analytics.bridge import build_paper_engine, ingest_batch

    bare = _snapshot(event_ms=1_700_000_000_000)          # venue shape, no marker
    assert hft_native.classify_json(bare) == hft_native.FeedMessageType.UNKNOWN

    engine = build_paper_engine(tick_size="0.50", center="30000.00")
    stats = ingest_batch(engine, [bare, _depth(event_ms=1_700_000_000_100)])
    assert (stats["snapshot"], stats["depth"], stats["malformed"]) == (0, 1, 1)


def test_event_kind_separates_depth_from_tape() -> None:
    assert event_kind(json.loads(_depth())) == "depth"
    assert event_kind(json.loads(
        annotate_snapshot(_snapshot(), "btcusdt"))) == "snapshot"
    assert event_kind(json.loads(_agg_trade())) == "trade"
    assert event_kind({"result": None, "id": 1}) == "other"


def test_client_archives_the_tape_unwrapped_and_counts_it(tmp_path) -> None:
    client = _client(tmp_path, settings=_settings(aux_streams=("aggTrade",)))
    event_ms = 1_700_000_000_000
    depth, trade = _depth(event_ms), _agg_trade(event_ms)
    items: List[Frame] = [
        (event_ms * NS_PER_MS + 5 * NS_PER_MS, _combined("btcusdt@depth@100ms", depth)),
        (event_ms * NS_PER_MS + 6 * NS_PER_MS, _combined("btcusdt@aggTrade", trade)),
    ]
    assert _consume(client, items) == 2

    stats = client.stats
    assert (stats.depth_frames, stats.trade_frames) == (1, 1)
    records = [record for path in _roll_files(tmp_path) for record in _read_roll(path)]
    assert records == [depth, trade]
    # The archived line is exactly what the native tape model consumes; without
    # this the capture and ``--fill-model trade`` would silently disagree.
    assert hft_native.classify_json(records[0]) == hft_native.FeedMessageType.DEPTH_UPDATE
    assert hft_native.classify_json(records[1]) == hft_native.FeedMessageType.TRADE
    tick = hft_native.parse_trade_json(records[1])
    assert float(tick.price) == pytest.approx(30000.50)
    assert float(tick.quantity) == pytest.approx(0.010)
    # ``m: false`` means the buyer was the taker: the print lifts the ask.
    assert tick.aggressor == hft_native.Side.BUY


def test_roll_file_name_records_every_stream(tmp_path) -> None:
    client = _client(tmp_path, settings=_settings(aux_streams=("aggTrade",)))
    _consume(client, [(1, _combined("btcusdt@aggTrade", _agg_trade()))])
    name = _roll_files(tmp_path)[0].name
    assert name.startswith("binance_futures_btcusdt_depth_100ms_aggTrade_")
    assert name.endswith(".ndjson.gz")


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

        async def open(self) -> None:
            raise ConnectionError("socket closed")

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
    # A feed that never opened gets no seed. Archiving one per attempt used to be
    # the behaviour, and it was worse than useless: a book snapshot with no
    # stream after it is a mid with no market behind it, and the reconciliation
    # below needs the socket to be delivering before the snapshot is fetched.
    assert stats.snapshots == 0
    assert client.writer_stats is not None
    assert client.writer_stats.records == 0
    assert client.stats.depth_frames == 0


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
    # The symbol and stream overrides apply to every stream on the socket,
    # including the tape that the repository config adds.
    assert settings.stream_url == (
        "wss://fstream.binance.com/stream"
        "?streams=ethusdt@depth@500ms/ethusdt@trade"
    )
    assert settings.compression == "none"
    assert settings.max_file_mb == 8
    assert settings.flush_interval_ms == 20
    assert args.duration == pytest.approx(1.5)


def test_cli_defaults_keep_the_yaml_settings() -> None:
    args = parse_args(["--dry-run"])
    assert build_settings(args) == load_settings().data_ingestion
    assert args.dry_run is True


def test_cli_adds_aux_streams() -> None:
    args = parse_args(["--aux-stream", "aggTrade", "--aux-stream", "markPrice@1s"])
    settings = build_settings(args)
    assert settings.aux_streams == ("aggTrade", "markPrice@1s")
    assert settings.stream_url == (
        "wss://fstream.binance.com/stream"
        "?streams=btcusdt@depth@100ms/btcusdt@aggTrade/btcusdt@markPrice@1s"
    )


def test_cli_depth_only_drops_the_configured_tape() -> None:
    settings = build_settings(parse_args(["--depth-only"]))
    assert settings.aux_streams == ()
    assert settings.stream_url == "wss://fstream.binance.com/ws/btcusdt@depth@100ms"


def test_cli_depth_only_wins_over_an_explicit_aux_stream() -> None:
    settings = build_settings(parse_args(["--aux-stream", "aggTrade", "--depth-only"]))
    assert settings.aux_streams == ()


def test_cli_rejects_unknown_compression() -> None:
    with pytest.raises(SystemExit):
        parse_args(["--compression", "lz4"])


# --------------------------------------------------------------------------- #
# Snapshot seeding
# --------------------------------------------------------------------------- #
def _snapshot(levels: int = 2, event_ms: int = 1_700_000_000_000,
              last_update_id: int = 42, symbol: Optional[str] = None) -> str:
    """A REST depth snapshot **in the shape the venue sends it**.

    Neither product's snapshot names its market, so ``symbol`` defaults to
    ``None``: a fixture that supplied it would hide the case where a mixed
    capture's seed is claimed by the wrong market.
    """
    bids = [[f"{100.0 - index * 0.5:.2f}", "1.0"] for index in range(levels)]
    asks = [[f"{100.5 + index * 0.5:.2f}", "1.0"] for index in range(levels)]
    payload: Dict[str, Any] = {
        "lastUpdateId": last_update_id, "E": event_ms, "T": event_ms,
        "bids": bids, "asks": asks,
    }
    if symbol is not None:
        payload["s"] = symbol
    return json.dumps(payload)


def test_a_snapshot_is_archived_as_the_first_line_of_the_roll(tmp_path) -> None:
    """The seed gives the analysis a mid before the first venue frame arrives.

    Without it a diff stream starts from an empty book: the timeline has no
    sample until enough deltas land (113 s in the 3 h capture), and every fill
    in that window is unmarkable.
    """
    client = _client(tmp_path, snapshot_fetcher=lambda url, timeout: _snapshot())

    async def _session() -> int:
        try:
            # The same order ``_consume_with_reconnect`` uses: connect, seed,
            # then read the stream.
            await client._seed_snapshot()
            return await client.consume(_FrameSource(
                [(1_700_000_000_100 * NS_PER_MS, _depth())]))
        finally:
            await client.aclose()

    assert asyncio.run(_session()) == 1
    lines = [record for path in _roll_files(tmp_path) for record in _read_roll(path)]
    seed = json.loads(lines[0])
    assert seed["lastUpdateId"] == 42
    # The venue sent no market and no event type; the archive must carry both, or
    # the seed is a line no consumer can attribute or recognise.
    assert seed["s"] == "BTCUSDT"
    assert seed["e"] == ingest_module.SNAPSHOT_MARKER
    assert client.stats.snapshots == 1
    assert client.stats.snapshot_failures == 0


def test_the_seeded_file_builds_a_monotonic_timeline(tmp_path) -> None:
    """The archive the client writes is analysable from its first line.

    This is the whole point of the seed: a diff stream resumed into an empty
    book leaves the mid timeline with no sample until enough deltas arrive, and
    the fills in that window cannot be marked.
    """
    from python_analytics.backtesting.markouts import build_timeline

    client = _client(tmp_path, snapshot_fetcher=lambda url, timeout: _snapshot(
        last_update_id=1))

    async def _session() -> None:
        try:
            # Seeded before the socket opens, so its venue time precedes every
            # frame that follows it, and its id precedes theirs so none of them
            # is discarded as already contained in the seed.
            await client._seed_snapshot()
            await client.consume(_FrameSource([
                (1_700_000_000_250 * NS_PER_MS,
                 _depth(1_700_000_000_200, b=[["99.99", "1.0"]],
                        a=[["100.51", "1.0"]])),
                (1_700_000_000_350 * NS_PER_MS,
                 _depth(1_700_000_000_300, b=[["100.00", "2.0"]],
                        a=[["100.50", "2.0"]])),
            ]))
        finally:
            await client.aclose()

    asyncio.run(_session())
    lines = [record for path in _roll_files(tmp_path) for record in _read_roll(path)]
    assert json.loads(lines[0])["lastUpdateId"] == 1

    timeline = build_timeline(lines)
    first_ts, first_mid, _ = next(timeline.samples())
    assert first_ts == 1_700_000_000_000 * NS_PER_MS
    assert first_mid == pytest.approx(100.25)
    # Seed, then each of the two frames: the mid follows the book, it does not
    # go missing because a fixture priced a level inconsistently.
    assert len(timeline) == 3
    assert timeline.stale == 0
    assert timeline.max_gap_s < 1.0


def test_a_live_session_opens_the_socket_before_it_seeds(tmp_path, monkeypatch) -> None:
    """The call-site order is what makes the handoff checkable at all.

    Binance's own procedure for a local book streams first and snapshots second,
    so the frames that land during the HTTP round trip exist and can be reconciled
    against the seed. Seeding before the socket opens also produced a monotonic
    file, but the updates between its ``lastUpdateId`` and the first frame were on
    no socket: nothing in the archive could tell a continuing book from one still
    holding a level the market deleted. This drives ``run()`` through the whole
    sequence -- redundant frame dropped, print kept, contiguous frame handoff --
    so a future edit to that order cannot pass.
    """
    from python_analytics.backtesting.markouts import build_timeline

    frame_ms = 1_700_000_000_500
    seed_ms = frame_ms - 300
    order: List[str] = []

    class _ThreeFrames:
        """Yields the frames a round trip buffers, then asks for the stop.

        Ending the session cleanly matters: a second attempt would archive a
        second seed, and the backward timestamp would be what failed the test
        rather than what it is checking.
        """

        def __init__(self, client: L2IngestClient) -> None:
            self._client = client
            self._left = (
                # Arrived while the snapshot was in flight, and is inside it:
                # archived, it would sit behind the seed that precedes it.
                (seed_ms - 100, _depth(seed_ms - 100, U=4, u=5)),
                # A print from the same window. Prints are the tape: the book
                # being reconciled is no reason to lose one, and a print before
                # the first mid is simply a print that cannot be marked yet.
                (seed_ms - 50, _agg_trade(seed_ms - 50)),
                # The first frame beyond the seed: its range starts at L + 1.
                (frame_ms, _depth(frame_ms, U=6, u=7,
                                  b=[["100.00", "2.0"]], a=[["100.50", "2.0"]])),
            )

        def __aiter__(self) -> "_ThreeFrames":
            return self

        async def open(self) -> None:
            order.append("open")
            return None

        async def __anext__(self) -> Frame:
            if not self._left:
                self._client.request_stop()
                raise StopAsyncIteration
            event_ms, raw = self._left[0]
            self._left = self._left[1:]
            return event_ms * NS_PER_MS, raw

        async def aclose(self) -> None:
            return None

    monkeypatch.setattr(ingest_module, "_SocketFrames", _ThreeFrames)

    def _fetch(url: str, timeout: float) -> str:
        order.append("seed")
        return _snapshot(event_ms=seed_ms, last_update_id=5)

    client = _client(
        tmp_path,
        settings=_settings(snapshot_limit=500),
        snapshot_fetcher=_fetch,
    )
    stats = asyncio.run(client.run())

    # The procedure, not just its bookkeeping: connecting first is what puts the
    # frames around the snapshot on the socket instead of in nobody's buffer.
    assert order == ["open", "seed"]
    assert stats.snapshots == 1
    assert stats.pre_handoff_drops == 1
    assert stats.handoff_gaps == 0
    # Received and archived are different counts: the dropped frame was real.
    assert stats.frames == 4 and stats.stored == 3

    lines = [record for path in _roll_files(tmp_path) for record in _read_roll(path)]
    assert len(lines) == 3
    seed = json.loads(lines[0])
    assert seed["lastUpdateId"] == 5 and seed["s"] == "BTCUSDT"
    assert json.loads(lines[1])["e"] == "aggTrade"

    timeline = build_timeline(lines)
    assert timeline.first_ts_ns == seed_ms * NS_PER_MS
    # The seed's own book, then the stream's: two samples, nothing discarded.
    assert len(timeline) == 2
    assert timeline.stale == 0
    assert timeline.handoff_gaps == 0


def test_a_non_contiguous_handoff_is_counted_not_silently_kept(tmp_path) -> None:
    """The frame that continues the seed must start where the seed ended.

    ``U`` ahead of ``lastUpdateId + 1`` means updates went by on no socket. The
    frame is still archived -- dropping market data to hide a gap is the opposite
    of what the capture is for -- but the session records that the book on the
    other side of it is unproven.
    """
    client = _client(
        tmp_path,
        snapshot_fetcher=lambda url, timeout: _snapshot(last_update_id=5),
    )

    async def _session() -> None:
        try:
            await client._seed_snapshot()
            await client.consume(_FrameSource([
                (1_700_000_000_600 * NS_PER_MS,
                 _depth(1_700_000_000_600, U=9, u=10,
                        b=[["100.00", "2.0"]], a=[["100.50", "2.0"]])),
            ]))
        finally:
            await client.aclose()

    asyncio.run(_session())
    assert client.stats.pre_handoff_drops == 0
    assert client.stats.handoff_gaps == 1
    lines = [record for path in _roll_files(tmp_path) for record in _read_roll(path)]
    assert len(lines) == 2


def test_a_frame_queued_during_the_seed_is_not_sampled_as_feed_latency(tmp_path) -> None:
    """The latency histogram is evidence about the feed, not about our own queue.

    The socket opens before the snapshot is fetched, and ``_SocketFrames`` stamps
    on dequeue -- so every frame that arrived during the REST round trip carries
    that round trip as "latency". Left in, one reconnect can put a multi-second
    sample in the p99, raise ``max`` and trip the threshold warning for a feed that
    was never slow. The seeding window is therefore not sampled at all.
    """
    client = _client(
        tmp_path,
        snapshot_fetcher=lambda url, timeout: _snapshot(last_update_id=5),
    )
    event_ms = 1_700_000_000_000
    queued = 400 * NS_PER_MS          # the REST round trip, above the 250 ms warn
    items: List[Frame] = [
        # Inside the seed: dropped, and never a latency sample.
        ((event_ms + 100) * NS_PER_MS + queued,
         _depth(event_ms + 100, U=4, u=5)),
        # A print held in the same queue: archived, not sampled.
        ((event_ms + 200) * NS_PER_MS + queued, _agg_trade(event_ms + 200)),
        # The frame that closes the window: still queued behind the snapshot.
        ((event_ms + 300) * NS_PER_MS + queued,
         _depth(event_ms + 300, U=6, u=7, b=[["100.0", "1"]], a=[["100.5", "1"]])),
        # The first frame that arrived after the window: the honest sample.
        ((event_ms + 400) * NS_PER_MS + 20 * NS_PER_MS,
         _depth(event_ms + 400, U=8, u=9, b=[["100.0", "1"]], a=[["100.5", "1"]])),
    ]

    async def _session() -> None:
        try:
            await client._seed_snapshot()
            await client.consume(_FrameSource(items))
        finally:
            await client.aclose()

    asyncio.run(_session())
    summary = client.latency.summary()
    assert summary.samples == 1
    assert summary.max_ms == pytest.approx(20.0)
    assert client.latency.breaches == 0
    # All four frames were still received and counted; only the sampling changed.
    assert client.stats.frames == 5
    assert client.stats.pre_handoff_drops == 1


def test_a_depth_frame_with_no_update_id_is_archived_not_dropped(tmp_path) -> None:
    """An id a capture cannot read is not a licence to delete a real update.

    The window closes on the first depth frame either way, so a stream that names
    no ids is archived exactly as it arrives: the honest failure is an
    unreconciled book, not a missing one.
    """
    client = _client(
        tmp_path,
        snapshot_fetcher=lambda url, timeout: _snapshot(last_update_id=5),
    )

    async def _session() -> None:
        try:
            await client._seed_snapshot()
            await client.consume(_FrameSource([
                (1_700_000_000_600 * NS_PER_MS,
                 _depth(1_700_000_000_600, U=None, u=None)),
            ]))
        finally:
            await client.aclose()

    asyncio.run(_session())
    assert client.stats.pre_handoff_drops == 0
    assert client.stats.handoff_gaps == 0
    lines = [record for path in _roll_files(tmp_path) for record in _read_roll(path)]
    assert len(lines) == 2


def test_an_unseeded_session_drops_nothing(tmp_path) -> None:
    """No seed means no book to be redundant with, so every frame is archived."""
    client = _client(tmp_path, settings=_settings(snapshot_on_connect=False))

    async def _session() -> None:
        try:
            await client._seed_snapshot()
            await client.consume(_FrameSource([
                (1_700_000_000_100 * NS_PER_MS, _depth(1_700_000_000_100, U=1, u=2)),
            ]))
        finally:
            await client.aclose()

    asyncio.run(_session())
    assert client.stats.snapshots == 0
    assert client.stats.pre_handoff_drops == 0
    lines = [record for path in _roll_files(tmp_path) for record in _read_roll(path)]
    assert len(lines) == 1


def test_a_spot_seed_does_not_file_the_host_clock_under_the_venue_name(
    tmp_path, monkeypatch
) -> None:
    """Venue time and host time are different domains, and ordering proves it.

    Spot's depth response carries no ``E``/``T``. Giving the seed this machine's
    clock under the venue's field name would put the book ahead of the frames that
    continue it whenever the host runs fast -- and ``build_timeline`` refuses a
    backwards sample, so one fabricated field loses a whole capture. The seed
    instead holds its book, and the first continuing frame carries the mid in the
    venue's own domain.
    """
    from python_analytics.backtesting.markouts import build_timeline

    venue_ms = 1_700_000_000_000
    host_ms = venue_ms + 10_000            # the host clock runs 10 s ahead

    class _OneDepthFrame:
        """Delivers one frame that continues the seed, then stops the session."""

        def __init__(self, client: L2IngestClient) -> None:
            self._client = client
            self._done = False

        def __aiter__(self) -> "_OneDepthFrame":
            return self

        async def open(self) -> None:
            return None

        async def __anext__(self) -> Frame:
            if self._done:
                self._client.request_stop()
                raise StopAsyncIteration
            self._done = True
            return ((venue_ms + 500) * NS_PER_MS,
                    _depth(venue_ms + 500, U=6, u=7,
                           b=[["100.0", "1"]], a=[["100.5", "1"]]))

        async def aclose(self) -> None:
            return None

    def _spot_body(url: str, timeout: float) -> str:
        return ('{"lastUpdateId": 5, "bids": [["100.0", "9"], ["99.5", "9"]],'
                ' "asks": [["100.5", "9"], ["101.0", "9"]]}')

    monkeypatch.setattr(ingest_module, "_SocketFrames", _OneDepthFrame)
    client = _client(
        tmp_path,
        snapshot_fetcher=_spot_body,
        clock=lambda: host_ms * NS_PER_MS,
    )
    stats = asyncio.run(client.run())

    assert stats.snapshots == 1
    assert stats.handoff_gaps == 0
    assert stats.no_timestamp == 0        # a deliberate absence, not a stray frame
    lines = [record for path in _roll_files(tmp_path) for record in _read_roll(path)]

    # Read the archive first: what this pins is the *placement*. A seed stamped
    # from the host clock either lands in the wrong domain (a mid before the
    # market it describes) or is caught by the backdated guard and silently
    # forfeits its sample. Neither is a venue-domain mid, which is the only thing
    # worth putting in the timeline.
    timeline = build_timeline(lines)
    assert timeline.seed_unstamped == 1
    assert timeline.seed_backdated == 0
    assert len(timeline) == 1
    assert timeline.first_ts_ns == (venue_ms + 500) * NS_PER_MS
    # The seeded book is what the frame was applied to: the 100.0 bid survives from
    # the seed, and the mid is the venue's, not the host's guess.
    assert timeline.at_or_before(timeline.first_ts_ns)[1] == pytest.approx(100.25)

    # Nothing host-dated is in the line at all: the archive keeps venue timestamps
    # only (decision ``P1-D1``), so nothing downstream can order by two clocks.
    seed_line = json.loads(lines[0])
    assert "E" not in seed_line and "T" not in seed_line
    assert not any("host" in key for key in seed_line)


def test_a_frame_already_contained_in_the_seed_is_not_applied_twice(tmp_path) -> None:
    """The REST request finished before the stream was read, so ids can overlap.

    A delta at or below the seed's ``lastUpdateId`` is already in the book; if it
    is applied again a level moves twice and the resulting mid is wrong in the
    way no human would notice -- it looks plausible.
    """
    from python_analytics.backtesting.markouts import build_timeline

    seed = _snapshot(event_ms=1_700_000_000_000, last_update_id=100)
    stale = _depth(1_700_000_000_100, U=50, u=60)
    fresh = _depth(1_700_000_000_200, U=101, u=101,
                   b=[["100.00", "2.0"]], a=[["100.50", "2.0"]])

    timeline = build_timeline([seed, stale, fresh])
    assert timeline.stale == 1
    assert len(timeline) == 2
    assert list(timeline.samples())[-1][1] == pytest.approx(100.25)


def test_the_seeded_snapshot_is_readable_by_the_mid_timeline(tmp_path) -> None:
    """The archived line is the shape the analysis builder consumes."""
    from python_analytics.backtesting.markouts import build_timeline

    client = _client(tmp_path, snapshot_fetcher=lambda url, timeout: _snapshot())
    asyncio.run(client._seed_snapshot())
    asyncio.run(client.aclose())
    lines = [record for path in _roll_files(tmp_path) for record in _read_roll(path)]

    timeline = build_timeline(lines)
    assert len(timeline) == 1
    assert timeline.at_or_before(1_700_000_000_000 * NS_PER_MS)[1] == pytest.approx(100.25)


def test_a_failed_snapshot_is_recorded_and_the_capture_continues(tmp_path) -> None:
    """A seed failure is a hole in the book, never a dead capture."""

    def _boom(url: str, timeout: float) -> str:
        raise OSError("connection refused")

    client = _client(tmp_path, snapshot_fetcher=_boom)
    asyncio.run(client._seed_snapshot())
    assert client.stats.snapshot_failures == 1
    assert client.stats.snapshots == 0

    _consume(client, [(1_700_000_000_100 * NS_PER_MS, _depth())])
    assert client.stats.frames == 1


def test_snapshot_seeding_can_be_switched_off(tmp_path) -> None:
    """``--no-snapshot`` must not fetch, and must not count a failure."""
    calls: List[str] = []

    def _record(url: str, timeout: float) -> str:
        calls.append(url)
        return _snapshot()

    client = _client(tmp_path, settings=_settings(snapshot_on_connect=False),
                     snapshot_fetcher=_record)
    asyncio.run(client._seed_snapshot())
    assert calls == []
    assert client.stats.snapshots == 0 and client.stats.snapshot_failures == 0


def test_the_snapshot_url_follows_the_endpoint_that_is_dialled() -> None:
    """The REST host is the venue's, and an unknown endpoint gets nothing."""
    assert depth_snapshot_url(EXCHANGE_ENDPOINTS["binance_futures"], "btcusdt") == (
        "https://fapi.binance.com/fapi/v1/depth?symbol=BTCUSDT&limit=1000")
    assert depth_snapshot_url(EXCHANGE_ENDPOINTS["binance_spot"], "ethusdt", 50) == (
        "https://api.binance.com/api/v3/depth?symbol=ETHUSDT&limit=50")
    assert depth_snapshot_url("wss://example.invalid/ws", "btcusdt") is None


def test_a_snapshot_is_stamped_with_only_what_the_venue_omitted() -> None:
    """The missing market is added; a timestamp never is.

    Neither product's REST depth response names its symbol, so an unlabelled line
    is the one record a mixed capture could attribute to the wrong book. Spot also
    sends no timestamp, and the capture's own receive time is not written either:
    it belongs to a different clock than the frames around it, and decision
    ``P1-D1`` keeps host times out of the archive. The seed therefore places its
    book and lets the venue-stamped frames place the mid.
    """
    spot = '{"lastUpdateId": 7, "bids": [["100.0", "1.0"]], "asks": [["100.5", "1.0"]]}'
    seeded = annotate_snapshot(spot, "btcusdt")
    assert seeded.startswith('{"e":"zephyr.bookSnapshot","s":"BTCUSDT",')
    # The venue's own bytes survive the textual insert unchanged.
    assert seeded.split('"BTCUSDT",', 1)[1] == spot[1:]
    parsed = json.loads(seeded)
    assert parsed["lastUpdateId"] == 7 and parsed["bids"] == [["100.0", "1.0"]]
    # No borrowed time, under the venue's name or anyone else's.
    assert "E" not in parsed and "T" not in parsed

    # A USDⓈ-M body already carries E/T, so only the market is added.
    futures = _snapshot(event_ms=1_700_000_000_000)
    assert json.loads(annotate_snapshot(futures, "btcusdt"))["E"] == 1_700_000_000_000
    assert json.loads(annotate_snapshot(futures, "btcusdt"))["s"] == "BTCUSDT"

    # A body that already names its market keeps the venue's spelling, and a body
    # with no time still gets none.
    named = ('{"lastUpdateId": 7, "s": "ETHUSDT",'
             ' "bids": [["100.0", "1.0"]], "asks": [["100.5", "1.0"]]}')
    annotated = json.loads(annotate_snapshot(named, "btcusdt"))
    assert annotated["s"] == "ETHUSDT"
    assert "E" not in annotated and "T" not in annotated


def test_only_a_body_with_a_book_in_it_counts_as_a_snapshot() -> None:
    """A 200 that is not a book must not be marked, archived, or counted.

    A proxy or captive portal answers with whatever it likes. Marking such a body
    would be worse than skipping it: the inserted fields trail a comma, so `{}`
    becomes invalid JSON, and the capture would log a seeded book it never
    received -- while archiving a line the parser has to reject into a file meant
    to be ground truth.
    """
    assert annotate_snapshot("not json", "btcusdt") is None
    assert annotate_snapshot("[1, 2, 3]", "btcusdt") is None
    assert annotate_snapshot("{}", "btcusdt") is None
    # An object shaped like a book response but with no book in it.
    assert annotate_snapshot('{"bids": [], "asks": []}', "btcusdt") is None
    # Right keys, empty levels: archiving this as a seed would replace a working
    # ladder with nothing at all.
    assert annotate_snapshot('{"lastUpdateId": 1}', "btcusdt") is None
    assert annotate_snapshot('{"lastUpdateId": 1, "bids": [], "asks": []}', "btcusdt") is None
    assert annotate_snapshot('{"lastUpdateId": 1, "bids": "x", "asks": "y"}', "btcusdt") is None
    # The real shape still annotates.
    marked = annotate_snapshot(_snapshot(), "btcusdt")
    assert marked is not None and '"lastUpdateId"' in marked


def test_a_non_book_depth_response_is_a_counted_seed_failure(tmp_path) -> None:
    """The failure is recorded and the archive stays clean."""
    client = _client(tmp_path, snapshot_fetcher=lambda url, timeout: "{}")
    asyncio.run(client._seed_snapshot())
    assert client.stats.snapshots == 0
    assert client.stats.snapshot_failures == 1

    _consume(client, [(1_700_000_000_100 * NS_PER_MS, _depth())])
    lines = [record for path in _roll_files(tmp_path) for record in _read_roll(path)]
    assert all("lastUpdateId" not in line for line in lines)
    assert len(lines) == 1


def test_the_snapshot_depth_is_validated_per_venue() -> None:
    """The venue answers a depth it does not serve with HTTP 400.

    Measured against the live API: USDⓈ-M accepts 5/10/20/50/100/500/1000 and
    rejects 7 and 1001. A configured value it rejects is worse than a typo -- the
    seed silently never happens, leaving the book blind for the exact reason the
    seed exists to prevent.
    """
    futures = EXCHANGE_ENDPOINTS["binance_futures"]
    spot = EXCHANGE_ENDPOINTS["binance_spot"]
    assert check_snapshot_limit(futures, 1000) == 1000
    assert check_snapshot_limit(spot, 1234) == 1234
    with pytest.raises(ConfigError, match="HTTP 400"):
        check_snapshot_limit(futures, 7)
    with pytest.raises(ConfigError, match="<= 1000"):
        check_snapshot_limit(futures, 1001)
    with pytest.raises(ConfigError, match="<= 5000"):
        check_snapshot_limit(spot, 6000)


def test_the_snapshot_ceiling_belongs_to_the_endpoint_being_dialled(
    tmp_path,
) -> None:
    """``exchange`` picks a default URL; the venue that answers sets the ceiling.

    Bounding the configured value by the configured *name* was wrong in both
    directions: spot serves ``limit=2000``, so a spot ``ws_base_url`` under
    ``exchange: binance_futures`` was refused over a number its own venue accepts,
    while a futures URL under ``exchange: binance_spot`` was waved through with a
    depth USD-M answers by HTTP 400 -- an unseeded capture that looks configured.
    """
    spot = EXCHANGE_ENDPOINTS["binance_spot"]
    futures = EXCHANGE_ENDPOINTS["binance_futures"]

    settings = load_settings(_write_settings(tmp_path, {
        "exchange": "binance_futures",
        "ws_base_url": spot,
        "snapshot_limit": 2000,
    })).data_ingestion
    assert settings.endpoint == spot
    assert settings.snapshot_limit == 2000

    with pytest.raises(ConfigError, match="<= 1000"):
        load_settings(_write_settings(
            tmp_path,
            {
                "exchange": "binance_spot",
                "ws_base_url": futures,
                "snapshot_limit": 2000,
            },
            name="wrong_venue.yaml",
        ))


def test_the_cli_snapshot_depth_goes_through_the_same_contract() -> None:
    """``dataclasses.replace`` does not re-run validation, so this is explicit."""
    args = parse_args(["--snapshot-limit", "500"])
    assert build_settings(args).snapshot_limit == 500
    with pytest.raises(ConfigError, match="--snapshot-limit"):
        build_settings(parse_args(["--snapshot-limit", "7"]))
