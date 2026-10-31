# Phase 1 — Real-Time L2 Ingestion & Compressed Storage Pipeline

**Project:** ZephyrHFT — low-latency market-making research platform
**Phase:** 1 of 4 — Data Plane Foundation
**Status:** ✅ **Delivered, validated and merged to `main`**

| Item | Value |
| --- | --- |
| Document ID | `ZEPHYR-DOC-P1-001` |
| Baseline commit | `57ba3a5` — *Merge PR #1 `feature/phase1-data-ingestion`* |
| Delivery branch | `feature/phase1-data-ingestion` |
| Verification | **138 / 138** Python tests · **all** C++ core tests · live venue capture |
| Primary artefact | `scripts/ingest_l2.py` (1,102 LOC) |
| Configuration | `config/settings.yaml` → `data_ingestion` block (16 keys) |
| Runtime | CPython 3.9+ (verified on 3.9.7), `asyncio`, `websockets` ≥12 <16 |
| Document owner | Data Platform / Low-Latency Architecture |
| Audience | Quant researchers, platform engineers, SRE, Phase 2–4 implementers |
| Last verified | 2026-10-22 (live capture + full test suite re-run) |

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [Scope, Assumptions and Non-Goals](#2-scope-assumptions-and-non-goals)
3. [Architecture](#3-architecture)
   - 3.1 [Component map](#31-component-map)
   - 3.2 [End-to-end system flow](#32-end-to-end-system-flow)
   - 3.3 [Frame lifecycle and per-frame cost budget](#33-frame-lifecycle-and-per-frame-cost-budget)
   - 3.4 [Concurrency model](#34-concurrency-model)
   - 3.5 [Feed-latency accounting](#35-feed-latency-accounting)
   - 3.6 [Reconnection state machine](#36-reconnection-state-machine)
   - 3.7 [Shutdown state machine](#37-shutdown-state-machine)
   - 3.8 [Backpressure and zero-loss policy](#38-backpressure-and-zero-loss-policy)
4. [Configuration & Command-Line Interface](#4-configuration--command-line-interface)
5. [Data Schema & Compression Format](#5-data-schema--compression-format)
6. [Test Strategy & Verification Metrics](#6-test-strategy--verification-metrics)
7. [Performance Characteristics & Capacity Planning](#7-performance-characteristics--capacity-planning)
8. [Operational Runbook](#8-operational-runbook)
9. [Known Limitations & Risk Register](#9-known-limitations--risk-register)
10. [Phase 2+ Handoff Roadmap](#10-phase-2-handoff-roadmap)
11. [Appendix](#11-appendix)

---

## 1. Executive Summary

Phase 1 delivers the **cold path of the data plane**: a single-process, asyncio-based
capture agent that subscribes to the Binance USD-M Futures L2 diff-depth stream
(`depth@100ms`), stamps every frame on arrival, continuously measures venue-to-host
feed latency, and archives the byte-exact payload into rotation-bounded
gzip-compressed NDJSON roll files.

The design principle is a strict separation between **the latency-critical receive
loop** and **everything expensive**: JSON parsing is the only per-frame CPU cost on
the event loop, while compression, disk I/O and `fsync` are delegated to a worker
thread. A slow disk therefore degrades *buffering*, never *completeness*.

Each connection is seeded with one absolute depth snapshot from the venue's REST
API, archived **before** the socket opens (`snapshot_on_connect`, `--no-snapshot`
to disable). The depth feed is a *diff* stream, so a reader that starts empty —
and every reader starts empty, including after a reconnect — has no mid until
enough deltas arrive; in a 3 h capture that took 113 s, and every fill in the
window was unmarkable (`docs/phase5_production_readiness.md` §11.8.7). The seed is
an extra NDJSON line of the same shape as a REST book snapshot (`lastUpdateId`,
`bids`, `asks`), annotated textually with the two things the venue's response
does not carry and this process knows: its receive time (`E`; the USDⓈ-M snapshot
has one, the spot snapshot has none) and its market (`s` — **neither** product
names its symbol in this response, and an unlabelled line is the one record a
mixed capture could attribute to the wrong book). The venue's own bytes are never
re-serialised.

What the seed cannot do is hand off contiguously: whatever changed between its
`lastUpdateId` and the first frame was not on any socket yet, so it is not in the
archive. The reconstructed book is therefore correct *from the seed forward*, up
to one REST round trip of level changes — enough for a mid timeline, which is
what the seed is for, and explicitly not a replacement for Binance's
event-id-contiguous local-book procedure. The analysis side enforces the part of
that procedure which is cheap and load-bearing: a diff frame whose last update id
is at or below the seed's is already inside the snapshot, and is counted in
`MarkoutTimeline.stale` rather than applied a second time (applying it twice moves
a level twice, and produces a mid that looks plausible).

### 1.1 Headline outcomes

| Objective | Result |
| --- | --- |
| Live L2 capture from venue | ✅ `wss://fstream.binance.com/ws/btcusdt@depth@100ms`, ~8–10 frames/s |
| Byte-exact archival, zero loss | ✅ 64/64 and 800/800 records round-tripped; 0 sequence breaks in `pu → u` chain |
| Compressed storage | ✅ 3.67× reduction at `compresslevel=1` (181 KB → 49 KB in a live run) |
| Feed-latency telemetry | ✅ p50/p95/p99/max + breach counter + clock-skew counter, every 10 s |
| Automatic recovery | ✅ Capped exponential backoff with jitter, ladder reset after 5 s healthy session |
| Clean shutdown (no corruption) | ✅ `SIGINT`/`SIGTERM` → socket close → buffer drain → gzip trailer → `fsync` |
| Configuration-driven | ✅ 16 validated YAML keys + 15 CLI flags, precedence CLI > YAML > defaults |
| Test coverage | ✅ 76 cases for the ingestion module, 138 repo-wide, 0 failures |

### 1.2 What this unlocks

Phase 2 (normalisation, feature engineering, replay) and Phase 3 (backtesting,
strategy research) both consume the roll files as **ground truth**. Because Phase 1
preserves the venue payload verbatim rather than a lossy interpretation of it, any
downstream schema decision can be revisited without re-capturing history.

---

## 2. Scope, Assumptions and Non-Goals

### 2.1 In scope — delivered

| ID | Requirement | Implementation |
| --- | --- | --- |
| P1-01 | Async WebSocket client for L2 depth | `L2IngestClient` + `_SocketFrames` (`websockets.connect`) |
| P1-02 | Auto-reconnect with exponential backoff | `_consume_with_reconnect()` + `reconnect_delay()` |
| P1-03 | Receive-timestamp stamping | `client._clock()` = `time.time_ns()` at socket egress |
| P1-04 | Feed-latency computation & telemetry | `LatencyTracker`, `_log_telemetry()` |
| P1-05 | gzip NDJSON rolling archive | `RawL2Writer`, `data/raw_l2/` |
| P1-06 | Non-blocking / buffered writes | `bytearray` + `asyncio.to_thread` worker |
| P1-07 | Graceful `SIGINT`/`SIGTERM` shutdown | `install_signal_handlers()`, `request_stop()`, `RawL2Writer.aclose()` |
| P1-08 | Centralised YAML configuration | `DataIngestionSettings` in `python_analytics/utils/config.py` |
| P1-09 | Dynamic symbol/stream selection | `--symbol`, `--stream`, `--url`, `--config` |
| P1-10 | Offline replay through the same code path | `--replay` → `L2IngestClient.replay()` |

### 2.2 Explicitly out of scope — deferred

| Deferred item | Target phase | Rationale |
| --- | --- | --- |
| Order-book reconstruction (`L2OrderBook` from deltas) | Phase 2 | Requires the sequence-chain validator and snapshot bootstrap |
| Normalised columnar format (Parquet/Arrow) | Phase 2 | Raw capture must exist first; schema decided from real data |
| Feature engineering (OFI, micro-price, depth slopes) | Phase 2 | Depends on the reconstructed book |
| Multi-symbol / multi-venue fan-out in one process | Phase 2+ | Phase 1 is deliberately one subscription per process |
| Local receive timestamps persisted **inside** the archive | Phase 2 (decision `P1-D1`) | See [§9.1](#91-archive-carries-no-local-receive-timestamp) |
| Authenticated / user-data streams (fills, positions) | Phase 4 | Not required for research capture |

### 2.3 Assumptions

1. **Host clock discipline.** Feed latency is computed against the venue event clock,
   so the host must run `chronyd`/`ntpd` (target: < 1 ms offset). Unsynced clocks make
   percentiles meaningless — the `clock_skew` counter exists to make that visible.
2. **Single writer per directory.** One process owns one `storage_path`. Roll-file
   sequence numbers are process-local (`00001`, `00002`, …); the UTC timestamp in the
   filename prevents cross-process collisions.
3. **Venue framing.** Binance frames its own messages, so per-message
   `permessage-deflate` is disabled — it would add CPU and jitter to the receive path
   for no benefit.
4. **Durability vs. throughput.** `fsync` runs once per roll-file close, not per flush.
   A power loss can cost at most the in-flight `flush_interval_ms` window.
5. **Clock monotonicity.** `time.time_ns()` is wall-clock (needed to compare with the
   venue's wall-clock timestamps); a system time step during a session shows up as a
   latency outlier, which is an accepted and visible trade-off.

---

## 3. Architecture

### 3.1 Component map

| Layer | Component | Responsibility | Owns |
| --- | --- | --- | --- |
| Transport | `_SocketFrames` | One connection as an async iterator; yields `(recv_ns, raw)`; explicit `aclose()` | Socket, ping/pong, close handshake |
| Orchestration | `L2IngestClient._consume_with_reconnect` | Session loop: connect → drain → classify → back off → repeat | Reconnect counter, backoff ladder |
| Hot path | `L2IngestClient._handle_frame` | Archive append → parse → timestamp extraction → latency record. Zero I/O, zero awaits | Per-frame cost |
| Measurement | `LatencyTracker` / `LatencySummary` | Rolling window of latency samples; p50/p95/p99/min/max/mean; breach and skew counters | 20,000-sample deque |
| Storage | `RawL2Writer` | Append buffer, flush cadence, rotation, gzip container, `fsync`, gzip trailer | File handles, sequence counter |
| Observability | `_log_telemetry` / `_print_summary` | Structured INFO line per interval, `FINAL` line, shutdown table | stdout |
| Configuration | `DataIngestionSettings` | Validation, endpoint resolution, URL/prefix/path derivation | YAML + CLI |
| Lifecycle | `install_signal_handlers` / `request_stop` | Thread-safe stop that also wakes a blocked reader | Signal delivery |

Module boundaries are deliberately narrow: the receive loop talks to the writer through
exactly two methods (`append`, `stats`) and to the socket through one tuple type
(`Frame = Tuple[int, str]`). This is what allows the entire production path to be driven
in tests with a list of strings.

### 3.2 End-to-end system flow

```text
 [1] VENUE          Binance USD-M Futures — wss://fstream.binance.com/ws/btcusdt@depth@100ms
      |
      |  ~10 depthUpdate frames/s, mean 2.8 KiB, 100 ms cadence (measured: 102 ms)
      v
 [2] SOCKET         websockets.connect(compression=None, max_queue=1024,
      |                                  ping_interval=20 s, open_timeout=10 s)
      |  (recv_ns, raw_json)   <- recv_ns = time.time_ns() the instant the frame leaves the socket
      v
 [3] RECEIVE PATH   _handle_frame()
      |              1. writer.append(raw)          -> bytearray (memcpy only)
      |              2. json.loads(raw)             -> only CPU cost on the loop
      |              3. event_timestamp_ns(payload) -> E, fallback T
      |              4. latency.record(recv_ns - E) -> counters
      |              ~28 us/frame (median) on real payloads; no syscalls, no awaits
      +--------------------------+
      |                          |
      v                          v
 [4a] LATENCY                [4b] ARCHIVE
      LatencyTracker              bytearray buffer (append-only, never drops)
      |  p50/p95/p99/max           |
      |  breaches, clock_skew      |  250 ms tick (flush_interval_ms) or shutdown
      v                           v
 [5] TELEMETRY               [6] WRITER TASK      detach chunk -> asyncio.to_thread(_write)
      INFO line / 10 s           |
      + FINAL line on exit       v
                           [7] WORKER THREAD       gzip(level=1) + write + Z_SYNC_FLUSH
                                 |
                                 v
                           [8] ROLL FILE      data/raw_l2/binance_futures_btcusdt_
                                 |            depth_100ms_<UTC>_<seq>.ndjson.gz
                                 |  rotate at 256 MiB or 900 s
                                 |  fsync + gzip trailer on close
                                 v
                           [9] CONSUMERS (Phase 2/3)  replay, normalisation, backtests
```

Two invariants fall out of this shape and are enforced by tests:

* **Stage 3 never blocks on stage 8.** The only object shared between them is a
  `bytearray` swapped by reference.
* **Stage 4b is lossless by construction.** `append()` cannot fail on disk pressure —
  there is no disk in the call.

### 3.3 Frame lifecycle and per-frame cost budget

A single frame traverses the system in four distinct time domains. Only the last two are
under our control, which is precisely why they are instrumented separately.

```text
 t0   venue stamps E (event time, ms)        |
 t1   venue egress -> internet               |  feed latency (measured)
 t2   kernel socket buffer -> websockets     |
 t3   recv_ns = time.time_ns()               |<-- measurement point
 t4   writer.append(raw)          ~1–3 µs    |
 t5   json.loads(raw)            ~20–40 µs   |  host processing (never measured
 t6   event_timestamp_ns + record   ~1 µs    |  as "latency", but it is the
 t7   loop ready for the next frame          |  throughput constraint)
```

| Cost centre | Where it runs | Measured | Bounded by |
| --- | --- | --- | --- |
| TLS decrypt + frame reassembly | `websockets` (loop) | vendor | `max_queue=1024` |
| Recv timestamp | loop | ~0.1 µs | — |
| Archive append (memcpy) | loop | ~1–3 µs per 2.8 KiB | RAM bandwidth |
| `json.loads` | loop | ~20–40 µs | payload size |
| Latency accounting | loop | ~1 µs | — |
| gzip compression | worker thread | ~1 ms per 177 KiB (~170–280 MiB/s @ level 1) | CPU core |
| `write()` + `Z_SYNC_FLUSH` | worker thread | ~sub-ms | page cache / disk |
| `fsync` | worker thread | ms–tens of ms | storage device |

Measured end-to-end receive-path cost on **real captured payloads**: **19–50 µs/frame**
(median 27.6 µs ⇒ ~36,000 frames/s on one core) versus a production load of ~10 frames/s —
a **~3,600×** CPU headroom margin. See [§7](#7-performance-characteristics--capacity-planning).

### 3.4 Concurrency model

Exactly **two execution contexts** exist, and their interaction is a one-way hand-off:

```text
  event loop thread (single-threaded asyncio)
  ┌──────────────────────────────────────────────────────────────────────────┐
  │  _SocketFrames.__anext__   ──await──▶ socket recv                       │
  │  consume()                 ──▶  _handle_frame()  ──▶  bytearray += ...   │
  │  _writer_loop              ──await──▶ asyncio.wait_for(wake, 250 ms)     │
  │                                  └─▶ flush_async()  ──┐                 │
  └───────────────────────────────────────────────────────│─────────────────┘
                                                          │ detached bytes only
                                                          ▼
  thread-pool worker (asyncio.to_thread)
  ┌──────────────────────────────────────────────────────────────────────────┐
  │  _write(chunk):  gzip.write → flush → rotate? → tell()                   │
  └──────────────────────────────────────────────────────────────────────────┘
```

| Rule | Enforcement |
| --- | --- |
| The loop never calls `gzip`, `write`, `flush` or `fsync` | Only `_write()` does, and only via `to_thread` |
| At most one `_write` in flight | `_writer_loop` awaits `flush_async()` before the next tick |
| File handles are never touched by two threads at once | `aclose()` joins the writer task *before* the final synchronous `flush()` |
| Frame data is copied exactly once into the buffer, then detached by reference | `_detach()` swaps `self._buffer` for a fresh `bytearray` |
| Shutdown never cancels a task mid-write | Cooperative stop via `_closed` + wake event (no `task.cancel()`) |

> **Design note — why the source is a class, not an async generator.**
> An async generator that is kept alive by an exception traceback stays registered with the
> loop and produces `Task was destroyed but it is pending` noise at interpreter shutdown.
> `_SocketFrames` is a class with an explicit `aclose()` so socket teardown is unambiguous
> on every exit path (normal end, exception, cancellation). This was a real defect found by
> the test suite, not a stylistic choice.

### 3.5 Feed-latency accounting

```text
 latency_ns = recv_ns − event_ts_ns
            = time.time_ns() − (E × 1_000_000)      # E preferred
                               (T × 1_000_000)      # fallback when E is absent

 recv_ns   wall-clock nanoseconds taken the instant the frame is yielded by websockets
 E         venue "event time", integer milliseconds (e.g. 1792683368185)
 T         venue "transaction time", integer milliseconds; T ≤ E by ~1 ms in practice
```

Integer arithmetic only: `int(raw) * 1_000_000`. No binary floating point touches the
measurement path, which avoids the rounding drift that would appear at millisecond
resolution on multi-hour sessions.

| Property | Value | Note |
| --- | --- | --- |
| Window | 20,000 samples (rolling `deque(maxlen=…)`) | ≈ 33 min at 10 frames/s |
| Percentile method | nearest-rank: `rank = ceil(q·n) − 1`, clamped | No interpolation → every reported value is an observed sample |
| Reported | `p50`, `p95`, `p99`, `min`, `max`, `mean`, `last` | p95/p99 included for tail triage |
| Cumulative counters | `breaches` (> `latency_warn_ms`), `negative` (venue clock ahead) | Never reset by the window |
| Warning throttle | At most one `WARNING` per second per process | Prevents log flooding during a venue stall |
| Cadence | `telemetry_interval_s` (default 10 s) + `FINAL` line | Percentiles are per-interval; counters are cumulative |

**Reading the numbers.** This metric is venue-to-host *transit* latency: exchange egress,
internet, kernel, and event-loop scheduling. It deliberately excludes our own processing
(measured separately, §3.3) so that a busy host does not masquerade as a slow venue.

**Interpreting anomalous values**

| Observation | Likely cause | Action |
| --- | --- | --- |
| `clock_skew` > 0 | Host clock behind the venue (unsynced NTP) | `chronyc tracking` / `ntpq -p`; re-sync and restart the capture |
| `p99 ≫ p50` (e.g. 30 ms vs 1.2 s) | Network jitter, ISP path change, venue congestion | Correlate with `reconnects`; consider a co-located VPS |
| `lat_ms` steadily climbing | Event loop starvation (oversized `max_queue`, or a co-tenant process) | Check CPU affinity/load; verify the receive path still has 0 awaits |
| `lat_breaches` growing but `p99` flat | Single outlier frames (large snapshots) | Expected: a post-reconnect snapshot is ~10× the median size |

### 3.6 Reconnection state machine

```text
                    ┌──────────────┐
                    │   CONNECT    │◀───────────────────────────────┐
                    └──────┬───────┘                                │
        open_timeout / DNS /│ TLS / HTTP 4xx                          │
        401 / 451 ──────────┤                                         │
                            │ handshake OK                            │
                            ▼                                         │
                    ┌──────────────┐   frames flow                     │
                    │  STREAMING   │──────────────┐                  │
                    └──────┬───────┘              │                  │
                           │                      │ ConnectionClosed │
             SIGINT/SIGTERM│                      │ TimeoutError     │
                           │                      │ OSError / TLS    │
                           │                      ▼                  │
                           │            ┌───────────────────────┐    │
                           │            │ session ≥ 5 s healthy?│    │
                           │            └────┬─────────────┬────┘    │
                           │            yes  │             │ no      │
                           │       attempt=0 │             │         │
                           │                 ▼             ▼         │
                           │      ┌────────────────────────────────┐ │
                           │      │ backoff: 0.5·2^n, cap 30 s,    │ │
                           │      │ ± jitter (uniform 0..1 × delay)│ │
                           │      │ limit reached? (0 = ∞)         │ │
                           │      └───────────────┬────────────────┘ │
                           │                      │ delay             │
                           │                      └───────────────────┘
                           ▼
                    ┌──────────────┐
                    │ DRAIN + CLOSE│   socket.close → flush → gzip trailer → fsync
                    └──────────────┘
```

| Behaviour | Detail |
| --- | --- |
| Ladder | `base · 2^attempt`, clamped to `reconnect_max_delay_s`; `attempt` increments per retry |
| Jitter | Symmetric ±25 % (`delay · (1 + 0.25·(2r−1))`), re-clamped to the cap; deterministic under a seeded `Random` so tests are reproducible |
| Reset | A session that stays up ≥ `HEALTHY_SESSION_S` (5 s) resets the ladder to attempt 0 |
| Give-up | `max_reconnect_attempts` (0 = retry forever, the default for a research capture host) |
| Fatal vs. transient | `FeedUnavailable` (e.g. `websockets` not installed) **aborts** — retrying cannot help. Everything else backs off and retries |
| Counters | `connect_failures` (never opened), `disconnects` (opened then lost), `reconnects` (retry cycles) |

### 3.7 Shutdown state machine

Graceful shutdown is the property most often lost in capture tools, and the one that
decides whether an archive is usable. The sequence below is exercised end-to-end on every
run and asserted by dedicated tests.

```text
  SIGINT / SIGTERM                 or  --duration elapsed  (_stop_after)
          │                                     │
          └──────────────┬──────────────────────┘
                         ▼
                 request_stop()
                 ├── flag = True                      (read by consume() loop guard)
                 ├── stop_event.set()                 (wakes backoff sleeps)
                 └── loop.call_soon_threadsafe(socket.close())   (wakes a blocked reader)
                         │
                         ▼
        consume() returns  →  _SocketFrames.aclose()  →  socket closed
                         │
                         ▼
        RawL2Writer.aclose()
        ├── _closed = True; wake event set                 (no task.cancel())
        ├── await writer_task                              (joins any in-flight _write)
        ├── flush()                                        (final chunk, synchronous)
        └── _close_file()
            ├── gzip handle close()  → writes CRC32 + ISIZE trailer
            ├── raw.flush()
            ├── os.fsync(fileno())                          (durable on disk)
            └── raw.close(); counters updated
                         │
                         ▼
        FINAL telemetry line  →  summary table  →  exit code 0
```

| Guarantee | Mechanism | Test |
| --- | --- | --- |
| No partially written gzip stream on clean exit | Trailer written by `GzipFile.close()` | `test_writer_fsyncs_and_leaves_a_valid_gzip_trailer` |
| Buffered records are never lost on shutdown | `flush()` after the writer task joins | `test_writer_aclose_flushes_pending_records` |
| Shutdown terminates promptly even while blocked on `recv` | Socket closed from the loop thread → reader raises `ConnectionClosed` | live `SIGINT`/`SIGTERM` runs |
| Repeated `aclose()` is idempotent | `_closed` guard | `test_writer_rejects_appends_after_close` |
| Exit codes are scriptable | `0` ok · `1` feed unavailable · `2` config error · `130` inherited `KeyboardInterrupt` | `main()` error paths |

### 3.8 Backpressure and zero-loss policy

The pipeline is **load-shedding-free by design**: there is no drop path anywhere.

```text
  buffer bytes
      │                                        ┌──── WARNING (once)  ≥ 64 MiB
      │                                        │
 64 MiB ────────────────────────────────────────┤  "disk slower than the feed
      │                                        │   (records are retained, never dropped)"
      │               normal saw-tooth         │
  0 MiB ────────────────────────────────────────┴──────────────────────────▶ time
             250 ms   250 ms   250 ms
             flush    flush    flush
```

| Condition | Behaviour | Rationale |
| --- | --- | --- |
| Disk keeps up (normal) | Buffer oscillates between 0 and one flush window (≈7 KB at 10 frames/s × 250 ms) | Steady state |
| Disk stalls | Buffer grows; latency tracking and capture continue | Losing market data is worse than growing RAM |
| Buffer ≥ 64 MiB (`HIGH_WATER_BYTES`) | One `WARNING`, latch released once it drops below 32 MiB | Operator visibility without log spam |
| Write raises (`ENOSPC`, `EIO`) | `write_errors++`, `logger.exception`, ingestion continues | A failed roll file must not kill the session; the count is in telemetry |
| Frame is malformed JSON | Archived anyway, `malformed++`, first 5 logged | The archive stays a complete record of what the wire delivered |
| Rotation, size rule | Requires `incoming > 0` and a non-empty current file | A final empty flush can never create a zero-byte roll file |
| Rotation, time rule | Fires on the first tick at/after `rotate_seconds`; may roll on an idle tick | Bounds file age even during a long outage |

---

## 4. Configuration & Command-Line Interface

Configuration is centralised in `config/settings.yaml` and parsed by
`python_analytics/utils/config.py` into a **frozen dataclass** (`DataIngestionSettings`).
There is no scattered `os.environ` access and no second source of truth: the CLI, the
library and the tests all resolve settings through `load_settings()`.

### 4.1 Resolution precedence

```text
  config/settings.yaml          CLI flags                dataclass defaults
  ────────────────────          ─────────                ──────────────────
   data_ingestion:        →     --symbol btcusdt    →    DataIngestionSettings()
        symbol: ethusdt         --max-file-mb 64
              │                       │                        │
              └──────────┬────────────┴────────────┬───────────┘
                         ▼                         ▼
              load_settings(path)          replace(settings, **overrides)
                         └──────────┬──────────────┘
                                    ▼
                        DataIngestionSettings (validated, frozen)
                                    │
                    ┌───────────────┼────────────────┐
                    ▼               ▼                ▼
              stream_url     storage_dir       file_suffix
        wss://…/btcusdt@…   <repo>/data/raw_l2   .ndjson.gz
```

* **CLI beats YAML beats defaults.** `build_settings()` collects only non-`None` CLI
  values and applies them with `dataclasses.replace`, so an omitted flag never clobbers a
  YAML value (`test_cli_defaults_keep_the_yaml_settings`).
* **Validation happens once, at load time.** A bad value raises `ConfigError` naming the
  exact dotted key (`data_ingestion.symbol`), and `main()` exits with code `2` — never
  mid-capture.
* **Empty or absent `data_ingestion` block** → defaults, so a minimal config file works.

### 4.2 `config/settings.yaml` — the `data_ingestion` block

```yaml
data_ingestion:
  enabled: true
  exchange: binance_futures        # keys of EXCHANGE_ENDPOINTS in utils/config.py
  symbol: btcusdt                  # lowercase, as required by the stream name
  stream_type: depth@100ms         # e.g. depth@100ms, depth@500ms, depth20@100ms
  storage_path: data/raw_l2        # relative to the repository root
  compression: gzip                # gzip | none
  compression_level: 1             # 1 = fastest (ingest path), 9 = smallest
  flush_interval_ms: 250           # max time a record waits in the write buffer
  max_file_mb: 256                 # rotate the roll file at this size (0 = never)
  rotate_seconds: 900              # ... or after this many seconds (0 = never)
  telemetry_interval_s: 10.0       # latency / throughput summary cadence
  latency_warn_ms: 250.0           # WARNING when feed latency exceeds this
  reconnect_base_delay_s: 0.5      # exponential backoff: 0.5, 1, 2, 4 ... capped
  reconnect_max_delay_s: 30.0
  max_reconnect_attempts: 0        # 0 = retry forever
  ws_base_url: ""                  # optional override of the exchange endpoint
```

### 4.3 Key reference

| Key | Type | Default | CLI override | Validation | Effect |
| --- | --- | --- | --- | --- | --- |
| `enabled` | bool | `true` | — | coerced with `bool()` — use a YAML boolean | `false` → exit without connecting |
| `exchange` | str | `binance_futures` | — | must be a key of `EXCHANGE_ENDPOINTS` (`binance_futures`, `binance_spot`) | selects the base URL |
| `symbol` | str | `btcusdt` | `--symbol` | lower-cased, then `[a-z0-9]{2,20}` | stream name segment |
| `stream_type` | str | `depth@100ms` | `--stream` | lower-cased, then `[a-z0-9]{1,20}(@[a-z0-9]{1,12})?` | stream name segment |
| `storage_path` | str | `data/raw_l2` | `--output-dir` | non-blank; relative → repo root | roll-file directory (created lazily) |
| `compression` | str | `gzip` | `--compression` | `gzip` \| `none` | container format and file suffix |
| `compression_level` | int | `1` | — | `1..9` | deflate effort (see §5.7) |
| `flush_interval_ms` | int | `250` | `--flush-interval-ms` | `≥ 1` | max record residency in the buffer |
| `max_file_mb` | int | `256` | `--max-file-mb` | `≥ 0` (0 = never) | size rotation threshold |
| `rotate_seconds` | int | `900` | `--rotate-seconds` | `≥ 0` (0 = never) | time rotation threshold |
| `telemetry_interval_s` | float | `10.0` | `--telemetry-interval` | `> 0` | telemetry cadence |
| `latency_warn_ms` | float | `250.0` | `--latency-warn-ms` | `> 0` | breach threshold |
| `reconnect_base_delay_s` | float | `0.5` | — | `≥ 0.001` | first backoff step |
| `reconnect_max_delay_s` | float | `30.0` | — | `≥ reconnect_base_delay_s` | backoff ceiling |
| `max_reconnect_attempts` | int | `0` | — | `≥ 0` (0 = ∞) | give-up policy |
| `ws_base_url` | str | `""` | `--url` | empty, or starts with `ws://` / `wss://` | endpoint override for proxies/VPNs |

Derived properties (never configured directly):

| Property | Derivation | Example |
| --- | --- | --- |
| `endpoint` | `ws_base_url` (trailing `/` stripped) or `EXCHANGE_ENDPOINTS[exchange]` | `wss://fstream.binance.com` |
| `stream_url` | `f"{endpoint}/{symbol}@{stream_type}"` | `wss://fstream.binance.com/ws/btcusdt@depth@100ms` |
| `storage_dir` | `Path(storage_path)`, absolute or resolved against repo root | `/…/ZephyrHFT/data/raw_l2` |
| `file_suffix` | `gzip` → `.ndjson.gz`, else `.ndjson` | `.ndjson.gz` |

### 4.4 Command-line reference

```bash
python3 scripts/ingest_l2.py [flags]
```

| Flag | Argument | Maps to | Default | Purpose |
| --- | --- | --- | --- | --- |
| `--config` | `PATH` | settings file | `config/settings.yaml` | Alternate YAML file |
| `--symbol` | `SYMBOL` | `symbol` | `btcusdt` | Stream symbol, e.g. `btcusdt`, `ethusdt` |
| `--stream` | `TYPE` | `stream_type` | `depth@100ms` | `depth@100ms`, `depth@500ms`, `depth20@100ms` |
| `--url` | `URL` | `ws_base_url` | `""` | Endpoint override (proxy, VPN, testnet, replay server) |
| `--output-dir` | `PATH` | writer directory | `data_ingestion.storage_path` | Roll-file destination |
| `--compression` | `gzip\|none` | `compression` | `gzip` | Container / suffix |
| `--duration` | `SECONDS` | — | `0` (until signal) | Stop after N seconds |
| `--max-file-mb` | `INT` | `max_file_mb` | `256` | Size rotation (`0` = never) |
| `--rotate-seconds` | `INT` | `rotate_seconds` | `900` | Time rotation (`0` = never) |
| `--flush-interval-ms` | `INT` | `flush_interval_ms` | `250` | Buffer flush cadence |
| `--telemetry-interval` | `SECONDS` | `telemetry_interval_s` | `10.0` | Telemetry cadence |
| `--latency-warn-ms` | `MS` | `latency_warn_ms` | `250.0` | Breach threshold |
| `--dry-run` | flag | `dry_run=True` | off | Connect + measure, write nothing |
| `--replay` | `PATH` | `replay()` | — | Ingest a local **plain** NDJSON archive instead of the socket (not `.gz`) |
| `--log-level` | `LEVEL` | logging | `INFO` | `DEBUG`, `INFO`, `WARNING`, … |

Exit codes: `0` success · `1` feed unavailable (e.g. `websockets` missing) ·
`2` invalid configuration · `130` interrupted before handlers were installed.

### 4.5 Recipes

```bash
# 1. Default capture (BTCUSDT per config/settings.yaml) for 30 s
python3 scripts/ingest_l2.py --duration 30

# 2. Another symbol, into the short-horizon roll layout
python3 scripts/ingest_l2.py --symbol ethusdt --duration 3600 --rotate-seconds 300

# 3. Latency probe only: no files, no disk, no fsync
python3 scripts/ingest_l2.py --dry-run --duration 10 --telemetry-interval 1

# 4. Higher-resolution stream (20 levels @ 100 ms snapshots)
python3 scripts/ingest_l2.py --stream depth20@100ms --output-dir data/raw_l2_snap

# 5. Through a proxy / alternate endpoint
python3 scripts/ingest_l2.py --url wss://fstream.binance.com/ws --duration 60

# 6. Offline: push the bundled sample through the exact production path
python3 scripts/ingest_l2.py --replay data/sample_events.ndjson --output-dir /tmp/raw_l2

# 7. Long soak: 900 s rolls, 256 MiB cap, 2 h
python3 scripts/ingest_l2.py --duration 7200 --max-file-mb 256 --rotate-seconds 900

# 8. Uncompressed capture (debugging with grep/tail on live files)
python3 scripts/ingest_l2.py --compression none --duration 60
```

**Symbol selection semantics.** `--symbol` rewrites only the stream name segment; the
directory, storage layout and rotation policy are unaffected, so two symbols captured
simultaneously (two processes) never share a file because the symbol is part of the
filename.

### 4.6 Validation matrix (rejection cases covered by tests)

| Section | Rejected inputs |
| --- | --- |
| `data_ingestion.compression` | `lz4` (anything outside `gzip`, `none`) |
| `data_ingestion.symbol` | `b` (too short), `btc/usdt` (separator) |
| `data_ingestion.stream_type` | `depth @100ms` (whitespace) |
| `data_ingestion.exchange` | `kraken` (not in `EXCHANGE_ENDPOINTS`) |
| `data_ingestion.compression_level` | `0`, `10` |
| `data_ingestion.flush_interval_ms` | `0` |
| `data_ingestion.telemetry_interval_s` | `0` |
| `data_ingestion.latency_warn_ms` | `0` |
| `data_ingestion.reconnect_base_delay_s` | `0` |
| `data_ingestion.reconnect_max_delay_s` | `0.1` (< base delay) |
| `data_ingestion.max_reconnect_attempts` | `-1` |
| `data_ingestion.ws_base_url` | `https://example.test` |
| `data_ingestion.storage_path` | `"  "` (blank) |
| `data_ingestion` (block) | a non-mapping value (e.g. a string or list) |

Every message contains the dotted key, which is why a misconfiguration is diagnosable
from the exit line alone:

```text
$ python3 scripts/ingest_l2.py --compression lz4
usage: ingest_l2.py ... error: argument --compression: invalid choice: 'lz4' ...
$ # and for YAML:
configuration error: 'data_ingestion.symbol' must be 2-20 alphanumerics (e.g. btcusdt): 'btc/usdt'
```

---

## 5. Data Schema & Compression Format

### 5.1 Wire payload — Binance USD-M Futures diff depth

The capture subscribes directly to the raw stream URL
(`wss://fstream.binance.com/ws/btcusdt@depth@100ms`), so every WebSocket message is one
`depthUpdate` event — no combined-stream envelope, no subscription request/ack traffic in
the steady state.

The following frame is **verbatim** from the live validation capture (level arrays
truncated for readability):

```json
{
  "e": "depthUpdate",
  "E": 1792683368185,
  "T": 1792683368182,
  "s": "BTCUSDT",
  "ps": "BTCUSDT",
  "U": 11892910002928,
  "u": 11892910016368,
  "pu": 11892910002749,
  "b": [["1000.00", "120.063"], ["31265.60", "0.216"], ["31365.60", "1.218"], ["...", "..."]],
  "a": [["1000.00", "0.000"], ["31365.60", "0.000"], ["31465.60", "0.366"], ["...", "..."]]
}
```

### 5.2 Field reference

| Field | Type | Present | Meaning | Phase 1 handling |
| --- | --- | --- | --- | --- |
| `e` | string | always | Event type, `"depthUpdate"` | Stored verbatim; not validated (forward compatible) |
| `E` | int (ms) | always | **Event time** — when the matching engine emitted the event | **Primary latency reference**: `latency_ns = recv_ns − E·10⁶` |
| `T` | int (ms) | always (futures) | **Transaction time** — when the transaction was accepted | Fallback latency reference when `E` is missing/unusable |
| `s` | string | always | Symbol, uppercase (`BTCUSDT`) | Stored; the configured `symbol` is lowercase in the URL |
| `ps` | string | futures | Pair symbol (identical to `s` in every observed frame) | Stored verbatim, unused |
| `U` | int | always | First update ID of this event | Sequence-chain start |
| `u` | int | always | Final update ID of this event | Sequence-chain end — must equal the next frame's `pu` |
| `pu` | int | futures | Previous event's `u` | **Gap detector** for Phase 2 (`pu[i] ≠ u[i−1]` ⇒ lost frame) |
| `b` | array | always | Bid updates: `[["price", "qty"], …]`; `qty == "0.000"` ⇒ delete level | Stored verbatim; applied to `L2OrderBook` in Phase 2 |
| `a` | array | always | Ask updates, same encoding as `b` | Stored verbatim; applied to `L2OrderBook` in Phase 2 |
| `st` | int | futures | Stream/status flag, constant `1` in every observed frame | Stored verbatim, not interpreted |
| — | — | — | Numbers are **strings** (`"31265.60"`) to preserve decimal precision | Never converted to `float` in Phase 1 |

> **Note on naming.** Binance names the side arrays `b` and `a`, not `bids`/`asks`, and the
> previous-sequence field `pu`. The archive preserves these names exactly; the
> `bids`/`asks` vocabulary belongs to the Phase 2 **normalised** schema, which is a
> projection of these records — not a replacement for them.

### 5.3 Archive record schema

**One record = one line = the exact bytes the venue sent**, terminated by `\n`.

```text
data/raw_l2/binance_futures_btcusdt_depth_100ms_20261022T153608Z_00001.ndjson.gz
└─ gunzip ─▶ <venue frame #1 bytes>\n<venue frame #2 bytes>\n … <venue frame #N bytes>\n
```

Consequences that matter downstream:

| Property | Detail |
| --- | --- |
| Lossless | No re-serialisation: key order, whitespace, number formatting and unknown fields survive |
| Forward compatible | A new venue field reaches Phase 2 without a Phase 1 code change |
| Self-describing per line | Each line is an independent JSON object; a truncated file still yields valid prefixes |
| Append-only | `bytearray += line.encode() + b"\n"`; existing bytes are never rewritten |
| Exactly one JSON object per line | Frames that are not valid JSON are still written (and counted as `malformed`) |

```python
import gzip, json

path = "data/raw_l2/binance_futures_btcusdt_depth_100ms_20261022T153608Z_00001.ndjson.gz"
with gzip.open(path, "rt", encoding="utf-8") as handle:
    for line in handle:
        update = json.loads(line)
        print(update["u"], update["pu"], len(update["b"]), len(update["a"]))
```

Frame kinds that can reach the archive:

| Frame | Has `E`? | Archived | Counted as |
| --- | --- | --- | --- |
| `depthUpdate` (normal) | yes | ✅ | `frames`, `stored`, latency sample |
| `depthUpdate` (post-reconnect snapshot) | yes | ✅ | identical path; much larger payload |
| Subscription ack / venue error object | no | ✅ | `no_timestamp` (still `stored`) |
| Truncated / non-JSON text | no | ✅ | `malformed` (first 5 logged at WARNING) |

### 5.4 File naming, layout and rotation

```text
<storage_path>/
└── <exchange>_<symbol>_<stream_type>_<UTC-open-time>_<sequence><suffix>

data/raw_l2/                                    # default rotate_seconds: 900 -> 15 min apart
├── binance_futures_btcusdt_depth_100ms_20261022T153608Z_00001.ndjson.gz
├── binance_futures_btcusdt_depth_100ms_20261022T155108Z_00002.ndjson.gz
└── binance_futures_btcusdt_depth_100ms_20261022T160608Z_00003.ndjson.gz
```

| Component | Format | Example | Why |
| --- | --- | --- | --- |
| `exchange` | config key | `binance_futures` | Prevents venue collisions in one directory |
| `symbol` | lower-case | `btcusdt` | One directory can hold many symbols |
| `stream_type` | `@` → `_` | `depth_100ms` | Filesystem-safe, still self-describing |
| open time | `%Y%m%dT%H%M%SZ`, UTC | `20261022T153608Z` | Lexicographic sort == chronological sort |
| sequence | zero-padded 5 digits, per process | `00001` | Guarantees a new name every rotation, even within one second |
| suffix | `.ndjson.gz` / `.ndjson` | `.ndjson.gz` | Compression is visible from `ls` |

Rotation is evaluated inside the writer thread, immediately before a write:

| Rule | Trigger | Config | Notes |
| --- | --- | --- | --- |
| Size | `file_bytes + incoming > max_file_mb·1 MiB` | `max_file_mb` (256) | Only when `incoming > 0` and the file has data |
| Time | `now − opened_at ≥ rotate_seconds` | `rotate_seconds` (900) | Bounds file age even during an outage |
| Sequence | On every open, `sequence += 1` and retry while the path exists | — | Never reuses a name |

At the default 10 frames/s, a roll file reaches ~25 MB in 900 s, so **time rotation
dominates** and the 256 MiB cap acts as a burst guard (a full snapshot storm would hit it
first).

### 5.5 gzip container details

```text
┌────────────────── one roll file = exactly one gzip member ───────────────────────────────────────┐
│ 1F 8B 08 00 00 00 00 00 04 FF    <- header: ID1 ID2 CM FLG MTIME=0 XFL=04(fastest) OS=FF       │
│ <deflate blocks; a Z_SYNC_FLUSH boundary at every flush_interval_ms>                             │
│ 5B 91 7F D9  E2 C4 02 00         <- trailer: CRC32=0xD97F915B, ISIZE=0x0002C4E2 = 181,474 B      │
└──────────────────────────────────────────────────────────────────────────────────────────────────┘
   (bytes above are from the live validation capture; ISIZE equals the decompressed size exactly)
```

| Decision | Implementation | Rationale |
| --- | --- | --- |
| Container | `gzip.GzipFile(fileobj=raw, mode="wb")` | Universal tooling (`gzip`, `gunzip`, `zcat`, `gzip -dc`, Python `gzip.open`) |
| Compression level | `settings.compression_level` (default `1`) | Fastest; see §5.7 |
| Header reproducibility | `mtime=0`, `filename=""` | Identical input ⇒ identical bytes ⇒ stable fixtures and diffable archives |
| Chunk boundary | `handle.flush()` after every chunk (`Z_SYNC_FLUSH`) | A crash loses at most one `flush_interval_ms` window; the file stays readable up to the last flush |
| Trailer | Emitted by `GzipFile.close()` | `CRC32` + `ISIZE` make `gzip -t` a real end-to-end check |
| Durability | `raw.flush()` + `os.fsync(raw.fileno())` on close | Survives a power loss of the *closed* files |
| Single member | One `GzipFile` per roll file | Streaming-safe; readers never need to resync at a member boundary |

> **Operational caveat — the open roll file has no trailer yet.** Until rotation or
> shutdown closes the file, `gzip -t` reports *"unexpected end of file"* and strict readers
> stop at the last flush boundary. This is inherent to streaming gzip, not a corruption.
> Verification therefore targets **closed** roll files (`gzip -t` + line count), and the
> live file is meant to be tailed, not validated.

### 5.6 Why gzip NDJSON for historical replay

The format is chosen for the **consumer** (Phase 2/3 replay loops), not for the producer.
A replay engine wants: sequential streaming reads, no external service, per-record
framing, and the ability to look at a file with `less` during an incident.

| Candidate | Ratio (L2 depth) | Write CPU | Read | Random access | Human-readable | Verdict |
| --- | --- | --- | --- | --- | --- | --- |
| Raw NDJSON | 1.00× | ~0 | fastest | line-based | ✅ | ❌ 3.7× the disk for no gain |
| **gzip NDJSON** | **0.27× (3.7×)** | **~0.02 % of a core** | sequential, 382 MiB/s | none (must stream) | ✅ per record | ✅ **chosen** |
| zstd NDJSON | ~0.22× | low | very fast | none | ✅ (with `zstdcat`) | ⏭ Phase 2 candidate (`P1-D2`): needs a non-stdlib dependency |
| Parquet / Arrow | ~0.20× | high (columnar writer) | fastest for scans | row groups | ❌ | ⏭ Phase 2 derived artefact — a *projection* of the archive |
| Protobuf / custom binary | ~0.25× | low | fast | needs index | ❌ | ❌ drops unknown venue fields; archive would stop being ground truth |
| SQLite / TimescaleDB | — | high | indexed | ✅ | ✅ | ❌ write amplification and operational surface for an append-only capture |

Decisive criteria, in order:

1. **Fidelity** — the archive must remain a byte-exact record of the wire, or Phase 2/3
   can never re-derive a decision from it. Only "keep the original text" satisfies this.
2. **Operational simplicity** — `gzip` is in the Python standard library and on every
   host; no extra daemon, no schema migration, no compaction step.
3. **Predictable replay cost** — sequential decompression of a 25 MB member is
   sub-100 ms; a replay loop over a day of BTCUSDT depth stays single-threaded.
4. **Cost** — 0.67 GB/day at level 1 is negligible; refinement (zstd/Parquet) is a Phase 2
   optimisation with no data loss, because the raw source is preserved.

> **Trade-off accepted:** compression means a roll file cannot be randomly sampled. Phase 2
> will address this with a derived index (line count / `U`→byte-offset per roll file),
> generated offline — never by changing the archive format.

### 5.7 Compression level trade-off (measured)

Measured on the 181,474-byte live capture (177 KiB), `zlib` raw deflate, single core,
CPython 3.9.7, median of 15 runs on a shared Apple-silicon machine. Throughput on a shared
laptop varies up to ±50 % between runs; the **ratios are stable** (they depend only on the
data), the CPU figures are indicative:

| `compression_level` | Output | Ratio | Compression throughput | Storage / day @10 rec/s | CPU @28 KB/s |
| --- | --- | --- | --- | --- | --- |
| **1 (default)** | **47,698 B** | **0.263 (3.80×)** | **170–280 MiB/s** | **~0.64 GB** | **~0.02 %** |
| 3 | 43,001 B | 0.237 (4.22×) | ~95 MiB/s | ~0.58 GB | ~0.03 % |
| 6 | 35,367 B | 0.195 (5.13×) | ~33–38 MiB/s | ~0.48 GB | ~0.08 % |
| 9 | 33,686 B | 0.186 (5.39×) | ~5 MiB/s | ~0.46 GB | ~0.5 % |

Decompression is far cheaper than compression: **382 MiB/s** on the same capture
(0.45 ms for 177 KiB), which is why replay stays interactive.

On-disk gzip output is ~4 % larger than raw-deflate figures (header, per-chunk
`Z_SYNC_FLUSH` boundaries, trailer): the live run wrote 49,454 B for 181,474 B of input,
i.e. **0.272 (3.67×)**.

**Decision:** default `1`. Ingest is the priority: level 9 would save ~28 % of
0.64 GB/day while costing **~35× the compression CPU** (0.02 % → 0.5 % of a core, and
~32 ms rather than ~1 ms of worker-thread hold time per block) — a bad trade for a
capture path whose only job is to keep up. Operators archiving to cold storage can
re-compress offline at level 6–9 without touching the capture path.

### 5.8 Capacity planning

Measured record-size distribution (64 live `BTCUSDT depth@100ms` frames):

| Statistic | Bytes |
| --- | --- |
| Mean | 2,835 |
| p50 | 1,967 |
| p95 | 5,874 |
| Max (post-reconnect snapshot burst) | 24,002 |
| Min | 802 |
| Levels per update (mean / max) | 127 / 1,135 |

Projections at the default `depth@100ms` cadence (≈10 frames/s):

| Metric | Typical | Worst case (every frame at max size) |
| --- | --- | --- |
| Ingest rate | 28 KB/s | 240 KB/s |
| Raw growth | 2.45 GB/day | 20.7 GB/day |
| Compressed growth (0.272) | **0.67 GB/day** | 5.6 GB/day |
| Roll files per day (`rotate_seconds: 900`) | 96 | 96 |
| Size per roll file | ~7 MB | ~60 MB |
| Compressed per month (1 symbol) | **~20 GB** | ~170 GB |
| Peak RAM (buffer, one flush window) | ~7 KB | ~60 KB (6,000× below the 64 MiB high-water mark) |

Rules of thumb:

* **Disk:** provision ~1 GB/day/symbol of compressed headroom plus one month of retention.
* **CPU:** one core can ingest ~3,600× the production rate of one symbol; the pipeline is
  I/O-bound long before it is CPU-bound.
* **Files:** `rotate_seconds: 900` yields ~96 files/day/symbol; raise it to 3,600 for
  fewer, larger files, or lower it to 300 for finer-grained replay windows.
* **`depth@500ms` / `depth20@100ms`** change the frame rate and payload size; re-derive the
  projection from a 10-minute capture rather than assuming these numbers.

### 5.9 Stream-integrity verification

Four independent checks, all executed against the live validation capture:

| # | Check | Command | Expected | Observed |
| --- | --- | --- | --- | --- |
| 1 | gzip container valid | `gzip -t <roll>.gz` | exit 0, no output | ✅ exit 0 |
| 2 | No record lost on flush/rotation | `gzip -dc <roll>.gz \| wc -l` vs telemetry `rec=` | equal | ✅ 64 == 64 |
| 3 | Sequence chain unbroken | `pu[i] == u[i-1]` for all `i ≥ 1` | 0 breaks | ✅ **0 breaks in 64 frames** |
| 4 | Venue cadence preserved | `ΔE` between consecutive frames | ~102 ms | ✅ min 102 / median 102 / max 102 ms |

```bash
# 1 + 2 — container integrity and record count (macOS and Linux safe)
gzip -t  data/raw_l2/*.ndjson.gz
gzip -dc data/raw_l2/*.ndjson.gz | wc -l

# 3 — gap detector: every frame must continue the previous one
python3 - <<'EOF'
import gzip, glob, json
previous = None
breaks = 0
for path in sorted(glob.glob("data/raw_l2/*.ndjson.gz")):
    with gzip.open(path, "rt") as handle:
        for line in handle:
            frame = json.loads(line)
            if previous is not None and frame.get("pu") != previous:
                breaks += 1
            previous = frame.get("u")
print("sequence breaks:", breaks)
EOF
```

> **Platform note.** BSD `zcat` (macOS default) appends `.Z` and cannot read `.gz` files —
> it fails with `can't stat: …ndjson.gz.Z`. Use `gzip -dc`, `gzcat`, or
> `python3 -m gzip -d` instead.

**Replay integrity (byte-level).** `--replay` pushes a local archive through the *same*
`_handle_frame` path and writes a new archive. Comparing the two decompressed byte streams
proves that nothing in the ingestion path mutates payloads:

| Run | Input records | Output records | Verified | Result |
| --- | --- | --- | --- | --- |
| `data/sample_events.ndjson` | 800 | 800 | 800 / 800 byte-identical | ✅ |
| Live capture (gunzipped to NDJSON) | 64 | 64 | `cmp` clean, 64 / 64 lines | ✅ |

```bash
gzip -dc data/raw_l2/<roll>.ndjson.gz > /tmp/live.ndjson          # replay needs plain NDJSON
python3 scripts/ingest_l2.py --replay /tmp/live.ndjson --output-dir /tmp/replay_check
gzip -dc /tmp/replay_check/*.ndjson.gz > /tmp/replay_out.ndjson
cmp /tmp/live.ndjson /tmp/replay_out.ndjson && echo "byte-identical"   # prints: byte-identical
```

---

## 6. Test Strategy & Verification Metrics

### 6.1 Strategy

The suite is written against **observable behaviour of the production path**, not against
private helpers: every test drives `L2IngestClient.consume()` (or `RawL2Writer`) exactly as
the socket would. Frame sources are plain async iterables, so no network is needed and the
tests stay deterministic and fast (< 1 s for the whole repository).

```text
                 +----------------------------------------------+
  Layer 4        | manual / scripted live runs (venue)          |  7 scenarios, §6.4
  (validation)   | live capture - rotation - SIGINT - SIGTERM   |
                 +----------------------+-----------------------+
                 +----------------------+-----------------------+
  Layer 3        | integration: client + writer + real gzip I/O |  tmp_path files, read
  (integration)  | consume() -> roll file -> gunzip -> assert   |  back and compared
                 +----------------------+-----------------------+
                 +----------------------+-----------------------+
  Layer 2        | component: writer, tracker, backoff, config  |  injected clock / RNG,
  (unit)         | injected clock / seeded RNG / fake socket    |  no real time
                 +----------------------+-----------------------+
                 +----------------------+-----------------------+
  Layer 1        | contract: frame parsing, timestamp fallback, |  parametrised edge cases
  (contract)     | invalid-configuration rejection              |
                 +----------------------------------------------+
```

Determinism levers: `clock=` and `rng=` injection, `max_file_bytes=` override, `tmp_path`
isolation, `monkeypatch` for `websockets` import failures, and gzip read-back so
assertions are made on **bytes**, not on in-memory state.

### 6.2 Inventory

| Group | Functions | Cases | What is proven |
| --- | --- | --- | --- |
| Configuration | 11 | 25 | YAML load, repo defaults, overrides, normalisation, 15 rejection paths, non-mapping block, `as_dict` |
| Latency extraction | 5 | 14 | `E` preferred over `T`, combined-stream unwrapping, 10 unusable payloads → `None` |
| Latency statistics | 6 | 8 | nearest-rank percentiles (0.0/0.5/1.0), breach counting, clock-skew separation, window bounds, counter persistence across resets |
| Reconnect backoff | 4 | 4 | doubling, cap, ±25 % jitter envelope, determinism under a seed |
| Roll-file writer | 8 | 8 | directory creation, gzip round-trip, flush-on-close, size/time rotation, plain NDJSON mode, append-after-close rejection, `fsync` + valid trailer, sequence never reused |
| Ingest client | 14 | 14 | end-to-end archive + latency, malformed/untimestamped counting, dry-run, breach flag, stop request, replay, bundled sample, combined-stream frames, binary frames, snapshot, configured URL, disabled ingestion, reconnect ladder + give-up, missing `websockets` abort |
| CLI plumbing | 3 | 3 | overrides applied, defaults preserved, invalid `--compression` rejected |
| **Module total** | **51** | **76** | `scripts/ingest_l2.py` + `data_ingestion` config |

Full repository (Python), re-run at the time of writing:

```console
$ python3 -m pytest
........................................................................ [ 52%]
..................................................................       [100%]
138 passed in 0.83s

$ python3 -m pytest --collect-only -q      # per-file breakdown
tests/python/test_bindings.py: 9
tests/python/test_data_ingestion.py: 76
tests/python/test_kill_switch.py: 28
tests/python/test_optimizer.py: 4
tests/python/test_paper_engine.py: 16
tests/python/test_strategy.py: 5

$ ctest --test-dir build --output-on-failure
1/1 Test #1: hft_tests ........................   Passed    0.03 sec
100% tests passed out of 1
```

| Metric | Value |
| --- | --- |
| Python tests passing | **138 / 138** |
| Ingestion-specific tests | 76 (51 functions) |
| Warnings | 0 |
| Runtime | 0.83 s (whole Python suite) |
| Network required | none |
| C++ core tests | `hft_tests` — all passed |

### 6.3 Invariants under test

| # | Invariant | Test evidence |
| --- | --- | --- |
| I1 | Every received frame is archived, byte-for-byte | `test_client_consumes_frames_measures_latency_and_archives` (reads the gzip back) |
| I2 | Nothing stays buffered forever: shutdown flushes the tail | `test_writer_aclose_flushes_pending_records` |
| I3 | A closed archive is a valid gzip member with a trailer | `test_writer_fsyncs_and_leaves_a_valid_gzip_trailer` |
| I4 | Rotation never loses or duplicates a record | `test_writer_rotates_by_size`, `test_writer_rotates_by_time` |
| I5 | A roll-file name is never reused | `test_writer_sequence_never_reuses_a_file_name` |
| I6 | Latency never mixes with clock skew | `test_latency_tracker_counts_clock_skew_separately` |
| I7 | Percentiles are per window, counters are cumulative | `test_latency_summary_reset_keeps_cumulative_counters` |
| I8 | Retries are bounded and give up when configured | `test_client_reconnects_with_backoff_and_gives_up` |
| I9 | A missing dependency aborts instead of retrying forever | `test_missing_websockets_aborts_instead_of_reconnecting` |
| I10 | Bad configuration fails before any connection is attempted | `test_invalid_values_are_rejected` (15 cases) |
| I11 | `--dry-run` writes nothing, anywhere | `test_client_dry_run_writes_nothing` |
| I12 | Live, replay and sample input share one code path | `test_client_replay_archives_a_local_archive`, `test_client_accepts_the_bundled_sample_archive` |

**Defects the suite caught during development** (kept as regression tests):

| Defect | Symptom | Fix |
| --- | --- | --- |
| `NameError` on the give-up path | `max_reconnect_attempts` reached ⇒ crash instead of a clean exit | bind the variable before use; covered by `test_client_reconnects_with_backoff_and_gives_up` |
| Async-generator leak | `Task was destroyed but it is pending` at loop shutdown when `consume()` raised | replaced the generator source with the `_SocketFrames` class + explicit `aclose()` |
| Unsafe writer cancellation | `task.cancel()` mid-write could leave a dangling `wait_for` inner task | cooperative writer stop (`_closed` + wake event), then join |

### 6.4 Real-world validation runs

Executed against the live Binance USD-M Futures feed on 2026-10-22 (UTC+7 host):

| # | Scenario | Command (abridged) | Expected | Observed |
| --- | --- | --- | --- | --- |
| V1 | Live capture | `--duration 7` | frames archived, one valid roll file on close | **64 frames / 8.2 s (7.8 fps)**; 181,474 B → 49,454 B (**ratio 0.27**); `gzip -t` OK |
| V2 | Time rotation | `--duration 7 --rotate-seconds 2` | ≥ 2 files, zero records lost | **4 files**, 21 + 20 + 21 + 3 = **65 records** (matches `rec=65`), all `gzip -t` OK, sequences `00001`–`00004` |
| V3 | `SIGTERM` while streaming | start, `kill -TERM` after 4 s | drain → trailer → `fsync`, clean exit | **251 records**, archive valid, `FINAL` line printed, process gone |
| V4 | `SIGINT` (Ctrl-C) | interactive | same as V3 | ✅ verified interactively; **not reproducible from a backgrounded shell** — see the caveat below |
| V5 | Offline replay | `--replay live.ndjson` | byte-identical re-archive | 800/800 and 64/64 records; `cmp` clean |
| V6 | Dry run | `--dry-run --duration 10` | no directory, no file | ✅ nothing written, latency still reported |
| V7 | Sequence chain | gap detector over V1 archive | 0 breaks | ✅ **0 breaks / 64 frames**, `ΔE` = 102 ms min = median = max |

Latency observed from a residential connection (indicative only; not a service-level figure):

| Run | p50 | p95 | p99 | max |
| --- | --- | --- | --- | --- |
| V1 (7.8 fps, 8.2 s) | 30.14 ms | 99.59 ms | 113.41 ms | 115.19 ms |
| V2 (4 roll files) | 37.40 ms | 106.99 ms | 120.19 ms | 120.19 ms |
| V3 (SIGTERM run, 27 s) | 36.60 ms | 98.87 ms | 116.61 ms | 289.99 ms |

> **Caveat — `SIGINT` from an automated harness.** POSIX requires a non-interactive shell
> to start background ("asynchronous list") jobs with `SIGINT` set to `SIG_IGN`. A capture
> launched as `… &` therefore inherits an ignored `SIGINT` and `kill -INT` has no effect —
> verified with a probe (`inherited: Handlers.SIG_IGN`). This is shell behaviour, not a
> defect: with the normal disposition the loop handler receives the signal
> (`RESULT: SIGINT delivered`). **Automation must stop the capture with `SIGTERM`;**
> `SIGINT` is for interactive use.

### 6.5 Acceptance criteria

| ID | Criterion | Status |
| --- | --- | --- |
| AC-1 | Subscribes to the configured stream and streams continuously without manual intervention | ✅ (verified runs of 8 s–27 s; multi-hour soak scheduled in Phase 2) |
| AC-2 | Archives every received frame, byte-exactly, with no drop path | ✅ (V1, V5) |
| AC-3 | Rotates on size **or** time without loss or duplicate records | ✅ (V2) |
| AC-4 | Reports p50/p95/p99/max latency and a breach counter per interval | ✅ (V1–V3) |
| AC-5 | Cleans up on `SIGTERM`/`SIGINT`: valid gzip, zero buffered records | ✅ (V3, V4) |
| AC-6 | Configuration errors fail fast with an actionable message | ✅ (`ConfigError`, exit 2) |
| AC-7 | Transient network loss recovers automatically with bounded backoff | ✅ (unit: ladder + give-up; live: reconnects observed) |
| AC-8 | Replay path is byte-identical to the capture path | ✅ (V5) |
| AC-9 | 100 % of tests green, no warnings | ✅ 138/138, 0 warnings |

---

## 7. Performance Characteristics & Capacity Planning

### 7.1 Measured microbenchmarks

Method: replay the recorded payloads through the **production** `_handle_frame` path
(parse + archive append + latency accounting) with the writer attached but not started, so
the buffer grows in RAM and no disk noise enters the measurement. Apple silicon,
CPython 3.9.7, single core.

| Workload | Frames | Cost/frame | Throughput | Notes |
| --- | --- | --- | --- | --- |
| Real venue frames (mean 2.8 KiB) | 40,000 | **19–50 µs (median 27.6 µs)** | **20,000–53,000 frames/s** | Representative of production |
| Small synthetic frames (271 B) | 40,000 | 5.6 µs | 178,000 frames/s | Lower bound (parse-dominated) |
| Deflate level 1 (177 KiB block) | 15 runs | 0.95–1.04 ms | 170–280 MiB/s | Worker thread only |
| Deflate level 6 (177 KiB block) | 15 runs | 4.5–5.3 ms | 33–38 MiB/s | 5× slower, 26 % smaller |
| gzip decompress (177 KiB block) | 20 runs | 0.45 ms | 382 MiB/s | Phase 2 replay cost |

### 7.2 Headroom

```text
  capacity (receive path)      20k–53k frames/s   ████████████████████████████████████████
  production load                    10 frames/s   ▏
  margin                          ~2,000–5,300×
```

At the observed payload mix, one core spends **~0.03 %** of its time on the receive path
(10 × 27.6 µs per second) and **~0.02 %** on compression. The pipeline is I/O- and
network-bound by three orders of magnitude; that headroom is what makes it safe to run the
capture alongside the research stack on a workstation.

Where the remaining CPU goes is *not* the capture: telemetry formatting once per interval,
Python's event-loop bookkeeping, and TLS decryption inside `websockets`.

### 7.2 Resource footprint (measured, one symbol)

| Resource | Observed | Notes |
| --- | --- | --- |
| CPU (total process) | **0.37 s CPU per 16.4 s wall ⇒ ~2.3 % of one core** | `user 0.28 s + sys 0.09 s`, includes interpreter start, TLS, JSON, gzip, telemetry |
| RSS | **~35 MB** | flat while streaming; grows only on disk backpressure |
| Disk (logical writes) | ~7 KB/s compressed | 2.45 GB/day raw → 0.67 GB/day gzip |
| Disk (`fsync`) | once per roll-file close | 96 events/day at default rotation |
| Network | ~28 KB/s inbound | plus TLS/ping overhead |

### 7.3 What is deliberately not optimised

| Not done | Why | Would matter at |
| --- | --- | --- |
| `orjson` / `ujson` for parsing | Parse is 0.03 % of a core; adds a binary dependency to a data-integrity tool | > 1,000 frames/s |
| Binary/protobuf archive | Loses venue fields; Phase 2 can derive it | Never for the raw tier |
| `mmap` / O_DIRECT writes | Complexity without a measured bottleneck | > 100 MB/s sustained |
| Multiprocess fan-out | One process per symbol is simpler and already idles at ~0 % | > 8 symbols on one host |
| Kernel bypass / FPGA capture | Out of scope for a research data plane | Phase 5+ (production trading) |

### 7.4 Reading the latency distribution

The observed p99/p50 ratio (≈3–8×) is dominated by the public internet path, not by the
process: the same host reports `lat_ms` in the tens of milliseconds whether the capture is
idle or busy. One 10 s window on the same host reported `p50=37.63 ms p95=920.68 ms
p99=1028.03 ms` — a path-level congestion event, with `lat_breaches` as the only cumulative
counter that moved. To separate host effects from network effects:

1. Run `--dry-run` for 60 s and record p50/p99 (no disk, no compression).
2. Run a full capture with the same duration and compare — a materially worse p99 in step
   2 means storage/CPU interference, not network.
3. Correlate `lat_breaches` with `files=` (rotation) events: if breaches cluster at roll
   boundaries on a slow disk, revisit `flush_interval_ms` / compression level.

---

## 8. Operational Runbook

### 8.1 Start, supervise, stop

```bash
# Interactive: Ctrl-C (SIGINT) stops cleanly
python3 scripts/ingest_l2.py --duration 3600

# Supervisor / CI / cron: always stop with SIGTERM (see §6.4 caveat)
nohup python3 scripts/ingest_l2.py --duration 0 \
      --output-dir /data/raw_l2 > /var/log/zephyr/ingest.log 2>&1 &
echo $! > /run/zephyr-ingest.pid
kill -TERM "$(cat /run/zephyr-ingest.pid)"
```

```ini
# systemd unit (sketch)
[Unit]
Description=ZephyrHFT L2 capture
After=network-online.target chronyd.service

[Service]
WorkingDirectory=/opt/zephyrhft
ExecStart=/usr/bin/python3 scripts/ingest_l2.py
KillSignal=SIGTERM        # lets the writer flush and fsync
TimeoutStopSec=30         # generous: one final chunk + fsync
Restart=always
RestartSec=5
User=zephyr

[Install]
WantedBy=multi-user.target
```

Operational rules:

| Rule | Reason |
| --- | --- |
| Stop with `SIGTERM`, not `SIGKILL` | `SIGKILL` skips the gzip trailer → the roll file needs `gzip -dc … \|\| true` to salvage |
| Allow ≥ 10 s between stop and restart | The next process reuses the same directory; a UTC second plus sequence keeps names distinct |
| Never run two captures into the same `storage_path` for the same symbol | Both would rotate independently and interleave sequence numbers |
| Fix the clock before drawing latency conclusions | `chronyd`/`ntpd`; check `clock_skew` in telemetry |
| Keep `data/raw_l2` on a local SSD | Network filesystems make `fsync` unbounded and can trip the 64 MiB high-water mark |

### 8.2 Telemetry reference

One line per `telemetry_interval_s` (`INFO`), plus `FINAL` at exit:

```text
INFO zephyr.ingest_l2: telemetry frames=191 rate=9.5/s lat_ms(p50=36.74 p95=136.21 p99=289.99 max=289.99) lat_breaches=1 files=1 rec=191 buf=0 out=0.20MiB ratio=0.27
INFO zephyr.ingest_l2: FINAL frames=251 rate=9.3/s lat_ms(p50=36.60 p95=98.87 p99=116.61 max=289.99) lat_breaches=1 files=1 rec=251 buf=0 out=0.26MiB ratio=0.27
```

(Both lines are verbatim from the same 27 s SIGTERM validation run: the periodic line was
emitted at 22:42:25, the `FINAL` line at 22:42:33. Percentiles are per-interval, so they
legitimately differ; counters such as `lat_breaches`, `rec` and `files` only accumulate.)

| Field | Meaning | Healthy value | Escalate when |
| --- | --- | --- | --- |
| `frames` | Frames received this session | grows ~10/s per symbol | flat while connected |
| `rate` | `frames / elapsed_s` | 8–10/s (`depth@100ms`) | < 5/s sustained (venue or network issue) |
| `lat_ms(p50/p95/p99/max)` | Feed latency percentiles for the window | p50 < 100 ms from a residential link | p99 > 1 s, or p50 climbing |
| `lat_breaches` | Samples above `latency_warn_ms` | 0 | Growing steadily |
| `clock_skew` | Negative latencies (venue clock ahead) | 0 | > 0 ⇒ fix time sync |
| `no_timestamp` | Frames without `E`/`T` (acks, errors) | 0 | > 0 mid-session ⇒ venue protocol change |
| `malformed` | Frames that failed `json.loads` | 0 | > 0 ⇒ wire corruption or wrong `--stream` |
| `reconnects` / `fail` | Retry cycles / never-established connections | 0 / 0 | Continuous growth ⇒ endpoint or network down |
| `files` | Roll files opened this session | `elapsed / rotate_seconds` | Far fewer ⇒ rotation misconfigured |
| `rec` | Records written | equals `frames` (minus buffer) | `rec ≪ frames` ⇒ writer starvation |
| `buf` | Records currently buffered | 0–1 flush window | Rising monotonically ⇒ disk too slow |
| `out` | Bytes on disk (compressed) | grows linearly | Flat while `rec` grows ⇒ write errors |
| `ratio` | `compressed / uncompressed` | 0.20–0.35 | > 0.6 ⇒ payload changed or wrong level |
| `write_errors` | Failed `_write` calls | absent (0) | Any non-zero ⇒ check `df -h`, `dmesg` |

Shutdown summary table (stdout, after `FINAL`):

```text
==========================================================================
  Stream        : wss://fstream.binance.com/ws/btcusdt@depth@100ms
  Frames        : 64  (7.8/s over 8.2s)
  Archived      : 64 frames  (0.17 MiB received)
  Roll files    : 1  (64 records, 0.05 MiB on disk)
  Storage       : /tmp/phase1doc
  Feed latency  : p50=30.14 ms  p95=99.59 ms  p99=113.41 ms  max=115.19 ms
==========================================================================
```

### 8.3 Daily verification checklist

```bash
# 1. Every closed roll file is a valid gzip member
gzip -t data/raw_l2/*.ndjson.gz

# 2. Record counts add up across the day
gzip -dc data/raw_l2/*.ndjson.gz | wc -l

# 3. No gaps in the venue sequence chain (§5.9 script)
#    expected on a clean day: sequence breaks: 0

# 4. Disk headroom (≈1 GB/day/symbol compressed)
du -sh data/raw_l2 && df -h data/raw_l2

# 5. Restore drill: replay yesterday's first roll file
gzip -dc data/raw_l2/<first>.ndjson.gz > /tmp/probe.ndjson
python3 scripts/ingest_l2.py --replay /tmp/probe.ndjson --output-dir /tmp/probe_out
```

### 8.4 Troubleshooting

| Symptom | Root cause | Action |
| --- | --- | --- |
| Exit code `1`, `FeedUnavailable: the 'websockets' package is required for live ingestion (pip install 'websockets>=12,<16')` | Dependency missing for this interpreter | `python3 -m pip install 'websockets>=12,<16'` |
| Exit code `2`, `configuration error: 'data_ingestion.x' …` | Invalid YAML value | Fix the named key; see §4.6 |
| `gzip: unexpected end of file` on the **newest** file | File is still open: no trailer yet | Expected. Verify only closed roll files |
| `reconnects` climbing, `rate` ≈ 0 | Endpoint/geo restriction (HTTP 451), DNS, TLS, proxy | Try `--url`, check `curl -I https://fstream.binance.com`, log at `DEBUG` |
| `clock_skew` > 0 | Host clock behind the venue | `chronyc tracking`; restart the capture after re-sync |
| `p99` > 1 s while `p50` is normal | Network jitter / ISP path | Correlate with `reconnects`; use a lower-latency host |
| `buf` rising, `out` flat | Disk cannot keep up (`fsync` stall, full disk) | `df -h`, `iostat`; free space or move `storage_path` to an SSD |
| `write_errors` > 0 | `ENOSPC` / `EIO` / permissions | Fix storage; ingestion continues (frames are still counted) |
| `malformed` > 0 | Wrong stream type, or a proxy rewriting frames | Check `--stream`; capture with `--log-level DEBUG` |
| `no_timestamp` > 0 | Subscription acks / venue error frames | Usually harmless; inspect the raw line if unexpected |
| `ratio` ≈ 1.0 | `--compression none` in effect | Check the startup line: `(gzip)` vs `(no storage)` |
| Empty roll file after an outage | Time rotation fired on an idle tick | Expected; harmless, safe to delete |
| `UnicodeDecodeError` when using `--replay` with `.gz` | `--replay` reads plain text only | `gzip -dc file.gz > file.ndjson` first (`P1-D3`) |
| `KeyboardInterrupt` traceback in a supervisor log | Process run without the loop handlers installed (early exit) | Exit code `130` is explicit; treat as an interrupted run |

### 8.5 Log-message catalogue

| Level | Message | Emitted when |
| --- | --- | --- |
| INFO | `ingest <url> -> <dir> (gzip)` | Startup, after settings resolution |
| INFO | `connected: <url>` | Handshake complete |
| INFO | `roll file open: <path>` | New roll file (also at start) |
| INFO | `roll file closed: <path> (N KiB uncompressed)` | Rotation or shutdown |
| INFO | `telemetry …` / `FINAL …` | Every `telemetry_interval_s` / at exit |
| INFO | `reconnecting in X.XXs (attempt N)` | Backoff scheduled |
| INFO | `shutdown requested; draining and closing the capture` | First `SIGINT`/`SIGTERM` |
| INFO | `duration reached; shutting down` | `--duration` elapsed |
| WARNING | `feed latency X ms > Y ms threshold` | Sample above `latency_warn_ms` (max 1/s) |
| WARNING | `connection closed after N frames` | Socket lost without an exception |
| WARNING | `write buffer at N.N MiB: disk slower than the feed (records are retained, never dropped)` | High-water mark crossed (once) |
| WARNING | `malformed frame #N: …` | First 5 unparseable frames |
| WARNING | `data_ingestion.enabled is false: exiting without capture` | `enabled: false` |
| ERROR | `giving up after N reconnect attempts` | `max_reconnect_attempts` exhausted |
| ERROR | `roll-file write failed; N bytes were in the chunk` | Worker-thread write exception (with traceback) |
| ERROR | `failed to finalise gzip stream` | `GzipFile.close()` failed during shutdown |
| ERROR | `the 'websockets' package is required for live ingestion (pip install 'websockets>=12,<16')` | `FeedUnavailable` |

---

## 9. Known Limitations & Risk Register

Phase 1 was scoped to produce **trustworthy raw data**, not a complete data platform. The
following limitations are known, documented and either accepted or scheduled.

### 9.1 Archive carries no local receive timestamp

**What.** The roll file stores the venue frame verbatim. The receive timestamp
(`recv_ns`) is used for telemetry and then discarded; it is **not** persisted per record.

**Why it matters.** Replay can reconstruct the market *as the venue saw it* (`E`, `T`,
`pu`, `u`), but cannot reconstruct *how late we saw it*. Any Phase 3 study of "what could
we have known at time t" needs a host-side arrival time.

**Why it was chosen.** Writing an envelope (`{"recv_ns": …, "payload": …}`) would break
byte-fidelity with the venue, triple the per-line parsing cost for consumers, and make the
archive un-greppable with `jq`/`less`. Raw fidelity was prioritised for Phase 1.

**Options for Phase 2 (decision `P1-D1`):**

| Option | Shape | Pros | Cons |
| --- | --- | --- | --- |
| A. Sidecar index | `*.recvns.bin` (8 B little-endian per record) | Zero change to the archive; optional; O(1) offset lookup | Two files to keep in sync |
| B. Opt-in envelope | `--record-recv-ts` writes `{"t":…,"d":<raw>}` | Self-contained | Breaks raw-fidelity mode; doubles quoting cost |
| C. Header comment | A single metadata line per roll file | Cheap | Only start-time resolution |

Recommendation: **A** — it keeps the raw archive immutable and adds the missing dimension.

### 9.2 `--replay` reads plain NDJSON only

`_file_frames()` opens the path in text mode, so a `.ndjson.gz` argument raises
`UnicodeDecodeError`. Workaround: `gzip -dc` first (documented in §5.9). Fix scheduled as
`P1-D3`: detect the gzip magic bytes and wrap with `gzip.open()`.

### 9.3 The open roll file has no gzip trailer

Inherent to streaming gzip (§5.5). Consequences: `gzip -t` fails on the live file; a
reader that requires a valid member must wait for rotation or shutdown. Salvage after
`SIGKILL`/power loss: `gzip -dc file.gz > file.ndjson` (partial output up to the last
flush) — at most one `flush_interval_ms` window is lost.

### 9.4 One subscription per process

No multiplexing: N symbols ⇒ N processes (measured **~35 MB RSS** per process while
streaming 242 records). Acceptable for research (1–4 symbols); revisit if Phase 3 needs a
full-market universe.

### 9.5 Deltas only — no book reconstruction

The archive contains incremental updates; there is no periodic snapshot and no
reconstruction. A consumer must start from a REST snapshot and apply deltas, treating a
`pu` mismatch as a gap (reconnect and re-snapshot). Implemented in Phase 2.

### 9.6 Latency resolution and clock dependence

`E` has millisecond resolution, so sub-millisecond jitter is not measurable. Wall-clock
`time.time_ns()` is required to compare against the venue, which means a clock step appears
as a latency outlier (counted in `clock_skew` when negative). Absolute accuracy is bounded
by host clock discipline (typically < 1 ms with `chronyd`).

### 9.7 Durability window

`fsync` runs per roll-file close, not per flush. A power loss can lose up to one
`flush_interval_ms` window of records plus any open file's trailer. Tighten with
`rotate_seconds: 60` on critical captures (more, smaller files).

### 9.8 No disk-space guard

On `ENOSPC` the writer logs, increments `write_errors`, and **keeps streaming into RAM**
(true to the zero-drop policy). Without external monitoring this becomes an OOM. Mitigate
with a disk-usage alert on `storage_path` and a retention job.

### 9.9 Frame-size ceiling is inherited from `websockets`

`max_size` is left at the library default (1 MiB). A single frame larger than that closes
the connection (code 1009) and triggers a reconnect loop. Observed maximum is 24 KB, so
there is ~40× headroom; if Binance ever ships larger snapshots, set `max_size=None`
explicitly.

### 9.10 Configuration footguns

`enabled` is coerced with `bool()`, so the YAML string `"false"` would evaluate to `True`.
Always use unquoted YAML booleans. `symbol`/`stream_type` are silently lower-cased, which
is intentional (the venue requires lowercase stream names).

### 9.11 Risk register

| ID | Risk | Impact | Likelihood | Mitigation | Phase |
| --- | --- | --- | --- | --- | --- |
| R1 | Reconnect silently drops a delta | Research uses a broken book | Medium | `pu` chain detector (§5.9); re-snapshot on break | 2 |
| R2 | Host clock drift corrupts latency stats | Wrong venue comparisons | Medium | `chronyd` + `clock_skew` telemetry (§8.2) | 1 (done) |
| R3 | Disk full during capture | Records pile in RAM; OOM | Low | `write_errors` telemetry, disk alerts, retention | Ops |
| R4 | Venue schema change (new/renamed fields) | Consumer breakage | Medium | Verbatim archival + `malformed`/`no_timestamp` counters | 2 |
| R5 | Geo-restriction / API change blocks the endpoint | Capture stops | Medium | `--url` override; `reconnects`/`fail` counters | 1 (done) |
| R6 | No local receive timestamps | Limits execution-realistic backtests | High | `P1-D1` sidecar index | 2 |
| R7 | Single-symbol processes exhaust ports/RAM | Scaling ceiling | Low | Documented; fan-out design in Phase 2+ | 3 |
| R8 | gzip reader assumes whole-file availability | Partial reads during live capture | Low | Documented (§5.5/§9.3) | 1 (done) |

---

## 10. Phase 2+ Handoff Roadmap

### 10.1 What Phase 1 hands over (the contract)

Phase 2 may rely on exactly these guarantees; anything else must be re-verified.

| Contract | Guarantee | Consumer implication |
| --- | --- | --- |
| C1 | Every line of every `.ndjson.gz` is one venue frame, verbatim | Parse directly; no envelope to strip |
| C2 | Ordering within a file is arrival order | A monotonic `E` sequence, except across a reconnect |
| C3 | `pu[i] == u[i-1]` unless frames were lost | Gap detector is free (`P1` verified 0 breaks) |
| C4 | Roll-file names sort chronologically | `sorted(glob(...))` is a valid timeline |
| C5 | Closed files are valid gzip members | `gzip.open()` / `gzip -dc` without special flags |
| C6 | Up to one `flush_interval_ms` window may be missing after a crash | Tolerate a short tail gap |
| C7 | No local receive time in the archive | Add `P1-D1` sidecar if needed (§9.1) |

### 10.2 Phase 2 — Normalisation, replay, features (next)

| # | Deliverable | Depends on | Notes |
| --- | --- | --- | --- |
| 1 | `L2OrderBook` reconstruction from deltas + REST snapshot bootstrap | C3 | Reuse `cpp_engine/include/orderbook.hpp` (`L2OrderBook`) through `python_analytics.native.hft_native` |
| 2 | Gap detector / session splitter | C3 | Emits `(file, line, reason)` for every `pu` break |
| 3 | Normalised columnar writer (Parquet/Arrow) | C1 | Derived artefact; raw tier stays the source of truth |
| 4 | Replay reader with gzip support | C2, C4, C5 | Extend `python_analytics/backtesting/replay.py::iter_jsonl` to auto-detect `.gz` |
| 5 | Feature module (OFI, micro-price, spread, depth slope) | 1, 4 | New `python_analytics/features/`, following `strategies/` + `simulator/` conventions |
| 6 | `--replay` gzip support (`P1-D3`) and receive-timestamp sidecar (`P1-D1`) | — | Small, high-leverage fixes |
| 7 | Multi-symbol capture (N processes → 1 supervisor) | — | Process-per-symbol stays the unit of isolation |
| 8 | Retention/compaction job (hot days → cold storage) | — | Raise `compression_level` offline |

### 10.3 Phase 3 — Research & backtesting

* Event-driven backtester consuming Phase 2 features; wires into the existing
  `python_analytics` strategy/optimizer stack (`test_strategy.py`, `test_optimizer.py`).
* Execution-realism studies (queue position, latency sensitivity) — the reason `P1-D1`
  matters.
* Parameter sweeps over months of captured L2 data.

### 10.4 Phase 4 — Paper / live trading

* The Avellaneda–Stoikov market maker on the live book (`test_paper_engine.py`).
* `RiskGuard` / kill-switch integration (`test_kill_switch.py`).
* Phase 1's capture runs alongside as the audit trail.

### 10.5 Decision log

| ID | Decision | Rationale | Revisit |
| --- | --- | --- | --- |
| `P1-D1` | Do **not** persist receive timestamps in the archive | Byte-fidelity and greppability win for Phase 1 | Phase 2 (sidecar, §9.1) |
| `P1-D2` | gzip over zstd/Parquet for the raw tier | stdlib, zero dependency, 3.7× is enough | Phase 2 (offline re-compression) |
| `P1-D3` | `--replay` accepts plain NDJSON only | Simplest reader; gunzip is one command | Phase 2 (auto-detect magic) |
| `P1-D4` | `compression_level: 1` default | Ingest priority; storage is cheap | If storage cost dominates |
| `P1-D5` | Backoff left running forever (`max_reconnect_attempts: 0`) | A research host should keep trying; operators can bound it | Never for capture |
| `P1-D6` | Writer in a worker thread, not a separate process | No IPC, no serialisation, RAM-bounded | If > 500 MB/s needed |

---

## 11. Appendix

### 11.1 Glossary

| Term | Meaning |
| --- | --- |
| **L2 / depth update** | Incremental change to price levels: `[price, quantity]` pairs per side; `qty = 0` deletes the level |
| **Diff depth stream** | Binance stream that pushes *changes* (`depth@100ms`), not snapshots |
| **`E` / `T`** | Venue event time / transaction time (ms). `E` is the latency reference |
| **`U` / `u` / `pu`** | First update ID of an event / final update ID / previous event's final ID (gap detector) |
| **Feed latency** | `recv_ns − E·10⁶`: venue-to-host transit time |
| **Clock skew** | Negative feed latency, i.e. the venue clock is ahead of the host clock |
| **Roll file** | One rotation-bounded archive file (`.ndjson.gz`) |
| **Flush window** | `flush_interval_ms`: maximum time a record stays in RAM |
| **High-water mark** | 64 MiB buffer threshold that raises a single `WARNING` (no data is dropped) |
| **`Z_SYNC_FLUSH`** | Deflate boundary that makes all preceding bytes readable without closing the stream |
| **Gzip trailer** | `CRC32` + `ISIZE`, written on close; required for `gzip -t` to succeed |
| **Backoff ladder** | `0.5 s · 2ⁿ` capped at 30 s, ±25 % jitter, reset after a 5 s healthy session |
| **Nearest-rank percentile** | `rank = ceil(q·n) − 1`: always reports an observed sample, never an interpolation |
| **Dry run** | Connect + measure, write nothing (`--dry-run`) |
| **Replay** | Re-ingest a local NDJSON archive through the production path (`--replay`) |

### 11.2 File map

| Path | Role | Lines |
| --- | --- | --- |
| `scripts/ingest_l2.py` | The capture agent: socket, hot path, latency, writer, CLI | 1,102 |
| `python_analytics/utils/config.py` | `DataIngestionSettings`, `EXCHANGE_ENDPOINTS`, validators, `load_settings`, `as_dict` | +210 in PR #1 |
| `python_analytics/utils/__init__.py` | Re-exports (`load_settings`, `configure_logging`, …) | +2 |
| `config/settings.yaml` | `data_ingestion` block (16 keys) | +21 |
| `tests/python/test_data_ingestion.py` | 76 cases (51 functions) for the module and its config | 743 |
| `data/sample_events.ndjson` | 800 synthetic `depthUpdate` frames for offline replay | fixture |
| `data/raw_l2/` | Capture output directory (git-ignored via `data/`) | runtime |
| `README.md` | User-facing Phase 1 section (mirrors this document) | +113 |
| `requirements.txt` | `websockets>=12,<16` | +1 |

### 11.3 Reproduce the numbers

```python
"""Phase 1 measurements: receive-path cost, compression trade-off, capacity.

Save as ``measure_phase1.py`` in the repository root, then run:

    python3 measure_phase1.py                                # newest data/raw_l2 capture
    python3 measure_phase1.py 'data/raw_l2/*.ndjson.gz'
"""
import glob
import gzip
import statistics
import sys
import tempfile
import time
import zlib
from pathlib import Path

# Make the package importable no matter where the script was saved.
for candidate in (Path(__file__).resolve().parent, Path.cwd()):
    if (candidate / "python_analytics").is_dir():
        sys.path.insert(0, str(candidate))
        break

from python_analytics.utils.config import DataIngestionSettings  # noqa: E402
from scripts.ingest_l2 import L2IngestClient  # noqa: E402

pattern = sys.argv[1] if len(sys.argv) > 1 else "data/raw_l2/*.ndjson.gz"
matches = sorted(glob.glob(pattern))
if not matches:
    raise SystemExit(f"no capture matches {pattern!r}: run a capture first (see §4.5)")

capture = matches[0]
lines = gzip.open(capture, "rt").read().splitlines()
sizes = sorted(len(line.encode()) for line in lines)
print(f"records={len(lines)} mean={statistics.mean(sizes):.0f}B "
      f"p50={sizes[len(sizes) // 2]}B p95={sizes[int(0.95 * len(sizes))]}B max={sizes[-1]}B")

client = L2IngestClient(DataIngestionSettings(), directory=Path(tempfile.mkdtemp()))
now, count = time.time_ns(), 0
client._handle_frame(now, lines[0])                      # warm-up
start = time.perf_counter()
for _ in range(500):
    for line in lines:
        client._handle_frame(now, line)
        count += 1
elapsed = time.perf_counter() - start
print(f"receive path: {elapsed / count * 1e6:.2f} us/frame -> {count / elapsed:,.0f} frames/s")

raw = gzip.open(capture, "rb").read()
for level in (1, 6, 9):
    timings = []
    for _ in range(30):
        t0 = time.perf_counter()
        compressor = zlib.compressobj(level, zlib.DEFLATED, -15)
        output = compressor.compress(raw) + compressor.flush()
        timings.append(time.perf_counter() - t0)
    median = statistics.median(timings)
    print(f"level {level}: {len(output)}B ratio={len(output) / len(raw):.3f} "
          f"-> {len(raw) / median / 1048576:.0f} MiB/s")
```

Run it after any capture to re-derive §5.7, §5.8 and §7.1 on your own hardware.

### 11.4 Document history

| Date | Author | Change |
| --- | --- | --- |
| 2026-10-22 | Data Platform | Initial Phase 1 reference; numbers re-measured against `main` @ `57ba3a5` (live capture, rotation, SIGTERM, replay, benchmarks) |

### 11.5 Notes on the original brief

Written down so future readers can reconcile the ticket wording with the code:

| Brief said | Implementation reality | Why |
| --- | --- | --- |
| "pydantic/yaml configuration" | **PyYAML + frozen dataclasses** (`DataIngestionSettings`) | No third-party runtime dependency; immutability and `dataclasses.replace` give the same guarantees for this shape |
| "`u`, `U`, `pu`, `bids`, `asks`" | Wire fields are `u`, `U`, `pu`, **`b`, `a`** (+ `e`, `E`, `T`, `s`, `ps`, `st`) | `bids`/`asks` is the *normalised* Phase 2 vocabulary; the archive uses the venue's names |
| "timestamp telemetry" persisted per frame | Receive timestamps drive telemetry only; the archive is verbatim | Documented as `P1-D1` with a Phase 2 sidecar remedy (§9.1) |
| "Python 3.11+" | Verified on **CPython 3.9.7**; 3.11/3.12-compatible source | `from __future__ import annotations`, no 3.10+ syntax; `websockets>=12,<16` |
| "p50/p99 latency" | Implemented as **p50/p95/p99/max** + breach + clock-skew counters | p95 is the useful early-warning tail between p50 and p99 |

---

*Document: `docs/phase1_roadmap.md` — Phase 1, ZephyrHFT. Numbers verified against
`main` @ `57ba3a5` on 2026-10-22; re-run §11.3 to refresh them.*
