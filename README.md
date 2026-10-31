# HFT Engine

A low-latency limit-order-book and trading-engine core written in C++20, with a
pybind11 extension and a Python layer for strategy research and replay.

The project is intentionally dependency-light: the hot path uses fixed-point
arithmetic, pre-allocated storage and a lock-free SPSC queue. Python is only
used at the orchestration/analytics boundary.

## Features

- **Fixed-point prices and quantities** (`Price`, `Quantity`, `Notional`, 8
  decimal places) with allocation-free parsing and formatting.
- **Cache-friendly L2 book** with a tick-indexed ladder, O(1) level updates,
  BBO tracking and allocation-free depth snapshots.
- **L3 order book** with id-indexed orders, open-addressing hash table and
  per-order execute/cancel semantics.
- **Lock-free SPSC ring buffer** for feed-thread to engine-thread hand-off.
- **Pre-trade risk guard**: order size/notional, position notional, daily loss,
  price band, post-only crossing and open-order limits.
- **O(1) order manager** with live-list iteration, partial fills and cancel;
  market/limit/post-only orders plus GTC/IOC/FOK handling.
- **Average-cost position accounting** with realised PnL on closes and
  mark-to-market unrealised PnL on depth/trade updates.
- **Latency histogram** (p50/p99/p999) measured at the event-loop boundary.
- **Allocation-free JSON and WebSocket frame parsing** for exchange feeds.
- **Python API** for building engines, submitting orders, replaying NDJSON and
  running an inventory-aware market-making example.
- **Avellaneda-Stoikov market maker** with reservation-price inventory skew,
  optimal spread and market-specific adapters (crypto/stock/commodities).
- **Local paper-trading simulator** with an asynchronous market-data feed
  (WebSocket / NDJSON), a taker-cross maker matching engine with self-trade
  prevention, a `trades_history` execution log, configurable fees/rebates and
  mark-to-market PnL.
- **Parameter optimizer** (grid search over `gamma`, `sigma`, `k`,
  `time_horizon`) scoring Sharpe, Sortino, drawdown and net PnL.
- **Latency benchmark** (`scripts/run_benchmark.py`) measuring tick-to-quote
  microseconds through the C++ `RiskGuard`.
- **Walk-forward OOS validation** (`scripts/run_walk_forward.py`): a frozen,
  fingerprinted parameter lock re-verified around every window, contiguous
  windows with a look-ahead guard, boundary-settled ledger accounting and
  per-window Sharpe/Sortino aggregated into one out-of-sample curve.
- **Production readiness gate** (`scripts/generate_readiness_report.py`):
  re-measures the Phase 1–4 promises (ingestion integrity, engine latency and
  ledger coherence, chaos self-healing, dead-man's switch, parameter lock) and
  prints **two** verdicts — build readiness and alpha (profitability at retail
  fees) — with JSON + Markdown artefacts.
- **Real-time L2 capture** (`scripts/ingest_l2.py`): asyncio WebSocket client
  that stamps every frame with `time.time_ns()`, reports venue-to-host latency
  percentiles and archives the raw payloads as gzip NDJSON roll files with
  backoff reconnection and graceful shutdown. Each connection starts with one
  absolute REST depth snapshot, archived **before** the socket opens, so a
  resumed *diff* stream is never analysed against a book that does not exist yet
  (a seed fetched after connecting is timestamped after the frames buffered
  meanwhile, and the mid timeline refuses a non-monotonic file).
- **Binance Futures testnet pilot** (`scripts/run_testnet_pilot.py`): signed REST
  (HMAC-SHA256, stdlib only), `exchangeInfo` filter validation, balance check,
  L2 depth sample and a dry-run or place → query → cancel order lifecycle against
  the testnet. Refuses to run against production.
- **Live execution layer** (`python_analytics/live/` + `scripts/run_live_trader.py`):
  a SQLite book of record written *before* each request, `clientOrderId`-keyed
  resolution of ambiguous submits, start-up reconciliation that adopts unseen
  venue orders and resolves vanished ones, one kill path that cancels and
  flattens, and an arming gate that refuses real money unless the readiness
  report shows a fresh positive alpha net.
- **Markout analyzer** (`python_analytics/backtesting/markouts.py` +
  `scripts/run_markout.py`): marks every fill against the mid 1 s / 10 s / 60 s
  later, signed by side, subtracts the fee and returns GO / MARGINAL / COSTLY /
  ADVERSE_SELECTION — the difference between "the fees ate the edge" and "the
  quote was picked off".
- **Edge curve** (`python_analytics/backtesting/edge_curve.py` +
  `scripts/run_edge_curve.py`): with no strategy in the loop, measures the gross
  markout a hypothetical quote resting `d` bps from the mid would have earned on
  a capture, and how often the tape reached that far. Under that modelled fill
  rule it names the distance at which a maker fee would be paid back, or reports
  that no distance in the grid is — the difference between "tune harder" and
  "this fee tier cannot be traded". Queue position, latency and order size are
  not modelled, so the number is conditional on the rule, not an execution
  estimate.

## Phase status

| Phase | Scope | Status | Reference |
| --- | --- | --- | --- |
| 1 | Real-time L2 capture to gzip NDJSON | Complete | [`docs/phase1_roadmap.md`](docs/phase1_roadmap.md) |
| 2 | Zero-allocation paper engine, paper trading | Complete | [`docs/phase2_technical_specification.md`](docs/phase2_technical_specification.md) |
| 3 | Batch replay, OFI alpha, optimisation, chaos | Complete | [`docs/phase3_backtesting_architecture.md`](docs/phase3_backtesting_architecture.md) |
| 3.1 / 3.2 | Network chaos injection, heartbeat dead-man's switch | Complete | [Chaos & stress testing](#chaos--stress-testing-phase-31--32) |
| **4** | **Walk-forward OOS validation, fingerprinted parameter lock** | **Complete / verified** | [`docs/phase4_walk_forward.md`](docs/phase4_walk_forward.md) |
| **5** | **Production readiness gate, build + alpha verdicts** | **Complete / verified (build GO 16/16; alpha NO-GO at retail fees)** | [`docs/phase5_production_readiness.md`](docs/phase5_production_readiness.md) |

The final pre-live check is one command:

```bash
python3 scripts/generate_readiness_report.py
# exit 0 = build GO (alpha printed), exit 1 = build NO-GO,
# exit 2 = usage error, exit 3 = alpha NO-GO (only with --require-positive-alpha)
```

## Repository layout

```text
.
├── cpp_engine/
│   ├── include/            # header-only core: book, engine, risk, types
│   ├── src/ws_parser.cpp   # JSON / WebSocket parser
│   ├── bindings/           # pybind11 module
│   ├── tests/              # self-contained C++ test binary
│   └── CMakeLists.txt
├── python_analytics/
│   ├── native/             # extension loader + engine factory
│   ├── strategies/         # strategy interface, A-S maker, optimizer
│   ├── simulator/          # paper matching engine + async market-data feed
│   ├── backtesting/        # replay pipeline and metrics
│   ├── exchange/           # Binance Futures env config + signed REST client
│   └── utils/              # YAML config and logging
├── scripts/
│   ├── run_paper_trading.py  # 60s live/mock paper-trading dashboard
│   ├── run_testnet_pilot.py  # Binance Futures testnet micro-pilot (signed REST)
│   ├── run_live_trader.py    # live reconcile / one post-only quote / kill switch
│   ├── ingest_l2.py          # real-time L2 capture -> gzip NDJSON roll files
│   ├── run_walk_forward.py   # Phase 4 OOS study + parameter lock
│   ├── run_markout.py        # per-fill markout: spread capture vs adverse selection
│   ├── run_edge_curve.py     # what a quote earns at each distance from the mid
│   ├── generate_readiness_report.py  # Phase 5 GO/NO-GO gate
│   └── run_benchmark.py      # tick-to-quote latency benchmark
├── docs/
│   ├── phase1_roadmap.md
│   ├── phase2_technical_specification.md
│   ├── phase3_backtesting_architecture.md
│   ├── phase4_walk_forward.md      # OOS mechanism, lock file, reproduction
│   └── phase5_production_readiness.md  # gates, thresholds, GO/NO-GO workflow
├── config/settings.yaml    # engine, risk, strategy and ingestion settings
├── config/walk_forward_lock.json  # committed, fingerprinted Phase 4 fit
├── data/sample_events.ndjson
├── data/raw_l2/            # capture output (created on demand, git-ignored)
├── reports/                # Phase 4/5 evidence: walk_forward, readiness, benchmark
├── tests/python/           # pytest integration tests
├── .circleci/config.yml    # per-commit CI: secret scan, build+test (py3.9/3.12), lint, sanitizers
├── .github/workflows/      # readiness.yml, codeql.yml, testnet-pilot.yml (release/ops gates)
└── main.py                 # demo / replay CLI
```

## Build

Prerequisites: CMake 3.20+, a C++20 compiler (AppleClang 15+/GCC 12+/Clang
16+), Python 3.9+ and `pip`.

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt

cmake -S cpp_engine -B build \
  -DCMAKE_BUILD_TYPE=Release \
  -Dpybind11_DIR="$(python -m pybind11 --cmakedir)"
cmake --build build --parallel
```

The extension is written to `python_analytics/native/hft_native*.so` so the
Python package can be imported directly from the repository root.

Run the C++ tests:

```bash
ctest --test-dir build --output-on-failure
# or directly
./build/tests/hft_tests
```

### Continuous integration

[`.circleci/config.yml`](.circleci/config.yml) runs the same sequence on
`ubuntu:24.04` for Python 3.9 and 3.12: it installs `build-essential` + `cmake`,
then `requirements.txt` (PyYAML, pybind11, pytest, websockets), configures
`cpp_engine`, builds `hft_native`, and runs `ctest`, `pytest` and
`python main.py --demo`. The extension is built into the checkout rather than
installed, so the job must build before it tests.

The image is plain `ubuntu:24.04` rather than a convenience image because the
engine compiles as C++23 and needs GCC 13; noble is the release that ships it
(13.3, with CMake 3.28). Python 3.9 is the one interpreter noble does not carry,
so the compatibility leg installs it from the deadsnakes PPA on that same image
and keeps the compiler the engine is known to build with.

The gates that are *not* per-commit stay on GitHub Actions because they depend
on GitHub-only features: `codeql.yml` (GitHub-native analysis),
`readiness.yml` (environments, required reviewers, tag attestation) and
`testnet-pilot.yml` (masked testnet credentials).

Secrets are now a per-commit concern as well. The `secret-scan` job installs
[`ggshield`](https://github.com/GitGuardian/ggshield) and scans exactly the
commits the pipeline introduces — the whole branch for a pull request, the
pushed commits for a branch build — so a leaked Binance key fails the build
before review, and existing history is left to the GitGuardian dashboard rather
than re-flagged on every push. It needs one project environment variable,
`GITGUARDIAN_API_KEY` (a personal access token with the `scan` scope from
*dashboard.gitguardian.com → API*); without it the job **fails** instead of
passing green, because a scanning gate that cannot run must not look like a
passing one. The fixtures in `tests/python/test_binance_*.py` use deliberately
fake credentials (`api_key="test-key-1234"`); if a detector ever trips on one,
record it under `secret.ignored_matches` in a `.gitguardian.yaml` rather than
disabling the job.

One-time setup, since CircleCI cannot be configured from inside the repo: add
the [CircleCI GitHub App](https://github.com/apps/circleci) to the repository,
then choose **Set Up Project** for this branch on circleci.com. The only secret
is `GITGUARDIAN_API_KEY` (see above); there are no orbs and no contexts. Enable
**Auto-cancel redundant workflows** in *Project Settings → Advanced* so the
push and pull-request pipelines for the same commit do not both finish.

### Manual extension build (fallback)

If CMake is unavailable, the extension can be compiled directly:

```bash
clang++ -std=c++20 -O2 -shared -fPIC -undefined dynamic_lookup \
  -I cpp_engine/include \
  $(python3-config --includes) $(python3 -m pybind11 --includes) \
  cpp_engine/src/ws_parser.cpp cpp_engine/bindings/pybind_module.cpp \
  -o "python_analytics/native/hft_native$(python3-config --extension-suffix)"
```

On Linux use `-shared -fPIC` and omit `-undefined dynamic_lookup`.

## Run

```bash
python3 main.py --demo
python3 main.py --events data/sample_events.ndjson --limit 1000
```

`--demo` applies a deterministic book, places a market order and prints engine,
strategy and replay metrics as JSON. `--events` runs the replay pipeline with
the inventory market maker attached.

Python API example:

```python
from python_analytics.native import hft_native
from python_analytics.native.factory import build_engine
from python_analytics.utils import load_settings

engine = build_engine(load_settings())
update = hft_native.parse_depth_json(
    '{"e":"depthUpdate","E":1,"U":1,"u":1,"s":"BTCUSDT",'
    '"b":[["29999.50","1.5"]],"a":[["30000.50","2.5"]]}'
)
engine.apply_depth(update)
order = hft_native.OrderRequest()
order.side = hft_native.Side.BUY
order.type = hft_native.OrderType.MARKET
order.quantity = hft_native.Quantity.from_string("0.5")
ack = engine.submit(order)
print(ack.accepted(), engine.position().net_quantity)
```

## Configuration

`config/settings.yaml` controls:

- `engine.tick_size` / `engine.center`: fixed-point grid for the ladder. Prices
  that are not on the grid are rejected by the book.
- `engine.stale_after_ns`: threshold used by `TradingEngine.is_stale()`.
- `engine.risk.*`: pre-trade limits applied by `RiskGuard`.
- `engine.pretrade.*`: `RiskManager` order-flow guards (size, position,
  fat-finger band, token-bucket rate limit); disabled by default.
- `kill_switch.*`: `KillSwitch` emergency halt (automatic drawdown / reject
  streaks plus a manual `trip()`); the guards are off when a limit is `0`.
- `data_ingestion.*`: real-time L2 capture (`exchange`, `symbol`,
  `stream_type`, `storage_path`, `compression`, roll-file rotation, telemetry
  cadence and reconnect backoff) used by `scripts/ingest_l2.py`. Set
  `exchange: binance_futures_testnet` to capture from the testnet stream.
- `strategy.*`: quote width, size, inventory cap and skew.
- `backtest.*`: replay limits and fill recording.

All decimals are parsed on the native side; no binary floating point is used
for prices. Invalid YAML raises `ConfigError` with the offending section.

The testnet pilot does **not** read credentials from YAML. It uses three
environment variables only:

| Variable | Meaning |
| --- | --- |
| `BINANCE_ENV` | `testnet` (default) or `production`; the pilot refuses anything but `testnet` |
| `BINANCE_API_KEY` | Testnet API key, sent as `X-MBX-APIKEY` |
| `BINANCE_API_SECRET` | Testnet secret, used for the HMAC-SHA256 signature |

Secrets are never logged, never included in `repr()` and never written to disk.
An empty or missing `BINANCE_ENV` resolves to `testnet`.

## Tests

```bash
# C++ (after CMake build)
ctest --test-dir build --output-on-failure

# Python (all modules: bindings, strategies, simulator, optimizer)
python3 -m pytest -v

# Chaos / resilience only (native heartbeat, injector, CLI harness)
python3 -m pytest tests/python/test_chaos_network.py -v
```

The C++ suite covers fixed-point parsing, SPSC concurrency, L2/L3 semantics,
risk rejections, latency buckets, JSON/WebSocket parsing, average-cost PnL,
IOC/FOK handling, the kill switch and the heartbeat dead-man's switch. Python
tests exercise the bindings (`test_bindings.py`), the Avellaneda-Stoikov
strategy (`test_strategy.py`), the paper engine and feed
(`test_paper_engine.py`), the parameter optimizer (`test_optimizer.py`), the
testnet REST client and pilot (`test_binance_testnet.py`, fully offline via an
injected transport) and the chaos layer end-to-end (`test_chaos_network.py`,
which drives `tests/chaos_network_test.py` itself).

### CMake / CTest not on `PATH`

If `cmake` or `ctest` is not found, the CMake wheel may be installed without
its `data/bin` directory on `PATH`. Use the Python module for configuration and
builds instead:

```bash
python3 -m cmake -S cpp_engine -B build \
  -DCMAKE_BUILD_TYPE=Release \
  -Dpybind11_DIR="$(python3 -m pybind11 --cmakedir)"
python3 -m cmake --build build --parallel
python3 -m cmake --build build --target test
```

The wheel also ships `ctest` under its `cmake/data/bin` directory; add that
directory to `PATH` if you prefer invoking `ctest` directly, or run
`./build/tests/hft_tests` as shown above.

## Continuous integration and pre-live gates

Every commit is built, sanitised, linted and measured before it can reach
`main`, and a nightly + tag workflow re-runs the full pre-live rehearsal.

| Workflow / job | Trigger | What it proves |
| --- | --- | --- |
| `config.yml` → `secret-scan` | push, PR | `ggshield` scans the commits this pipeline introduces; a credential added by the branch fails the build before review |
| `config.yml` → `build-and-test-py39`, `build-and-test-py312` | push, PR | C++ core + pybind11 extension build on both ends of the supported range, CTest, pytest, CLI smoke test, and coverage ≥ 80 % (measured once, on 3.12) |
| `config.yml` → `lint` | push, PR | `flake8` over `python_analytics`, `scripts` and `tests` with the repo config in `.flake8` |
| `config.yml` → `sanitizers` | push, PR | the engine compiled with `-fsanitize=address,undefined` and its suite run under both — a use-after-free or an overflow fails the build (leak reports are advisory until a clean baseline exists) |
| `readiness.yml` → `GO / NO-GO gate` | weekdays 03:17 UTC, tag `v*`, manual | `pip-audit`, clean rebuild, CTest, pytest, replay of the **committed** lock (`--from-lock`), latency benchmark, build verdict + alpha verdict, evidence bundle as an artifact |
| `readiness.yml` → `attest release verdict` | tag `v*` | refuses to attest a tag unless the report is `GO` for **both** build and alpha; bind the `production` environment to required reviewers to force a human sign-off |
| `codeql.yml` | push, PR, weekly | CodeQL analysis of the C++ and Python trees |

`config.yml` is the CircleCI pipeline (`.circleci/config.yml`); everything else
is still a GitHub Actions workflow. Branch protection should require
`ci/circleci: secret-scan`, `ci/circleci: build-and-test-py39`,
`ci/circleci: build-and-test-py312`, `ci/circleci: lint` and
`ci/circleci: sanitizers`.

The same gates locally (the sanitised build deliberately skips the extension,
so it links `hft_core` directly and compiles in seconds):

```bash
python3 -m pip install -r requirements-dev.txt

# Style
python3 -m flake8 --count python_analytics scripts tests main.py

# Memory safety
cmake -S cpp_engine -B build/sanitize -DCMAKE_BUILD_TYPE=Debug \
  -DHFT_BUILD_PYTHON=OFF -DHFT_ENABLE_NATIVE=OFF -DHFT_ENABLE_LTO=OFF \
  -DCMAKE_CXX_FLAGS="-fsanitize=address,undefined -fno-omit-frame-pointer -g"
cmake --build build/sanitize --parallel
ASAN_OPTIONS=halt_on_error=1 ctest --test-dir build/sanitize --output-on-failure

# Coverage floor
python3 -m pytest --cov=python_analytics --cov-fail-under=80
```

**Running the readiness gate from Actions.** `Production readiness` →
*Run workflow* accepts the same knobs as the CLI:

| Input | Default | Effect |
| --- | --- | --- |
| `source` | `data/sample_events.ndjson` | archive whose Phase 1 integrity is verified |
| `evidence-events` | `2000` | synthetic frames behind the engine/chaos/OOS evidence |
| `max-engine-p99-us` | `50` | engine p99 ceiling; relax it on a noisy runner, not in code |
| `max-reanchor-p99-us` | `200` | p99 ceiling for frames that move the priced window; its own budget, because a re-anchor translates the whole ladder |
| `min-oos-sharpe` | *(empty)* | set it to make profitability blocking; empty keeps it `info` |
| `recompute-walk-forward` | `false` | ignore `reports/walk_forward.json` and rerun the study |

The verdict is printed to the run summary and the whole `reports/` directory is
uploaded as `readiness-reports` (30-day retention) — that bundle, not the log,
is the audit trail for a release. To make the gates mandatory, protect `main`
with the required status checks `build + test (py3.9)`, `build + test (py3.12)`,
`lint (flake8)` and `asan + ubsan (C++ core)`, and tag releases only after a
green `Production readiness` run.

Wall-clock latency on a shared runner is dominated by scheduler noise, so the
benchmark step is informational there; the authoritative latency gate is the
in-engine histogram behind `phase2.engine_latency_p99`. Run
`run_benchmark.py --max-p99-us … --max-p999-us …` on the host that will trade.

## Real-time L2 ingestion (Phase 1)

`scripts/ingest_l2.py` is the capture front end: it subscribes to an exchange
depth stream over WebSocket and writes the raw frames to gzip-compressed NDJSON
roll files that later stages (feature building, replay, backtests) consume. Each
connection is seeded with one REST depth snapshot first — the depth feed is a
*diff* stream, so without it the archive has no mid until enough deltas arrive.

> Full architecture, data schema, capacity numbers and the Phase 2 handoff:
> [`docs/phase1_roadmap.md`](docs/phase1_roadmap.md).

```bash
# Capture for 30 s into data/raw_l2 using config/settings.yaml
python3 scripts/ingest_l2.py --duration 30

# Measure latency only, write nothing
python3 scripts/ingest_l2.py --dry-run --duration 10

# Push the offline sample through the exact capture path (no network)
python3 scripts/ingest_l2.py --replay data/sample_events.ndjson --output-dir /tmp/raw_l2
```

Useful flags: `--symbol`, `--stream` (`depth@100ms`, `depth@500ms`,
`depth20@100ms`), `--url` (endpoint override), `--output-dir`, `--compression`
(`gzip`/`none`), `--max-file-mb`, `--rotate-seconds`, `--flush-interval-ms`,
`--telemetry-interval`, `--latency-warn-ms`, `--duration` and `--log-level`.
`--dry-run` skips storage; `--replay` feeds an existing NDJSON archive through
the same code path instead of the socket.

### What a run does

| Stage | Behaviour |
| --- | --- |
| Connect | `wss://fstream.binance.com/ws/<symbol>@<stream_type>`; the endpoint comes from `data_ingestion.exchange` (see `EXCHANGE_ENDPOINTS`) or `ws_base_url` |
| Timestamp | `time.time_ns()` the moment the frame leaves the socket |
| Latency | `recv_ns - E*1_000_000` (falls back to `T`); p50/p95/p99/max per interval + breach counter |
| Archive | one `bytearray` append per frame; gzip and disk writes happen on a worker thread |
| Rotate | at `max_file_mb` or `rotate_seconds`, whichever comes first |
| Reconnect | capped exponential backoff with jitter, reset after a healthy session |
| Shutdown | `SIGINT`/`SIGTERM` close the socket, drain the buffer and write the gzip trailer |

### Output layout

```text
data/raw_l2/
└── binance_futures_btcusdt_depth_100ms_20261021T171011Z_00001.ndjson.gz
```

The name carries exchange, symbol, stream, UTC start time and a sequence number,
so roll files sort chronologically and never collide. Every frame the venue
sends is written verbatim (one JSON object per line), including frames without
an event timestamp — those are counted as `no_timestamp` so a hole in the
capture is always attributable.

```python
import gzip, json

with gzip.open("data/raw_l2/binance_futures_btcusdt_depth_100ms_..._00001.ndjson.gz",
               "rt", encoding="utf-8") as handle:
    for line in handle:
        update = json.loads(line)  # {"e": "depthUpdate", "E": 1700000000000, ...}
```

### Telemetry

One INFO line per `data_ingestion.telemetry_interval_s` (plus a `FINAL` line at
exit and a summary table):

```text
telemetry frames=812 rate=9.9/s lat_ms(p50=12.31 p95=18.04 p99=41.22 max=41.22) lat_breaches=0 files=1 rec=800 buf=12 out=0.02MiB ratio=0.12
```

`lat_ms` is the rolling window; `lat_breaches` counts samples above
`latency_warn_ms` (also logged as a `WARNING`, at most once per second).
`clock_skew` counts negative latencies, i.e. the venue clock ahead of ours —
worth checking `ntpd`/`chronyd` when it is non-zero.

### Guarantees and limits

- The receive loop performs **no I/O**: a slow disk grows the in-memory buffer
  (a `WARNING` at 64 MiB) but never drops a frame.
- A hard crash can lose at most one `flush_interval_ms` window; a clean shutdown
  always leaves a readable gzip archive.
- A roll file being written has no gzip trailer yet, so it only reads cleanly
  after rotation or shutdown (`zcat` reports "unexpected end of file" before
  that, which is expected).
- Latency is only as accurate as the host clock: sync with NTP/PTP before
  drawing conclusions about the venue.
- One symbol/stream per process — run one process per subscription.

## Live forward paper trading (Phase 2)

Phase 2 wires the Phase 1 `.ndjson.gz` archives straight into the C++23 core and
executes a deterministic paper-trading simulator with **L2 depth-based slippage**.

```bash
# Replay the bundled sample through the C++ paper engine (taker orders, slippage, fees)
python3 scripts/run_live_paper.py --source data/sample_events.ndjson --order-quantity 0.5

# Live-forward a Phase 1 capture (gzip NDJSON) into the paper engine
python3 scripts/run_live_paper.py --source data/raw_l2/<roll>.ndjson.gz \
    --tick-size 0.1 --center 81500 --taker-fee-bps 5 --maker-fee-bps -1
```

| Stage | Behaviour |
| --- | --- |
| Bridge | `python_analytics/bridge.py` streams lines in bulk through `hft_native.ingest_lines()`; per-level parsing and book updates run allocation-free in C++ |
| Order book | `paper_engine.process_update()` applies Binance diffs (`b`/`a`, volume `0` removes a level) and flags `pu`/`u` sequence gaps |
| Matching | `paper_engine.submit()` walks the real ladder level by level (`matching.hpp`) so market/crossing orders realise realistic slippage; resting limits fill as maker |
| Account | `paper_account` tracks cash, maker/taker fees, realized/unrealized PnL and equity in fixed point |
| Telemetry | per-frame processing latency is histogrammed in C++ (p50/p99/p999 ns) and printed per batch |

New C++ headers: `cpp_engine/include/{matching,paper_account,paper_engine,ingest}.hpp`
(all `snake_case`, header-only, C++23). The Python test `tests/python/test_live_paper.py`
exercises the bridge end-to-end.

The full engineering specification — architecture, ladder mathematics, fee/PnL formulas,
CLI reference, benchmark methodology and the operational runbook — lives in
[`docs/phase2_technical_specification.md`](docs/phase2_technical_specification.md).

## High-performance backtesting & alpha optimisation (Phase 3)

Phase 3 turns the Phase 2 `paper_engine` into a batch historical replay and
optimisation platform, coupling Order-Flow Imbalance into the Avellaneda–Stoikov
reservation price.

```bash
# Single-archive backtest with an OFI-skewed Avellaneda–Stoikov market maker
python3 scripts/run_backtest.py \
  --source data/raw_l2/binance_futures_btcusdt_depth_100ms_20261021T173329Z_00001.ndjson.gz \
  --tick-size 0.1 --center 84570 \
  --strategy avellaneda_stoikov --gamma 0.1 --k 5 --ofi-sensitivity 0.5 \
  --maker-bps 2 --taker-bps 5 --order-quantity 0.01 \
  --report data/backtests/run_001

# Parameter optimisation (grid / random / Bayesian)
python3 -c "from python_analytics.strategies.optimizer import grid_search; \
  from python_analytics.simulator.paper_engine import synthetic_depth_messages; \
  lines=[next(synthetic_depth_messages(mid=100.0,tick=0.5,seed=1)) for _ in range(200)]; \
  print(grid_search({'gamma':[0.1,0.5],'k':[5,10]}, lines, metric='composite', \
  tick_size=0.5, center=100.0, order_quantity=0.1).best_params)"
```

Fee flags are fractional basis points — `--taker-bps 0.5` charges 0.005% and a
negative `--maker-bps` models a **rebate tier**. The defaults are retail USDⓈ-M
VIP 0 (`2` / `5` bps, from `python_analytics/costs.py`): a rebate is a privilege
you have to hold, not a default you get to assume, and assuming one is precisely
how a losing study reports a profit. Use
`BinanceFuturesClient.commission_rate(symbol)` to read the rate your account
actually pays. `--report` accepts an output directory or
a `.json` path (resolved to its parent directory), and `--plot` adds
`equity.png` when `matplotlib` is installed.

| Stage | Behaviour |
| --- | --- |
| Replay | `BulkReplayer` streams `.ndjson.gz` archives through `hft_native.ingest_lines()` (zero-allocation C++ matching), with an optional per-frame strategy hook, gap accounting and REST-snapshot seeding |
| Alpha | `OfiSignal` (EMA z-score) shifts the A-S reservation price by `ofi_sensitivity · ofi_z`; the spread includes the `γσ²(T−t)` term |
| Optimisation | `grid_search`, `random_search` and `bayesian_search` (scipy/skopt optional) minimise drawdown + inventory variance while maximising Sharpe and fill ratio |
| Reporting | `quant_summary` emits realized/unrealized PnL, Sharpe/Sortino, max drawdown, win rate, profit factor, holding time, turnover, fill rate, slippage and latency percentiles; `export_report` writes JSON/CSV/PNG |

The full plan is in
[`docs/phase3_backtesting_architecture.md`](docs/phase3_backtesting_architecture.md).

## Chaos & stress testing (Phase 3.1 / 3.2)

Two failure modes dominate a live crypto feed: a socket that dies silently and a
gateway that answers 900 ms late. Both leave the strategy quoting against a
stale book, and neither raises an exception. `ChaosNetworkInjector` reproduces
them at the frame boundary; the native `HeartbeatMonitor` turns the resulting
silence into a `KillSwitch` halt.

```bash
# Repeated outages: 400 ms down every second, 100 ms heartbeat deadline
python3 tests/chaos_network_test.py --mode disconnect_storm \
  --interval 1.0 --disconnect-s 0.4 --duration 8 \
  --heartbeat-timeout-ms 100 --recovery-delay 0.5

# Latency spikes: 200-1000 ms on 15% of frames (seeded = reproducible)
python3 tests/chaos_network_test.py --mode latency_injection \
  --latency-min-ms 200 --latency-max-ms 1000 --latency-probability 0.15 \
  --seed 7 --duration 8 --heartbeat-timeout-ms 500

# The same injection inside the Phase 2 live-forward loop
python3 scripts/run_live_paper.py --source data/sample_events.ndjson \
  --chaos-mode disconnect_storm --chaos-interval 2 --chaos-disconnect-s 1 \
  --frame-delay 0.01 --heartbeat-timeout-ms 500
```

| Layer | Behaviour |
| --- | --- |
| `disconnect_storm` | blocks the feed for `--disconnect-s` every `--interval`; the frame in hand is lost, the outage window produces no data, and every reconnect bumps the epoch |
| `latency_injection` | sleeps `uniform(min, max)` ms on `--latency-probability` of frames; nothing is dropped, so the book stays consistent but stale |
| `HeartbeatMonitor` (C++) | `beat()` is one release store; `poll()` trips the switch once per expiry window, so a 20 Hz watchdog cannot inflate the trip counter |
| `HeartbeatWatchdog` (Python) | supervision thread: timeout → `TRIGGERED` → `RECOVERING` → `reset()` only after a fresh frame proves the feed is back |
| `PaperSession` | checks the switch before every order and stages `cancel_all` / `flatten` for the engine thread |

The harness logs one line per disruption with a marker — `!!` disconnect, `++`
reconnect, `~~` latency, `..` shutdown, `!!!` heartbeat timeout / kill switch —
then prints the evidence table and a verdict:

```
  [PASS] feed survived                      407 forwarded, 407 depth events ingested
  [PASS] disconnect storm executed          3 outages / 3 reconnects, epoch=3
  [PASS] dead-man's switch tripped          trips=4, watchdog timeouts=3, limit=250ms
  [PASS] recovered to NORMAL                state=NORMAL, 3 recovery cycle(s)
  [PASS] engine left flat and clean         open_orders=0, net=+0.00000000
  RESULT: PASS
```

Exit codes are `0` (pass), `2` (any check failed) and `130` (interrupted);
`--json-out report.json` writes the full evidence — event log, heartbeat stats,
kill-switch stats, engine metrics and checks — for a CI artefact.

The split of responsibilities is deliberate: the *injector never trips the
switch*. It only stops delivering frames, the ingestion loop stops beating, and
the monitor decides the feed is dead — the same causal chain as production.
`KillSwitch::trip()` then runs the registered hooks, which stage
`cancel_all -> flatten` and are executed by `PaperSession.service_remediation()`
on the thread that owns the C++ `paper_engine` (single-threaded by contract, so
remediation must not run on the supervision thread). Blocking itself is one
atomic load and takes effect immediately.

Auto-recovery is conditioned on evidence of life: after the remediation window
the watchdog waits for a frame that arrived *after* the trip before it calls
`reset()`. A long outage therefore stays halted (orders blocked, position flat)
for its whole duration instead of flapping between `TRIGGERED` and `NORMAL`.
Recovery is also scoped to the halt the heartbeat caused: if a drawdown breach,
a reject streak or an operator already holds the switch, the expiry is still
reported but the halt is left to its owner, and a trip that lands during the
recovery window cancels the automatic reset.

Ctrl+C is a graceful path, not an escape: SIGINT/SIGTERM end the loop at a frame
boundary and then trip the switch, so resting orders are cancelled and the
position is flattened before the summary is printed.

## Local Paper Trading & Simulation Guide

The repository ships a self-contained paper-trading loop that streams depth
data into the C++ `L2Book`, matches the Avellaneda-Stoikov quotes locally and
tracks PnL in real time.

### Live / mock simulation (60 seconds)

```bash
# Deterministic random-walk mock feed (no network required)
python3 scripts/run_paper_trading.py --source mock --duration 60

# Replay the offline NDJSON sample on a loop
python3 scripts/run_paper_trading.py --offline --duration 60

# Live Binance futures partial-depth stream (falls back to NDJSON offline)
python3 scripts/run_paper_trading.py --duration 60
```

Useful flags: `--duration`, `--interval` (dashboard cadence), `--gamma`,
`--sigma`, `--k`, `--order-quantity`, `--inventory-limit`, `--maker-fee-bps`
(default `2` = 0.02%, retail VIP 0; negative = rebate tier), `--taker-fee-bps`
(default `5` = 0.05%),
`--self-trade-prevention` and `--trades-out` (see below).
Press `Ctrl-C` to stop cleanly: the loop cancels open orders and prints a
summary (total trades, win rate, Sharpe approximation, final PnL, max
drawdown).

The WebSocket source is `wss://stream.binance.com:9443/ws/btcusdt@depth20@100ms`;
if the `websockets` package is missing or the connection drops, the feed falls
back to `data/sample_events.ndjson` (looped) so the simulation keeps running.

### Matching semantics (taker cross)

Resting maker quotes fill when external market liquidity trades through them
on a depth tick:

| Resting quote | Fill condition | Fill side |
| --- | --- | --- |
| Bid at `bid_price` | `market_best_ask <= bid_price` | `BUY` |
| Ask at `ask_price` | `market_best_bid >= ask_price` | `SELL` |

Fill size is capped by the visible quantity at the touch, so a thin book
produces a partial fill instead of inventing liquidity. Each execution updates
inventory, cash (including maker rebates / taker fees), the volume-weighted
cost basis and realised PnL, then appends a row to
`PaperMatchingEngine.trades_history`:

```python
engine.trades_history[-1]
# {'timestamp': 1700000000.5, 'exchange_ts': 1700000000500, 'sequence': 42,
#  'side': 'BUY', 'price': 29999.5, 'size': 0.05, 'quantity': 0.05,
#  'order_id': 7, 'is_maker': True, 'fee': -0.0015, 'realized_pnl': 0.0015,
#  'inventory': 0.05, 'cash': -1499.9985}
```

`timestamp` is the exchange event time in seconds, taken from the depth tick
that crossed the quote, and falls back to wall-clock time when the feed carries
no timestamp. `size` mirrors `quantity` for reporting convenience. The property
returns a defensive copy, and `--trades-out trades.json` writes the full log to
disk.

**Self-trade prevention.** A post-only quote that would cross the bot's own
resting opposite quote never rests. `--self-trade-prevention` selects
`cancel_resting` (default: retire the stale quote), `reject` (refuse the new
quote with reason `SELF_TRADE_PREVENTED`) or `none`. This stops a single tick
from printing a fictitious round trip against ourselves, and the count is
reported as `Self-Match Stop` in the summary.

### Latency benchmark

```bash
# Report only
python3 scripts/run_benchmark.py --ticks 50000

# Pre-live budget: archive the numbers and fail when the tail regresses
python3 scripts/run_benchmark.py --ticks 50000 \
  --json reports/latency_benchmark.json --max-p99-us 200 --max-p999-us 1000
```

Measures the tick-to-quote hot path in microseconds (p50/p99/p99.9): JSON
parse → C++ `L2Book` apply → Avellaneda-Stoikov quote generation → C++
`RiskGuard` validation. GC is disabled during the loop to eliminate collection
pauses. `--json` writes the raw report for trending; `--max-p99-us` /
`--max-p999-us` turn the run into a gate (exit `1` on breach) — run that on the
host that will trade, with cores pinned, not on a shared CI runner.

### Parameter optimisation

```python
from python_analytics.strategies.optimizer import grid_search

result = grid_search(
    {"gamma": [0.05, 0.1, 0.5], "sigma": [0.01, 0.02], "k": [1.0, 1.5],
     "time_horizon": [30.0, 60.0]},
    open("data/sample_events.ndjson"),
    metric="sharpe",
)
print(result.best_params, result.best_score)
for row in result.grid:
    print(row)
```

`grid_search` replays the NDJSON lines through `PaperMatchingEngine`, scoring
each parameter combination on net PnL, Sharpe, Sortino and maximum drawdown.

## Architecture

- **C++20 core (`cpp_engine/`)** — header-only `L2Book`, `OrderManager`,
  `RiskGuard`, `TradingEngine`, lock-free SPSC queues, allocation-free JSON and
  WebSocket parsing, plus the `KillSwitch` / `HeartbeatMonitor` resilience pair.
  Exposed to Python via `pybind11` as `hft_native`.
- **Strategy layer (`python_analytics/strategies/`)** — `MarketConfig`,
  `MicroPriceCalculator`, `AvellanedaStoikovStrategy`, and the `optimizer`
  grid-search utility. Owns all pricing mathematics.
- **Simulator (`python_analytics/simulator/`)** — `PaperMatchingEngine`
  (maker matching, inventory, cash, fees, PnL) and `MarketDataFeed`
  (WebSocket / NDJSON / synthetic). `PaperMatchingEngine` implements the same
  duck-typed surface as `TradingEngine`, so the strategy quotes against it
  unchanged.
- **Chaos (`python_analytics/chaos/`)** — `ChaosNetworkInjector` (frame-gate
  disconnect storms / latency injection) and `HeartbeatWatchdog` (supervises
  the native heartbeat and drives the switch through
  `TRIGGERED -> RECOVERING -> NORMAL`).
- **Backtesting (`python_analytics/backtesting/`)** — NDJSON replay plus
  Sharpe/drawdown metric helpers.
- **Scripts (`scripts/`)** — the paper-trading runner and latency benchmark.

## Mathematical model

### Avellaneda-Stoikov market making

- Micro-price (volume-weighted mid):
  `S = (AskVol * Bid + BidVol * Ask) / (BidVol + AskVol)`.
- Reservation (indifference) price:
  `R = S - q * gamma * sigma^2 * (T - t)`, where `q` is inventory,
  `gamma` risk aversion, `sigma` volatility and `(T - t)` the remaining horizon.
- Optimal half-spread: `delta = (1 / gamma) * ln(1 + gamma / k)`.
- Raw quotes: `bid = R - delta / 2`, `ask = R + delta / 2`.
- `STOCK`/`COMMODITIES` quotes are rounded to `tick_size` and session-gated;
  `CRYPTO` quotes continuously with a dynamic EMA micro-volatility estimate.

### Order Flow Imbalance (OFI)

Following Cont, Kukanov & Stoikov (2014), OFI measures top-of-book pressure
across consecutive ticks:

- `OFI_bid = +B_t` if the bid price ticked up, `-B_{t-1}` if it ticked down,
  else `B_t - B_{t-1}`.
- `OFI_ask = +A_t` if the ask ticked down, `-A_{t-1}` if it ticked up, else
  `A_t - A_{t-1}`.
- `OFI = OFI_bid - OFI_ask`.

## RiskGuard safety boundaries

Every order passes through the C++ `RiskGuard` before matching. Enforced limits
(configured in `config/settings.yaml`):

- `max_order_quantity` / `max_order_notional` — per-order size caps.
- `max_position_notional` — worst-case exposure (net + resting orders).
- `max_daily_loss` — realised + unrealised PnL floor.
- `max_price_deviation` — distance from the mid the order may rest at.
- `max_open_orders` — live order-count cap.
- `post-only` crossing is rejected; market orders are repriced to the touch for
  notional checks.

The Python strategy mirrors these limits (reads `engine.risk_limits()`) and
scales down or pauses quoting before submission; the C++ guard remains
authoritative.

## RiskManager order-flow guards

`RiskManager` sits in front of matching and catches the failure modes
`RiskGuard` is price-oriented about but cannot express: bursts of orders,
runaway sizes, and fat-finger quotes far from the mid. It is cache-line
aligned, uses relaxed atomics for live tuning and a lock-free token bucket
(ahead-of-time "TAT" accounting, no mutex, no syscall).

| Guard | Config key | Rejection |
| --- | --- | --- |
| non-finite / non-positive price or quantity | — | `REJECTED_INVALID_ORDER` |
| per-order base quantity | `max_order_size` | `REJECTED_MAX_ORDER_SIZE` |
| resulting net position | `max_position` | `REJECTED_POSITION_LIMIT` |
| distance from BBO mid | `max_price_deviation_pct` | `REJECTED_FAT_FINGER` |
| orders per second / burst | `order_rate_limit`, `order_burst` | `REJECTED_RATE_LIMIT` |

Checks run in that order and the rate limiter is evaluated last, so a token is
only consumed by an order that would otherwise be accepted — rejected orders
never eat rate budget. A non-usable mid (empty book) fails **closed**, and
`max_price_deviation_pct = 0` disables the band.

```python
from python_analytics.native import hft_native

config = hft_native.RiskManagerConfig()
config.max_position = 5.0        # base units, signed net
config.max_order_size = 0.5
config.max_price_deviation_pct = 0.005   # 50 bps
config.order_rate_limit = 25.0
config.order_burst = 10.0
risk = hft_native.RiskManager(config)

status = risk.validate_order("BTCUSDT", 30_000.0, 0.4, hft_native.Side.BUY, 0.0, 30_001.0)
hft_native.risk_status_name(status)      # 'APPROVED'
stats = risk.stats()                     # evaluated / approved / rejected / by_status
```

`hft_native.is_approved(status)` classifies any status, `engine.risk_stats()`
exposes the same snapshot from `TradingEngine`, and `risk.validate_order_at(now_ns, ...)`
takes an explicit timestamp so replay and tests are deterministic. Enable the
gate through `engine.risk_manager` (native) or `risk_manager=...` on
`PaperMatchingEngine`; `config.engine.pretrade` feeds both, and is off by
default so replays stay byte-identical.

The native default (`RiskManagerConfig{}`) is fully armed, so a standalone
`hft_native.RiskManager()` rejects oversized orders out of the box; use
`hft_native.RiskManagerConfig.disabled()` for a pass-through gate.

## Emergency kill switch

`KillSwitch` is the halt button: one acquire load of an 8-bit atomic decides
whether an order may leave the process, so the check costs nothing measurable
on the hot path. State moves `NORMAL -> TRIGGERED -> RECOVERING -> NORMAL`;
both non-normal states block orders, so a half-flattened book never re-quotes.

| Trigger | API | Config |
| --- | --- | --- |
| operator halt | `trip(reason)` | — (always wins, even when disabled) |
| drawdown breach | `check_drawdown(x)` | `max_drawdown_limit` |
| reject streak | `record_order_rejected()` / `record_order_accepted()` | `max_consecutive_rejects` |
| feed silence (dead-man's switch) | `HeartbeatMonitor.poll()` | `HeartbeatConfig.timeout_ns` |
| clear the halt | `reset()` | — |

`trip()` allocates nothing (the reason is copied into a fixed 192-byte buffer)
and is a single atomic RMW, so concurrent trips from a control thread and a
strategy thread cannot tear the state. It then runs the registered hooks in
`cancel_all -> flatten -> log` order; hook exceptions are swallowed so a broken
remediation callback can never undo the halt.

```python
from python_analytics.native import hft_native
from python_analytics.native.factory import build_kill_switch
from python_analytics.utils import load_settings

switch = build_kill_switch(load_settings())   # armed from kill_switch.* in YAML
switch.set_cancel_all_hook(lambda: print("cancel all"))
switch.set_flatten_hook(lambda: print("flatten"))
switch.set_trip_hook(lambda state, reason: print(f"{state}: {reason}"))

switch.check_order_allowed()                  # True
switch.trip("ops: FTX headline")
switch.get_state()                            # SystemState.TRIGGERED
switch.check_order_allowed()                  # False
stats = switch.get_stats()                    # trip_count / reason / timestamp / blocked_orders
switch.begin_recovery()                       # still blocking, cancels in flight
switch.reset()                                # NORMAL, order flow resumes
```

`max_drawdown_limit` is an absolute drawdown in quote currency and both
automatic guards are inert when their limit is `0`; `kill_switch.enabled`
disables only that automation. A manual `trip()` always stops trading.

`HeartbeatMonitor` (`cpp_engine/include/risk/heartbeat.hpp`) is the dead-man's
switch for the feed: `beat()` once per delivered frame, `poll()` from a
supervision thread. Silence longer than `timeout_ns` trips the attached switch
once per outage (the latch is cleared by the next beat, so a 20 Hz watchdog
cannot inflate `trip_count`), and `reset()` restarts the window while keeping
the telemetry, mirroring `KillSwitchStats`.

```python
from python_analytics.bridge import PaperSession
from python_analytics.chaos import HeartbeatWatchdog
from python_analytics.native import hft_native
from python_analytics.native.factory import build_kill_switch
from python_analytics.utils import load_settings

switch = build_kill_switch(load_settings())
session = PaperSession.create("0.1", "84570", kill_switch=switch)

heartbeat = hft_native.HeartbeatMonitor(hft_native.HeartbeatConfig())
heartbeat.arm(switch)
watchdog = HeartbeatWatchdog(heartbeat, kill_switch=switch).start()
heartbeat.beat()          # ...on every frame the feed delivers
```

`PaperSession` takes the switch directly: `market()` returns
`{"accepted": False, "blocked": True, "reason": "kill_switch:TRIGGERED"}`
instead of submitting, and the cancel/flatten hooks stage their work for
`service_remediation()` on the engine thread (the C++ `paper_engine` is
single-threaded). Both runners wire this automatically — see
[Chaos & stress testing](#chaos--stress-testing-phase-31--32).

## Walk-forward out-of-sample validation (Phase 4)

> **Status: complete and verified.** Committed baseline: 2 OOS windows over 800
> events, parameter lock `a3aa7022dbbbab72`, 4 pre/post-window lock checks, 1
> chronology check, ledger residual `+1.4e-7`.
>
> Full mechanism — windowing, boundary settlement, lock fingerprinting,
> look-ahead proofs, artefact schema, CLI reference and NO-GO troubleshooting:
> [`docs/phase4_walk_forward.md`](docs/phase4_walk_forward.md).

A single backtest over the data you tuned on is an in-sample number. Phase 4
splits the archive into contiguous windows, freezes the parameters in a lock file
*before* the first window, re-verifies them after every window, and chains the
ledger across boundaries so the reported equity belongs to one continuous
account.

```bash
# Two contiguous OOS windows over the bundled sample, lock file written for audit
python3 scripts/run_walk_forward.py --source data/sample_events.ndjson \
  --window-events 400 --k 5.0 \
  --write-lock config/walk_forward_lock.json --report-dir reports

# Verify a lock you did not create: replay it verbatim, no flags to drift
python3 scripts/run_walk_forward.py --source data/raw_l2/*.ndjson.gz \
  --window-events 5000 --from-lock --lock-file config/walk_forward_lock.json

# Same check while restating the parameters (refuses to start on any drift)
python3 scripts/run_walk_forward.py --source data/raw_l2/*.ndjson.gz \
  --window-events 5000 --k 5.0 --lock-file config/walk_forward_lock.json
```

| Layer | Behaviour |
| --- | --- |
| `LockedParameters` | One frozen parameter set (strategy, venue class, costs, sizing) with a `sha256`-derived fingerprint; `diff()` names every drifted field, and an unknown `MarketType` is rejected at construction instead of mid-run |
| `WalkForwardRunner` | Slices each archive into contiguous windows, builds a *fresh* strategy per window, and runs the native paper engine over it — window `n+1` never sees window `n`'s events |
| Look-ahead guard | Every event must be non-decreasing in `E` and confined to its own window; an out-of-order or overlapping event raises `LookAheadError` rather than silently shifting a fill |
| Boundary settlement | A position opened inside a window is settled at that window's last mark, so `realized + boundary_settled + unrealized` closes the ledger to ~1e-7 across the whole run |
| Aggregation | `aggregate_windows` reports net/gross PnL, per-window Sharpe/Sortino (from the chained equity curve), hit rate, fill ratio, profit factor and the ledger residual |

## Production readiness gate (Phase 5)

> **Status: build complete and verified — `GO`, 16/16 gated checks; alpha
> `NO-GO` at retail fees (2 bps maker / 5 bps taker).**
>
> Gate inventory, thresholds, latency-budget policy, chaos/heartbeat invariants,
> report schema, the build/alpha split and per-check NO-GO troubleshooting:
> [`docs/phase5_production_readiness.md`](docs/phase5_production_readiness.md).

`scripts/generate_readiness_report.py` re-measures every Phase 1–4 promise and
prints two verdicts against explicit thresholds — build readiness (does the
machinery keep its promises?) and alpha (does *this* parameter set earn after the
fees a retail account actually pays?) — so "is this build shippable?" and "is
this strategy worth running?" are separate commands with separate exit codes,
not opinions.

```bash
# The pre-live gate: JSON + Markdown under reports/, exit 0 (build GO) / 1 / 2
python3 scripts/generate_readiness_report.py

# Console verdict only, plus the opt-in profitability gates
python3 scripts/generate_readiness_report.py --no-report --min-oos-sharpe 0.5
python3 scripts/generate_readiness_report.py --no-report --require-positive-alpha

# Acceptance run on the trading host, at the production latency budget
python3 scripts/generate_readiness_report.py \
  --max-engine-p99-us 5.0 --max-engine-p999-us 50 --evidence-events 20000

# Force a fresh OOS study instead of reusing reports/walk_forward.json
python3 scripts/generate_readiness_report.py --recompute-walk-forward
```

Sample verdict — the honest combination at retail fees:

```text
==============================================================================
  BUILD READINESS:     GO
  ALPHA (PROFITABILITY): NO-GO
  BUILD: GO (16/16 gated checks passed)
  ALPHA: NO-GO (1/3 profitability checks passed at 2.0 / 5.0 bps maker/taker)
    ALPHA FAIL alpha.net_pnl: -15.7830 USDT (-0.0158%); -2.0500 before +13.7330 of fees
    ALPHA FAIL alpha.fee_wall: gross edge -0.299 bps vs 2 bps maker fee (break-even maker fee -0.299 bps)
    Phase 1 - data ingestion               3/3 checks
    Phase 2 - paper execution              7/7 checks
    Phase 3 - resilience                   3/3 checks
    Phase 4 - out-of-sample validation     5/5 checks
    Alpha - profitability at modelled fees 1/3 checks
==============================================================================
```

| Phase | Gates |
| --- | --- |
| 1 — ingestion | settings self-consistency (positive tick/center, kill switch armed), archive integrity (`0` malformed, `0` sequence gaps) on the real NDJSON, ingest rate (info) |
| 2 — execution | engine p99 ≤ 50 µs, p999 ≤ 500 µs, re-anchor p99 ≤ 200 µs over frames that actually move the window, replay ≥ 1,000 depth events/s, equity-vs-PnL ledger residual ≤ 1e-2, `0` rejected orders, fill activity > 0 % |
| 3 — resilience | disconnect storm self-heals (`reconnects == outages`, every frame accounted for), latency injection spikes but never exceeds its ceiling, dead-man's switch trips on silence, blocks orders, releases only on an explicit reset and can trip again |
| 4 — validation | parameter lock re-verified, look-ahead chronology verified on every boundary, aggregate ledger residual ≤ 2e-2, ≥ 2 OOS windows, optional `--min-oos-sharpe` |
| alpha | `net_pnl >= 0` after modelled fees, `maker_bps >= 0` (no rebate assumed), and — when the study traded notional — a break-even maker fee above the gross edge (`fee_wall`) — reported on every run, gating only with `--require-positive-alpha` |

Engine, chaos and walk-forward evidence come from a seeded synthetic book
(`--evidence-events`, `--seed`), so every deterministic check reproduces on any
machine (latency is the exception — see the budget note below);
`reports/walk_forward.json` is reused when present (`--recompute-walk-forward`
forces a fresh study). Build and alpha are never merged: a green build with a
losing parameter set is the *expected* state of this repository at retail fees
(`alpha.net_pnl` is `-15.78 USDT` on the committed lock, `-2.05` even at zero
fees), and printing both is what keeps a cost assumption from laundering a red
strategy into a green one. Artefacts: `reports/production_readiness_report.{json,md}`.

**Latency budget tiers.** The 50 µs default is a machine-tolerant tripwire for
shared runners, not the performance target. The budget a co-located trading host
must hold is **engine p99 <= 5.0 µs**, applied at promotion time with
`--max-engine-p99-us 5.0` (measured: `1.4 µs` p99 at 20 k frames on an idle host,
`8–11 µs` under load). Tightening that ceiling does not tighten the re-anchor
row: frames that move the priced window keep their own `--max-reanchor-p99-us`
(`200 µs` by default), because they translate the whole ladder and their p99
tracks host memory bandwidth rather than the code. See
[`docs/phase5_production_readiness.md` §3](docs/phase5_production_readiness.md#3-latency-and-throughput-policy)
for the measured bands, the p999 noise caveat and the reasoning.

**Promotion sequence:** tests green →
`run_walk_forward.py --from-lock --lock-file config/walk_forward_lock.json`
exit 0 → readiness build `GO` at the 5.0 µs budget on the trading host →
readiness alpha `GO` (tag attestation requires both) → tag, with the
`production` environment reviewer signing the attestation.
The copy-pasteable checklist lives in
[`docs/phase5_production_readiness.md` §10](docs/phase5_production_readiness.md#10-pre-live-promotion-checklist).

## Binance Futures testnet micro-pilot

`scripts/run_testnet_pilot.py` is the first end-to-end exchange loop: it signs
REST requests, validates orders against the live `exchangeInfo` filters, checks
the account balance, samples the L2 depth stream and exercises the order
lifecycle. It is deliberately **testnet-only** and defaults to a dry run.

```bash
# Optional: keep the keys in an untracked .env (see .env.example)
cp .env.example .env && set -a && . ./.env && set +a

export BINANCE_ENV=testnet          # optional: this is the default
export BINANCE_API_KEY=...          # from testnet.binancefuture.com
export BINANCE_API_SECRET=...

# Connectivity, filters, balance, 10 s of L2, and a validated dry-run order
python3 scripts/run_testnet_pilot.py

# Skip the WebSocket probe (useful on a REST-only host)
python3 scripts/run_testnet_pilot.py --skip-l2

# Place -> query -> cancel one passive limit order on the testnet
python3 scripts/run_testnet_pilot.py --skip-l2 --order-test submit

# Faster L2 sample with a longer order offset from mark price
python3 scripts/run_testnet_pilot.py --l2-seconds 5 --order-offset-bps 100
```

| Check | What it proves |
| --- | --- |
| REST connectivity | `/fapi/v1/ping` and `/fapi/v1/time`; request timestamps are offset by the measured clock skew |
| Exchange filters | `exchangeInfo` parsed for tick size, step size, minQty, minNotional and PERCENT_PRICE |
| Mark price | `/fapi/v1/premiumIndex` returns a usable reference price |
| Account balance | Signed `/fapi/v3/account` (falling back to `/fapi/v2/account` on 404) shows available USDT |
| L2 depth stream | `wss://stream.binancefuture.com/ws/<symbol>@depth20@100ms` delivers parsable best bid/ask frames |
| Order sizing | The requested quantity is step-aligned and satisfies minNotional at the current mark |
| Order lifecycle | `dry-run` (default) builds and validates the order; `submit` posts it, queries it, cancels it and confirms the terminal state |

Endpoints come from `BINANCE_ENDPOINTS` in `python_analytics/utils/config.py`:
testnet REST `https://testnet.binancefuture.com`, testnet WS
`wss://stream.binancefuture.com/ws`; production REST `https://fapi.binance.com`,
production WS `wss://fstream.binance.com/ws`.

**Exit codes:** `0` all checks passed, `1` at least one check failed, `2`
configuration or usage error (missing credentials, unknown `BINANCE_ENV`,
production requested), `130` interrupted.

**Safety rails.**

- `BINANCE_ENV=production` is refused before any connection is opened.
- Credentials come from the environment only; `repr()` and every log line mask
  the key and never print the secret.
- A BUY limit rests below mark (`--order-offset-bps`, default 50, must be
  positive) and a SELL above it, so the order is never immediately marketable.
- `--order-test submit` is the only mode that sends `POST /fapi/v1/order`, and
  it is explicitly logged as such.
- `BTCUSDT` enforces a `minNotional` (100 USDT on mainnet, 50 USDT on the
  current testnet), so a small `--quantity` can fail even though it meets
  `minQty`. The pilot computes the exact minimum (`required_quantity`) at the
  live mark and reports it instead of silently changing your size; on the
  current testnet mark, `0.001 BTC` clears the 50 USDT floor comfortably.

The client (`python_analytics/exchange/binance_futures.py`) is stdlib-only
(`urllib`, `hmac`, `hashlib`) and takes an injectable transport, so
`tests/python/test_binance_testnet.py` runs entirely offline. The testnet
WebSocket endpoint is also available to `scripts/ingest_l2.py` via
`data_ingestion.exchange: binance_futures_testnet`; public market data needs no
credentials.

#### Live-trading primitives

The pilot only needs "place, query, cancel". The same client also carries the
order semantics a real-money session needs, all of them offline-tested in
`tests/python/test_binance_live_primitives.py`:

| Call | What it does |
| --- | --- |
| `new_order(..., post_only=True)` | Sends `timeInForce=GTX`, so the venue rejects a quote that would cross instead of filling it as a taker |
| `new_order(..., reduce_only=True)` | Marks an exit order so it can only shrink a position |
| `new_order(..., close_position=True)` | Sends `closePosition=true` (stop-type orders only); refuses a quantity or `reduceOnly` |
| `stop_market_close_position(...)` | Venue-side `STOP_MARKET` on `MARK_PRICE` that flattens the position even if this process is gone |
| `close_position_market(...)` / `flatten(...)` | Reduce-only market exit; `flatten` reads the live position first and picks the opposite side |
| `positions(...)` / `open_orders(...)` | Typed `Position` / `OpenOrder` snapshots for start-up reconciliation; `positions(..., with_mark_price=True)` fills `markPrice` from `premiumIndex` (account rows omit it) and `notional` falls back to the entry price when no mark price is known |
| `set_leverage(...)` / `set_margin_mode(...)` | Explicit 1..125x and `ISOLATED`/`CROSSED`; `-4046` ("no need to change") is treated as success |
| `BinanceRateLimitError` | Raised for HTTP 429/418 so a caller can back off instead of retrying an order |

Nothing here changes the pilot: it still refuses `BINANCE_ENV=production`, and
`_request` still has no retry loop, so a failed submit can never be duplicated
silently.

### Live execution harness

`scripts/run_live_trader.py` is the runtime that turns those primitives into a
session you can restart: `python_analytics/live` keeps a SQLite book of record,
resolves ambiguous submits by `clientOrderId`, reconciles at start-up and owns
the kill path.

```bash
# Reconcile only: adopt what the venue holds, resolve what vanished
python3 scripts/run_live_trader.py --store state/live_orders.sqlite3

# One post-only quote, 5 bps from the book mid
python3 scripts/run_live_trader.py --submit --side BUY --quantity 0.002

# Kill switch: cancel the symbol and flatten the position
python3 scripts/run_live_trader.py --cancel-all
```

| Property | How it is enforced |
| --- | --- |
| No invisible orders | The intent row is committed before `POST /fapi/v1/order`; `clientOrderId` is the dedupe key |
| No guessing after a timeout | `BinanceTransportError`, and the `-1006`/`-1007` "execution status unknown" codes, → query by client id → `SUBMITTED` / `LOST` (`-2013`) / `UNKNOWN` |
| No resurrected orders | `FILLED`/`CANCELLED`/`REJECTED`/`EXPIRED`/`LOST` are terminal in the store |
| No blind cancels | Reconciliation adopts foreign orders instead of killing them |
| No silent unwind failure | `panic()` returns the failures it hit; the script logs them and exits non-zero |
| No real money without evidence | Production needs `--live` **and** a fresh positive alpha in `reports/production_readiness_report.json`; `--cancel-all` is exempt, because the kill switch must not depend on an arming flag |

The last row is the one that matters today: the committed study is `NO-GO`
(`net_pnl = -15.78 USDT` after fees), so `--live` exits `3` until either the
study turns green or someone writes `--override-alpha "<reason>"`, which is
recorded in the order store's audit trail. The remaining gaps — markout,
funding, a re-quoting loop, alerting — are listed in
[`docs/phase5_production_readiness.md` §11.3](docs/phase5_production_readiness.md#113-what-is-still-missing-before-this-is-a-business).

### Markout: is the quote being picked off?

PnL cannot tell "the fees ate the edge" from "the queue ran me over", and those
two failures need opposite fixes. `scripts/run_markout.py` marks every fill
against the mid that followed it:

```text
markout_bps(h) = side * (mid(t + h) - fill_price) / fill_price * 10_000
net_edge_bps(h) = markout_bps(h) - maker_fee_bps
```

```bash
# mark the study's own fills, window-for-window, on the engine's event clock
python3 scripts/run_markout.py

# mark a real session's fills (venue clock, ms) against a capture
python3 scripts/run_markout.py --fills state/fills.json --source 'data/raw_l2/*.jsonl.gz'
```

| Verdict | Meaning | Exit |
| --- | --- | --- |
| `GO` | markout at the primary horizon clears the fee with `t >= 1.96` | `0` |
| `MARGINAL` | positive after fees, but not statistically distinguishable from zero | `1` |
| `COSTLY` | the spread captured does not cover the fee | `1` |
| `ADVERSE_SELECTION` | the mid moves against the fills, so resting *adds* a cost | `1` |
| `INCONCLUSIVE` | fewer than `--min-samples` marked fills | `3` |

A simulated fill's local record time is a `steady_clock` stamp, so it cannot be
joined to an archive afterwards; the engine also carries the venue event time of
whatever filled it (`Fill.event_ts`), and `MarkoutReplayer` measures the fill
inside the replay against that, where the mid behind `h = 0` is the book that
produced the fill. Fills missing a sample within `--tolerance-ms` are reported
as truncated rather than interpolated, because an invented price is
indistinguishable from an edge once it reaches a report.

**What the committed study shows** (`python3 scripts/run_markout.py`):

| horizon | fills | mean bps | t | win % | net of 2 bps fee |
| --- | --- | --- | --- | --- | --- |
| 0 s | 230 | −0.334 | n/a (σ≈0) | 0.0% | −2.334 |
| 1 s | 227 | −0.328 | −4.96 | 36.1% | −2.328 |
| 10 s | 199 | −0.405 | −2.10 | 42.7% | −2.405 |
| 60 s | 55 | −0.709 | −1.04 | 45.5% | −2.709 |

Verdict `ADVERSE_SELECTION`: the average resting quote is marked 0.334 bps
*through* the mid, so it never captures the half-spread it is trying to earn.
That offset is what the fill rule produces, not a reading of the tape: this
sample carries no prints, and under the `touch` model a resting order fills on
the frame the opposite touch reaches its price, which leaves the mid exactly
half a spread away by construction. The doc's §11.4 makes the same point at
length. On the 55 fills that have a 60 s sample, the mean drifts from −0.334 to
−0.709 bps — a −0.38 bps move on the same fills, not a comparison of two
different populations. (The `h = 0` column is the starting point, not the 1 s
mean: on those same 55 fills the 1 s mean is −0.155 bps, so quoting −0.328 here
would mix a 227-fill average into a same-fill comparison.)

The 1 s markout totals −2.23 USDT against the study's gross PnL of −2.05 USDT.
This comparison is indicative only: the markout covers 227 of the study's 230
fills and values each at its own 1 s mid, while gross PnL uses the study's
position accounting and valuation times. The proximity of these totals does
not establish a ledger reconciliation; that would require the same fills and
valuation time in both calculations.

That is a strategy bug, not a plumbing bug: the paper engine fills a quote that
was posted inside the touch, while live `post_only` (`timeInForce=GTX`) would
have rejected it. Whether the quote can be made to rest passively *and* clear 2
bps is the open question for the next study — markout is what makes it
answerable.

### Edge curve: is there any distance at which the fee is paid back?

Every markout answer so far is a sentence about the fills a strategy happened to
get. `scripts/run_edge_curve.py` asks the prior question with no strategy in the
loop: for every print in a capture, an order resting `d` bps on the passive side
would have filled if the print reached it, and the fill is marked from the
quote's own price at each horizon. The output is edge against distance, plus the
fill rate still available out there.

On 3 h of the same instrument (643,077 prints over 12,000 s, 14 rolls, tick
`0.10`, median mid `76,900`, `2.0 bps` maker fee, session ended `-0.24 %`):

| distance bps | ticks | fills/min | 1 s mean | 1 s t | 10 s mean |
| --- | --- | --- | --- | --- | --- |
| 0.0125 | 1 | 1,831.6 | **−1.326** | −657.8 | −1.303 |
| 0.1 | 8 | 1,541.1 | −1.295 | −577.8 | −1.241 |
| 0.5 | 38 | 890.0 | −1.145 | −356.1 | −0.980 |
| 1 | 77 | 379.9 | −1.076 | −178.0 | −0.671 |
| 1.5 | 115 | 193.5 | −1.076 | −99.3 | −0.296 |
| 2 | 154 | 128.2 | −0.971 | −54.8 | **+0.121** |
| 3 | 231 | 92.2 | −0.522 | −13.2 | **+1.489** |

Verdict `NO_CLEARING_DISTANCE`. At a one-second holding time the gross markout is
negative at every distance the tape reaches — worst at the touch (`−1.33 bps`,
`t = −658`, 7.2 % of fills positive), improving monotonically with distance and
never clearing zero. A maker cannot quote its way out of the fee.

A round trip is what a maker actually does, and it pays the fee **twice**:
`2 * markout − 2 * fee`. On this tape that is `−5.94 bps` at the best 1 s
distance and `−3.76 bps` at the best 10 s one, so the fee ceiling — the per-side
rate at which a round trip breaks even — is `+0.12 bps` at a 10 s hold and
negative at 1 s. Retail is `2.0 bps`. The one cell that would clear a retail fee
(`4 bps` out, 10 s hold, `+0.25 bps` per round trip) is computed on **7.3 %** of
that distance's fills: coverage falls with distance because far fills happen in
the violent stretches where the depth stream breaks, and the curve now refuses to
let such a row decide (`min_coverage`, default 25 %).
[`docs/phase5_production_readiness.md` §11.8](docs/phase5_production_readiness.md#118-the-edge-curve-what-a-quote-earns-at-each-distance)
carries the arithmetic and §11.8.7 names what is missing for go-live.

### Continuous integration

`.github/workflows/testnet-pilot.yml` runs two jobs:

- **offline pilot tests** — on pushes and pull requests that touch the exchange
  surface: `flake8`, `pytest tests/python/test_binance_testnet.py`, the `--help`
  smoke test, and a check that missing credentials exit `2`. No network, no
  secrets, so it is safe on forks.
- **live testnet** — manual `workflow_dispatch` only, gated on the offline job.
  It runs against `https://testnet.binancefuture.com` using the
  `BINANCE_API_KEY` / `BINANCE_API_SECRET` **environment secrets** of the
  `testnet` environment.

Configure the secrets once (Settings → Environments → `testnet`; add a required
reviewer there if a dispatch should need a second pair of eyes):

```bash
gh secret set BINANCE_API_KEY --env testnet
gh secret set BINANCE_API_SECRET --env testnet
```

Then: Actions → **Testnet pilot** → Run workflow. The inputs mirror the CLI
(`order-test`, `symbol`, `quantity`, `l2-seconds`, `min-ticks`, `skip-l2`), plus
`confirm-submit` — which must be `true` before `order-test: submit` will place
an order — and `runner`, which lets you target a self-hosted label when Binance
answers GitHub's egress with HTTP 451 (restricted region). The workflow never
schedules the live job and never stores credentials in the repository; the run
log and its PASS/FAIL table are uploaded as the `testnet-pilot-log` artifact and
mirrored to the job summary.

## Troubleshooting

- **`BINANCE_API_KEY and BINANCE_API_SECRET not set`** — the pilot reads the
  environment only; `.env` is not auto-loaded. Run
  `set -a && . ./.env && set +a` (or export both variables) in the same shell.
- **`pilot verdict: FAIL` with `Account balance`** — the key lacks testnet USDT.
  Claim funds from the testnet faucet, or run with `--order-test none` for a
  read-only connectivity check.
- **`quantity ... is below minNotional`** — the pilot prints the exact minimum
  for the current mark; raise `--quantity` accordingly.
- **Order rejected with `-4061`** — the testnet account is in hedge mode; the
  pilot submits one-way orders, so switch the account to one-way mode (or add
  `positionSide` support) before using `--order-test submit`.
- **`ModuleNotFoundError: No module named 'python_analytics'`** — run scripts
  from the repository root (`python3 scripts/...`); the scripts bootstrap
  `sys.path` themselves.
- **`No module named 'hft_native'`** — build the extension first
  (`cmake -S cpp_engine -B build ... && cmake --build build`), and ensure the
  interpreter matches the `.so` tag (e.g. `cpython-39-darwin.so` needs 3.9).
- **Empty book / no fills in `--source mock`** — the ladder is centered on
  `settings.engine.center`; pass `--center` matching your asset's price range.
- **`Total Trades: 0`** — matching only fires when the market trades through a
  resting quote, so a feed whose price never moves cannot fill anything. Use
  `--source mock`, or an NDJSON file with a moving price (the bundled
  `data/sample_events.ndjson` contains 800 moving ticks). Check that the feed
  is actually advancing: `--offline --duration 5` should report thousands of
  trades with the default centre of 30000.
- **`SELF_TRADE_PREVENTED` ack reason** — the new quote would have crossed our
  own resting opposite quote. Use `--self-trade-prevention cancel_resting`
  (default) or cancel both sides before re-quoting, as the strategies do.
- **No WebSocket data** — install `websockets` (`pip install websockets`) or run
  `--offline` / `--source mock`; a missing client falls back to NDJSON.
- **`ingest_l2.py` exits with "the 'websockets' package is required"** — the
  capture client has no offline fallback by design: install the dependency, or
  use `--replay <archive>` to exercise the pipeline without a socket.
- **`zcat`/`gzip` reports "unexpected end of file" on a roll file** — the file is
  still open; gzip archives only become complete when the writer rotates them
  (`max_file_mb`/`rotate_seconds`) or the process shuts down cleanly.
- **`ingest_l2.py` writes nothing** — check `--dry-run`, and that
  `data_ingestion.enabled` is `true`; `--output-dir` overrides `storage_path`.
- **Latency looks negative** — the venue clock is ahead of the host clock
  (`clock_skew` counter in telemetry); sync with NTP/PTP before trusting
  latency numbers.
- **`--offline` shows an empty book** — confirm `data/sample_events.ndjson`
  exists and its prices are within ±4096 ticks of the configured center.
- **Order/position rejections** — inspect the `RejectReason` on the ack
  (`NOTIONAL_LIMIT`, `POSITION_LIMIT`, `PRICE_BAND`, ...) and relax the
  corresponding `engine.risk.*` value in `config/settings.yaml`.

## Design notes

- **Single writer per stage.** The feed thread owns `publish_depth`/`publish_trade`;
  the engine thread owns `poll` and `submit`; readers use the seqlock-protected
  BBO/depth snapshots.
- **No heap in the hot path.** Events travel by value through a fixed-capacity
  ring; books, orders and the hash table are pre-allocated.
- **Pre-allocated, heap-owned state.** `TradingEngine` carries roughly 3.7 MB
  of inline buffers; on constrained thread stacks construct it with
  `std::make_unique` (the Python binding does this already).
- **Fixed-point only.** `Price` and `Quantity` are 64-bit scaled integers.
  Convert at configuration/display boundaries only.
- **Risk before matching.** Every order passes through `RiskGuard` and the
  order manager before any fill is simulated.
- **Portfolio identity.** `equity == initial_cash + realized + unrealized` holds
  after every tick: a fill updates cash, inventory and the cost basis together,
  and a partial reduction keeps the remaining position's basis (only a flip
  through zero resets it).
- **Taker cross, never self-match.** Resting quotes fill only against external
  top-of-book liquidity, and self-trade prevention keeps a crossed pair of our
  own quotes from ever resting, so the simulator cannot print PnL against
  itself.
- **Capture stays off the receive path.** `ingest_l2.py` appends each frame to an
  in-memory buffer and compresses/writes it on a worker thread, so gzip and disk
  I/O never add jitter to the timestamps used for latency measurement.

## Limitations

- The **C++ engine** ships no exchange gateway; `publish_depth`/`publish_trade`
  are the adapter boundary. The Python `MarketDataFeed` provides a WebSocket
  client (Binance partial-depth) with an offline NDJSON fallback.
- The C++ `TradingEngine` only simulates market/taker orders at the touch; it
  does not match resting maker quotes. The Python `PaperMatchingEngine`
  provides maker matching, fees/rebates and mark-to-market PnL for paper
  trading and backtesting: a resting quote fills when the opposite side of the
  touch crosses it, capped by the visible size at that price (partial fills),
  with the execution logged to `trades_history`.
- Fees are modelled only in `PaperMatchingEngine` (configurable maker rebate /
  taker fee). The native `Fill.fee` remains zero because the C++ engine does
  not model fees.
- Feed sequence gaps are not repaired and there is no snapshot bootstrap:
  `first_update_id`/`final_update_id` are parsed for diagnostics, but the
  operator must reconnect or resynchronise after a gap.
- **C++** ships no exchange gateway; the live path is Python. There is no
  funding ledger or alerting yet, `scripts/run_live_trader.py` places at most
  one quote per run, and markout only covers fills you recorded (a quote that
  never filled is invisible to it). Order state *is* now persisted and
  reconciled across restarts (`python_analytics/live`,
  `state/live_orders.sqlite3`), arming real money is gated on the readiness
  report's alpha net rather than on a flag, and per-fill markout is available
  from `scripts/run_markout.py` — see
  `docs/phase5_production_readiness.md` §11.
- L2 capture is **raw**: `ingest_l2.py` archives exchange payloads but does not
  normalise them into `L2Book` updates or repair sequence gaps, there is no
  snapshot bootstrap, and one process handles one symbol/stream.
- The included market maker is an educational example, not a profitable or
  production-ready strategy.

Trade at your own risk. Test against paper accounts before connecting any live
venue.
