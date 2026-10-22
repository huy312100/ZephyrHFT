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
- **Real-time L2 capture** (`scripts/ingest_l2.py`): asyncio WebSocket client
  that stamps every frame with `time.time_ns()`, reports venue-to-host latency
  percentiles and archives the raw payloads as gzip NDJSON roll files with
  backoff reconnection and graceful shutdown.

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
│   └── utils/              # YAML config and logging
├── scripts/
│   ├── run_paper_trading.py  # 60s live/mock paper-trading dashboard
│   ├── ingest_l2.py          # real-time L2 capture -> gzip NDJSON roll files
│   └── run_benchmark.py      # tick-to-quote latency benchmark
├── config/settings.yaml    # engine, risk, strategy and ingestion settings
├── data/sample_events.ndjson
├── data/raw_l2/            # capture output (created on demand, git-ignored)
├── tests/python/           # pytest integration tests
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
  cadence and reconnect backoff) used by `scripts/ingest_l2.py`.
- `strategy.*`: quote width, size, inventory cap and skew.
- `backtest.*`: replay limits and fill recording.

All decimals are parsed on the native side; no binary floating point is used
for prices. Invalid YAML raises `ConfigError` with the offending section.

## Tests

```bash
# C++ (after CMake build)
ctest --test-dir build --output-on-failure

# Python (all modules: bindings, strategies, simulator, optimizer)
python3 -m pytest -v
```

The C++ suite covers fixed-point parsing, SPSC concurrency, L2/L3 semantics,
risk rejections, latency buckets, JSON/WebSocket parsing, average-cost PnL,
IOC/FOK handling and the full engine flow. Python tests exercise the bindings
(`test_bindings.py`), the Avellaneda-Stoikov strategy (`test_strategy.py`), the
paper engine and feed (`test_paper_engine.py`) and the parameter optimizer
(`test_optimizer.py`).

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

## Real-time L2 ingestion (Phase 1)

`scripts/ingest_l2.py` is the capture front end: it subscribes to an exchange
depth stream over WebSocket and writes the raw frames to gzip-compressed NDJSON
roll files that later stages (feature building, replay, backtests) consume.

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
(default `-1` = 0.01% rebate), `--taker-fee-bps` (default `5` = 0.05%),
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
python3 scripts/run_benchmark.py --ticks 50000
```

Measures the tick-to-quote hot path in microseconds (p50/p99/p99.9): JSON
parse → C++ `L2Book` apply → Avellaneda-Stoikov quote generation → C++
`RiskGuard` validation. GC is disabled during the loop to eliminate collection
pauses.

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
  WebSocket parsing. Exposed to Python via `pybind11` as `hft_native`.
- **Strategy layer (`python_analytics/strategies/`)** — `MarketConfig`,
  `MicroPriceCalculator`, `AvellanedaStoikovStrategy`, and the `optimizer`
  grid-search utility. Owns all pricing mathematics.
- **Simulator (`python_analytics/simulator/`)** — `PaperMatchingEngine`
  (maker matching, inventory, cash, fees, PnL) and `MarketDataFeed`
  (WebSocket / NDJSON / synthetic). `PaperMatchingEngine` implements the same
  duck-typed surface as `TradingEngine`, so the strategy quotes against it
  unchanged.
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
`KillSwitch` is standalone today — attach it to `PaperMatchingEngine` (or a
live order router) through the cancel/flatten hooks.

## Troubleshooting

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
- There is no persistence, account reconciliation or exchange order gateway.
- L2 capture is **raw**: `ingest_l2.py` archives exchange payloads but does not
  normalise them into `L2Book` updates or repair sequence gaps, there is no
  snapshot bootstrap, and one process handles one symbol/stream.
- The included market maker is an educational example, not a profitable or
  production-ready strategy.

Trade at your own risk. Test against paper accounts before connecting any live
venue.
