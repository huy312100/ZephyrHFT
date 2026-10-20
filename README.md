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
  (WebSocket / NDJSON), a maker matching engine, configurable fees/rebates and
  mark-to-market PnL.
- **Parameter optimizer** (grid search over `gamma`, `sigma`, `k`,
  `time_horizon`) scoring Sharpe, Sortino, drawdown and net PnL.
- **Latency benchmark** (`scripts/run_benchmark.py`) measuring tick-to-quote
  microseconds through the C++ `RiskGuard`.

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
│   └── run_benchmark.py      # tick-to-quote latency benchmark
├── config/settings.yaml    # engine, risk and strategy settings
├── data/sample_events.ndjson
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
(default `-1` = 0.01% rebate), `--taker-fee-bps` (default `5` = 0.05%).
Press `Ctrl-C` to stop cleanly: the loop cancels open orders and prints a
summary (total trades, win rate, Sharpe approximation, final PnL, max
drawdown).

The WebSocket source is `wss://stream.binance.com:9443/ws/btcusdt@depth20@100ms`;
if the `websockets` package is missing or the connection drops, the feed falls
back to `data/sample_events.ndjson` (looped) so the simulation keeps running.

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

## Troubleshooting

- **`ModuleNotFoundError: No module named 'python_analytics'`** — run scripts
  from the repository root (`python3 scripts/...`); the scripts bootstrap
  `sys.path` themselves.
- **`No module named 'hft_native'`** — build the extension first
  (`cmake -S cpp_engine -B build ... && cmake --build build`), and ensure the
  interpreter matches the `.so` tag (e.g. `cpython-39-darwin.so` needs 3.9).
- **Empty book / no fills in `--source mock`** — the ladder is centered on
  `settings.engine.center`; pass `--center` matching your asset's price range.
- **No WebSocket data** — install `websockets` (`pip install websockets`) or run
  `--offline` / `--source mock`; a missing client falls back to NDJSON.
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

## Limitations

- The **C++ engine** ships no exchange gateway; `publish_depth`/`publish_trade`
  are the adapter boundary. The Python `MarketDataFeed` provides a WebSocket
  client (Binance partial-depth) with an offline NDJSON fallback.
- The C++ `TradingEngine` only simulates market/taker orders at the touch; it
  does not match resting maker quotes. The Python `PaperMatchingEngine`
  provides maker matching, fees/rebates and mark-to-market PnL for paper
  trading and backtesting.
- Fees are modelled only in `PaperMatchingEngine` (configurable maker rebate /
  taker fee). The native `Fill.fee` remains zero because the C++ engine does
  not model fees.
- Feed sequence gaps are not repaired and there is no snapshot bootstrap:
  `first_update_id`/`final_update_id` are parsed for diagnostics, but the
  operator must reconnect or resynchronise after a gap.
- There is no persistence, account reconciliation or exchange order gateway.
- The included market maker is an educational example, not a profitable or
  production-ready strategy.

Trade at your own risk. Test against paper accounts before connecting any live
venue.
