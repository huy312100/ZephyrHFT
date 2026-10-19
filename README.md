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
│   ├── strategies/         # strategy interface + market maker
│   ├── backtesting/        # replay pipeline and metrics
│   └── utils/              # YAML config and logging
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

# Python
python -m pytest
```

The C++ suite covers fixed-point parsing, SPSC concurrency, L2/L3 semantics,
risk rejections, latency buckets, JSON/WebSocket parsing, average-cost PnL,
IOC/FOK handling and the full engine flow. Python tests exercise the bindings,
strategy quoting and replay pipeline.

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

- This repository ships **no exchange WebSocket client**. `publish_depth` and
  `publish_trade` are the adapter boundary; connect your own feed or replay
  NDJSON files.
- Market orders are simulated against the top of book only; there is no
  matching engine for resting strategy orders.
- Fees, rebates and exchange acknowledgements are not modelled; `Fill.fee`
  stays zero and positions use average-cost accounting.
- Feed sequence gaps are not repaired and there is no snapshot bootstrap:
  `first_update_id`/`final_update_id` are parsed for diagnostics, but the
  operator must reconnect or resynchronise after a gap.
- There is no persistence, account reconciliation or exchange order gateway.
- The included market maker is an educational example, not a profitable or
  production-ready strategy.

Trade at your own risk. Test against paper accounts before connecting any live
venue.
