# ZephyrHFT — Phase 5: Production Readiness Gate

**Automated GO / NO-GO validation of the Phase 1–4 invariants**

| Field | Value |
| --- | --- |
| Document ID | `ZHFT-DOC-P5` |
| Version | 1.0.0 |
| Status | Implemented / Verified |
| Depends on | Phase 1 (capture), Phase 2 (`paper_engine`), Phase 3 (chaos, heartbeat), Phase 4 (walk-forward) |
| Related | [`phase4_walk_forward.md`](./phase4_walk_forward.md), [`phase2_technical_specification.md`](./phase2_technical_specification.md) |
| Code | `scripts/generate_readiness_report.py` |
| Artefacts | `reports/production_readiness_report.{json,md}` |
| Workflow | `.github/workflows/readiness.yml` |

---

## 1. Purpose

Phase 5 replaces the release checklist with an executable one:

```bash
python3 scripts/generate_readiness_report.py
```

It re-measures every promise the earlier phases made — ingestion integrity, engine
latency, ledger coherence, chaos self-healing, dead-man's switch behaviour,
parameter locking, look-ahead prevention and OOS coverage — and prints **two
verdicts** against **explicit, named thresholds**:

```text
==============================================================================
  BUILD READINESS:     GO
  ALPHA (PROFITABILITY): NO-GO
  generated 2026-10-30T00:39:08+00:00
------------------------------------------------------------------------------
  BUILD: GO (16/16 gated checks passed)
  ALPHA: NO-GO (1/3 profitability checks passed at 2.0 / 5.0 bps maker/taker)
    ALPHA FAIL alpha.net_pnl: -15.7830 USDT (-0.0158%); -2.0500 before +13.7330 of fees (required >= 0 after modelled fees)
    ALPHA FAIL alpha.fee_wall: gross edge -0.299 bps vs 2 bps maker fee | break-even maker fee -0.299 bps (required gross edge > maker fee)
    Phase 1 - data ingestion               3/3 checks
    Phase 2 - paper execution              7/7 checks
    Phase 3 - resilience                   3/3 checks
    Phase 4 - out-of-sample validation     5/5 checks
    Alpha - profitability at modelled fees 1/3 checks
------------------------------------------------------------------------------
  wrote: reports/production_readiness_report.json
  wrote: reports/production_readiness_report.md
==============================================================================
```

"Shippable?" is a command with an exit code, not an opinion in a chat thread.
The answer is deliberately split in two, because "the machinery works" and "this
parameter set makes money" are different claims and one can be true while the
other is false (§1.3).

### 1.1 Design principles

| Principle | Consequence |
| --- | --- |
| **Measure, do not assert** | Every check re-runs the invariant and reports the measured value next to the threshold |
| **Determinism** | Engine, chaos and OOS *ledger* evidence come from a seeded synthetic book (`--evidence-events`, `--seed`), so the ledger, chaos and OOS counts reproduce on any machine; only Phase 1 integrity reads the real archive, while engine **latency and replay throughput** are host-sensitive (§3.3) |
| **Machinery ≠ alpha** | Profitability is measured on every run but gated separately (§1.3): a build can be production-ready while the alpha is not, and conflating the two hides which one is broken |
| **Fail loud, fail closed** | A failed check is named in `summary.blocking`, echoed as `BLOCKING …` on stderr, and returns exit `1` |
| **Evidence over logs** | The JSON report carries every raw measurement, so an auditor can recompute the verdict without re-running the study |

### 1.2 CLI contract

| Exit | Meaning |
| --- | --- |
| `0` | **GO** — every gated build check passed (alpha is reported but does not vote) |
| `1` | **NO-GO** — at least one gated check failed, or validation raised (`ParameterLockError`, `LookAheadError`, `WalkForwardError`) |
| `2` | Usage error: missing archive, unreadable settings, invalid argument |
| `3` | **ALPHA NO-GO** — build is `GO` but the parameter set does not make money at the modelled fees; returned **only** with `--require-positive-alpha` |

The gate writes `reports/production_readiness_report.json` and `.md` unless
`--no-report` is passed.

### 1.3 Two verdicts: build readiness vs alpha

A single "GO" cannot answer two different questions, so the gate answers both and
never lets one impersonate the other:

| Verdict | Answers | Gates the exit code by default | Contains |
| --- | --- | --- | --- |
| **BUILD** | Does the machinery keep the promises of phases 1–4? | yes (`0` / `1`) | the 16 gated build checks, plus informational rows |
| **ALPHA** | Does *this* parameter set earn after the fees a retail account actually pays? | no — only with `--require-positive-alpha` (`3`) | `alpha.net_pnl`, `alpha.fee_assumption`, `alpha.fee_wall` |

Both are always measured and both are printed; `summary.blocking` (build) and
`summary.alpha_blocking` (alpha) are separate keys, and a failing alpha check can
never appear in `summary.blocking`. That split is what stops a red strategy from
being excused as a red build — and stops a green build from laundering a red
strategy. CI therefore gates the build by default (a strategy regression should
not mask a broken engine), while the tag attestation requires **both** (§7).

The alpha block is deliberately blunt about costs:

| ID | Measures | Passes when |
| --- | --- | --- |
| `alpha.net_pnl` | Phase 4 aggregate `net_pnl` after fees, plus the same number with fees added back | `>= 0` after modelled fees |
| `alpha.fee_assumption` | The fee assumption itself (maker/taker bps, round-trip cost) | `maker_bps >= 0` — a rebate is not a retail rate |
| `alpha.fee_wall` | The same result in fee units: gross edge per unit of traded notional vs the maker fee, naming the break-even fee | `gross edge > maker fee`; emitted only when the study traded notional |

Fees are already inside realized/unrealized PnL (both come from the cash ledger),
so `pnl_before_fees = net_pnl + fees_paid` reports what the study would have earned
for free. When *that* number is also negative, no cost-model change will rescue
the strategy — the quoting logic itself does not earn, and the honest fix is the
signal, not the fee tier.

---

## 2. Gate inventory

16 gated build checks by default across 18 table rows (16 gates + 2
informational rows); enabling `--min-oos-sharpe` adds one gate for 17 gated
checks across 19 rows. The three alpha checks (§1.3) are reported alongside these
but are gated separately, so they are not counted here.

### 2.1 Phase 1 — data ingestion

| ID | Measures | Passes when |
| --- | --- | --- |
| `phase1.configuration` | `config/settings.yaml` parsed: tick size, centre, pre-trade state, kill-switch state | tick > 0, centre > 0, kill switch enabled, and `max_position > 0` **if** pre-trade risk is armed |
| `phase1.archive_integrity` | Full pass over `--source`: malformed frames and venue-sequence gaps | `0` malformed, `0` gaps, and `> 0` lines |
| `phase1.ingest_throughput` | Archive ingest rate | *informational* — recorded, never blocking |

Pre-trade guards are legitimately off in the deterministic replay profile, so
their state is reported but not gated; if they are on, their limits must be sane.
A sequence gap is treated as a **data defect, not a replay decision**: the gate
refuses to call an archive healthy when the venue skipped updates.

### 2.2 Phase 2 — paper execution

| ID | Measures | Threshold |
| --- | --- | --- |
| `phase2.engine_latency_p99` | Engine p99 over the evidence replay (C++ `apply_depth`: book update + mark + resting match) | `<= 50 µs` (`--max-engine-p99-us`) |
| `phase2.engine_latency_p999` | Engine p999 | `<= 500 µs` (`--max-engine-p999-us`) |
| `phase2.reanchor_latency_p99` | Engine p99 over `--reanchor-frames` frames that each move the priced window (the `recenter` path) | `<= 200 µs` (`--max-reanchor-p99-us`); fails unless the run actually re-anchored |
| `phase2.replay_throughput` | Depth events/s through the replay path | `>= 1,000` (`--min-throughput-fps`) |
| `phase2.ledger_residual` | `Δequity − realized − unrealized` | `<= 1e-2` (`--ledger-tolerance`) |
| `phase2.order_rejections` | Rejected / submitted orders | exactly `0` rejected |
| `phase2.fill_activity` | Filled / submitted quantity | `> 0 %` |

A rejected order is a **configuration error, not bad luck**: the strategy quoted
outside its own risk limits. Fill activity exists to prove the evidence book
actually trades — a book that cannot fill would make every PnL figure vacuous.

The evidence replay never leaves the window it was seeded on, so its p99 covers
`apply_depth` but never `recenter`. `phase2.reanchor_latency_p99` closes that
gap: it alternates between two books more than a window apart, so every frame
proves the anchor is wrong and every frame re-anchors a populated ladder. The
check also fails when the run never re-anchored, because a comfortable p99 for a
path that was never taken is not evidence.

Its ceiling is **not** the engine budget, and it is deliberately looser
(`--max-reanchor-p99-us`, 200 µs). A re-anchor moves the whole ladder -- two
`memmove`s of the quantity array plus the bitmap shift -- so it costs roughly
12× a seeded frame on the same host (§3.3), and its p99 tracks host memory
bandwidth rather than the code. Reusing the engine's 50 µs here would turn a
5 µs production run into a NO-GO on the re-anchor row alone while measuring the
memmove's share of a loaded machine.

### 2.3 Phase 3 — resilience

| ID | Measures | Passes when |
| --- | --- | --- |
| `phase3.disconnect_storm` | 200 frames through a seeded disconnect-storm profile (2 outage windows) | `>= 1` outage, `reconnects == outages`, `frames_forwarded + frames_dropped == frames_seen`, and both a forwarded and a dropped count `> 0` |
| `phase3.latency_injection` | 120 frames with 1–20 ms spikes at 25 % probability | `>= 1` spike, `max_latency_ms <= 20 ms` ceiling, every frame accounted for |
| `phase3.deadman_switch` | Native `HeartbeatMonitor` + `KillSwitch`, 500 ms timeout | trips on silence, blocks orders while halted, stays halted until an explicit reset, releases after reset, and can trip again |

The storm schedule is scaled to the run length
(`interval = max(5 × frame_interval, span / 3)`, disconnect = `0.4 × interval`),
because a burst that ends before the first silence window opens would pass a
chaos gate without ever exercising chaos.

### 2.4 Phase 4 — out-of-sample validation

| ID | Measures | Threshold |
| --- | --- | --- |
| `phase4.parameter_lock` | Pre/post-window lock read-backs from `reports/walk_forward.json` | `>= 1` check and `verified` |
| `phase4.no_look_ahead` | Chronology checks across window boundaries | `verified`, `chronology_verified`, `chronology_checks > 0` |
| `phase4.ledger_residual` | Aggregate residual including boundary settlements | `<= 1e-2 × windows` (2e-2 for the default two windows) |
| `phase4.window_coverage` | Windows and events in the study | `>= 2` windows (`--min-windows`) and `> 0` events |
| `phase4.oos_sharpe` | OOS Sharpe | *opt-in*: `>= --min-oos-sharpe` when supplied |
| `phase4.oos_sharpe_info` | OOS Sharpe and net PnL | *informational*; notes that the evidence book is a zero-drift random walk, so ~0 Sharpe is expected and a real archive is required to judge alpha |

By default the gate consumes the committed `reports/walk_forward.json`, so it
verifies the study that was reviewed. `--recompute-walk-forward` runs a fresh
study in-process instead (and `--window-events` then controls its slicing).

A reused artefact is trusted only when its recorded lock matches the parameters
the gate is actually running with. A study produced at another fee tier — or on
a different strategy/venue grid — is ignored and a fresh one is computed, with
the mismatched fields listed in `inputs.walk_forward_drift`; reporting an old
study's net PnL under today's fee labels would be exactly the dishonesty the
alpha checks exist to prevent. Decimal-string fields (`tick_size`, `center`,
`order_quantity`) are compared numerically, so `"0.5"` and `"0.50"` are the same
grid, while a fee change is always drift.

> **The two studies are not interchangeable.** The committed artefact is the
> bundled-sample study (Sharpe `-3.987`); a recompute runs over the *synthetic
> evidence book*, which is a zero-drift random walk and therefore scores ~`0.18`.
> Recomputing is how you check that the pipeline still produces a valid study —
> it is not how you re-derive a market edge. Gate profitability
> (`--min-oos-sharpe`) against the committed study, or run
> `run_walk_forward.py --source <real capture>` and pass the resulting artefact
> with `--walk-forward-json`.

### 2.5 Threshold table written into every report

| Threshold | Default | Flag |
| --- | --- | --- |
| `max_engine_p99_us` | `50.0` | `--max-engine-p99-us` |
| `max_engine_p999_us` | `500.0` | `--max-engine-p999-us` |
| `max_reanchor_p99_us` | `200.0` | `--max-reanchor-p99-us` |
| `min_throughput_fps` | `1000` | `--min-throughput-fps` |
| `ledger_tolerance` | `0.01` | `--ledger-tolerance` |
| `max_archive_malformed` | `0` | *fixed* |
| `max_archive_gaps` | `0` | *fixed* |
| `min_chaos_outages` | `1` | *fixed* |
| `min_oos_windows` | `2` | `--min-windows` |
| `min_oos_sharpe` | `None` | `--min-oos-sharpe` |

Recording the thresholds inside the artefact is what makes a past verdict
re-readable: a report that says `GO` under a 500 µs ceiling is not evidence of a
5 µs system, and the JSON says which ceiling applied.

---

## 3. Latency and throughput policy

### 3.1 What the percentile actually measures

The engine histogram is recorded in C++ around `apply_depth()`:

```cpp
const auto start = now_ns();
// ... ladder update, mark-to-market, match_resting()
latency_.record(now_ns() - start);
```

So `engine p99` covers **book update + mark + resting-order matching per depth
frame**, measured on the engine's own clock. It excludes the Python strategy hook
and the replay harness — exactly the part of the path the C++ core owns.

Percentiles are **bucket upper edges** from a 100 ns histogram: `p99 = 1.10 µs`
means the true p99 lies in `(1.00, 1.10] µs`. Every latency figure in this
project is therefore a conservative (upper-bound) reading.

### 3.2 The four latency tiers

| Tier | Metric | Target | Enforced by | Scope |
| --- | --- | --- | --- | --- |
| **Production budget** | engine p99 | **<= 5.0 µs** | `--max-engine-p99-us 5.0` | The ceiling we accept for a co-located trading host; pass the flag explicitly when promoting a build |
| **Default gate** | engine p99 | `<= 50 µs` | default of `--max-engine-p99-us` | Machine-tolerant tripwire that catches order-of-magnitude regressions on shared runners, where scheduler noise dominates |
| **Tail** | engine p999 | `<= 500 µs` | default of `--max-engine-p999-us` | Includes cache-cold first touches of the book ladder; a genuine tail-latency bug blows straight through it |
| **Re-anchor** | p99 over frames that move the window | `<= 200 µs` | default of `--max-reanchor-p99-us` | Its own budget, never the engine's: the frame translates the whole ladder, so it costs ~12× a seeded frame and tracks host memory bandwidth (§3.3) |

The comparison is inclusive: the gate passes when `p99_us <= max_p99_us`, so a
reading of exactly `5.0 µs` is a **GO** and `5.1 µs` is a NO-GO.

Supporting gates: replay throughput `>= 1,000` depth events/s
(`--min-throughput-fps`) and a ledger residual `<= 1e-2` (`--ledger-tolerance`).

The 50 µs default is deliberately **not** the performance target. It is the value
that fails only when something is genuinely wrong, so the nightly job does not go
red because a cloud runner was noisy. The 5.0 µs budget is the number the trading
host must hold, and it is applied at promotion time:

```bash
# Acceptance run on the host that will trade (expects GO at the production budget)
python3 scripts/generate_readiness_report.py \
  --max-engine-p99-us 5.0 --max-engine-p999-us 50 \
  --evidence-events 20000 --seed 7 \
  --report-dir reports --stem readiness_trading_host
```

Run it with the engine pinned to isolated cores, turbo/thermal behaviour
understood, and nothing else scheduled on the box. On a busy or unpinned machine
the same command returns **NO-GO**: that is the signal to characterise the host,
not to relax the budget.

### 3.3 Observed numbers

Every row below was produced by this code base on the same developer host
(macOS); only the run length and the background load differ.

| Observation | p50 | p99 | p999 | At 50 µs / 500 µs | At 5 µs / 50 µs |
| --- | --- | --- | --- | --- | --- |
| 20,000-frame evidence book, idle host | `0.4 µs` | `1.4 µs` | `8.1 µs` | GO | GO |
| 2,000-frame evidence book, idle host | `0.5 µs` | `3.2 µs` | `723.9 µs` | NO-GO (p999 only) | NO-GO |
| 2,000-frame evidence book, concurrent load | `0.5 µs` | `8.2–11.3 µs` | `93–724 µs` | GO, unless the tail trips | NO-GO |
| Phase 2 warm ingest harness (`docs/phase2_technical_specification.md`) | `0.9 µs` | `1.10–1.30 µs` | — | GO | GO |
| Re-anchor path, 1,000 frames that each move the window | `4.9–24.8 µs` | `5.8–63 µs` | `11–139 µs` | GO (own `200 µs` gate) | n/a (not a seeded frame) |

Reading the table:

- **p50 is stable** (0.4–0.9 µs) across every environment: the median frame costs
  what the design says it costs.
- **p99 moves with load** (1.4 → 11.3 µs), which is why the default gate is 50 µs
  and why the 5.0 µs budget belongs on a pinned host.
- **p999 is the noise canary.** The 723.9 µs row is a real, reproduced example:
  one idle 2,000-frame run failed `phase2.engine_latency_p999` at the 500 µs
  ceiling while three consecutive re-runs passed. A single p999 breach is a
  statement about the machine, not about the code — re-run on an idle host before
  investigating.
- **Re-anchoring costs one memmove.** The last row is the frame that moves the
  price window: the ladder is translated in place (two `memmove`s of the quantity
  array plus a 128-word bitmap shift each), so the p50 goes from ~1 µs to
  `4.9 µs` on a quiet host and `24.8 µs` on one at load 3–5, with the p99 moving
  from `5.8` to `63 µs`. The ratio to the seeded frame on the *same* host stays
  ~12–13× in both cases, which is the memmove's share of a frame and not a
  property of the translate; the absolute numbers scale with memory bandwidth.
  That is why the check has its own ceiling (`--max-reanchor-p99-us`, 200 µs)
  rather than borrowing the engine's 50 µs — a borrowed ceiling would have
  failed this row on the loaded host while the code was unchanged.
- **The 5.0 µs budget is achievable here** when the host is idle and the book is
  warm (1.4 µs at 20k frames) and fails when it is not. That is exactly how a
  budget should behave; it is not a number to chase on a laptop, and it is a
  statement about *seeded* frames — a re-anchor is a market-structure event, not
  a steady-state frame, and it is the only place where the whole ladder moves at
  once.

The warm Phase 2 harness remains the reference for what the engine costs when
nothing else is running: ~30–40 k frames/s at 0.9 µs p50 and 1.10–1.30 µs p99.

### 3.4 Wall-clock benchmark

`scripts/run_benchmark.py` measures end-to-end tick → parse → quote → risk in
Python and can gate on its own budget:

```bash
python3 scripts/run_benchmark.py --ticks 3000 --seed 7 \
  --max-p99-us 50 --max-p999-us 500
# exit 1 on breach, 0 when inside the budget
```

In CI this step is informational: a shared runner's p99 moves by an order of
magnitude with unrelated load, so a wall-clock failure there would describe the
runner. The authoritative latency gate remains the in-engine histogram
(`phase2.engine_latency_p99`); the benchmark budget belongs on the trading host,
next to the 5.0 µs engine budget.

---

## 4. Chaos and heartbeat invariants

### 4.1 Disconnect storm

| Step | Invariant |
| --- | --- |
| Outages happen | `outages >= 1` (default profile: 2) |
| Recovery is automatic | `reconnects == outages` |
| Nothing is lost or duplicated | `frames_forwarded + frames_dropped == frames_seen` |
| The scenario is actually exercised | `frames_forwarded > 0` **and** `frames_dropped > 0` |

A profile that forwards everything (or drops everything) fails: it would prove
only that the injector could be constructed.

### 4.2 Latency injection

| Step | Invariant |
| --- | --- |
| Spikes occur | `latency_events >= 1` |
| The ceiling holds | `max_latency_ms <= 20 ms` (the configured maximum) |
| Accounting holds | `frames_forwarded + frames_dropped == frames_seen` |

Spikes are drawn from `[1, 20] ms` at 25 % probability with a fixed seed, so the
scenario is adversarial but reproducible.

### 4.3 Dead-man's switch

The gate drives the **native** `HeartbeatMonitor` and `KillSwitch` through a full
lifecycle with a 500 ms timeout:

| # | Action | Required observation |
| --- | --- | --- |
| 1 | `beat(1.0 s)`, then `check_order_allowed()` | Orders allowed before the timeout |
| 2 | `poll(1.4 s)` | Does **not** trip before the deadline |
| 3 | `poll(1.6 s)` | Trips; `last_trip_reason` starts with `heartbeat_timeout` |
| 4 | `check_order_allowed()` while halted | Blocked |
| 5 | `monitor.reset(2.0 s)` **without** clearing the switch | Still blocked — silence alone must not resume trading |
| 6 | `switch.reset()`, then `check_order_allowed()` | Released |
| 7 | `beat(2.1 s)`, `poll(2.8 s)` | Trips again — a guard that cannot re-trip is dead |

Steps 5 and 7 are the ones worth keeping: recovery must be an **explicit operator
action**, and a recovered feed must still be able to halt. A switch that
auto-releases on the next heartbeat converts a data outage into an unguarded
position.

The readiness profiles run against a monotonic fake clock, so the checks are
deterministic and finish in milliseconds; real-time behaviour of the same
injectors is covered by the Phase 3 harness and the chaos test module.

---

## 5. CLI workflow

### 5.1 The commands that matter

```bash
# 1. The gate. Artefacts under reports/, exit 0 (GO) / 1 (NO-GO) / 2 (usage)
python3 scripts/generate_readiness_report.py

# 2. Gate with profitability blocking, using the committed OOS study
python3 scripts/generate_readiness_report.py --min-oos-sharpe 0.5

# 2b. Fresh in-process study instead of the committed artefact
#     (the synthetic evidence book has ~0 Sharpe, so pair this with no
#      profitability gate, or expect a deliberate phase4.oos_sharpe NO-GO)
python3 scripts/generate_readiness_report.py --recompute-walk-forward

# 3. Console verdict only (no artefacts, nothing to commit)
python3 scripts/generate_readiness_report.py --no-report

# 4. Verify Phase 1 integrity against a real capture instead of the sample
python3 scripts/generate_readiness_report.py \
  --source data/raw_l2/binance_futures_btcusdt_depth_100ms_20261021T173329Z_00001.ndjson.gz

# 5. Production latency budget, on the host that will trade (§3.2)
python3 scripts/generate_readiness_report.py \
  --max-engine-p99-us 5.0 --max-engine-p999-us 50 --evidence-events 20000

# 6. Write a named bundle for an audit packet
python3 scripts/generate_readiness_report.py \
  --report-dir reports/release-2026-10-24 --stem pre_live

# 7. Is there any distance from the mid where a quote pays its fee? (§11.8)
#    exit 0 a distance clears, 1 none does, 2 usage, 3 no priced print
python3 scripts/run_edge_curve.py \
  --source data/raw_l2/*.ndjson.gz --tick-size 0.1
```

### 5.2 How the evidence is produced

| Input | Used for |
| --- | --- |
| `--source` (default `data/sample_events.ndjson`) | Phase 1 integrity + ingest rate — the only check that reads real data |
| `--evidence-events` (default 2000) | Frames in the seeded synthetic book behind the Phase 2 and Phase 4 measurements |
| `--seed` (default 7) | Makes the evidence book bit-reproducible |
| `--chaos-frames` (default 200) | Frames pushed through each Phase 3 chaos profile |
| `--walk-forward-json` (default `reports/walk_forward.json`) | Phase 4 evidence, reused when present |
| `--recompute-walk-forward` | Ignores that artefact and runs a fresh study in-process (sliced by `--window-events`; `0` = ¼ of `--evidence-events`) |
| `--min-windows` (default 2) | Minimum OOS window count for `phase4.window_coverage` |

The synthetic book is re-stamped onto a fixed 250 ms grid rather than the wall
clock, so walk-forward windows look sequential and chronology checks are
meaningful. Without that, every frame would share a timestamp and the look-ahead
proof would be vacuous.

### 5.3 Flag reference

| Group | Flags | Defaults |
| --- | --- | --- |
| Evidence | `--source`, `--evidence-events`, `--seed`, `--chaos-frames`, `--window-events`, `--min-windows` | `data/sample_events.ndjson`, `2000`, `7`, `200`, `0`, `2` |
| Frozen parameters | `--gamma`, `--sigma`, `--k`, `--time-horizon`, `--inventory-limit`, `--ofi-sensitivity`, `--ofi-decay`, `--ofi-cap` | `0.1`, `0.02`, `5.0`, `60.0`, `5.0`, `0.0`, `0.95`, `3.0` |
| Venue / costs | `--market-type`, `--tick-size`, `--center`, `--maker-bps`, `--taker-bps`, `--initial-cash`, `--order-quantity` | `CRYPTO`, settings, settings, `2.0`, `5.0`, `100000.0`, `0.01` |
| Gates | `--max-engine-p99-us`, `--max-engine-p999-us`, `--max-reanchor-p99-us`, `--reanchor-frames`, `--min-throughput-fps`, `--ledger-tolerance`, `--min-oos-sharpe`, `--require-positive-alpha` | `50.0`, `500.0`, `200.0`, `1000`, `1000`, `1e-2`, `None`, off |
| Walk-forward evidence | `--walk-forward-json`, `--recompute-walk-forward` | `reports/walk_forward.json`, off |
| Output | `--report-dir`, `--stem`, `--no-report`, `--log-level` | `reports`, `production_readiness_report`, off, `ERROR` |

> **Defaults follow the committed fit.** The gate's frozen `--k` default is `5.0`,
> matching `config/walk_forward_lock.json`, even though `run_walk_forward.py`
> defaults to `k = 1.5`. This keeps a reused `reports/walk_forward.json` consistent
> with the parameters the gate would have used; override both together or not at
> all.
>
> **Fee defaults are the retail rate, not a rebate.** `--maker-bps` defaults to
> `2.0` and `--taker-bps` to `5.0` (USDⓈ-M VIP 0), matching
> `python_analytics.costs` and every script default. A negative maker fee is a
> liquidity-provider rebate that a retail account does not receive; passing one
> makes `alpha.fee_assumption` fail on purpose. Use
> `BinanceFuturesClient.commission_rate(symbol)` to record what the account
> actually pays instead of assuming a tier.

---

## 6. Reading the report

### 6.1 Markdown

`reports/production_readiness_report.md` opens with both verdicts, the
gated-check count, the evidence source and the walk-forward origin, then repeats
the checks grouped by phase. When something is blocking, a **Blocking failures**
table is inserted before the phase sections, so a reader never has to diff
PASS/FAIL rows to find the reason for a NO-GO:

```markdown
## Build verdict: **GO**

## Alpha verdict: **NO-GO**

### Alpha failures

| Check | Measured | Required |
| --- | --- | --- |
| `alpha.net_pnl` | -15.7830 USDT (-0.0158%); -2.0500 before +13.7330 of fees | >= 0 after modelled fees |
| `alpha.fee_wall` | gross edge -0.299 bps vs 2 bps maker fee | gross edge > maker fee |

### Blocking failures

| Check | Measured | Required |
| --- | --- | --- |
| `phase2.engine_latency_p99` | 8.2 us (p50 0.5 us) | <= 1 us |
```

A per-phase table follows, with the measured value, the threshold and the status;
informational checks are explicitly marked `PASS (info)` so they are never
mistaken for gates.

### 6.2 JSON

`reports/production_readiness_report.json` is the artefact to archive:

| Key | Content |
| --- | --- |
| `verdict` | Build verdict: `"GO"` / `"NO-GO"` |
| `alpha_verdict` | Alpha verdict: `"GO"` / `"NO-GO"` (never merged into `verdict`) |
| `generated_at` | ISO-8601 UTC |
| `summary` | `checks`, `gated`, `passed`, `failed`, `blocking[]` (build check IDs), `alpha_checks`, `alpha_failed`, `alpha_blocking[]` |
| `thresholds` | The exact ceilings in force for this run (§2.5), including `maker_bps`, `taker_bps`, `round_trip_cost_bps` and `require_positive_alpha` |
| `inputs` | Source archive, evidence description, parameter fingerprint, walk-forward origin (plus `walk_forward_drift` when a reused artefact was rejected), `costs` |
| `checks[]` | One object per check: `id`, `phase`, `title`, `measured`, `threshold`, `passed`, `gated`, `note` (`phase: "alpha"` marks profitability) |
| `evidence` | Raw measurements: `configuration`, `ingestion`, `engine`, `reanchor`, `chaos`, `deadman`, `walk_forward`, `alpha` |

`summary.blocking` is the machine-readable form of the same fact the console
prints, so a pipeline can gate on the JSON without parsing prose:

```bash
python3 scripts/generate_readiness_report.py --report-dir /tmp/gate >/dev/null 2>&1
python3 - <<'PY'
import json, pathlib
report = json.loads(pathlib.Path("/tmp/gate/production_readiness_report.json").read_text())
print(report["verdict"], report["summary"]["blocking"])
print(report["alpha_verdict"], report["summary"]["alpha_blocking"])
PY
```

### 6.3 Committed baseline (development host)

Produced by `python3 scripts/generate_readiness_report.py` on the bundled sample,
reading the committed `reports/walk_forward.json` (retail fees: 2 bps maker /
5 bps taker):

| Phase | Representative measurement | Status |
| --- | --- | --- |
| 1 — ingestion | `0` malformed / `0` gaps in 800 lines; ~496 k lines/s | PASS |
| 2 — execution | p99 `1.7 µs`, p999 `10.8 µs` (host-dependent — §3.3), re-anchor p99 `5.8–63 µs` over 1,000 moving frames, `16.7 k` depth events/s, residual `-1.1e-7`, `0/3480` rejected, fills `16.4 %` | PASS |
| 3 — resilience | 2 outages / 2 reconnects / 2 frames dropped; 36 spikes, max `19.8 ms`; switch tripped on `heartbeat_timeout: silence=600ms>=limit=500ms` | PASS |
| 4 — validation | lock `b29a62ed93caf69a` (4 checks); 1 chronology check across 2 windows; residual `+1.4e-7`; 2 windows / 800 events | PASS |
| **Build verdict** | **16/16 gated checks passed** | **GO** |
| Alpha (profitability) | `-15.7830 USDT` (`-2.0500` before `+13.7330` of fees) at 2/5 bps; Sharpe `-3.987` (info); `0/2` windows positive | **NO-GO (1/3)** |

Read that last row as the headline: **the machinery is ready and the strategy is
not**. At the rebate assumption this branch removed (`maker_bps = -1.0`) the same
800 events reported `+4.82 USDT` (Sharpe `1.31`, `2/0` windows positive; a
different lock, since the fees are part of the fingerprint); the fee change alone
was worth `20.60 USDT`, and even at *zero* fees the run loses `2.05 USDT`. That is
the honest state of the alpha workstream, and no fee-tier editing can turn it
green.

Latency and throughput vary per run (§3.3); the ledger, window, lock, chaos and
alpha counts are deterministic.

---

## 7. Continuous enforcement

`scripts/generate_readiness_report.py` is only a gate if something runs it
unattended. `.github/workflows/readiness.yml` does:

| Trigger | Purpose |
| --- | --- |
| Weekdays `03:17 UTC` (cron) | Nightly rehearsal against the committed lock and sample archive |
| Tag `v*` | Release evidence: the tag cannot be attested without a `GO` build **and** a `GO` alpha |
| `workflow_dispatch` | Manual run with `source`, `evidence-events`, `max-engine-p99-us`, `min-oos-sharpe` and `recompute-walk-forward` inputs |

The `GO / NO-GO gate` job runs, in order: `pip-audit` over the runtime
dependencies, a clean CMake build of the C++ core and pybind11 extension, CTest,
pytest, the Phase 4 replay of the **committed** lock (`run_walk_forward.py
--from-lock`), the informational latency benchmark, then this gate; it publishes
the verdict to the job summary and uploads `reports/` as the `readiness-reports`
artefact (30-day retention). Concurrency is non-cancelling, so a scheduled run
cannot be silenced by a push.

The `attest release verdict` job (tags only) downloads that artefact and fails
unless **both** headlines say `GO`: a release ships the strategy, not just the
engine, so a green build is not sufficient evidence for a tag. To gate a
non-tag pipeline on alpha as well, pass `--require-positive-alpha` (exit `3`).
Binding the `production` environment to required reviewers turns tag attestation
into a human sign-off rather than a rubber stamp.

> The evidence bundle — not the console log — is the audit trail. Export it if
> retention longer than 30 days is required.

---

## 8. Troubleshooting a NO-GO

`BLOCKING <id>: <measured> (required <threshold>)` names exactly what failed.
Map the ID to the action:

| Blocking ID | Likely cause | Action |
| --- | --- | --- |
| `phase1.configuration` | Kill switch disabled, non-positive tick/centre, or pre-trade armed with `max_position <= 0` | Fix `config/settings.yaml`; a NO-GO here means the shipped config is not self-consistent |
| `phase1.archive_integrity` | Malformed lines or a sequence gap in `--source` | Re-capture; a gap is a venue/stream defect, not something to "tolerate" |
| `phase2.engine_latency_p99` | Noisy host, debug build, or a genuine regression (e.g. allocation in the apply path) | Confirm an optimised build; re-run idle; only then relax `--max-engine-p99-us`, and record why in the release notes |
| `phase2.engine_latency_p999` | Cold book, or scheduler interference during the run | Re-run on an idle host first: a single p999-only breach on a shared machine is load, not code. Only if it reproduces should you profile the matching path — or raise the ceiling deliberately and record why |
| `phase2.replay_throughput` | Archive on a slow filesystem, or an interpreter running with tracing enabled | Move the archive to local disk; check `PYTHONPROFILEIMPORTTIME`/debuggers are off |
| `phase2.ledger_residual` | Accounting defect: a fill or a mark that does not reconcile | Reproduce with `run_walk_forward.py --window-events 0`, compare realized vs equity change |
| `phase2.order_rejections` | Strategy quoting outside its own risk limits (`NOTIONAL_LIMIT`, `PRICE_BAND`, …) | Align `inventory_limit` / `initial_cash` / `order_quantity` with the risk config; rejects are configuration errors |
| `phase2.fill_activity` | Evidence book cannot trade (price outside the ladder band) | Check `--center` / `--tick-size`; a book with no fills makes every PnL number vacuous |
| `phase3.disconnect_storm` | Injector fired no outage, or frames were lost/duplicated | Inspect `evidence.chaos.disconnect_storm`; a missing outage means the schedule is shorter than the run |
| `phase3.latency_injection` | No spikes, or a spike exceeded the configured ceiling | Same: read the counters in the JSON evidence |
| `phase3.deadman_switch` | Switch released without an explicit reset, or failed to re-trip | Treat as a **safety defect**: do not trade until it is fixed and the gate is green |
| `phase4.parameter_lock` | Lock not verified, or zero checks (no windows) | Re-run `run_walk_forward.py --from-lock`; check the artefact is the committed study |
| `phase4.no_look_ahead` | No chronology checks (single window / no `E` timestamps) | Use ≥ 2 windows and an archive with exchange timestamps |
| `phase4.ledger_residual` | Boundary settlement mismatch across windows | Recompute the study (`--recompute-walk-forward`); inspect `boundary_settled_pnl` |
| `phase4.window_coverage` | Fewer windows than `--min-windows` | Lower `--window-events`, add archives, or lower `--min-windows` deliberately |
| `phase4.oos_sharpe` | Opt-in profitability gate not met | This is the alpha talking, not the machinery: do not weaken the threshold to ship |
| `alpha.net_pnl` (alpha block, never build-blocking) | The frozen parameter set loses money after modelled fees | Read `evidence.alpha.pnl_before_fees`: if it is also negative the quoting logic does not earn and the signal needs work; if it is positive, the spread is merely smaller than the fee — widen it or stop quoting that book. Never fix this by assuming a rebate |
| `alpha.fee_assumption` | `--maker-bps` is negative (a rebate tier) | Pass the retail rate (`2.0`) or the account's real `commissionRate`; a negative maker fee flatters every passive fill |

Other failure shapes:

| Output | Exit | Meaning |
| --- | --- | --- |
| `error: source archive not found: …` | `2` | `--source` path is wrong — no partial verdict is produced |
| `validation failure: …` | `1` | The Phase 4 study itself raised (`ParameterLockError` / `LookAheadError`); the gate refuses to grade an invalid study |
| `ALPHA NO-GO: the machinery is ready but this parameter set does not make money…` | `0` (or `3` with `--require-positive-alpha`) | Build is fine, strategy is not. Expected at retail fees; do not promote the parameter set |
| `ModuleNotFoundError: hft_native` | — | Build the extension first (`cmake -S cpp_engine -B build -DHFT_BUILD_PYTHON=ON && cmake --build build`) |

---

## 9. Limitations

| Limitation | Consequence |
| --- | --- |
| Engine, chaos and OOS evidence come from a **synthetic book** | The verdict proves the machinery, not a market edge. `--source` only drives the Phase 1 integrity check, so judging alpha requires a Phase 4 study over a real capture (`run_walk_forward.py --source <capture> ...`) passed explicitly with `--walk-forward-json <path>` — do not rely on the default `reports/walk_forward.json` — plus an explicit `--min-oos-sharpe` |
| The synthetic book is a zero-drift random walk | Expect Sharpe ≈ 0 with a fee drag; the committed sample reports `-3.987` (negative because it is charged 2 bps of maker fees on both sides), which is a cost-model statement, not a market verdict |
| Engine p99 is host-sensitive | Two runs on the same machine, differing only in background load, move p99 from `1.4 µs` to `11.3 µs` and p999 from `8 µs` to `724 µs` (§3.3). The 5.0 µs budget is only meaningful with the cores and the load characterised |
| Chaos runs on a fake monotonic clock | Deterministic and fast, but it does not exercise the socket, the TLS stack or the kernel's retransmit timers |
| The dead-man's switch is driven directly | It proves monitor + kill-switch semantics, not the full live feed loop that must eventually beat into it |
| The live layer (`python_analytics/live`, §11) gives a durable order store, reconciliation and a kill path, but it cannot manufacture an edge | It bounds the damage of being wrong; it does not make the strategy right. That is why arming real money is gated on a fresh alpha `GO` rather than on the build verdict |
| Markout exists (§11.4) but funding accounting and alerting do not | Adverse selection is now measured per fill; a live session that *looks* flat can still be losing to funding or to an unresolved `UNKNOWN` order nobody was paged about |
| `pip-audit` is a separate CI step | Its result is not part of the GO/NO-GO verdict; read the workflow log (one dev-only advisory is currently ignored) |
| A report is a point-in-time snapshot | Re-run per release **and per host**; a `GO` from last month on another machine is not evidence about this one |

The gate reduces the set of things that can go wrong before real money; it does
not replace a staged rollout, position limits on the venue, or a paper-trading
window against the live feed.

---

## 10. Pre-live promotion checklist

Run in order, from a clean checkout, and archive the output of steps 3 and 4:

```bash
# 1. Unit and system tests
python3 -m pytest
ctest --test-dir build --output-on-failure

# 2. The committed fit replays verbatim (no flags to drift)
python3 scripts/run_walk_forward.py \
  --source data/sample_events.ndjson --window-events 400 \
  --from-lock --lock-file config/walk_forward_lock.json

# 3. The gate: must print GO and exit 0
python3 scripts/generate_readiness_report.py

# 4. Same gate on the trading host, at the production latency budget,
#    written to its own stem so it does not overwrite step 3
python3 scripts/generate_readiness_report.py \
  --max-engine-p99-us 5.0 --max-engine-p999-us 50 --evidence-events 20000 \
  --report-dir reports --stem readiness_trading_host

# 5. Archive both bundles, then commit the refreshed artefacts
#    (reports/ is git-ignored, so -f is required)
git add -f reports/production_readiness_report.json reports/production_readiness_report.md \
           reports/readiness_trading_host.json reports/readiness_trading_host.md
git commit -m "readiness: GO on <host> $(date -u +%Y-%m-%dT%H:%MZ)"
```

| Gate | Requirement |
| --- | --- |
| `pytest` / `ctest` | Green; no skipped phase-specific tests |
| Walk-forward replay | Exit `0`, lock fingerprint unchanged |
| Readiness (default) | `GO`, exit `0`, bundle archived |
| Readiness (trading host) | `GO` at `--max-engine-p99-us 5.0` |
| Tag | Only after the above; the `production` environment reviewer signs the attestation |

If the fit changes, regenerate the lock with `--write-lock` and re-review the
study — never hand-edit `config/walk_forward_lock.json`. A study that no longer
matches its lock is not evidence, and the gate will say so.

---

## 11. Live execution layer

`python_analytics/live` turns "the strategy wants a quote" into an order the
process and the venue agree exists. It is the part that decides whether a bad
day is a bad day or an unbounded loss.

### 11.1 What it guarantees

| Guarantee | Mechanism |
| --- | --- |
| An intent is durable before the venue sees it | `OrderStore.record_intent` commits `INTENT` + `client_order_id` first; a crash mid-submit leaves a row, not a surprise |
| A timeout is never guessed | After `BinanceTransportError` the order is queried by `clientOrderId`; found → `SUBMITTED`, `-2013` → `LOST`, anything else → `UNKNOWN` |
| An ambiguous submit is resolved, not rejected | HTTP 5xx, `-1006` and `-1007` all mean "execution status unknown": the row is marked `UNKNOWN` and queried by `clientOrderId`; only an ordinary 4xx is recorded as `REJECTED`. A failed lookup leaves `UNKNOWN` for reconciliation to chase, never `REJECTED` |
| Terminal states are terminal | `FILLED`, `CANCELLED`, `REJECTED`, `LOST` refuse further transitions (`LiveStoreError`) |
| Orders this process did not place are adopted, not ignored | `reconcile()` records them as `ADOPTED` — passively; it never cancels a manual order |
| Orders that vanished are resolved | `reconcile()` queries each stale id and writes the venue's answer |
| The kill path tells the truth | `panic()` cancels all, flattens only if a position exists, resolves each working order after a successful cancel, reports an order the venue still holds as a failure, leaves an order `UNKNOWN` when the cancel or the follow-up query fails, returns the list of failures |
| Real money needs evidence, not a flag | `require_alpha_go()` reads `reports/production_readiness_report.json`: every `alpha.*` check must pass, `evidence.alpha.net_pnl > 0`, the timestamp must be within `--max-report-age-hours` |

### 11.2 Running it

```bash
# testnet: reconcile only (safe, no orders)
export BINANCE_ENV=testnet BINANCE_API_KEY=... BINANCE_API_SECRET=...
python3 scripts/run_live_trader.py --store state/live_orders.sqlite3

# testnet: one post-only quote, 5 bps from the mid
python3 scripts/run_live_trader.py --submit --side BUY --quantity 0.002 --offset-bps 5

# kill switch: cancel the symbol and flatten (works on any environment)
python3 scripts/run_live_trader.py --cancel-all

# production: refused unless --live AND the alpha gate is green
python3 scripts/run_live_trader.py --live
```

Exit codes: `0` done/clean, `1` drift, an unresolved order or a rejected submit
that was not a post-only cross (`-2010` "would immediately trigger", or the
futures `-5022` "could not be executed as maker"), `2` configuration or
environment refusal, `3` the alpha gate refused to arm, `130` interrupted. On
an interrupt, cancellation and flattening are **attempted**, not guaranteed: a
failed clock sync, cancel or flatten is logged on the way out, so inspect the
venue before assuming the book is flat.

### 11.3 What is still missing before this is a business

1. **An edge.** The committed study is `NO-GO`: `net_pnl = -15.78 USDT` over 229
   fills with `2.0 bps` maker / `5.0 bps` taker charged. Nothing in this layer
   changes that sign. Real capture + a re-run study is the work.
2. **~~Markout.~~** Shipped in §11.4. It answers the question item 1 cannot: the
   study's maker fills are marked at `-0.33 bps` one second after the trade, so
   the quote is being picked off and the fee is not the only problem.
3. **Funding.** Perpetual funding is charged every 8h and is currently not in
   the ledger; a carry-heavy session will drift from the paper model.
4. **A loop.** `run_live_trader.py` places at most one quote. Re-quoting,
   inventory limits and a per-session loss cap belong in a supervisor that
   calls `LiveExecutor`, not in this script.
5. **Alerting.** `panic()` returns failures; nothing pages a human yet.

Until 1, 3 and 4 exist, a production run is `--cancel-all` plus a manual smoke
test under `--override-alpha`, and the override sentence is the audit record
that says so.

### 11.4 Markout: is the quote earning the spread or being run over?

`python_analytics/backtesting/markouts.py` marks every fill against the mid that
followed it, signed by side and net of the fee it paid:

    markout_bps(h)  = side * (mid(t + h) - fill_price) / fill_price * 10_000
    net_edge_bps(h) = markout_bps(h) - maker_bps

`h = 0` is spread capture (a maker that rests at the bid should show roughly
`+half_spread`); longer horizons are adverse selection, the mid leaving before
inventory can be worked out. `scripts/run_markout.py` reports the distribution
per horizon, per maker/taker and per maker side, and prints one verdict.

| Verdict | Test | Exit |
| --- | --- | --- |
| `GO` | `markout - fee > --min-edge-bps` at the primary horizon with `t >= 1.96` | `0` |
| `MARGINAL` | positive after fees, `t < 1.96` | `1` |
| `COSTLY` | the captured spread does not cover the fee | `1` |
| `ADVERSE_SELECTION` | mean markout `< 0` with `t <= -1.96` | `1` |
| `INCONCLUSIVE` | fewer than `--min-samples` marked fills | `3` |

Three design decisions matter more than the statistics:

* **Simulated fills are marked in-loop, never joined.** A fill carries two
  clocks. `Fill.timestamp` is the local record time, a `steady_clock` stamp with
  an arbitrary epoch, so it can never be looked up in a capture;
  `Fill.event_ts` is the venue event time of whatever filled the order (the
  depth frame, or the trade print under the tape model), copied from the event
  that the engine matched against. `MarkoutReplayer` measures the fill during the
  replay instead — the mid behind `h = 0` is the book that produced the fill, and
  later horizons are later frames of the same feed — stamping each fill with
  `event_ts` so a `trade`-model fill is placed at its print, not at the depth
  frame that preceded it. Venue-clock fills (a real session exported as JSON) go
  through `--fills` instead, where the join is legitimate because both sides
  carry the exchange timestamp.
* **A missing sample is truncated, not interpolated.** A horizon with no sample
  inside `--tolerance-ms` is counted and excluded. Carrying the last mid forward
  across a feed outage is how a hole becomes a fake edge.
* **Units are recovered from the frame, not from the magnitude.** Two clocks
  reach the hook: `Bbo.exchange_ts` carries the venue's raw `E` in
  **milliseconds** despite the C++ type being named `Nanos`, while an archive
  without timestamps advances a synthetic nanosecond counter. The replayer
  chooses by asking whether `exchange_ts` is non-zero, because a magnitude rule
  would flip units mid-replay (the synthetic counter passes the millisecond
  range after 1000 frames). `to_nanos()` never routes a nanosecond value through
  a float, because `1.7e18` is beyond float64's exact integer range.

#### What the committed study shows

`python3 scripts/run_markout.py` on `data/sample_events.ndjson`, 230 maker fills
(0 taker), lock `b29a62ed93caf69a`, fee `2.0 bps`:

| horizon | fills | mean bps | median | t | win % | net of fee |
| --- | --- | --- | --- | --- | --- | --- |
| 0 s | 230 | −0.334 | −0.333 | n/a (σ≈0) | 0.0% | −2.33 |
| 1 s | 227 | −0.328 | −0.334 | −4.96 | 36.1% | −2.33 |
| 10 s | 199 | −0.405 | −0.334 | −2.10 | 42.7% | −2.41 |
| 60 s | 55 | −0.709 | −1.000 | −1.04 | 45.5% | −2.71 |

Verdict: **`ADVERSE_SELECTION`**. Two readings, both uncomfortable:

* **The entry offset is exactly half the spread.** Mean entry offset is
  `−0.334 bps` against a `0.667 bps` spread, and the `0 s` distribution has
  `sd = 0.000` and `t = −48289`. A statistic that lands on half the spread with
  zero variance on every fill is not measuring a market: it is measuring the
  fill rule. Under the legacy `touch` model a resting order fills on the frame
  the opposite touch reaches its price, so the mid is *always* half a spread
  away by construction. §11.5 is the rest of this argument.
* **It keeps getting worse on the same fills.** Over the 55 fills that have a
  60 s sample at both ends, the mean moves from `−0.334 bps` to `−0.709 bps`, a
  `−0.38 bps` drift. Separately, the `1 s` markout totals `−2.23 USDT` against
  the study's gross PnL of `−2.05 USDT`; that comparison is indicative only —
  the markout covers 227 of the study's 230 fills and values each at its own
  1 s mid, while gross PnL uses the study's position accounting and valuation
  times. The proximity of these totals does not establish a ledger
  reconciliation; that would require the same fills and valuation time in both
  calculations.

Neither reading can be fixed by tuning costs. The next study has to post a
quote that actually rests behind the touch (and model post-only rejection), or
find an edge large enough to survive being filled adversely — and it has to be
measured by a fill model that can distinguish the two (§11.5).

#### Known limits of the measurement

* Fills are a selected sample: a quote that never filled cannot be marked, and
  the fills that did are exactly the ones the market chose to give you.
* Above and beyond the fee, nothing else is priced in — funding, queue position
  and inventory risk are outside the number.
* With `--frames-per-hook n`, a fill and its mid are both attributed to the hook
  that ended the batch, so horizons shorter than `n` frames are not measurable.
  Keep the study's `--frames-per-hook` when comparing to its report.

### 11.5 Fill models: the difference between a market and a rule

Every profitability number in this document is produced by the paper engine's
matching logic, so the matching logic decides the answer. Until it was made
selectable, there was only one model, and it had a structural bias:

> `match_resting()` filled the **entire** resting quantity at the order's price
> on the frame the opposite touch reached it. It never filled on the far more
> common event — a trade printing at the quote's own price while the book is
> unchanged — because a depth snapshot cannot show one.

Selecting only the frames where the market moved *through* the quote is
selecting adverse fills by construction. The `−0.334 bps` markout in §11.4 is
that rule printed back out, with `sd = 0`.

The engine now implements three models, selected with `--fill-model`:

| Model | A resting order fills when | Needs | Bias |
| --- | --- | --- | --- |
| `touch` (default) | the opposite touch reaches its price; full size, immediately | depth only | fills only on price moves through the quote → adverse by construction |
| `queue` | displayed depth at its own price is consumed, after the queue ahead of it | depth only | blind to quotes resting inside the spread, which never appear in the book → under-fills |
| `trade` | an aggressor print at or through its price, after the queue ahead of it | depth **and** trade prints | the reference model: queue priority, partial fills, no book-crossing guess |

`touch` remains the default so committed studies stay reproducible, but a study
whose fills were produced by it cannot answer a question about maker economics.

#### Measured: the same study, three models

The committed lock (`b29a62ed93caf69a`), `data/sample_events.ndjson`,
`--window-events 400`, retail fees:

| Fill model | fills | fill ratio | net PnL | trade prints seen |
| --- | --- | --- | --- | --- |
| `touch` | 229 | 16.47 % | −15.78 USDT | 0 |
| `queue` | 0 | 0.00 % | 0.00 USDT | 0 |
| `trade` | 0 | 0.00 % | 0.00 USDT | 0 |

Two facts fall out, and they are the honest state of the alpha workstream:

1. **The archive has no trades.** `trade_events` is the count of prints the
   engine consumed; it is `0`, so the tape model correctly refuses to invent
   fills. `data/sample_events.ndjson` and the 93-frame `data/raw_l2` capture are
depth only. **A maker-fill study is not possible on either.**
2. **The depth-only queue model fills nothing here**, because the strategy posts
   inside the spread, where no displayed depth exists to be consumed. That is
   not a bug in the model; it is the model telling you what L2 snapshots cannot
   see.

So the choice is: capture `trade` alongside `depth@100ms` and study fills
from the tape (§11.5.2), or keep quoting on a model that answers a different
question.

#### 11.5.1 Quotes must exist on the venue's grid

The audit that produced this section also found a second defect in the same
path. `adjusted_quotes()` rounded to the tick **only** for `STOCK` and
`COMMODITIES`, so every crypto quote was priced between ticks:

```text
raw quotes: 30000.80077572704 / 30001.19922427296   (tick 0.5)
```

An order at `30000.80077572704` cannot be represented in the ladder and a real
venue rejects it. Quotes are now snapped for every venue, **outwardly** — bid
down, ask up — so snapping can only widen the market and can never turn a valid
two-sided quote into a crossed or degenerate one. Nearest-tick rounding would
collapse both sides onto one price whenever the modelled spread is under a tick,
which is exactly the case for the committed fit (`~0.20` against a `0.5` tick).

This did not change the build verdict; it moved the study by `+0.69 USDT`
(`−16.47` → `−15.78`), and `reports/walk_forward.json` has been regenerated so
the committed artefact matches the code that produced it. A `GO` from an
artefact older than the strategy is not evidence about the strategy.

#### 11.5.2 What the next study needs

The tape model is only as good as its input:

| Requirement | Why |
| --- | --- |
| `trade` captured next to `depth@100ms` | without prints, `--fill-model trade` fills nothing and says so. `@aggTrade` is documented but silent on this endpoint (§11.6.1), so the stream to archive is `trade` |
| Hours-to-days of capture, not 9 seconds | the repro `data/raw_l2` file holds 93 frames; it cannot carry a study |
| A spread that clears the fee | a `0.667 bps` book against `2 bps` maker is a structural loss: you need `> 4 bps` of round-trip edge or a fee tier near zero |
| `--fill-model trade` recorded with the artefact | `aggregate.fill_model` now carries it, so a net PnL is never read without its model |

Run the comparison yourself:

```bash
for m in touch queue trade; do
  python3 scripts/run_walk_forward.py \
    --source data/sample_events.ndjson --window-events 400 \
    --from-lock --lock-file config/walk_forward_lock.json \
    --fill-model $m --report-dir reports/fillmodel --stem wf_$m
done
python3 scripts/run_markout.py --fill-model trade
```

The economic ceiling is worth stating plainly: at USDⓈ-M VIP 0 a maker pays
`2 bps` per fill and earns half the spread. On a book quoting `0.667 bps`, a
passive strategy **cannot** be profitable from spread capture alone, whatever
the fill model says. The fee tier — not the quoting logic — is the binding
constraint, and no amount of parameter search moves it.

### 11.6 The first study on a real tape

§11.5 ended with a requirement: capture the tape, then re-run. That capture is
now implemented (`data_ingestion.aux_streams`), and this section records what
happened when it was pointed at the live venue — including one defect that no
synthetic archive could ever have exposed.

#### 11.6.1 Capturing the tape: `@aggTrade` is silent, `@trade` is not

The client now subscribes to the *combined* stream
(`<endpoint>/stream?streams=<sym>@depth@100ms/<sym>@trade`) so prints and depth
share one socket, in one arrival order, in one archive. Two facts were
established empirically rather than from documentation:

| Subscription | Frames in 5 s | Result |
| --- | --- | --- |
| `btcusdt@aggTrade` | 0 | accepted, then silent — also inside `/stream?streams=` |
| `btcusdt@trade` | ~40/s | individual prints, `e=trade`, `p/q/T/m` complete |
| `btcusdt@bookTicker` | ~40/s | works (not captured: `depth` gives the same touch) |

So `aggTrade` is documented but empty on this endpoint, while `trade` delivers.
Both parse identically (`JsonFeedParser::classify` accepts `trade` and
`aggTrade`), which is why the config names `trade` and the flag still accepts
anything the venue serves. A depth-only capture now logs a warning at startup
instead of letting `--fill-model trade` fail silently later.

The combined envelope `{"stream":..,"data":..}` is sliced off **textually**
before archiving, so each line stays the venue's own payload bytes. Round-tripping
through `json` would reformat numbers and the archive would stop being ground
truth; the `stream` name is redundant because the payload carries `s` itself.

A 151 s capture (7,756 records: 1,466 depth + 6,290 prints, 1.34 MiB gzipped)
is committed at
`data/raw_l2/binance_futures_btcusdt_depth_100ms_trade_20261029T163929Z_00001.ndjson.gz`.
It is a smoke test, not a study.

#### 11.6.2 The ladder was quoting against nothing

Feeding that archive to the engine produced `bbo = (0, 0)` on **every frame**:
the book was empty and the mark price stayed pinned at the configured `30000`.

The cause is structural, not a bug in the feed. The price ladder is a
direct-indexed array of `8192` ticks anchored at `engine.center`, so with
`tick_size=0.50`, `center=30000` it covers only `[27952, 32048]`. The live market
was at `81 200`. Every level was rejected by `slot_for`, `top()` kept returning
an empty book, and the study reported a plausible flat run — a failure that is
indistinguishable from a quiet market and can survive an entire research cycle
unnoticed. `TradingEngine` (the live path) had the identical defect: it
configured the book once from `EngineConfig` and never re-anchored.

Both engines now re-anchor. `reframe_anchor()` (in `orderbook.hpp`) moves the
window onto the frame's own touch when the frame carries a price the window
cannot hold. Four properties matter:

- **The test is the frame, not the book.** The first version of this rule only
  fired while the book was *blind* (one side empty). That leaves the second
  failure open: once the ladder holds levels it is no longer blind, so a market
  that leaves the window again is rejected in full while the old, believable
  BBO keeps being published to the quoting and matching paths — and
  `unpriced_frames` stays at zero, so nothing looks wrong. A populated book
  that cannot hold the frame is exactly as useless as an empty one, so the rule
  now fires for it too.
- **The tick is never guessed.** It is the venue's grid; a tick that does not
  divide the venue's prices is a configuration error. Faking a finer grid would
  hide it, so instead `metrics.unpriced_frames` exposes it.
- **The rule is idempotent.** Once the anchor equals the frame's touch it returns
  "no move", so an unrepresentable grid cannot make the book clear itself on
  every frame.
- **Depth is carried across.** `recenter()` keeps every level the new window can
  still hold instead of dropping the lot. Re-anchoring is a constant slot shift,
  so it is a `memmove` of the ladder plus a shift of the occupancy bitmap and a
  recount of the live set — no allocation, no per-level copy, and no cap on how
  many levels survive (a 200-level-per-side book keeps all 200 across a move and
  loses only the level that leaves the window). The swap is published through the
  seqlock so a reader never sees a half-rebuilt book, and an off-grid centre
  clears the ladder rather than mis-indexing it. What cannot fit is gone —
  and because the default `depth@100ms` feed is a *diff* stream, a dropped level
  is not repeated until it changes. After a re-anchor that loses depth, a live
  deployment must reseed from a REST snapshot (`seed_from_snapshot`) instead of
  trusting the near touch; the `depth20@100ms` partial-book stream is a
  snapshot-style exception and does not make the diff path safe.
- **A frame is truncated toward the touch, not from the front.** `DepthUpdate`
  holds `kMaxDepthLevels` (32) levels per side, but a partial-book burst on this
  feed carries hundreds — one frame here listed 407 bids and 440 asks — and the
  venue promises no ordering: those bids arrived *ascending*, starting at
  `30 895`. Keeping the first 32 therefore discarded the real touch, and the
  parser's "best bid" became a price ~800 USDT below the market — which the
  re-anchor rule above then dutifully chased: 78 re-anchors and 37 unpriced
  frames on the same archive. `parse_levels` now keeps the highest bids and the
  lowest asks, evicting the furthest level instead, because the touch is the only
  quantity the engine reads. `test_json_parser_keeps_the_levels_nearest_the_touch`
  pins it.
- **It is observable.** `book_recenters`, `rejected_levels` and
  `unpriced_frames` are carried through `ReplaySummary` into
  `aggregate`, and printed on the study's own summary line:

```
fills 147 under the trade model | tape 6290 print(s) | book: 1 unpriced frame(s), 4 re-anchor(s)
```

`rejected_levels` alone is not a health signal — a real feed legitimately quotes
levels thousands of ticks away (this capture carried one at `1000.00`), and they
are all dropped by design. One unpriced frame out of 1,466 means the book was the
market for 99.93 % of the capture; the 7,864 rejected levels are the far ones,
not the touch.

#### 11.6.3 Touch versus tape, on real prints

Same archive, same venue lock (`config/venue_binance_futures_btcusdt.json`:
tick `0.10`, seed centre `80000`), 4 windows of 2,000 events:

| model | fills | trades | net PnL | fees | **before fees** |
| --- | --- | --- | --- | --- | --- |
| `touch` | 61 | 8 | `-9.32` | `9.93` | `+0.61` |
| `trade` | 147 | 45 | `-7.64` | `8.09` | `+0.45` |

Two things changed with the model, and both matter:

1. The tape model fills **more** often (147 vs 61): a print at the quote fills an
   order the touch model cannot see, so the legacy model was not "conservative",
   it was measuring a different (and rarer) event. It also completes five times
   as many round trips (45 vs 8), which is why its numbers mean more.
2. Loss per trade fell from `-1.16` to `-0.17` USDT, and pre-fee PnL is `+0.61`
   (touch) against `+0.45` (trade): at this sample size the strategy captures
   slightly *more* than nothing before the fee, and the whole loss is the `2 bps`
   maker charge. The earlier `-15.57` was mostly a symptom of the phantom anchor
   described in §11.6.2, not of the strategy.

The markout study tells the same story with more resolution:

| model | h=0 mean | h=0 median | h=0 sd | win % | h=1s mean | t | verdict |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `touch` | `+0.139 bps` | `+0.086` | `0.61` | 57.1 | `-0.412` | `-4.91` | INCONCLUSIVE (n=6) |
| `trade` | `+0.232 bps` | `+0.092` | `0.36` | 100.0 | `-0.404` | `-11.92` | INCONCLUSIVE (n=28) |

Both models agree on the shape, and it is not a tail. At `h=0` every tape-model
fill is positive (100 % win) because the fill is marked against the mid it was
quoted *away* from — that number measures the quote offset, not an edge. One
second later every single fill is negative (`-0.404 bps`, 0 % win, `t = -11.92`):
**the capture decays to nothing inside the marking window**, uniformly, rather
than being destroyed by a minority of toxic prints. Neither column clears the
`30`-sample bar, so both verdicts stay INCONCLUSIVE — the 1 s sign is
consistent (`28/28` → `0/28` positive on the tape model's 28 marked fills) but
it is still one 151 s capture.

The conclusion it sharpens: the strategy is not "wrong", it is
**mis-priced against its own fee**:

- `h=0` net after `2 bps` maker = `-1.77 bps` per fill.
- `h=1s` net after `2 bps` maker = `-2.40 bps` per fill.
- To break even on a `2 bps` fee, the fill needs `> +2 bps` of markout edge; it
  delivers `+0.23 bps` at `h=0` and `-0.40 bps` at the horizon that matters.

#### 11.6.4 The OFI knob does not rescue it (yet)

The strategy carries `ofi_sensitivity` (order-flow-imbalance coupling), disabled
at `0.0` in the lock. Swept on this capture with the tape model:

| `ofi_sensitivity` | fills | trades | net PnL | gross edge |
| --- | --- | --- | --- | --- |
| `0.0` (locked) | 147 | 45 | `-7.64` | `+0.110 bps` |
| `0.5` | 172 | 51 | `-7.88` | `-0.132 bps` |
| `1.0` | 175 | 50 | `-8.66` | `-0.318 bps` |
| `2.0` | 153 | 44 | `-10.25` | `-0.489 bps` |
| `4.0` | 188 | 57 | `-10.59` | `-0.556 bps` |

It makes things worse at every setting: the skew does add fills (147 → 188) but
it takes the pre-fee capture from `+0.11 bps` to `-0.56 bps`, so it is buying
fills that lose money. One caveat on the sweep itself: `--from-lock` makes the
lock file the single source of truth and *ignores* CLI overrides, so these rows
are run with explicit `--k 1.5 --gamma 0.1 --tick-size 0.1 --center 80000
--maker-bps 2 --taker-bps 5 --ofi-sensitivity ...` and no lock. With 151 s of
data this is a hint, not a finding — fitting a toxicity filter needs hours of
tape and a proper in-sample/out-of-sample split, which is exactly what §11.5.2
asks for.

#### 11.6.5 What this leaves as the next decision

The machinery is now honest end to end: the archive carries the tape, the
engine follows the market, a blind book is reported instead of hidden, and every
PnL is stamped with the model and grid that produced it. The remaining gap is
commercial, not engineering, and the options are ordered by what the evidence
supports:

1. **Fix the fee.** `+0.23 bps` of mean capture at `h=0` against a `2 bps` maker
   fee is not a parameter problem. Either reach a fee tier where the capture
   clears (VIP/BNB discounts, or a rebate venue), or trade a book whose spread is
   several bps wide. This is the only lever that moves the sign.
2. **Attack the decay.** The `h=0 → 1s` flip is uniform (100 % → 0 % positive),
   not a fat tail, so a filter cannot fix it by dropping bad fills — it has to
   change *when the quote is present*, i.e. predict the mid move rather than
   select among fills. Needs hours of tape; the sweep in §11.6.4 is the shape of
   the experiment, not its answer.
3. **Accumulate data.** 151 s cannot separate alpha from noise. `ingest_l2.py`
   already runs for days (reconnects, rotation, graceful drain); the study just
   needs the archive.

Until (1) is resolved, the ALPHA verdict stays **NO-GO**, and it should: the
report is telling the truth about a strategy that cannot pay its own fees.


### 11.7 The fee wall: what the sign actually depends on

§11.6 measured the strategy's capture. This section measures the other side of
the ledger and answers the only question that decides whether the codebase can
ever be profitable: **what maker fee can this edge pay?**

#### 11.7.1 The arithmetic of the wall

Read directly from the committed 151 s archive (1,466 depth frames):

| Quantity | Value |
| --- | --- |
| Median quoted spread | `0.01229 bps` = **1 tick** (0.10 on ~81,370) |
| Spread p90 / max | `0.467 bps` (38 ticks) / `13.75 bps` |
| Retail VIP 0 maker fee | `2.0 bps` = **16.27 USDT** = **162.7 ticks** |
| Fee as a multiple of the whole median spread | **163x** |
| Fee as a multiple of the half-spread available at the touch | **325x** |
| Depth frames whose spread is wide enough to capture 2 bps per side | **0.205 %** |

A passive maker at the touch earns half the spread when filled — about
`0.006 bps` here — and pays `2 bps`. The instrument's entire quoted spread is
smaller than 1 % of one fill's fee, so no amount of queue skill changes the
sign. The strategy must therefore earn *outside* the spread, i.e. it needs the
mid to move in its favour after the fill, and that is exactly the quantity
adverse selection eats.

#### 11.7.2 Where the gross edge peaks

The strategy's quote width is set by the Avellaneda-Stoikov `k`
(`half = (1/gamma) * ln(1 + gamma/k)`, `gamma = 0.1`). Sweeping it on the same
archive, tape fill model, 4 windows x 2,000 events, `2.0 bps` maker fee
(diagnostic sweep with explicit `--tick-size 0.1 --center 80000 --k ...`; these
rows are not a locked study):

| `k` | half-spread | fills | trades | pre-fee PnL | **gross edge** | net at 2 bps |
| --- | --- | --- | --- | --- | --- | --- |
| `1.5` (locked) | `0.08 bps` | 147 | 45 | `+0.45` | `+0.110 bps` | `-7.64` |
| `0.2` | `0.50 bps` | 117 | 19 | `+0.91` | `+0.259 bps` | `-6.09` |
| `0.05` | `1.35 bps` | 73 | 9 | `+2.07` | `+0.848 bps` | `-2.81` |
| `0.018` | `2.31 bps` | 36 | 0 | `+2.57` | `+1.264 bps` | `-1.50` |
| `0.006` | `3.53 bps` | 28 | 0 | `+2.18` | `+1.916 bps` | `-0.10` |
| `0.002` | `4.83 bps` | 11 | 0 | `+1.59` | `+2.441 bps` | `+0.29` |
| `0.0008` | `5.94 bps` | 11 | 0 | `+1.01` | `+3.096 bps` | `+0.36` |

The shape is the finding, and it is not the shape a parameter search would want:

* Gross edge climbs monotonically, `+0.11 bps` at the locked width to
  `+3.10 bps` at the widest — but the fill count falls from 147 to 11 and the
  completed round trips from 45 to **zero** over the same range.
* From `k = 0.018` down nothing closes inside a window: every fill stays open
  until the window boundary flattens it, so the PnL is a mark, not a trade. The
  two positive net rows are 11 one-way fills with 0 round trips, and their gross
  edge is computed against 3,254–6,509 USDT of notional that barely moved. That
  is not a measured edge; it is a small sample divided by a small denominator.
* Among the widths that still complete round trips (`k >= 0.05`), the capture is
  `+0.11 … +0.85 bps` and net is negative at every single row.

So the break-even maker fee on this tape is at most **~0.85 bps** at a width that
still completes trades, and `+0.110 bps` at the locked `k` — against a retail
maker fee of `2.0 bps`. The earlier reading of a `+1.015 bps` peak was measured
before the depth parser kept the levels nearest the touch (§11.6.2); the peak it
found was traded against a phantom book and does not survive the fix.

Two different runs feed the numbers in this section, and they are not
interchangeable. Every `+0.110 bps` above comes from the real archive; the
`-0.299 bps` that §11.7.3 shows in the gate's output is the committed *sample*
study, a different dataset:

| Run | Data | Fills / trades | Notional | Pre-fee | Gross edge |
| --- | --- | --- | --- | --- | --- |
| Real tape, locked `k` | the 151 s archive, tick `0.1` | 147 / 45 | `40,444.89 USDT` | `+0.445` | `+0.110 bps` |
| Committed sample study | `data/sample_events.ndjson`, tick `0.5`, centre `30,000` | 229 / 105 | `68,664.88 USDT` | `-2.050` | `-0.299 bps` |

#### 11.7.3 What the code now reports

The measurement above is only useful if it cannot be lost again, so the
break-even fee is now a first-class number:

* `python_analytics/costs.py` — `gross_edge_bps(pre_fee_pnl, traded_notional)`
  and the `FeeWall` dataclass. Gross PnL over the notional it was earned on is
  the *only* unit in which an edge and a fee rate are directly comparable, and
  the break-even maker fee is exactly that ratio.
* `walk_forward` — the aggregate carries `gross_edge_bps`, the console prints

  ```
  fee wall: gross edge +0.110 bps vs 2 bps maker fee | break-even maker fee +0.110 bps
  ```

  and the markdown gains `Gross edge (pre-fee)` and `Break-even maker fee` rows.
* `generate_readiness_report.py` — a third alpha check, `alpha.fee_wall`,
  restates `alpha.net_pnl` in fee units:

  ```
  ALPHA FAIL alpha.fee_wall: gross edge -0.299 bps vs 2 bps maker fee
                             | break-even maker fee -0.299 bps
  ```

  with `breakeven_maker_bps`, `fee_wall_viable` and `fee_wall_multiple` in the
  evidence block. It is emitted only when the study traded notional — an empty
  run has no unit to charge a fee against, and reporting `0.0` would read as a
  measured edge.

That last check is what a production arming decision already consumes:
`run_live_trader.py --live` refuses to arm without a fresh alpha GO, so a fee
tier that cannot be paid now blocks the deployment in code instead of in a
meeting.

#### 11.7.4 The decision this forces

1. **The fee tier is the business case, not a detail.** `+0.11 … +0.85 bps` is
   the ceiling this edge can pay on this instrument at a width that still
   completes round trips. Retail VIP 0 charges `2.0 bps`. Only a materially
   better maker rate — a high volume tier, a market-maker/rebate programme, or a
   venue whose quoted spread is a few bps wide — clears it. No parameter set
   moves the sign on its own: `k` widens the capture by `~0.74 bps` in total
   before the trade count collapses to zero.
2. **The wide end is unmeasured, not better.** The only rows whose net PnL is
   positive (`k = 0.002` and `0.0008`) hold 11 fills and 0 completed round trips
   apiece; they are a boundary mark on a position that never round-tripped, so
   they cannot be read as an edge. Either way the fee wall does not depend on
   them — it comes from the spread and the fee schedule §11.7.1 — but the shape
   does, so accumulating hours of tape is still the prerequisite for quoting at
   the peak width.
3. **Everything else is second order.** Toxicity filters, micro-price and OFI
   skew act on a `0.1–0.8 bps` capture against a `2 bps` cost. They are worth
   building once the edge is larger than the fee, not before.

With `alpha.fee_wall` in the report, the promotion checklist in §10 cannot pass
a study whose edge is smaller than the fee it will be charged, and the number
that has to change is now named on every run.

### 11.8 The edge curve: what a quote earns at each distance

§11.6 measured the strategy's fills and §11.7 measured the fee against the
spread. Both leave one question open, and it is the one that decides whether a
maker can be viable on this instrument at *any* setting: **is there a distance
from the mid at which a resting quote is paid more than its fee, and does the
tape ever go there?**

#### 11.8.1 The measurement, and what it is not

`scripts/run_edge_curve.py` answers it with no strategy in the loop. For every
print in a capture, an order resting `d` basis points on the passive side would
have been filled if the print traded at or through its price; that hypothetical
fill is marked from **the quote's own price** — a print that trades through us
fills us at our limit, not at its own price — at the configured horizons, and
the maker fee is subtracted. Each distance is then summarised by edge and by how
often the tape reached it.

Two boundaries matter, because they are what keep this from being a fantasy:

- **The quote is re-anchored to the mid before every print.** That models a maker
  that reprices on each update — which is what this engine does — and it is not
  the same experiment as the hold-until-filled walk-forward of §11.6. A quote
  that stays put is filled when the market walks to it, and those fills are the
  adverse ones; §11.6 already measured them. The curve measures the other half:
  what is available to a maker who keeps his distance.
- **Queue position, latency and our size are not modelled.** All three make
  fills less likely, never more, so the fill count is an upper bound and the
  edge is a best case. A distance that fails here cannot be rescued upstream.

#### 11.8.2 The first tape: 151 s, and what it could not see

151 s capture, 6,240 prints, 1,466 depth frames, tick `0.10`, median mid
`81,389.95`, `2.0 bps` maker fee. `ticks` is that distance on the venue grid:

| distance bps | ticks | fills | fills/min | `h=0` | `h=1s` | `h=1s` t | win % | `h=10s` |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `0.0125` | 1 | 3,111 | 1,249.7 | `+0.018` | `-0.801` | `-76.03` | 4 | `-1.201` |
| `0.1` | 8 | 2,501 | 1,004.7 | `+0.104` | `-0.752` | `-63.81` | 6 | `-1.112` |
| `0.5` | 41 | 1,167 | 468.8 | `+0.510` | `-0.424` | `-22.96` | 22 | `-0.693` |
| `1` | 81 | 231 | 92.8 | `+1.002` | `+0.131` | `+3.18` | 50 | `-0.604` |
| `1.5` | 122 | 0 | 0.0 | — | — | — | — | — |
| `2` | 163 | 0 | 0.0 | — | — | — | — | — |

The `h=0` column is not an edge and is printed to keep the table honest: it
reproduces the distance almost exactly (`+1.002` at `1 bps`) because a fill is
marked against the mid it was quoted away from.

This first run read two things into that table: that `1 bps` was the only
distance that paid, and that the tape never reached `1.5 bps` or beyond. **Both
were artefacts of a two-and-a-half-minute window**, and 15 more minutes were
enough to show it: `1.5` and `2 bps` fill, and `+0.13 bps` at `1 bps` does not
survive three hours. The smoke test stays in this document because deleting it
would hide the lesson — a 151 s capture cannot separate a plateau from a regime,
which is exactly what §11.6.5 warned about.

#### 11.8.3 Three hours of the same instrument

A 3 h 20 m capture on the same endpoint (`scripts/ingest_l2.py --duration 12000`,
`depth@100ms` + `trade`), 12 consecutive rolls, **613,900 prints over 10,800 s**.
The tape is not committed — `data/` is git-ignored — so these numbers are
reproducible by re-capturing, not by cloning; the command is at the end of this
section. Tick `0.10`, median mid `76,890`, `2.0 bps` maker fee, mid ended the
session at `-0.24 %`:

| distance bps | ticks | fills | fills/min | `h=0` | `h=1s` | `h=1s` t | `h=1s` n | win % | `h=10s` | `h=10s` t |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `0.0125` | 1 | 351,042 | 1,950.3 | `+0.019` | `-1.332` | `-640.6` | 332,890 | 7.3 | `-1.302` | `-258.0` |
| `0.1` | 8 | 295,553 | 1,642.0 | `+0.111` | `-1.301` | `-562.7` | 278,924 | 8.2 | `-1.239` | `-223.8` |
| `0.5` | 38 | 171,702 | 953.9 | `+0.504` | `-1.153` | `-348.5` | 155,282 | 12.0 | `-0.979` | `-130.1` |
| `1` | 77 | 74,507 | 413.9 | `+1.009` | `-1.084` | `-176.3` | 58,355 | 18.8 | `-0.668` | `-53.3` |
| `1.5` | 115 | 38,559 | 214.2 | `+1.508` | `-1.077` | `-98.9` | 22,597 | 18.6 | `-0.287` | `-14.9` |
| `2` | 154 | 25,637 | 142.4 | `+2.007` | `-0.971` | `-54.8` | 9,675 | 26.0 | **`+0.121`** | `+4.4` |
| `3` | 231 | 18,436 | 102.4 | `+3.013` | `-0.522` | `-13.2` | 2,474 | 54.0 | **`+1.489`** | `+33.4` |

Two caveats travel with this table, and both are the study's own honesty rather
the maker's:

- **`n` is smaller than `fills` at every distance.** About 16,000 fills per
  distance had no mid inside the 2 s tolerance and are excluded, never
  interpolated. They are not a random subset: the websocket reconnect at `18:52`
  left a 113 s hole in the mid timeline, and 15,261 of the `3 bps` fills landed
  inside it. That episode was the most violent of the session, so the surviving
  means are if anything **flattering**. The module now reports the count
  (`truncated`) instead of shrinking the sample quietly.
- **The per-roll spread is the regime.** At the touch, the `1 s` markout is
  negative in **12 of 12** rolls (`-1.12` to `-1.49 bps`). At `2 bps` it is
  negative in 10 of 12 and positive in two (`+1.31`, `+0.15`). A single roll
  would have supported almost any story; twelve make the sign of the touch
  unambiguous and the sign of the far quotes obviously regime-dependent.

#### 11.8.4 What the shape says

1. **At the touch, being filled is the adverse event.** One tick out, the `1 s`
   markout is `-1.33 bps` over 332,890 fills, `t = -641`, and only 7.3 % of fills
   are positive one second later. This is not a trend: the session ended where it
   started, and the damage is inside the second after each print. §11.6.3 reached
   the same conclusion from the strategy's own fills (`-0.404 bps`, 0 % win) with
   6,000x less data.
2. **Distance helps monotonically and never clears `1 s`.** `-1.33` at the touch,
   `-1.30` at 8 ticks, `-1.15` at 38, `-1.08` at 77, `-0.97` at 154, `-0.52` at
   231. Every one of those `t` statistics is far past `-1.96`. A maker cannot
   quote its way out of the fee while it holds the fill for a second.
3. **The `10 s` horizon flips, and only far out.** `-1.30` at the touch, still
   negative at 77 and 115 ticks, then `+0.12` at 154 ticks and `+1.49` at 231
   (`t = +33`). Short-horizon momentum, longer-horizon reversion: after a sweep
   that reaches 2–3 bps the price keeps going for a second and then comes back
   over ten. That reversion is the first positive gross edge in this study.
4. **The break-even fee is negative at a 1 s holding time and positive at 10 s.**
   At `1 s` the best cell is `-0.97 bps`, so no maker fee at or above zero is
   payable — that configuration needs a *rebate*. At `10 s` and `2–3 bps` out,
   the tape pays `+0.12 … +1.49 bps` gross, so a fee below that clears it.
5. **The smoke test's `+0.13 bps` cell is now `-1.08`.** Same instrument, same
   distance, 72x the tape, opposite sign. Nothing about the method changed; the
   window did.

#### 11.8.5 What this changes, and what it does not

It does not change the verdict: `alpha.fee_wall` still fails, and it should — the
strategy quotes at the touch, where the tape is unambiguous. What it changes is
which hypothesis is left standing.

**The market-maker hypothesis is answered, and the answer is no.** A re-quoting
maker that holds a fill for a second loses `0.97–1.33 bps` of markout at every
distance the tape reaches, before any fee. At retail `2 bps` that is a `3 bps`
loss per fill; with a *zero* fee it is still a loss. No parameter, filter or
queue position changes that, because the number is what the mid does after the
fill.

**The sweep-reversion hypothesis is what the tape actually supports.** Quote
154–231 ticks out, get filled by a sweep, hold ten seconds:

| configuration | fills/min | gross `h=10s` | fee that clears | sample |
| --- | --- | --- | --- | --- |
| `2 bps` (154 ticks) | 142.4 | `+0.121 bps` | `< 0.12 bps` | 10,098 |
| `3 bps` (231 ticks) | 102.4 | `+1.489 bps` | `< 1.49 bps` | 2,897 |

That is not a market maker: it is a strategy that rests far from the touch,
accepts a fill only when the market runs, and then waits for the reversion. Its
edge is real in this tape and its risks are not measured here — inventory held
for ten seconds through a trend, the `2 bps` row's `+0.12 bps` sitting well
inside a fee, and the `3 bps` row's 2,474 marked one-second fills being a small
sample of a violent regime. It needs its own study before it means anything, and
it is the same finding as §11.6.5 option 2 ("attack the decay") from the
strategy side.

The rest of the business case is unchanged but now quantified: a fee tier is
still a prerequisite for anything that holds a fill for a second, a wider-grid
venue is still the other lever, and the curve is how you tell which of the two
you are looking at.

```bash
# the method on the committed 151 s tape (reproduces the first table)
python3 scripts/run_edge_curve.py \
  --source data/raw_l2/binance_futures_btcusdt_depth_100ms_trade_20261029T163929Z_00001.ndjson.gz \
  --tick-size 0.1 --distances-bps 0.0125,0.025,0.05,0.1,0.25,0.5,1,1.5,2

# a multi-roll capture, read as one chronological tape (the 3 h numbers)
python3 scripts/run_edge_curve.py \
  --source data/raw_l2/binance_futures_btcusdt_depth_100ms_trade_20261030T*_0000[1-9].ndjson.gz \
           data/raw_l2/binance_futures_btcusdt_depth_100ms_trade_20261030T*_0001[0-2].ndjson.gz \
  --tick-size 0.1 --distances-bps 0.0125,0.1,0.5,1,1.5,2,3
# exit 0 a distance clears, 1 none does, 2 usage, 3 no priced print
```

