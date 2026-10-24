// SPDX-License-Identifier: MIT
// Dead-man's switch for the market-data feed.
//
// A `KillSwitch` answers "may an order leave the process?". This monitor
// answers the complementary question: "is the feed that justifies an order
// still alive?". A socket that dies silently — no FIN, no error, just no more
// frames — is the failure mode an exchange heartbeat never reports, and it is
// exactly what a disconnect storm or an injected lag spike produces.
//
// Design rules (mirrors risk/kill_switch.hpp):
//   * `beat()` is the hot path: one release store plus one relaxed counter.
//     No mutex, no allocation, no syscall, so it can be called once per frame.
//   * `poll()` is the cold supervision path. It is idempotent per expiry
//     window — only the first call after the deadline trips — so a 20 Hz
//     watchdog cannot inflate the trip counter to 20 trips per second.
//   * The monitor never owns the `KillSwitch`. `arm()` stores a raw pointer
//     that must outlive the monitor (the pybind11 binding keeps the Python
//     object alive via `keep_alive`). An unarmed monitor still reports
//     expiry through `poll()`, which keeps it usable for pure telemetry.
//   * Time is explicit (`std::int64_t` nanoseconds, `0` means "now") so tests
//     can drive the whole state machine without sleeping.
#pragma once

#include <atomic>
#include <cstdint>

#include "risk/kill_switch.hpp"
#include "types.hpp"

namespace hft {

// ---------------------------------------------------------------------------
// HeartbeatConfig
// ---------------------------------------------------------------------------
// `timeout_ns` is the longest silence that still counts as a live feed. `0`
// disables expiry, mirroring `KillSwitchConfig`'s "0 disables" convention so a
// caller can run the monitor as a pure latency gauge.
struct HeartbeatConfig {
    std::int64_t timeout_ns = 1'000'000'000;  // 1 s

    [[nodiscard]] constexpr bool expiry_enabled() const noexcept {
        return timeout_ns > 0;
    }
};

// ---------------------------------------------------------------------------
// HeartbeatStats
// ---------------------------------------------------------------------------
// Point-in-time snapshot. `max_silence_ns` is the worst gap observed by a
// `poll()` call, which is what an incident review actually wants: the feed
// cadence that preceded the outage, not just the deadline that was crossed.
struct HeartbeatStats {
    std::uint64_t beats = 0;
    std::uint64_t timeouts = 0;
    std::int64_t last_beat_ns = 0;
    std::int64_t silence_ns = 0;
    std::int64_t max_silence_ns = 0;
    std::int64_t timeout_ns = 0;
    bool expired = false;
    bool armed = false;
    // State of the attached kill switch (NORMAL when unarmed).
    SystemState state = SystemState::NORMAL;
};

// ---------------------------------------------------------------------------
// HeartbeatMonitor
// ---------------------------------------------------------------------------
class alignas(kCacheLine) HeartbeatMonitor {
public:
    explicit HeartbeatMonitor(const HeartbeatConfig& config = HeartbeatConfig()) noexcept;

    HeartbeatMonitor(const HeartbeatMonitor&) = delete;
    HeartbeatMonitor& operator=(const HeartbeatMonitor&) = delete;
    HeartbeatMonitor(HeartbeatMonitor&&) = delete;
    HeartbeatMonitor& operator=(HeartbeatMonitor&&) = delete;

    // Monotonic clock in nanoseconds; the same reference `types.hpp` uses.
    [[nodiscard]] static std::int64_t now_ns() noexcept;

    // ---- hot path -------------------------------------------------------
    // Records a frame. `timestamp_ns == 0` samples the monotonic clock.
    // Re-arms the expiry window, so a single beat after an outage lets the
    // monitor detect the *next* outage without an operator reset.
    void beat(std::int64_t timestamp_ns = 0) noexcept;

    // ---- observation ----------------------------------------------------
    [[nodiscard]] std::int64_t last_beat_ns() const noexcept;
    // Silence since the last beat (or since construction/reset), clamped at 0.
    [[nodiscard]] std::int64_t silence_ns(std::int64_t now = 0) const noexcept;
    [[nodiscard]] bool is_expired(std::int64_t now = 0) const noexcept;
    [[nodiscard]] HeartbeatStats stats(std::int64_t now = 0) const noexcept;

    // ---- supervision ----------------------------------------------------
    // Trips the armed kill switch when the deadline has passed. Returns true
    // only for the call that reported *this* expiry window; subsequent polls
    // return false until a new beat re-arms the window. Never throws.
    [[nodiscard]] bool poll(std::int64_t now = 0) noexcept;

    // ---- wiring ---------------------------------------------------------
    void arm(KillSwitch& kill_switch) noexcept;
    void detach() noexcept;
    [[nodiscard]] bool armed() const noexcept;

    // ---- configuration / control ---------------------------------------
    [[nodiscard]] std::int64_t timeout_ns() const noexcept;
    // `0` (or negative) disables expiry; see `HeartbeatConfig`.
    void set_timeout_ns(std::int64_t timeout_ns) noexcept;
    // Restarts the window and clears the latch. Telemetry (beats, timeouts,
    // worst silence) survives, mirroring ``KillSwitchStats`` across
    // ``KillSwitch::reset()``. Does **not** touch the attached kill switch:
    // clearing a halt is an operator decision.
    void reset(std::int64_t now = 0) noexcept;

private:
    // ---- hot path (its own cache line) ---------------------------------
    alignas(kCacheLine) std::atomic<std::int64_t> last_beat_ns_{0};
    std::atomic<std::int64_t> timeout_ns_{0};
    // True between "the deadline was reported" and "the next beat".
    std::atomic<bool> latched_{false};
    // Window start used before the first beat and after `reset()`.
    std::atomic<std::int64_t> started_ns_{0};

    // ---- cold path: telemetry + wiring ----------------------------------
    alignas(kCacheLine) std::atomic<std::uint64_t> beats_{0};
    std::atomic<std::uint64_t> timeouts_{0};
    std::atomic<std::int64_t> max_silence_ns_{0};
    std::atomic<KillSwitch*> kill_switch_{nullptr};
};

static_assert(alignof(HeartbeatMonitor) == kCacheLine,
              "HeartbeatMonitor must own a cache line");
static_assert(std::atomic<std::int64_t>::is_always_lock_free,
              "the beat timestamp must be a lock-free atomic");
static_assert(std::atomic<KillSwitch*>::is_always_lock_free,
              "the kill-switch pointer must be a lock-free atomic");

}  // namespace hft
