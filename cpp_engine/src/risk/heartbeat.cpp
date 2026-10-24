// SPDX-License-Identifier: MIT
// Heartbeat monitor implementation. `beat()` is one release store; `poll()`
// runs on the supervision thread and is the only path that can allocate (the
// kill-switch reason string) or take a lock (inside `KillSwitch::trip`).
#include "risk/heartbeat.hpp"

#include <algorithm>
#include <array>
#include <cstdio>
#include <string>
#include <string_view>

namespace hft {
namespace {

// Silence shorter than this is treated as zero for reporting; a caller that
// passes a stale timestamp must not see a negative gap.
[[nodiscard]] constexpr std::int64_t clamped_silence(std::int64_t now,
                                                     std::int64_t reference) noexcept {
    return now > reference ? now - reference : 0;
}

}  // namespace

HeartbeatMonitor::HeartbeatMonitor(const HeartbeatConfig& config) noexcept {
    timeout_ns_.store(config.timeout_ns, std::memory_order_relaxed);
    started_ns_.store(now_ns(), std::memory_order_relaxed);
}

std::int64_t HeartbeatMonitor::now_ns() noexcept {
    return static_cast<std::int64_t>(hft::now_ns());
}

// ---------------------------------------------------------------------------
// Hot path
// ---------------------------------------------------------------------------
void HeartbeatMonitor::beat(std::int64_t timestamp_ns) noexcept {
    const std::int64_t stamp = timestamp_ns != 0 ? timestamp_ns : now_ns();
    // Publish the stamp before clearing the latch: a poll that observes the
    // cleared latch always observes a fresh timestamp, so it cannot trip on
    // the interval it just saw a beat for. A poll racing *between* the two
    // stores may report one extra timeout for an outage that has already
    // ended; `KillSwitch::trip` is idempotent, so the cost is one redundant
    // trip event, never a torn state.
    last_beat_ns_.store(stamp, std::memory_order_release);
    beats_.fetch_add(1, std::memory_order_relaxed);
    latched_.store(false, std::memory_order_release);
}

std::int64_t HeartbeatMonitor::last_beat_ns() const noexcept {
    return last_beat_ns_.load(std::memory_order_acquire);
}

std::int64_t HeartbeatMonitor::silence_ns(std::int64_t now) const noexcept {
    const std::int64_t reference = last_beat_ns_.load(std::memory_order_acquire);
    const std::int64_t window_start = started_ns_.load(std::memory_order_relaxed);
    return clamped_silence(now != 0 ? now : now_ns(),
                           reference != 0 ? reference : window_start);
}

bool HeartbeatMonitor::is_expired(std::int64_t now) const noexcept {
    const std::int64_t timeout = timeout_ns_.load(std::memory_order_relaxed);
    if (timeout <= 0) {
        return false;
    }
    return silence_ns(now) >= timeout;
}

HeartbeatStats HeartbeatMonitor::stats(std::int64_t now) const noexcept {
    const std::int64_t at = now != 0 ? now : now_ns();
    const std::int64_t silence = silence_ns(at);
    HeartbeatStats out;
    out.beats = beats_.load(std::memory_order_relaxed);
    out.timeouts = timeouts_.load(std::memory_order_relaxed);
    out.last_beat_ns = last_beat_ns_.load(std::memory_order_acquire);
    out.silence_ns = silence;
    out.max_silence_ns =
        std::max(max_silence_ns_.load(std::memory_order_relaxed), silence);
    out.timeout_ns = timeout_ns_.load(std::memory_order_relaxed);

    KillSwitch* switch_ptr = kill_switch_.load(std::memory_order_acquire);
    out.expired = out.timeout_ns > 0 && silence >= out.timeout_ns;
    out.armed = switch_ptr != nullptr;
    out.state = switch_ptr != nullptr ? switch_ptr->get_state() : SystemState::NORMAL;
    return out;
}


// ---------------------------------------------------------------------------
// Supervision
// ---------------------------------------------------------------------------
bool HeartbeatMonitor::poll(std::int64_t now) noexcept {
    const std::int64_t at = now != 0 ? now : now_ns();
    const std::int64_t silence = silence_ns(at);

    // Track the worst gap even while the feed is healthy: this is the number
    // an operator needs to tell "the feed slowed down" from "the feed stopped".
    std::int64_t observed = max_silence_ns_.load(std::memory_order_relaxed);
    while (silence > observed &&
           !max_silence_ns_.compare_exchange_weak(observed, silence,
                                                  std::memory_order_relaxed,
                                                  std::memory_order_relaxed)) {
    }

    const std::int64_t timeout = timeout_ns_.load(std::memory_order_relaxed);
    if (timeout <= 0 || silence < timeout) {
        return false;
    }
    // Only the first poll of an expiry window reports it. The latch is cleared
    // by the next beat (or by `reset()`), so a 20 Hz watchdog produces one trip
    // per outage instead of twenty.
    if (latched_.exchange(true, std::memory_order_acq_rel)) {
        return false;
    }
    timeouts_.fetch_add(1, std::memory_order_relaxed);

    KillSwitch* switch_ptr = kill_switch_.load(std::memory_order_acquire);
    if (switch_ptr != nullptr) {
        std::array<char, 96> reason{};
        const double silence_ms = static_cast<double>(silence) / 1'000'000.0;
        const double timeout_ms = static_cast<double>(timeout) / 1'000'000.0;
        const int written = std::snprintf(reason.data(), reason.size(),
                                          "heartbeat_timeout: silence=%.0fms>=limit=%.0fms",
                                          silence_ms, timeout_ms);
        if (written > 0) {
            const auto length = static_cast<std::size_t>(written);
            const std::string_view text(
                reason.data(), length < reason.size() ? length : reason.size() - 1);
            try {
                // `trip` runs the cancel/flatten hooks, which may allocate or
                // call into Python; a supervision thread must never let that
                // turn into std::terminate.
                switch_ptr->trip(std::string(text));
            } catch (...) {
            }
        }
    }
    return true;
}

// ---------------------------------------------------------------------------
// Wiring
// ---------------------------------------------------------------------------
void HeartbeatMonitor::arm(KillSwitch& kill_switch) noexcept {
    kill_switch_.store(&kill_switch, std::memory_order_release);
}

void HeartbeatMonitor::detach() noexcept {
    kill_switch_.store(nullptr, std::memory_order_release);
}

bool HeartbeatMonitor::armed() const noexcept {
    return kill_switch_.load(std::memory_order_acquire) != nullptr;
}

// ---------------------------------------------------------------------------
// Configuration / control
// ---------------------------------------------------------------------------
std::int64_t HeartbeatMonitor::timeout_ns() const noexcept {
    return timeout_ns_.load(std::memory_order_relaxed);
}

void HeartbeatMonitor::set_timeout_ns(std::int64_t timeout_ns) noexcept {
    timeout_ns_.store(timeout_ns, std::memory_order_relaxed);
}

void HeartbeatMonitor::reset(std::int64_t now) noexcept {
    const std::int64_t at = now != 0 ? now : now_ns();
    // Order matters: restart the window before clearing the latch so a
    // concurrent poll sees either the old window (already reported) or the new
    // one (not yet expired), never "old silence with a cleared latch".
    //
    // Telemetry survives, exactly like `KillSwitchStats` surviving
    // `KillSwitch::reset()`: an incident report needs the outages that
    // happened, not just the ones after the last operator action.
    started_ns_.store(at, std::memory_order_relaxed);
    last_beat_ns_.store(0, std::memory_order_release);
    latched_.store(false, std::memory_order_release);
}

}  // namespace hft
