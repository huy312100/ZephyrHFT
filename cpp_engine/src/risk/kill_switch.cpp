// SPDX-License-Identifier: MIT
// Kill switch implementation. Everything in this translation unit is cold:
// the strategy thread only ever calls the accessors declared in the header
// (`is_tripped`, `get_state`, `check_order_allowed`).
#include "risk/kill_switch.hpp"

#include <cstdio>
#include <cstring>
#include <exception>
#include <utility>

namespace hft {
namespace {

constexpr std::string_view kUnspecifiedReason{"unspecified"};

// `std::snprintf` into a stack buffer: no allocation, no exception, always
// NUL-terminated, and truncation is reported through the returned view.
template <std::size_t N, typename... Args>
[[nodiscard]] std::string_view format_into(std::array<char, N>& buffer,
                                           const char* format,
                                           Args... args) noexcept {
    const int written = std::snprintf(buffer.data(), buffer.size(), format, args...);
    if (written <= 0) {
        return {};
    }
    const auto length = static_cast<std::size_t>(written);
    return std::string_view(buffer.data(), length < buffer.size() ? length : buffer.size() - 1);
}

[[nodiscard]] std::string_view normalized(std::string_view reason) noexcept {
    return reason.empty() ? kUnspecifiedReason : reason;
}

}  // namespace

KillSwitch::KillSwitch(const KillSwitchConfig& config) {
    set_config(config);
}

// ---------------------------------------------------------------------------
// Hot path
// ---------------------------------------------------------------------------
bool KillSwitch::is_tripped() const noexcept {
    return state_.load(std::memory_order_acquire) != SystemState::NORMAL;
}

SystemState KillSwitch::get_state() const noexcept {
    return state_.load(std::memory_order_acquire);
}

bool KillSwitch::check_order_allowed() const noexcept {
    // One acquire load on the fast path. The counter increment only runs for
    // blocked attempts, so a healthy session never pays for the telemetry.
    if (state_.load(std::memory_order_acquire) == SystemState::NORMAL) {
        return true;
    }
    blocked_orders_.fetch_add(1, std::memory_order_relaxed);
    return false;
}

// ---------------------------------------------------------------------------
// Control
// ---------------------------------------------------------------------------
void KillSwitch::trip(const std::string& reason) {
    trip_impl(reason);
}

void KillSwitch::trip_impl(std::string_view reason) noexcept {
    // A single RMW is the whole concurrency protocol: any number of threads
    // may trip concurrently, the state always converges on TRIGGERED, and no
    // reader can observe a torn enum.
    state_.exchange(SystemState::TRIGGERED, std::memory_order_acq_rel);
    record_trip(reason);
}

bool KillSwitch::trip_if_running(std::string_view reason) noexcept {
    SystemState observed = state_.load(std::memory_order_acquire);
    while (observed != SystemState::TRIGGERED) {
        if (state_.compare_exchange_weak(observed, SystemState::TRIGGERED,
                                         std::memory_order_acq_rel,
                                         std::memory_order_acquire)) {
            record_trip(reason);
            return true;
        }
        // `observed` now holds the competitor's value; loop only while the
        // switch had not been tripped when we lost the race.
    }
    return false;
}

void KillSwitch::record_trip(std::string_view reason) noexcept {
    const Nanos timestamp = now_ns();
    trip_count_.fetch_add(1, std::memory_order_relaxed);
    last_trip_timestamp_ns_.store(static_cast<std::int64_t>(timestamp),
                                  std::memory_order_relaxed);
    // A trip is a fresh incident: an operator who resets and re-arms must not
    // inherit a reject streak from before the halt.
    consecutive_rejects_.store(0, std::memory_order_relaxed);
    store_reason(normalized(reason));
    run_emergency_actions(normalized(reason));
}

void KillSwitch::store_reason(std::string_view reason) noexcept {
    reason_lock_.lock();
    const std::size_t length = reason.size() < reason_.size() ? reason.size() : reason_.size();
    if (length != 0) {
        std::memcpy(reason_.data(), reason.data(), length);
    }
    reason_length_ = length;
    reason_lock_.unlock();
}

void KillSwitch::run_emergency_actions(std::string_view reason) noexcept {
    // Held only around dispatch: a hook that trips again on this thread is
    // tolerated (recursive mutex) and no state lock is ever held here, so a
    // control thread calling `reset()` or `get_stats()` cannot deadlock.
    try {
        std::lock_guard<std::recursive_mutex> guard(hook_mutex_);
        // Cancel first: flattening a book that still has resting orders would
        // simply be re-filled by them.
        if (cancel_all_hook_) {
            try {
                cancel_all_hook_();
            } catch (...) {
                // Remediation failures must never unwind the trip.
            }
        }
        if (flatten_hook_) {
            try {
                flatten_hook_();
            } catch (...) {
            }
        }
        if (trip_hook_) {
            try {
                trip_hook_(SystemState::TRIGGERED, reason);
            } catch (...) {
            }
        }
    } catch (...) {
        // Even a lock failure must not unwind a halt that already happened.
    }
}

void KillSwitch::reset() {
    // Release so a strategy thread that sees NORMAL also sees every write the
    // remediation path performed while the switch was tripped.
    state_.store(SystemState::NORMAL, std::memory_order_release);
    consecutive_rejects_.store(0, std::memory_order_relaxed);
    reset_count_.fetch_add(1, std::memory_order_relaxed);
}

bool KillSwitch::begin_recovery() noexcept {
    SystemState expected = SystemState::TRIGGERED;
    return state_.compare_exchange_strong(expected, SystemState::RECOVERING,
                                          std::memory_order_acq_rel,
                                          std::memory_order_acquire);
}

KillSwitchStats KillSwitch::get_stats() const noexcept {
    KillSwitchStats stats;
    stats.trip_count = trip_count_.load(std::memory_order_relaxed);
    stats.last_trip_timestamp_ns = last_trip_timestamp_ns_.load(std::memory_order_relaxed);
    stats.state = state_.load(std::memory_order_acquire);
    stats.consecutive_rejects = consecutive_rejects_.load(std::memory_order_relaxed);
    stats.blocked_orders = blocked_orders_.load(std::memory_order_relaxed);
    stats.reset_count = reset_count_.load(std::memory_order_relaxed);

    reason_lock_.lock();
    try {
        // The only allocating statement in this translation unit, and the
        // reason the exception barrier exists: a failed allocation must
        // degrade the diagnostic text, not terminate the process mid-flatten.
        stats.last_trip_reason.assign(reason_.data(), reason_length_);
    } catch (...) {
    }
    reason_lock_.unlock();
    return stats;
}

// ---------------------------------------------------------------------------
// Automatic guards
// ---------------------------------------------------------------------------
bool KillSwitch::check_drawdown(double drawdown) noexcept {
    const double limit = max_drawdown_limit_.load(std::memory_order_relaxed);
    if (!enabled_.load(std::memory_order_relaxed) || !(limit > 0.0)) {
        return false;
    }
    // Written as `drawdown < limit` -> return: a NaN drawdown compares false
    // and therefore trips instead of slipping through the guard.
    if (drawdown < limit) {
        return false;
    }
    std::array<char, 96> buffer{};
    const std::string_view text =
        format_into(buffer, "drawdown=%.2f>=limit=%.2f", drawdown, limit);
    return trip_if_running(text);
}

bool KillSwitch::record_order_rejected() noexcept {
    const std::int32_t limit = max_consecutive_rejects_.load(std::memory_order_relaxed);
    if (!enabled_.load(std::memory_order_relaxed) || limit <= 0) {
        return false;
    }
    const std::uint32_t streak =
        consecutive_rejects_.fetch_add(1, std::memory_order_relaxed) + 1;
    if (static_cast<std::int64_t>(streak) < static_cast<std::int64_t>(limit)) {
        return false;
    }
    std::array<char, 96> buffer{};
    const std::string_view text =
        format_into(buffer, "consecutive_rejects=%u>=limit=%d",
                    static_cast<unsigned>(streak), static_cast<int>(limit));
    return trip_if_running(text);
}

void KillSwitch::record_order_accepted() noexcept {
    consecutive_rejects_.store(0, std::memory_order_relaxed);
}

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------
KillSwitchConfig KillSwitch::config() const noexcept {
    KillSwitchConfig out;
    out.enabled = enabled_.load(std::memory_order_relaxed);
    out.max_drawdown_limit = max_drawdown_limit_.load(std::memory_order_relaxed);
    out.max_consecutive_rejects = max_consecutive_rejects_.load(std::memory_order_relaxed);
    return out;
}

void KillSwitch::set_config(const KillSwitchConfig& config) noexcept {
    enabled_.store(config.enabled, std::memory_order_relaxed);
    max_drawdown_limit_.store(config.max_drawdown_limit, std::memory_order_relaxed);
    max_consecutive_rejects_.store(config.max_consecutive_rejects, std::memory_order_relaxed);
}

bool KillSwitch::enabled() const noexcept {
    return enabled_.load(std::memory_order_relaxed);
}

void KillSwitch::set_enabled(bool enabled) noexcept {
    enabled_.store(enabled, std::memory_order_relaxed);
}

// ---------------------------------------------------------------------------
// Hooks
// ---------------------------------------------------------------------------
void KillSwitch::set_trip_hook(TripHook hook) {
    std::lock_guard<std::recursive_mutex> guard(hook_mutex_);
    trip_hook_ = std::move(hook);
}

void KillSwitch::set_cancel_all_hook(EmergencyAction action) {
    std::lock_guard<std::recursive_mutex> guard(hook_mutex_);
    cancel_all_hook_ = std::move(action);
}

void KillSwitch::set_flatten_hook(EmergencyAction action) {
    std::lock_guard<std::recursive_mutex> guard(hook_mutex_);
    flatten_hook_ = std::move(action);
}

void KillSwitch::clear_hooks() {
    std::lock_guard<std::recursive_mutex> guard(hook_mutex_);
    trip_hook_ = nullptr;
    cancel_all_hook_ = nullptr;
    flatten_hook_ = nullptr;
}

}  // namespace hft
