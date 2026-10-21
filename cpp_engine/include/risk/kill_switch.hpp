// SPDX-License-Identifier: MIT
// Emergency kill switch: the one flag every order consults before it reaches
// the venue.
//
// Design rules:
//   * The hot-path queries (`check_order_allowed`, `is_tripped`, `get_state`)
//     are a single acquire load of an 8-bit atomic: no mutex, no allocation,
//     no syscall, no branch beyond the state compare.
//   * `trip()` never allocates. It copies the reason into a fixed-size buffer
//     so an emergency stop still works when the allocator is exhausted, when
//     the process is already degraded, or when `trip()` is called from a
//     signal-adjacent supervision thread.
//   * Everything off the hot path (reason text, hooks, telemetry) is guarded by
//     a spinlock / recursive mutex, so a control thread can trip, reset, or
//     re-tune limits while the strategy thread keeps submitting orders.
//   * `KillSwitchConfig::enabled` gates only the *automatic* guards
//     (`check_drawdown`, `record_order_rejected`). A manual `trip()` always
//     wins: an operator must be able to flatten even when the automation is
//     configured off.
#pragma once

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>
#include <type_traits>

#include "types.hpp"

namespace hft {

// ---------------------------------------------------------------------------
// SystemState
// ---------------------------------------------------------------------------
// Values are SCREAMING_CASE because the enum crosses the pybind11 boundary
// (`hft_native.SystemState.TRIGGERED`), matching `RiskStatus` and `Side`.
enum class SystemState : std::uint8_t {
    // Orders are admitted, automatic guards are armed.
    NORMAL = 0,
    // Orders are blocked. Reached by a manual trip or by an automatic guard.
    TRIGGERED = 1,
    // Remediation window: cancels / flattening are still in flight and the
    // book may be inconsistent. Orders stay blocked until `reset()`.
    RECOVERING = 2,
};

inline constexpr std::size_t kSystemStateCount = 3;

[[nodiscard]] constexpr std::string_view system_state_name(SystemState state) noexcept {
    switch (state) {
        case SystemState::NORMAL:
            return "NORMAL";
        case SystemState::TRIGGERED:
            return "TRIGGERED";
        case SystemState::RECOVERING:
            return "RECOVERING";
    }
    return "UNKNOWN";
}

// ---------------------------------------------------------------------------
// KillSwitchConfig
// ---------------------------------------------------------------------------
// `max_drawdown_limit` is an absolute drawdown magnitude in quote currency
// (never a ratio); `0` disables the drawdown guard. `max_consecutive_rejects`
// is an order count; `0` disables the reject-streak guard.
struct KillSwitchConfig {
    bool enabled = true;
    double max_drawdown_limit = 0.0;
    std::int32_t max_consecutive_rejects = 0;

    [[nodiscard]] constexpr bool drawdown_guard_enabled() const noexcept {
        return enabled && max_drawdown_limit > 0.0;
    }
    [[nodiscard]] constexpr bool reject_guard_enabled() const noexcept {
        return enabled && max_consecutive_rejects > 0;
    }
};

// ---------------------------------------------------------------------------
// KillSwitchStats
// ---------------------------------------------------------------------------
// Point-in-time snapshot. `last_trip_timestamp_ns` and `last_trip_reason`
// deliberately survive `reset()` so post-mortem tooling can explain why a
// session was halted; `trip_count` is cumulative for the process lifetime.
struct KillSwitchStats {
    std::uint64_t trip_count = 0;
    std::int64_t last_trip_timestamp_ns = 0;
    std::string last_trip_reason{};
    SystemState state = SystemState::NORMAL;
    std::uint32_t consecutive_rejects = 0;
    std::uint64_t blocked_orders = 0;
    std::uint64_t reset_count = 0;
};

// ---------------------------------------------------------------------------
// KillSwitch
// ---------------------------------------------------------------------------
class alignas(kCacheLine) KillSwitch {
public:
    // Logger bridge. Invoked once per trip *after* the cancel/flatten actions,
    // so the reason text reflects a state that is already safe. Must not throw;
    // exceptions are swallowed so a broken sink cannot resurrect the trip.
    using TripHook = std::function<void(SystemState state, std::string_view reason)>;
    // Order-cancellation / position-flattening hooks. They run in this order:
    // cancel resting orders first (so a flatten cannot be re-filled), then
    // flatten the residual position.
    using EmergencyAction = std::function<void()>;

    explicit KillSwitch(const KillSwitchConfig& config = KillSwitchConfig());

    KillSwitch(const KillSwitch&) = delete;
    KillSwitch& operator=(const KillSwitch&) = delete;
    KillSwitch(KillSwitch&&) = delete;
    KillSwitch& operator=(KillSwitch&&) = delete;

    // ---- hot path -------------------------------------------------------
    // `true` whenever the switch is not NORMAL (TRIGGERED or RECOVERING).
    [[nodiscard]] bool is_tripped() const noexcept;
    [[nodiscard]] SystemState get_state() const noexcept;
    // The single call the order path needs. Blocked attempts are counted
    // (relaxed RMW) for telemetry; the NORMAL path is one acquire load and one
    // compare.
    [[nodiscard]] bool check_order_allowed() const noexcept;

    // ---- control --------------------------------------------------------
    // Thread-safe, idempotent transition to TRIGGERED. Every call is counted
    // as a trip event and overwrites the recorded reason/timestamp.
    void trip(const std::string& reason);
    // Returns the switch to NORMAL from any state and clears the reject
    // streak. Trip telemetry is preserved. Safe to call when already NORMAL.
    void reset();
    // TRIGGERED -> RECOVERING. Orders stay blocked; use it to signal that
    // cancels/flattening are underway. Returns false if not TRIGGERED.
    [[nodiscard]] bool begin_recovery() noexcept;
    [[nodiscard]] KillSwitchStats get_stats() const noexcept;

    // ---- automatic guards ----------------------------------------------
    // `drawdown` is a non-negative magnitude in quote currency. Fails closed:
    // a NaN comparison trips instead of slipping through.
    // Both guards return true only for the call that actually tripped.
    [[nodiscard]] bool check_drawdown(double drawdown) noexcept;
    // Call once per rejected order. Comparing with `>=` (not `>`) means the
    // Nth consecutive reject is the one that trips.
    [[nodiscard]] bool record_order_rejected() noexcept;
    // Call once per accepted order; clears the consecutive-reject streak.
    void record_order_accepted() noexcept;

    // ---- configuration --------------------------------------------------
    [[nodiscard]] KillSwitchConfig config() const noexcept;
    void set_config(const KillSwitchConfig& config) noexcept;
    [[nodiscard]] bool enabled() const noexcept;
    void set_enabled(bool enabled) noexcept;

    // ---- hooks ----------------------------------------------------------
    // `set_*` is safe to call concurrently with `trip()` and never fires the
    // hook. Dispatch holds a recursive mutex, so a hook may call `trip()` on
    // the same thread but must not block or re-register callbacks.
    void set_trip_hook(TripHook hook);
    void set_cancel_all_hook(EmergencyAction action);
    void set_flatten_hook(EmergencyAction action);
    void clear_hooks();

private:
    // Minimal non-allocating spinlock: `std::mutex::lock()` may throw
    // `std::system_error`, which would turn the `noexcept` accessors into
    // `std::terminate`. Only `trip()` / `reset()` / `get_stats()` ever
    // contend here, and they are cold.
    class SpinLock {
    public:
        void lock() noexcept {
            std::uint32_t spins = 0;
            while (flag_.test_and_set(std::memory_order_acquire)) {
                if (++spins < 64) {
                    continue;
                }
                spins = 0;
                std::this_thread::yield();
            }
        }
        void unlock() noexcept { flag_.clear(std::memory_order_release); }

    private:
        std::atomic_flag flag_ = ATOMIC_FLAG_INIT;
    };

    // Allocation-free trip path shared by the public API and the auto guards.
    void trip_impl(std::string_view reason) noexcept;
    // Cancel -> flatten -> log. Runs outside the telemetry lock.
    void run_emergency_actions(std::string_view reason) noexcept;
    // Counts one trip event and fires the hooks. Call after the state move.
    void record_trip(std::string_view reason) noexcept;
    // Returns true only for the call that moved the switch out of NORMAL.
    [[nodiscard]] bool trip_if_running(std::string_view reason) noexcept;
    void store_reason(std::string_view reason) noexcept;

    // ---- hot path state (its own cache line) ----------------------------
    alignas(kCacheLine) std::atomic<SystemState> state_{SystemState::NORMAL};
    std::atomic<bool> enabled_{true};
    std::atomic<double> max_drawdown_limit_{0.0};
    std::atomic<std::int32_t> max_consecutive_rejects_{0};
    std::atomic<std::uint32_t> consecutive_rejects_{0};
    mutable std::atomic<std::uint64_t> blocked_orders_{0};

    // ---- cold path: telemetry + control ---------------------------------
    alignas(kCacheLine) std::atomic<std::uint64_t> trip_count_{0};
    std::atomic<std::int64_t> last_trip_timestamp_ns_{0};
    std::atomic<std::uint64_t> reset_count_{0};
    mutable SpinLock reason_lock_{};
    std::array<char, 192> reason_{};
    std::size_t reason_length_{0};
    std::recursive_mutex hook_mutex_{};
    TripHook trip_hook_{};
    EmergencyAction cancel_all_hook_{};
    EmergencyAction flatten_hook_{};
};

static_assert(alignof(KillSwitch) == kCacheLine, "KillSwitch must own a cache line");
static_assert(std::is_trivially_copyable_v<KillSwitchConfig>);
static_assert(std::atomic<SystemState>::is_always_lock_free,
              "the hot-path state must be a lock-free atomic");
static_assert(std::atomic<double>::is_always_lock_free,
              "the drawdown limit must be a lock-free atomic");

}  // namespace hft
