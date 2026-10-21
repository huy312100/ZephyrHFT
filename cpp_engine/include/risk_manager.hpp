// SPDX-License-Identifier: MIT
// Pre-trade risk manager: the last gate before an order leaves the process.
//
// Scope: this is the *direct* counterpart to `risk_guard.hpp`. RiskGuard owns
// the fixed-point, book-relative checks (notional caps, open-order and
// daily-loss limits); RiskManager owns the order-flow and fat-finger checks
// that are expressed in exchange-native units (base size, percentage of mid,
// orders per second). Both run before `TradingEngine::submit()` touches the
// order manager.
//
// Design rules (identical to the rest of the hot path):
//   * No heap allocation, no exceptions, no RTTI, no virtual dispatch.
//   * Every mutable field is a lock-free std::atomic, so limits can be tuned
//     from a control thread while the strategy thread is submitting orders.
//   * The rate limiter commits its decision with a single CAS on one 64-bit
//     word, so there is no mutex and no two-word tearing.
//   * `validate_order()` is branch-predictable and allocation-free.
#pragma once

#include <algorithm>
#include <array>
#include <atomic>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <string_view>

#include "types.hpp"

namespace hft {

// ---------------------------------------------------------------------------
// RiskStatus
// ---------------------------------------------------------------------------
// Values are named in SCREAMING_CASE because this enum is a public contract
// that crosses the pybind11 boundary (Python strategies branch on
// `hft_native.RiskStatus.APPROVED`). The rest of the C++ core keeps PascalCase.
enum class RiskStatus : std::uint8_t {
    APPROVED = 0,
    REJECTED_POSITION_LIMIT = 1,
    REJECTED_RATE_LIMIT = 2,
    REJECTED_FAT_FINGER = 3,
    REJECTED_MAX_ORDER_SIZE = 4,
    // Not part of the original contract, but required for a fail-closed gate:
    // a NaN price makes every floating-point comparison false, so without this
    // check a NaN order would sail through all the other guards.
    REJECTED_INVALID_ORDER = 5,
};

inline constexpr std::size_t kRiskStatusCount = 6;

[[nodiscard]] constexpr bool is_approved(RiskStatus status) noexcept {
    return status == RiskStatus::APPROVED;
}

[[nodiscard]] constexpr std::string_view risk_status_name(RiskStatus status) noexcept {
    switch (status) {
        case RiskStatus::APPROVED:
            return "APPROVED";
        case RiskStatus::REJECTED_POSITION_LIMIT:
            return "REJECTED_POSITION_LIMIT";
        case RiskStatus::REJECTED_RATE_LIMIT:
            return "REJECTED_RATE_LIMIT";
        case RiskStatus::REJECTED_FAT_FINGER:
            return "REJECTED_FAT_FINGER";
        case RiskStatus::REJECTED_MAX_ORDER_SIZE:
            return "REJECTED_MAX_ORDER_SIZE";
        case RiskStatus::REJECTED_INVALID_ORDER:
            return "REJECTED_INVALID_ORDER";
    }
    return "UNKNOWN";
}

// Bridges the new status vocabulary onto the engine's existing reject enum so
// `OrderAck.reason` keeps working for downstream reporting.
[[nodiscard]] constexpr RejectReason to_reject_reason(RiskStatus status) noexcept {
    switch (status) {
        case RiskStatus::APPROVED:
            return RejectReason::None;
        case RiskStatus::REJECTED_POSITION_LIMIT:
            return RejectReason::PositionLimit;
        case RiskStatus::REJECTED_RATE_LIMIT:
            return RejectReason::RateLimit;
        case RiskStatus::REJECTED_FAT_FINGER:
            return RejectReason::PriceBand;
        case RiskStatus::REJECTED_MAX_ORDER_SIZE:
        case RiskStatus::REJECTED_INVALID_ORDER:
            return RejectReason::InvalidQuantity;
    }
    return RejectReason::None;
}

// ---------------------------------------------------------------------------
// TokenBucket
// ---------------------------------------------------------------------------
// Lock-free token bucket: at most `burst` tokens, refilled at `rate_per_sec`.
//
// A token bucket is fully described by (rate r, depth b): it holds at most b
// tokens, gains r tokens per second, and admits one order per token consumed.
// The obvious implementation -- "count tokens, remember the last refill" --
// needs two atomics that cannot be updated together without a lock.
//
// Instead we store the *theoretical arrival time* (TAT): the instant at which
// the bucket would be empty. Time passing refills the bucket implicitly, so the
// whole state fits in one std::atomic<int64_t> and one CAS commits the
// decision:
//
//     interval  T   = 1s / r
//     tolerance tau = b * T
//     admit   <=>   max(tat, now) + T - now <= tau
//
// After k back-to-back orders from idle, tat = k*T, so orders 1..b are admitted
// and order b+1 is refused; once the burst is spent only one order per T gets
// through, i.e. exactly r orders/second. This is algebraically the same filter
// as a token bucket, just with the token count factored out of the state.
class TokenBucket {
public:
    constexpr TokenBucket() noexcept = default;

    TokenBucket(double rate_per_sec, double burst) noexcept {
        configure(rate_per_sec, burst);
    }

    TokenBucket(const TokenBucket&) = delete;
    TokenBucket& operator=(const TokenBucket&) = delete;

    // A rate <= 0 (or non-finite) disables limiting: every call is admitted.
    // Safe to call concurrently with try_acquire(); a rate change keeps the
    // virtual clock intact so an in-flight burst is not refreshed for free.
    void configure(double rate_per_sec, double burst) noexcept {
        double rate = rate_per_sec;
        double depth = burst;
        if (!(rate > 0.0) || !std::isfinite(rate)) {
            rate = 0.0;
            depth = 0.0;
        }
        if (!(depth >= 1.0) || !std::isfinite(depth)) {
            depth = 1.0;
        }
        if (depth > 1.0e6) {
            depth = 1.0e6;  // keeps `depth * interval` inside int64
        }
        const std::int64_t interval =
            rate > 0.0
                ? std::max<std::int64_t>(1, static_cast<std::int64_t>(1.0e9 / rate))
                : 0;
        const std::int64_t tolerance =
            interval > 0
                ? static_cast<std::int64_t>(depth * static_cast<double>(interval))
                : 0;
        interval_ns_.store(interval, std::memory_order_relaxed);
        tolerance_ns_.store(tolerance, std::memory_order_relaxed);
        rate_.store(rate, std::memory_order_relaxed);
        burst_.store(depth, std::memory_order_relaxed);
    }

    // Consumes one token at `now_ns`; true when the order may proceed.
    [[nodiscard]] bool try_acquire(Nanos now_ns) noexcept {
        const std::int64_t interval = interval_ns_.load(std::memory_order_relaxed);
        if (interval <= 0) {
            return true;  // limiting disabled
        }
        const std::int64_t tolerance = tolerance_ns_.load(std::memory_order_relaxed);
        const auto now = static_cast<std::int64_t>(now_ns);
        std::int64_t tat = tat_ns_.load(std::memory_order_relaxed);
        for (;;) {
            const std::int64_t base = tat > now ? tat : now;
            const std::int64_t next = base + interval;
            // The bucket would overflow its depth: refuse, leaving it untouched
            // so an approved order is never charged for a rejected one.
            if (next - now > tolerance) {
                return false;
            }
            if (tat_ns_.compare_exchange_weak(tat, next, std::memory_order_relaxed,
                                              std::memory_order_relaxed)) {
                return true;
            }
            // `tat` now holds the observed value; retry against fresh state.
        }
    }

    // Restarts the virtual clock so the next call sees a full burst.
    void reset(Nanos now_ns) noexcept {
        tat_ns_.store(static_cast<std::int64_t>(now_ns), std::memory_order_relaxed);
    }

    [[nodiscard]] bool enabled() const noexcept {
        return interval_ns_.load(std::memory_order_relaxed) > 0;
    }

    [[nodiscard]] double rate_per_sec() const noexcept {
        return rate_.load(std::memory_order_relaxed);
    }

    [[nodiscard]] double burst() const noexcept {
        return burst_.load(std::memory_order_relaxed);
    }

    // Free capacity, for dashboards and tests only -- never for a decision.
    [[nodiscard]] double level(Nanos now_ns) const noexcept {
        const std::int64_t interval = interval_ns_.load(std::memory_order_relaxed);
        if (interval <= 0) {
            return 0.0;
        }
        const std::int64_t tolerance = tolerance_ns_.load(std::memory_order_relaxed);
        const std::int64_t tat = tat_ns_.load(std::memory_order_relaxed);
        const std::int64_t ahead = tat - static_cast<std::int64_t>(now_ns);
        const double tokens =
            static_cast<double>(tolerance - ahead) / static_cast<double>(interval);
        return std::clamp(tokens, 0.0, burst_.load(std::memory_order_relaxed));
    }

private:
    std::atomic<std::int64_t> tat_ns_{0};
    std::atomic<std::int64_t> interval_ns_{0};
    std::atomic<std::int64_t> tolerance_ns_{0};
    std::atomic<double> rate_{0.0};
    std::atomic<double> burst_{0.0};
};

static_assert(std::atomic<std::int64_t>::is_always_lock_free,
              "the rate limiter must stay lock-free");
static_assert(std::atomic<double>::is_always_lock_free,
              "risk limits are tuned live from another thread");

// ---------------------------------------------------------------------------
// RiskManagerConfig
// ---------------------------------------------------------------------------
// Defaults are what a live crypto market maker should start from: 50 orders per
// second with a 50-order burst, a 5% fat-finger band, 100 base units per child
// order and a 1000-unit net position cap. A limit <= 0 (or NaN) disables that
// individual guard, which is how `disabled()` switches the whole thing off for
// replay/backtest, where simulated flow is deliberately unthrottled.
struct RiskManagerConfig {
    bool enabled = true;
    double max_position = 1'000.0;          // |net position|, base units
    double max_order_size = 100.0;          // base units per order
    double max_price_deviation_pct = 0.05;  // fraction of mid (0.05 == 5%)
    double order_rate_limit = 50.0;         // orders / second
    double order_burst = 50.0;              // bucket depth

    [[nodiscard]] static constexpr RiskManagerConfig disabled() noexcept {
        return RiskManagerConfig{false, 0.0, 0.0, 0.0, 0.0, 0.0};
    }
};

struct RiskManagerStats {
    std::uint64_t evaluated{0};
    std::uint64_t approved{0};
    std::uint64_t rejected{0};
    std::array<std::uint64_t, kRiskStatusCount> by_status{};
    double tokens_available{0.0};

    [[nodiscard]] std::uint64_t count(RiskStatus status) const noexcept {
        return by_status[static_cast<std::size_t>(status)];
    }
};

// Optional allocation-free rejection sink, e.g. a lock-free logger. The hook
// runs on the submitting thread, so it must not allocate or block.
using RiskRejectHook = void (*)(void* context, RiskStatus status,
                                std::string_view symbol) noexcept;

// ---------------------------------------------------------------------------
// RiskManager
// ---------------------------------------------------------------------------
class alignas(kCacheLine) RiskManager {
public:
    // Default construction is *armed*: it behaves exactly like
    // `RiskManagerConfig{}` (50 orders/s, 5% band, 100-unit order cap, 1000-unit
    // position cap). Callers that need a dormant gate - replay, backtests -
    // construct it from `RiskManagerConfig::disabled()`.
    RiskManager() noexcept : RiskManager(RiskManagerConfig{}) {}

    explicit RiskManager(const RiskManagerConfig& config) noexcept {
        configure(config);
    }

    RiskManager(const RiskManager&) = delete;
    RiskManager& operator=(const RiskManager&) = delete;

    void configure(const RiskManagerConfig& config) noexcept {
        enabled_.store(config.enabled, std::memory_order_relaxed);
        max_position_.store(sanitize_limit(config.max_position),
                            std::memory_order_relaxed);
        max_order_size_.store(sanitize_limit(config.max_order_size),
                              std::memory_order_relaxed);
        max_price_deviation_pct_.store(
            sanitize_limit(config.max_price_deviation_pct),
            std::memory_order_relaxed);
        bucket_.configure(config.order_rate_limit, config.order_burst);
    }

    [[nodiscard]] RiskManagerConfig config() const noexcept {
        RiskManagerConfig out;
        out.enabled = enabled_.load(std::memory_order_relaxed);
        out.max_position = max_position_.load(std::memory_order_relaxed);
        out.max_order_size = max_order_size_.load(std::memory_order_relaxed);
        out.max_price_deviation_pct =
            max_price_deviation_pct_.load(std::memory_order_relaxed);
        out.order_rate_limit = bucket_.rate_per_sec();
        out.order_burst = bucket_.burst();
        return out;
    }

    // ---- live tuning -----------------------------------------------------
    void set_enabled(bool enabled) noexcept {
        enabled_.store(enabled, std::memory_order_relaxed);
    }

    void set_max_position(double limit) noexcept {
        max_position_.store(sanitize_limit(limit), std::memory_order_relaxed);
    }

    void set_max_order_size(double limit) noexcept {
        max_order_size_.store(sanitize_limit(limit), std::memory_order_relaxed);
    }

    void set_max_price_deviation_pct(double limit) noexcept {
        max_price_deviation_pct_.store(sanitize_limit(limit),
                                       std::memory_order_relaxed);
    }

    void set_order_rate_limit(double rate_per_sec, double burst) noexcept {
        bucket_.configure(rate_per_sec, burst);
    }

    void set_reject_hook(RiskRejectHook hook, void* context = nullptr) noexcept {
        hook_context_.store(context, std::memory_order_relaxed);
        hook_.store(hook, std::memory_order_relaxed);
    }

    // ---- accessors -------------------------------------------------------
    [[nodiscard]] bool enabled() const noexcept {
        return enabled_.load(std::memory_order_relaxed);
    }

    [[nodiscard]] double max_position() const noexcept {
        return max_position_.load(std::memory_order_relaxed);
    }

    [[nodiscard]] double max_order_size() const noexcept {
        return max_order_size_.load(std::memory_order_relaxed);
    }

    [[nodiscard]] double max_price_deviation_pct() const noexcept {
        return max_price_deviation_pct_.load(std::memory_order_relaxed);
    }

    [[nodiscard]] double order_rate_limit() const noexcept {
        return bucket_.rate_per_sec();
    }

    [[nodiscard]] double tokens_available(Nanos now_ns) const noexcept {
        return bucket_.level(now_ns);
    }

    // Clock-free convenience overloads for interactive use and dashboards.
    [[nodiscard]] double tokens_available() const noexcept {
        return bucket_.level(now_ns());
    }

    [[nodiscard]] const TokenBucket& rate_limiter() const noexcept { return bucket_; }

    // ---- validation ------------------------------------------------------
    // Primary entry point. `symbol` is only observed by the reject hook; taking
    // a string_view accepts std::string, std::string_view and literals without
    // copying. Pass `current_position` as the signed net position *before* this
    // order and `current_mid_price` from the latest coherent BBO.
    [[nodiscard]] RiskStatus validate_order(std::string_view symbol, double price,
                                            double quantity, Side side,
                                            double current_position,
                                            double current_mid_price) noexcept {
        return validate_order_at(now_ns(), symbol, price, quantity, side,
                                 current_position, current_mid_price);
    }

    // Deterministic overload: the caller supplies the clock, which is what the
    // unit tests and any replay harness use to make rate limiting reproducible.
    [[nodiscard]] RiskStatus validate_order_at(Nanos now_ns, std::string_view symbol,
                                               double price, double quantity, Side side,
                                               double current_position,
                                               double current_mid_price) noexcept;

    void reset(Nanos now_ns) noexcept {
        bucket_.reset(now_ns);
        evaluated_.store(0, std::memory_order_relaxed);
        for (auto& counter : counters_) {
            counter.store(0, std::memory_order_relaxed);
        }
    }

    [[nodiscard]] RiskManagerStats stats(Nanos now_ns) const noexcept;

    // Snapshot using the steady clock; run this off the hot path only.
    [[nodiscard]] RiskManagerStats stats() const noexcept { return stats(now_ns()); }

private:
    [[nodiscard]] static double sanitize_limit(double value) noexcept {
        return (std::isfinite(value) && value > 0.0) ? value : 0.0;
    }

    std::atomic<bool> enabled_{true};
    std::atomic<double> max_position_{0.0};
    std::atomic<double> max_order_size_{0.0};
    std::atomic<double> max_price_deviation_pct_{0.0};
    std::atomic<RiskRejectHook> hook_{nullptr};
    std::atomic<void*> hook_context_{nullptr};
    std::atomic<std::uint64_t> evaluated_{0};
    std::array<std::atomic<std::uint64_t>, kRiskStatusCount> counters_{};
    TokenBucket bucket_{};
};

inline RiskStatus RiskManager::validate_order_at(
    Nanos now_ns, std::string_view symbol, double price, double quantity, Side side,
    double current_position, double current_mid_price) noexcept {
    if (!enabled_.load(std::memory_order_relaxed)) {
        return RiskStatus::APPROVED;  // backtest/replay: zero-observable overhead
    }
    evaluated_.fetch_add(1, std::memory_order_relaxed);

    RiskStatus status = RiskStatus::APPROVED;

    // 1. Structural validity. `!(x > 0)` is a single predicate that also catches
    //    NaN, which would otherwise compare false against every bound below and
    //    slip straight through the gate.
    if (!(quantity > 0.0) || !std::isfinite(quantity) || !(price > 0.0) ||
        !std::isfinite(price) || !std::isfinite(current_position)) {
        status = RiskStatus::REJECTED_INVALID_ORDER;
    } else {
        const double max_size = max_order_size_.load(std::memory_order_relaxed);
        const double max_pos = max_position_.load(std::memory_order_relaxed);
        const double max_dev =
            max_price_deviation_pct_.load(std::memory_order_relaxed);
        const double signed_size = side == Side::Buy ? quantity : -quantity;
        const double projected = current_position + signed_size;

        if (max_size > 0.0 && quantity > max_size) {
            status = RiskStatus::REJECTED_MAX_ORDER_SIZE;
        } else if (max_pos > 0.0 && std::fabs(projected) > max_pos) {
            // Sells that reduce an existing long shrink |projected| and pass;
            // only exposure growth can trip this.
            status = RiskStatus::REJECTED_POSITION_LIMIT;
        } else if (max_dev > 0.0) {
            // Fail closed when there is no usable reference price: without a mid
            // there is no way to prove the order sits inside the band.
            if (!(current_mid_price > 0.0) || !std::isfinite(current_mid_price) ||
                std::fabs(price - current_mid_price) / current_mid_price > max_dev) {
                status = RiskStatus::REJECTED_FAT_FINGER;
            }
        }

        // 2. Rate limiting is last, and only for orders that would otherwise be
        //    sent: the bucket models the *exchange's* request budget, so an
        //    order rejected locally must not burn a token.
        if (status == RiskStatus::APPROVED && !bucket_.try_acquire(now_ns)) {
            status = RiskStatus::REJECTED_RATE_LIMIT;
        }
    }

    counters_[static_cast<std::size_t>(status)].fetch_add(1,
                                                          std::memory_order_relaxed);
    if (status != RiskStatus::APPROVED) {
        if (const RiskRejectHook hook = hook_.load(std::memory_order_relaxed)) {
            hook(hook_context_.load(std::memory_order_relaxed), status, symbol);
        }
    }
    return status;
}

inline RiskManagerStats RiskManager::stats(Nanos now_ns) const noexcept {
    RiskManagerStats out;
    out.evaluated = evaluated_.load(std::memory_order_relaxed);
    for (std::size_t index = 0; index < kRiskStatusCount; ++index) {
        out.by_status[index] = counters_[index].load(std::memory_order_relaxed);
    }
    out.approved = out.by_status[static_cast<std::size_t>(RiskStatus::APPROVED)];
    out.rejected = out.evaluated - out.approved;
    out.tokens_available = bucket_.level(now_ns);
    return out;
}

}  // namespace hft
