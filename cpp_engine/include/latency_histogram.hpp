// SPDX-License-Identifier: MIT
// Lock-free latency histogram with fixed 100 ns buckets. Recording is a single
// relaxed fetch_add, so it is safe on the hot path and across threads.
#pragma once

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>

#include "types.hpp"

namespace hft {

class LatencyHistogram {
public:
    static constexpr std::uint64_t kBucketWidthNs = 100;
    static constexpr std::size_t kBuckets = 16'384;  // 100 ns .. 1.6384 ms
    static constexpr std::size_t kTailBucket = kBuckets - 1;

    LatencyHistogram() noexcept { reset(); }
    LatencyHistogram(const LatencyHistogram&) = delete;
    LatencyHistogram& operator=(const LatencyHistogram&) = delete;

    void record(Nanos duration_ns) noexcept {
        const std::size_t bucket = classify(duration_ns);
        buckets_[bucket].fetch_add(1, std::memory_order_relaxed);
        count_.fetch_add(1, std::memory_order_relaxed);
        total_ns_.fetch_add(duration_ns, std::memory_order_relaxed);
        std::uint64_t current = max_ns_.load(std::memory_order_relaxed);
        while (duration_ns > current &&
               !max_ns_.compare_exchange_weak(current, duration_ns,
                                              std::memory_order_relaxed)) {
        }
    }

    void reset() noexcept {
        for (auto& bucket : buckets_) {
            bucket.store(0, std::memory_order_relaxed);
        }
        count_.store(0, std::memory_order_relaxed);
        total_ns_.store(0, std::memory_order_relaxed);
        max_ns_.store(0, std::memory_order_relaxed);
    }

    [[nodiscard]] std::uint64_t count() const noexcept {
        return count_.load(std::memory_order_relaxed);
    }
    [[nodiscard]] std::uint64_t max_ns() const noexcept {
        return max_ns_.load(std::memory_order_relaxed);
    }
    [[nodiscard]] std::uint64_t mean_ns() const noexcept {
        const std::uint64_t n = count();
        return n == 0 ? 0 : total_ns_.load(std::memory_order_relaxed) / n;
    }

    // `quantile` in [0, 1]. Returns the upper edge of the matching bucket.
    [[nodiscard]] std::uint64_t percentile(double quantile) const noexcept {
        const std::uint64_t total = count();
        if (total == 0) {
            return 0;
        }
        if (quantile <= 0.0) {
            quantile = 0.0;
        } else if (quantile >= 1.0) {
            quantile = 1.0;
        }
        const auto target = static_cast<std::uint64_t>(
            quantile * static_cast<double>(total) + 0.5);
        const std::uint64_t wanted = target == 0 ? 1 : target;
        std::uint64_t seen = 0;
        for (std::size_t i = 0; i < kBuckets; ++i) {
            seen += buckets_[i].load(std::memory_order_relaxed);
            if (seen >= wanted) {
                return (static_cast<std::uint64_t>(i) + 1) * kBucketWidthNs;
            }
        }
        return max_ns();
    }

    [[nodiscard]] std::uint64_t p50() const noexcept { return percentile(0.50); }
    [[nodiscard]] std::uint64_t p99() const noexcept { return percentile(0.99); }
    [[nodiscard]] std::uint64_t p999() const noexcept { return percentile(0.999); }

private:
    [[nodiscard]] static std::size_t classify(Nanos ns) noexcept {
        const std::uint64_t bucket = ns / kBucketWidthNs;
        return bucket < kTailBucket ? static_cast<std::size_t>(bucket) : kTailBucket;
    }

    std::array<std::atomic<std::uint32_t>, kBuckets> buckets_{};
    std::atomic<std::uint64_t> count_{0};
    std::atomic<std::uint64_t> total_ns_{0};
    std::atomic<std::uint64_t> max_ns_{0};
};

}  // namespace hft
