// SPDX-License-Identifier: MIT
// Single-Producer / Single-Consumer lock-free ring buffer.
//
// Layout: [ producer cache line ][ consumer cache line ][ payload ... ]
//
// Memory ordering:
//   producer: loads consumer cursor (acquire), writes payload, publishes
//             producer cursor (release)
//   consumer: loads producer cursor (acquire), reads payload, publishes
//             consumer cursor (release)
//
// Capacity is a power of two so index masking is one AND. T must be trivially
// copyable: the payload is memcpy'd, never constructed on the consumer side.
#pragma once

#include <algorithm>
#include <array>
#include <atomic>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <type_traits>
#include <utility>

#include "types.hpp"

namespace hft {

template <typename T, std::size_t Capacity>
class SpscRingBuffer {
    static_assert(Capacity >= 2, "ring buffer needs at least two slots");
    static_assert(std::has_single_bit(Capacity), "capacity must be a power of two");
    static_assert(std::is_trivially_copyable_v<T>, "payload must be trivially copyable");

public:
    using value_type = T;
    static constexpr std::size_t kCapacity = Capacity;
    static constexpr std::size_t kMask = Capacity - 1;

    SpscRingBuffer() noexcept = default;
    SpscRingBuffer(const SpscRingBuffer&) = delete;
    SpscRingBuffer& operator=(const SpscRingBuffer&) = delete;

    [[nodiscard]] static constexpr std::size_t capacity() noexcept { return kCapacity; }

    // Producer-side operation. Returns false when full.
    [[nodiscard]] bool try_push(const T& item) noexcept {
        const std::uint64_t head = producer_.cursor.load(std::memory_order_relaxed);
        if (head - producer_.cached_consumer >= kCapacity) {
            producer_.cached_consumer = consumer_.cursor.load(std::memory_order_acquire);
            if (head - producer_.cached_consumer >= kCapacity) {
                return false;
            }
        }
        buffer_[head & kMask] = item;  // trivial copy, no allocator involvement
        producer_.cursor.store(head + 1, std::memory_order_release);
        return true;
    }

    template <typename... Args>
    [[nodiscard]] bool try_emplace(Args&&... args) noexcept {
        T item(std::forward<Args>(args)...);
        return try_push(item);
    }

    // Consumer-side operation. Returns false when empty.
    [[nodiscard]] bool try_pop(T& out) noexcept {
        const std::uint64_t tail = consumer_.cursor.load(std::memory_order_relaxed);
        if (tail == consumer_.cached_producer) {
            consumer_.cached_producer = producer_.cursor.load(std::memory_order_acquire);
            if (tail == consumer_.cached_producer) {
                return false;
            }
        }
        out = buffer_[tail & kMask];
        consumer_.cursor.store(tail + 1, std::memory_order_release);
        return true;
    }

    [[nodiscard]] bool peek(T& out) noexcept {
        const std::uint64_t tail = consumer_.cursor.load(std::memory_order_relaxed);
        if (tail == consumer_.cached_producer) {
            consumer_.cached_producer = producer_.cursor.load(std::memory_order_acquire);
            if (tail == consumer_.cached_producer) {
                return false;
            }
        }
        out = buffer_[tail & kMask];
        return true;
    }

    // Bulk drain used by batch consumers / vectorised feature builders.
    // Returns the number of elements copied into `out`.
    [[nodiscard]] std::size_t pop_bulk(T* out, std::size_t max_items) noexcept {
        if (out == nullptr || max_items == 0) {
            return 0;
        }
        const std::uint64_t tail = consumer_.cursor.load(std::memory_order_relaxed);
        const std::uint64_t head = producer_.cursor.load(std::memory_order_acquire);
        const std::uint64_t available = head - tail;
        const std::size_t count = static_cast<std::size_t>(
            available < max_items ? available : max_items);
        if (count == 0) {
            consumer_.cached_producer = head;
            return 0;
        }

        const std::size_t start = static_cast<std::size_t>(tail & kMask);
        const std::size_t first_chunk = std::min(count, kCapacity - start);
        std::memcpy(out, buffer_.data() + start, first_chunk * sizeof(T));
        if (first_chunk < count) {
            std::memcpy(out + first_chunk, buffer_.data(),
                        (count - first_chunk) * sizeof(T));
        }
        consumer_.cursor.store(tail + count, std::memory_order_release);
        return count;
    }

    [[nodiscard]] std::size_t size_approx() const noexcept {
        const std::uint64_t head = producer_.cursor.load(std::memory_order_acquire);
        const std::uint64_t tail = consumer_.cursor.load(std::memory_order_acquire);
        return static_cast<std::size_t>(head - tail);
    }

    [[nodiscard]] bool empty() const noexcept { return size_approx() == 0; }
    [[nodiscard]] bool full() const noexcept { return size_approx() >= kCapacity; }

    void reset() noexcept {
        producer_.cursor.store(0, std::memory_order_relaxed);
        consumer_.cursor.store(0, std::memory_order_relaxed);
        producer_.cached_consumer = 0;
        consumer_.cached_producer = 0;
    }

private:
    // Producer and consumer cursors live on separate cache lines to avoid
    // false sharing between pinned feed-handler and strategy cores.
    struct alignas(kCacheLine) ProducerState {
        std::atomic<std::uint64_t> cursor{0};
        std::uint64_t cached_consumer{0};
        std::uint64_t padding[7]{};
    };

    struct alignas(kCacheLine) ConsumerState {
        std::atomic<std::uint64_t> cursor{0};
        std::uint64_t cached_producer{0};
        std::uint64_t padding[7]{};
    };

    ProducerState producer_{};
    ConsumerState consumer_{};
    alignas(kCacheLine) std::array<T, kCapacity> buffer_{};
};

static_assert(sizeof(SpscRingBuffer<std::uint64_t, 1024>) >= 1024 * 8);

}  // namespace hft
