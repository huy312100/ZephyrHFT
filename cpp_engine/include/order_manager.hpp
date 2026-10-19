// SPDX-License-Identifier: MIT
// Fixed-capacity order manager: intrusive live-order list + open-addressed
// order-id index. Every order lives in a pre-allocated 64-byte slot, so the
// manager performs zero heap allocation for the lifetime of the process.
#pragma once

#include <array>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <optional>

#include "types.hpp"

namespace hft {

template <std::size_t Capacity = 8192>
class OrderManager {
    static_assert(Capacity >= 2 && std::has_single_bit(Capacity));

public:
    static constexpr std::size_t kCapacity = Capacity;
    static constexpr std::size_t kHashCapacity = std::bit_ceil(Capacity * 2);
    static constexpr std::size_t kHashMask = kHashCapacity - 1;

    // Exactly one cache line: hot fields are touched together on fill/cancel.
    struct alignas(64) Slot {
        OrderId id{0};
        Price price{};
        Quantity quantity{};
        Quantity filled{};
        Nanos created_ts{0};
        Nanos updated_ts{0};
        std::int32_t next_live{-1};
        std::int32_t prev_live{-1};
        std::int32_t next_free{-1};
        Side side{Side::Buy};
        OrderType type{OrderType::Limit};
        OrderStatus status{OrderStatus::New};
        std::uint8_t post_only{0};
    };

    OrderManager() noexcept { reset(); }
    OrderManager(const OrderManager&) = delete;
    OrderManager& operator=(const OrderManager&) = delete;

    void reset() noexcept {
        hash_keys_.fill(0);
        hash_values_.fill(-1);
        for (std::size_t i = 0; i < Capacity; ++i) {
            slots_[i] = Slot{};
            slots_[i].next_free =
                (i + 1 < Capacity) ? static_cast<std::int32_t>(i + 1) : -1;
        }
        free_head_ = 0;
        live_head_ = -1;
        live_tail_ = -1;
        open_count_ = 0;
        created_count_ = 0;
        tombstones_ = 0;
        next_id_ = 1;
    }

    // Returns the assigned order id, or 0 if the request was rejected.
    [[nodiscard]] OrderId create(const OrderRequest& request,
                                 bool post_only = false) noexcept {
        if (request.quantity.raw() <= 0 || free_head_ < 0) {
            return 0;
        }
        if (request.type == OrderType::Limit && request.price.raw() <= 0) {
            return 0;
        }
        const OrderId id = request.client_id != 0 ? request.client_id : generate_id();
        if (id == 0 || find_slot(id) >= 0) {
            return 0;
        }
        maybe_rehash();
        const std::int32_t index = free_head_;
        free_head_ = slots_[index].next_free;
        auto& slot = slots_[index];
        slot.id = id;
        slot.price = request.price;
        slot.quantity = request.quantity;
        slot.filled = Quantity{};
        slot.created_ts = request.timestamp;
        slot.updated_ts = request.timestamp;
        slot.side = request.side;
        slot.type = request.type;
        slot.status = OrderStatus::New;
        slot.post_only = static_cast<std::uint8_t>(post_only ? 1 : 0);
        slot.next_live = -1;
        slot.prev_live = live_tail_;
        slot.next_free = -1;
        if (live_tail_ >= 0) {
            slots_[live_tail_].next_live = index;
        } else {
            live_head_ = index;
        }
        live_tail_ = index;
        hash_insert(id, index);
        ++open_count_;
        ++created_count_;
        return id;
    }

    // Applies a fill. `remaining_out` receives the order's remaining quantity.
    [[nodiscard]] bool apply_fill(OrderId id, Quantity quantity, Price price,
                                  Nanos timestamp, Quantity& remaining_out) noexcept {
        const std::int32_t index = find_slot(id);
        if (index < 0 || quantity.raw() <= 0) {
            return false;
        }
        auto& slot = slots_[index];
        if (is_terminal(slot.status)) {
            return false;
        }
        std::int64_t fill_raw = quantity.raw();
        const std::int64_t remaining_raw = slot.quantity.raw() - slot.filled.raw();
        if (fill_raw > remaining_raw) {
            fill_raw = remaining_raw;
        }
        if (fill_raw <= 0) {
            return false;
        }
        slot.filled = Quantity::from_raw(slot.filled.raw() + fill_raw);
        slot.updated_ts = timestamp;
        // A limit order keeps its original limit price. Only market orders,
        // which have no resting price, track the execution price.
        if (slot.type == OrderType::Market && price.raw() > 0) {
            slot.price = price;
        }
        remaining_out = Quantity::from_raw(slot.quantity.raw() - slot.filled.raw());
        if (remaining_out.raw() == 0) {
            slot.status = OrderStatus::Filled;
            unlink(index);
            hash_erase(id);
            release_slot(index);
            --open_count_;
        } else {
            slot.status = OrderStatus::PartiallyFilled;
        }
        return true;
    }

    [[nodiscard]] bool cancel(OrderId id, Nanos timestamp,
                              Quantity& cancelled_out) noexcept {
        const std::int32_t index = find_slot(id);
        if (index < 0) {
            return false;
        }
        auto& slot = slots_[index];
        cancelled_out = Quantity::from_raw(slot.quantity.raw() - slot.filled.raw());
        slot.updated_ts = timestamp;
        slot.status = OrderStatus::Cancelled;
        unlink(index);
        hash_erase(id);
        release_slot(index);
        --open_count_;
        return true;
    }

    [[nodiscard]] std::optional<OrderView> find(OrderId id) const noexcept {
        const std::int32_t index = find_slot(id);
        if (index < 0) {
            return std::nullopt;
        }
        return to_view(slots_[index]);
    }

    [[nodiscard]] std::size_t open_orders() const noexcept { return open_count_; }
    [[nodiscard]] std::uint64_t created() const noexcept { return created_count_; }
    [[nodiscard]] bool empty() const noexcept { return live_head_ < 0; }

    // Sums unfilled quantity of every live order by side. Used by pre-trade
    // risk so several resting orders cannot jointly breach position limits.
    void pending_quantities(Quantity& buy, Quantity& sell) const noexcept {
        buy = Quantity{};
        sell = Quantity{};
        for (std::int32_t index = live_head_; index >= 0;
             index = slots_[index].next_live) {
            const Slot& slot = slots_[index];
            const Quantity remaining = slot.quantity - slot.filled;
            if (slot.side == Side::Buy) {
                buy += remaining;
            } else {
                sell += remaining;
            }
        }
    }

    template <typename Fn>
    void for_each_live(Fn&& fn) noexcept {
        std::int32_t index = live_head_;
        while (index >= 0) {
            Slot& slot = slots_[index];
            const std::int32_t next = slot.next_live;
            fn(slot);
            index = next;
        }
    }

private:
    [[nodiscard]] static constexpr bool is_terminal(OrderStatus status) noexcept {
        return status == OrderStatus::Filled || status == OrderStatus::Cancelled ||
               status == OrderStatus::Rejected;
    }

    [[nodiscard]] OrderId generate_id() noexcept {
        while (next_id_ != 0 && find_slot(next_id_) >= 0) {
            ++next_id_;
        }
        const OrderId id = next_id_++;
        if (next_id_ == 0) {
            next_id_ = 1;
        }
        return id;
    }

    void unlink(std::int32_t index) noexcept {
        auto& slot = slots_[index];
        if (slot.prev_live >= 0) {
            slots_[slot.prev_live].next_live = slot.next_live;
        } else {
            live_head_ = slot.next_live;
        }
        if (slot.next_live >= 0) {
            slots_[slot.next_live].prev_live = slot.prev_live;
        } else {
            live_tail_ = slot.prev_live;
        }
        slot.prev_live = -1;
        slot.next_live = -1;
    }

    void release_slot(std::int32_t index) noexcept {
        slots_[index].next_free = free_head_;
        free_head_ = index;
    }

    [[nodiscard]] OrderView to_view(const Slot& slot) const noexcept {
        OrderView view;
        view.order_id = slot.id;
        view.price = slot.price;
        view.quantity = slot.quantity;
        view.filled = slot.filled;
        view.created_ts = slot.created_ts;
        view.updated_ts = slot.updated_ts;
        view.side = slot.side;
        view.status = slot.status;
        view.type = slot.type;
        view.post_only = slot.post_only;
        return view;
    }

    [[nodiscard]] static constexpr std::uint64_t mix(OrderId key) noexcept {
        std::uint64_t x = key + 0x9e3779b97f4a7c15ULL;
        x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9ULL;
        x = (x ^ (x >> 27)) * 0x94d049bb133111ebULL;
        return x ^ (x >> 31);
    }

    [[nodiscard]] std::int32_t find_slot(OrderId id) const noexcept {
        std::size_t index = static_cast<std::size_t>(mix(id)) & kHashMask;
        for (;;) {
            const std::int32_t value = hash_values_[index];
            if (value == -1) {
                return -1;
            }
            if (value >= 0 && hash_keys_[index] == id) {
                return value;
            }
            index = (index + 1) & kHashMask;
        }
    }

    void hash_insert(OrderId id, std::int32_t slot) noexcept {
        std::size_t index = static_cast<std::size_t>(mix(id)) & kHashMask;
        std::size_t first_tombstone = kHashCapacity;
        for (;;) {
            const std::int32_t value = hash_values_[index];
            if (value == -1) {
                if (first_tombstone != kHashCapacity) {
                    index = first_tombstone;
                    --tombstones_;
                }
                hash_keys_[index] = id;
                hash_values_[index] = slot;
                return;
            }
            if (value == -2 && first_tombstone == kHashCapacity) {
                first_tombstone = index;
            }
            index = (index + 1) & kHashMask;
        }
    }

    void hash_erase(OrderId id) noexcept {
        std::size_t index = static_cast<std::size_t>(mix(id)) & kHashMask;
        for (;;) {
            const std::int32_t value = hash_values_[index];
            if (value == -1) {
                return;
            }
            if (value >= 0 && hash_keys_[index] == id) {
                hash_values_[index] = -2;
                ++tombstones_;
                return;
            }
            index = (index + 1) & kHashMask;
        }
    }

    void maybe_rehash() noexcept {
        if (tombstones_ < kHashCapacity / 4) {
            return;
        }
        hash_values_.fill(-1);
        tombstones_ = 0;
        for (std::size_t i = 0; i < Capacity; ++i) {
            if (!is_terminal(slots_[i].status) && slots_[i].id != 0) {
                hash_insert(slots_[i].id, static_cast<std::int32_t>(i));
            }
        }
    }

    std::array<Slot, Capacity> slots_{};
    std::array<OrderId, kHashCapacity> hash_keys_{};
    std::array<std::int32_t, kHashCapacity> hash_values_{};
    std::int32_t free_head_{0};
    std::int32_t live_head_{-1};
    std::int32_t live_tail_{-1};
    std::size_t open_count_{0};
    std::uint64_t created_count_{0};
    std::size_t tombstones_{0};
    OrderId next_id_{1};
};

static_assert(sizeof(typename OrderManager<8192>::Slot) == 64,
              "order slot must be exactly one cache line");

using DefaultOrderManager = OrderManager<8192>;

}  // namespace hft
