// SPDX-License-Identifier: MIT
// Lock-free L2 / L3 order book.
//
// Concurrency model
// -----------------
// A single feed-handler thread owns the write side (the classic HFT design:
// one core parses the socket, publishes to lock-free queues). Readers on the
// strategy core never take a mutex; they take a seqlock snapshot, retry if a
// publication raced with them, and therefore never block the writer.
//
// The price ladder is a direct-indexed array: updating a level is O(1) and
// never allocates. Occupancy bitmaps let depth walks skip empty levels in
// 64-level strides. Bid and ask ladders are each cache-line aligned to avoid
// false sharing.
#pragma once

#include <array>
#include <atomic>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <type_traits>

#include "types.hpp"

namespace hft {

struct alignas(64) BookSnapshot {
    static constexpr std::size_t kMaxLevels = 32;
    std::array<BookLevel, kMaxLevels> bids{};
    std::array<BookLevel, kMaxLevels> asks{};
    std::uint32_t bid_count{0};
    std::uint32_t ask_count{0};
    std::uint64_t sequence{0};
    std::uint32_t reserved{0};
    Bbo bbo{};
};

namespace detail {

// A single side of a direct-indexed price ladder.
// `higher_is_better` is true for bids, false for asks.
template <std::size_t MaxTicks>
class alignas(64) LadderSide {
    static_assert(MaxTicks >= 128, "ladder must hold a meaningful price range");
    static_assert(std::has_single_bit(MaxTicks), "MaxTicks must be a power of two");

public:
    static constexpr std::size_t kMaxTicks = MaxTicks;
    static constexpr std::size_t kHalf = MaxTicks / 2;
    static constexpr std::size_t kWords = MaxTicks / 64;

    LadderSide() noexcept {
        configure(Price::from_raw(1'000'000), Price::from_raw(3'000'000'000'000), true);
    }

    void configure(Price tick_size, Price center, bool higher_is_better) noexcept {
        tick_raw_ = tick_size.raw() > 0 ? tick_size.raw() : 1;
        center_raw_ = center.raw();
        higher_is_better_ = higher_is_better;
        quantities_.fill(0);
        occupancy_.fill(0);
        best_slot_ = -1;
        live_levels_ = 0;
    }

    // Absolute quantity update (L2 semantics). A zero quantity removes the level.
    [[nodiscard]] bool set(Price price, std::int64_t quantity) noexcept {
        std::size_t slot = 0;
        if (!slot_for(price, slot)) {
            return false;
        }
        set_slot(slot, quantity < 0 ? 0 : quantity);
        return true;
    }

    // Signed delta update (L3 aggregation semantics). Rejects underflow.
    [[nodiscard]] bool add(Price price, std::int64_t delta) noexcept {
        std::size_t slot = 0;
        if (!slot_for(price, slot)) {
            return false;
        }
        const std::int64_t next = quantities_[slot] + delta;
        if (next < 0) {
            return false;
        }
        set_slot(slot, next);
        return true;
    }

    [[nodiscard]] bool has_best() const noexcept { return best_slot_ >= 0; }
    [[nodiscard]] std::int32_t best_slot() const noexcept { return best_slot_; }

    [[nodiscard]] Price best_price() const noexcept {
        return best_slot_ < 0 ? Price{} : price_at(static_cast<std::size_t>(best_slot_));
    }

    [[nodiscard]] Quantity best_quantity() const noexcept {
        return best_slot_ < 0
                   ? Quantity{}
                   : Quantity::from_raw(quantities_[static_cast<std::size_t>(best_slot_)]);
    }

    [[nodiscard]] Quantity quantity_at(Price price) const noexcept {
        std::size_t slot = 0;
        if (!slot_for(price, slot)) {
            return Quantity{};
        }
        return Quantity::from_raw(quantities_[slot]);
    }

    // Whether the ladder window can hold this price at all, ignoring whether it
    // sits on the tick grid. Used to decide when the grid itself is wrong.
    [[nodiscard]] bool contains(Price price) const noexcept {
        if (tick_raw_ <= 0) {
            return false;
        }
        const std::int64_t delta = price.raw() - center_raw_;
        const std::int64_t span = static_cast<std::int64_t>(kHalf) * tick_raw_;
        return delta >= -span && delta < span;
    }

    [[nodiscard]] std::size_t live_levels() const noexcept { return live_levels_; }
    [[nodiscard]] std::int64_t tick_raw() const noexcept { return tick_raw_; }
    [[nodiscard]] std::int64_t center_raw() const noexcept { return center_raw_; }

    // Walk price-ordered levels without allocating. Returns levels written.
    [[nodiscard]] std::size_t walk(BookLevel* out, std::size_t max_levels) const noexcept {
        if (out == nullptr || max_levels == 0 || best_slot_ < 0) {
            return 0;
        }
        std::size_t written = 0;
        std::int32_t slot = best_slot_;
        while (slot >= 0 && written < max_levels) {
            out[written].price = price_at(static_cast<std::size_t>(slot));
            out[written].quantity =
                Quantity::from_raw(quantities_[static_cast<std::size_t>(slot)]);
            out[written].order_count = 1;
            out[written].reserved = 0;
            ++written;
            slot = next_slot(higher_is_better_ ? slot - 1 : slot + 1);
        }
        return written;
    }

private:
    [[nodiscard]] bool slot_for(Price price, std::size_t& out) const noexcept {
        const std::int64_t delta = price.raw() - center_raw_;
        if (tick_raw_ <= 0 || delta % tick_raw_ != 0) {
            return false;
        }
        const std::int64_t tick = delta / tick_raw_;
        const std::int64_t slot = tick + static_cast<std::int64_t>(kHalf);
        if (slot < 0 || slot >= static_cast<std::int64_t>(MaxTicks)) {
            return false;
        }
        out = static_cast<std::size_t>(slot);
        return true;
    }

    [[nodiscard]] Price price_at(std::size_t slot) const noexcept {
        const std::int64_t offset =
            (static_cast<std::int64_t>(slot) - static_cast<std::int64_t>(kHalf)) * tick_raw_;
        return Price::from_raw(center_raw_ + offset);
    }

    void set_slot(std::size_t slot, std::int64_t quantity) noexcept {
        const std::int64_t previous = quantities_[slot];
        quantities_[slot] = quantity;
        if (quantity > 0) {
            mark_occupied(slot);
            if (previous <= 0) {
                ++live_levels_;
            }
            const auto candidate = static_cast<std::int32_t>(slot);
            if (best_slot_ < 0 ||
                (higher_is_better_ ? candidate > best_slot_ : candidate < best_slot_)) {
                best_slot_ = candidate;
            }
        } else {
            clear_occupied(slot);
            if (previous > 0) {
                --live_levels_;
            }
            if (best_slot_ == static_cast<std::int32_t>(slot)) {
                best_slot_ = next_slot(higher_is_better_ ? best_slot_ - 1 : best_slot_ + 1);
            }
        }
    }

    void mark_occupied(std::size_t slot) noexcept {
        occupancy_[slot >> 6] |= (std::uint64_t{1} << (slot & 63));
    }
    void clear_occupied(std::size_t slot) noexcept {
        occupancy_[slot >> 6] &= ~(std::uint64_t{1} << (slot & 63));
    }

    // Next occupied slot at or beyond `from` in the walk direction.
    [[nodiscard]] std::int32_t next_slot(std::int32_t from) const noexcept {
        if (from < 0 || from >= static_cast<std::int32_t>(MaxTicks)) {
            return -1;
        }
        std::size_t word = static_cast<std::size_t>(from) >> 6;
        std::uint64_t bits = occupancy_[word];
        if (higher_is_better_) {
            const std::size_t offset = static_cast<std::size_t>(from) & 63U;
            if (offset != 63U) {
                bits &= (std::uint64_t{1} << (offset + 1U)) - 1U;
            }
            for (;;) {
                if (bits != 0) {
                    return static_cast<std::int32_t>(
                        word * 64U + 63U -
                        static_cast<std::size_t>(std::countl_zero(bits)));
                }
                if (word == 0) {
                    return -1;
                }
                --word;
                bits = occupancy_[word];
            }
        } else {
            bits &= ~((std::uint64_t{1} << (static_cast<std::size_t>(from) & 63U)) - 1U);
            for (;;) {
                if (bits != 0) {
                    return static_cast<std::int32_t>(
                        word * 64U + static_cast<std::size_t>(std::countr_zero(bits)));
                }
                ++word;
                if (word >= kWords) {
                    return -1;
                }
                bits = occupancy_[word];
            }
        }
    }

    std::int64_t tick_raw_{1'000'000};
    std::int64_t center_raw_{3'000'000'000'000};
    bool higher_is_better_{true};
    std::int32_t best_slot_{-1};
    std::int32_t live_levels_{0};
    std::array<std::int64_t, MaxTicks> quantities_{};
    std::array<std::uint64_t, kWords> occupancy_{};
};

}  // namespace detail

// ---------------------------------------------------------------------------
// L2 order book
// ---------------------------------------------------------------------------
template <std::size_t MaxTicks = 8192>
class alignas(64) L2OrderBook {
public:
    static constexpr std::size_t kMaxTicks = MaxTicks;
    // Depth carried across a re-anchor. Every level the new window can still
    // hold is kept; this bounds the copy, not the book.
    static constexpr std::size_t kRetainedLevels = 64;

    L2OrderBook() noexcept = default;
    L2OrderBook(Price tick_size, Price center) noexcept { configure(tick_size, center); }
    L2OrderBook(const L2OrderBook&) = delete;
    L2OrderBook& operator=(const L2OrderBook&) = delete;

    void configure(Price tick_size, Price center) noexcept {
        begin_write();
        configure_owned(tick_size, center);
        end_write();
    }

    // Re-anchors the price window on `center`, keeping the tick. Levels the new
    // window can still hold are carried across, so a re-anchor does not throw
    // away depth it could keep; anything outside the new window is dropped,
    // because the grid it was indexed on no longer exists.
    //
    // A ladder anchored on a stale constant is blind -- it accepts nothing and
    // reports an empty book rather than a bad price, which is the failure this
    // method exists to undo. Note that a *diff* depth feed only repeats a level
    // when it changes, so a dropped level is not rebuilt on its own: after a
    // re-anchor that loses depth, a live deployment must reseed from a REST
    // snapshot (``seed_from_snapshot``) rather than trust the near touch.
    void recenter(Price center) noexcept {
        const Price tick = Price::from_raw(bid_.tick_raw());
        std::array<BookLevel, kRetainedLevels> bids{};
        std::array<BookLevel, kRetainedLevels> asks{};
        const std::size_t bid_count = bid_.walk(bids.data(), bids.size());
        const std::size_t ask_count = ask_.walk(asks.data(), asks.size());
        begin_write();
        configure_owned(tick, center);
        for (std::size_t i = 0; i < bid_count; ++i) {
            (void)bid_.set(bids[i].price, bids[i].quantity.raw());  // false = outside
        }
        for (std::size_t i = 0; i < ask_count; ++i) {
            (void)ask_.set(asks[i].price, asks[i].quantity.raw());
        }
        end_write();
    }

    [[nodiscard]] Price center() const noexcept {
        return center_price();
    }

    // A side with no levels means the feed cannot be represented at all, which
    // is different from a genuinely empty market: no venue quotes one-sided
    // forever, so a blind ladder is always a configuration/grid problem.
    [[nodiscard]] bool blind() const noexcept {
        return bid_.live_levels() == 0 || ask_.live_levels() == 0;
    }

    [[nodiscard]] bool contains(Price price) const noexcept {
        return bid_.contains(price);
    }

    // L2 semantics: absolute quantity at a price. Zero removes the level.
    [[nodiscard]] bool apply_level(Side side, Price price, Quantity quantity,
                                   Nanos timestamp = 0) noexcept {
        begin_write();
        const bool ok = ladder(side).set(price, quantity.raw());
        if (ok) {
            if (timestamp != 0) {
                last_exchange_ts_ = timestamp;
            }
            ++updates_;
        }
        end_write();
        return ok;
    }

    // Displayed quantity resting at ``price`` on one side; zero when the level
    // is empty or the price is outside the ladder. Read-only, allocation-free.
    [[nodiscard]] Quantity quantity_at(Side side, Price price) const noexcept {
        return ladder(side).quantity_at(price);
    }

    // Signed delta update, useful for L3 aggregation or trade prints.
    [[nodiscard]] bool apply_delta(Side side, Price price, Quantity delta,
                                   Nanos timestamp = 0) noexcept {
        begin_write();
        const bool ok = ladder(side).add(price, delta.raw());
        if (ok) {
            if (timestamp != 0) {
                last_exchange_ts_ = timestamp;
            }
            ++updates_;
        }
        end_write();
        return ok;
    }

    void set_sequence(std::uint64_t sequence, Nanos exchange_ts = 0,
                      Nanos recv_ts = 0) noexcept {
        begin_write();
        if (sequence != 0) {
            last_sequence_ = sequence;
        }
        if (exchange_ts != 0) {
            last_exchange_ts_ = exchange_ts;
        }
        if (recv_ts != 0) {
            last_recv_ts_ = recv_ts;
        }
        end_write();
    }

    void clear() noexcept {
        begin_write();
        bid_.configure(Price::from_raw(bid_.tick_raw()), Price::from_raw(bid_.center_raw()),
                       true);
        ask_.configure(Price::from_raw(ask_.tick_raw()), Price::from_raw(ask_.center_raw()),
                       false);
        last_exchange_ts_ = 0;
        last_recv_ts_ = 0;
        last_sequence_ = 0;
        updates_ = 0;
        end_write();
    }

    [[nodiscard]] Bbo top() const noexcept {
        Bbo bbo;
        for (int attempt = 0; attempt < 64; ++attempt) {
            const std::uint32_t start = sequence_.load(std::memory_order_acquire);
            if ((start & 1U) != 0) {
                continue;
            }
            fill_bbo(bbo);
            std::atomic_thread_fence(std::memory_order_acquire);
            if (sequence_.load(std::memory_order_relaxed) == start) {
                return bbo;
            }
        }
        return bbo;  // best-effort under pathological writer pressure
    }

    [[nodiscard]] bool snapshot(BookSnapshot& out) const noexcept {
        for (int attempt = 0; attempt < 64; ++attempt) {
            const std::uint32_t start = sequence_.load(std::memory_order_acquire);
            if ((start & 1U) != 0) {
                continue;
            }
            out.bid_count = static_cast<std::uint32_t>(
                bid_.walk(out.bids.data(), BookSnapshot::kMaxLevels));
            out.ask_count = static_cast<std::uint32_t>(
                ask_.walk(out.asks.data(), BookSnapshot::kMaxLevels));
            out.sequence = last_sequence_;
            fill_bbo(out.bbo);
            std::atomic_thread_fence(std::memory_order_acquire);
            if (sequence_.load(std::memory_order_relaxed) == start) {
                return true;
            }
        }
        return false;
    }

    [[nodiscard]] std::size_t depth(Side side, BookLevel* out,
                                    std::size_t max_levels) const noexcept {
        for (int attempt = 0; attempt < 64; ++attempt) {
            const std::uint32_t start = sequence_.load(std::memory_order_acquire);
            if ((start & 1U) != 0) {
                continue;
            }
            const std::size_t count = ladder(side).walk(out, max_levels);
            std::atomic_thread_fence(std::memory_order_acquire);
            if (sequence_.load(std::memory_order_relaxed) == start) {
                return count;
            }
        }
        return 0;
    }

    [[nodiscard]] Price best_bid() const noexcept { return bid_.best_price(); }
    [[nodiscard]] Price best_ask() const noexcept { return ask_.best_price(); }
    [[nodiscard]] std::size_t live_levels(Side side) const noexcept {
        return ladder(side).live_levels();
    }
    [[nodiscard]] std::uint64_t updates() const noexcept { return updates_; }
    [[nodiscard]] Nanos last_update_ts() const noexcept { return last_exchange_ts_; }
    [[nodiscard]] Price tick_size() const noexcept { return Price::from_raw(bid_.tick_raw()); }
    [[nodiscard]] Price center_price() const noexcept {
        return Price::from_raw(bid_.center_raw());
    }

private:
    using Ladder = detail::LadderSide<MaxTicks>;

    // The write half of ``configure``, for callers already holding the seqlock.
    void configure_owned(Price tick_size, Price center) noexcept {
        bid_.configure(tick_size, center, true);
        ask_.configure(tick_size, center, false);
        last_exchange_ts_ = 0;
        last_recv_ts_ = 0;
        last_sequence_ = 0;
        updates_ = 0;
    }

    [[nodiscard]] const Ladder& ladder(Side side) const noexcept {
        return side == Side::Buy ? bid_ : ask_;
    }
    [[nodiscard]] Ladder& ladder(Side side) noexcept {
        return side == Side::Buy ? bid_ : ask_;
    }

    void fill_bbo(Bbo& out) const noexcept {
        const bool has_bid = bid_.has_best();
        const bool has_ask = ask_.has_best();
        out.bid_price = bid_.best_price();
        out.ask_price = ask_.best_price();
        out.bid_quantity = bid_.best_quantity();
        out.ask_quantity = ask_.best_quantity();
        out.exchange_ts = last_exchange_ts_;
        out.recv_ts = last_recv_ts_;
        out.sequence = last_sequence_;
        out.valid = static_cast<std::uint8_t>(has_bid && has_ask);
    }

    // Writer-side seqlock publication. One writer thread only.
    void begin_write() noexcept {
        const std::uint32_t start = sequence_.load(std::memory_order_relaxed);
        sequence_.store(start + 1U, std::memory_order_relaxed);
        std::atomic_thread_fence(std::memory_order_release);
    }
    void end_write() noexcept {
        const std::uint32_t start = sequence_.load(std::memory_order_relaxed);
        sequence_.store(start + 1U, std::memory_order_release);
    }

    alignas(64) Ladder bid_{};
    alignas(64) Ladder ask_{};
    mutable std::atomic<std::uint32_t> sequence_{0};
    std::uint64_t last_sequence_{0};
    Nanos last_exchange_ts_{0};
    Nanos last_recv_ts_{0};
    std::uint64_t updates_{0};
};

using L2Book = L2OrderBook<8192>;

// Whether a depth frame proves the ladder is anchored on a price the window can
// no longer hold, and if so where it must move.
//
// A ladder covers `center +/- kHalf` ticks. Anchor it on a constant that the
// live market has left behind and every level is rejected while `top()` keeps
// returning an empty book -- a failure that looks exactly like a quiet market,
// so it can survive a whole study unnoticed. The book that is merely out of date
// is worse: once it holds levels it is no longer ``blind()``, so the next time
// the market leaves the window it rejects the whole frame while still publishing
// that believable -- and wrong -- BBO to the quoting and matching paths, with
// ``unpriced_frames`` at zero. The test is therefore the frame, not the book:
// when this frame carries a touch the window cannot hold, the anchor is
// demonstrably wrong whatever the ladder currently contains. The frame's own
// touch is the only evidence available about where the market is.
//
// The tick is never touched: it is the venue's grid, and a wrong tick has to
// stay visible (as rejected levels) instead of being guessed around. The rule is
// idempotent -- once the anchor equals the frame's touch it returns zero -- so an
// unrepresentable grid cannot make the book thrash, clearing itself every frame.
// Two conditions are required as evidence, not one: the touch must be outside
// *and* most of the frame must be unrepresentable, so a corrupt print cannot
// drag the book onto a price the market never traded at.
//
// Returns the price to re-anchor on, or a default (zero) Price for "no move".
template <std::size_t MaxTicks>
[[nodiscard]] inline Price reframe_anchor(const L2OrderBook<MaxTicks>& book,
                                          const DepthUpdate& update) noexcept {
    Price best_bid{};
    Price best_ask{};
    bool has_bid = false;
    bool has_ask = false;
    // How much of this frame the current window can hold. The touch alone is not
    // evidence: real captures carry stray levels thousands of ticks away, and a
    // frame with one implausible price would otherwise drag the anchor onto a
    // corrupt level and lose the whole book with it.
    std::uint32_t levels = 0;
    std::uint32_t held = 0;
    for (std::uint32_t i = 0; i < update.bid_count; ++i) {
        const LevelDelta& level = update.bids[i];
        if (level.quantity.raw() <= 0) {
            continue;
        }
        ++levels;
        if (book.contains(level.price)) {
            ++held;
        }
        if (!has_bid || level.price.raw() > best_bid.raw()) {
            best_bid = level.price;
            has_bid = true;
        }
    }
    for (std::uint32_t i = 0; i < update.ask_count; ++i) {
        const LevelDelta& level = update.asks[i];
        if (level.quantity.raw() <= 0) {
            continue;
        }
        ++levels;
        if (book.contains(level.price)) {
            ++held;
        }
        if (!has_ask || level.price.raw() < best_ask.raw()) {
            best_ask = level.price;
            has_ask = true;
        }
    }
    if (levels == 0) {
        return Price{};
    }
    if ((!has_bid || book.contains(best_bid)) && (!has_ask || book.contains(best_ask))) {
        return Price{};  // the window can hold this frame: nothing is wrong yet
    }
    // The touch is outside the window. Move the anchor only when most of the frame
    // cannot be held either -- a lone bad price is a bad print, not proof that the
    // market has left -- unless the book is blind, in which case it has nothing to
    // lose and nothing to keep it right.
    if (!book.blind() && static_cast<std::uint64_t>(held) * 2 > levels) {
        return Price{};
    }
    // Mid of whatever sides this frame touches, floored onto the tick grid.
    const Price::rep mid = has_bid && has_ask
                               ? (best_bid.raw() + best_ask.raw()) / 2
                               : (has_bid ? best_bid.raw() : best_ask.raw());
    const Price::rep tick = book.tick_size().raw();
    const Price::rep anchor = tick > 0 ? mid - (mid % tick) : mid;
    if (anchor == book.center().raw()) {
        return Price{};  // already anchored there: the grid, not the anchor, is wrong
    }
    return Price::from_raw(anchor);
}

// ---------------------------------------------------------------------------
// L3 order book: fixed order pool + open-addressed order-id index.
// The L2 aggregate is published through the same seqlock, so strategy readers
// get coherent BBO/depth while the matching thread mutates individual orders.
// ---------------------------------------------------------------------------
template <std::size_t MaxOrders = 8192, std::size_t MaxTicks = 8192>
class alignas(64) L3OrderBook {
public:
    static constexpr std::size_t kMaxOrders = MaxOrders;
    static constexpr std::size_t kHashCapacity = std::bit_ceil(MaxOrders * 2);
    static constexpr std::size_t kHashMask = kHashCapacity - 1;

    struct alignas(64) RestingOrder {
        OrderId id{0};
        Price price{};
        Quantity quantity{};
        Quantity filled{};
        Nanos timestamp{0};
        std::int32_t next_free{-1};
        Side side{Side::Buy};
        std::uint8_t live{0};
        std::uint8_t reserved[3]{};
    };

    L3OrderBook() noexcept {
        configure(Price::from_raw(1'000'000), Price::from_raw(3'000'000'000'000));
    }
    L3OrderBook(Price tick_size, Price center) noexcept { configure(tick_size, center); }
    L3OrderBook(const L3OrderBook&) = delete;
    L3OrderBook& operator=(const L3OrderBook&) = delete;

    void configure(Price tick_size, Price center) noexcept {
        aggregate_.configure(tick_size, center);
        reset_pool();
    }

    [[nodiscard]] bool add_order(OrderId id, Side side, Price price, Quantity quantity,
                                 Nanos timestamp = 0) noexcept {
        if (id == 0 || quantity.raw() <= 0 || find_slot(id) >= 0 || free_head_ < 0) {
            return false;
        }
        if (!aggregate_.apply_delta(side, price, quantity, timestamp)) {
            return false;
        }
        maybe_rehash();
        const std::int32_t slot = free_head_;
        free_head_ = orders_[slot].next_free;
        auto& order = orders_[slot];
        order.id = id;
        order.price = price;
        order.quantity = quantity;
        order.filled = Quantity{};
        order.timestamp = timestamp;
        order.side = side;
        order.live = 1;
        order.next_free = -1;
        hash_insert(id, slot);
        ++open_count_;
        return true;
    }

    [[nodiscard]] bool cancel_order(OrderId id, Quantity quantity = Quantity{}) noexcept {
        return reduce_order(id, quantity, 0, false);
    }

    [[nodiscard]] bool execute_order(OrderId id, Quantity quantity,
                                     Nanos timestamp = 0) noexcept {
        return reduce_order(id, quantity, timestamp, true);
    }

    [[nodiscard]] std::optional<OrderView> find(OrderId id) const noexcept {
        const std::int32_t slot = find_slot(id);
        if (slot < 0) {
            return std::nullopt;
        }
        const auto& order = orders_[slot];
        OrderView view;
        view.order_id = order.id;
        view.price = order.price;
        view.quantity = order.quantity + order.filled;
        view.filled = order.filled;
        view.created_ts = order.timestamp;
        view.updated_ts = order.timestamp;
        view.side = order.side;
        view.status =
            order.filled.raw() > 0 ? OrderStatus::PartiallyFilled : OrderStatus::New;
        view.type = OrderType::Limit;
        view.post_only = 0;
        return view;
    }

    [[nodiscard]] Bbo top() const noexcept { return aggregate_.top(); }
    [[nodiscard]] bool snapshot(BookSnapshot& out) const noexcept {
        return aggregate_.snapshot(out);
    }
    [[nodiscard]] std::size_t depth(Side side, BookLevel* out,
                                    std::size_t max_levels) const noexcept {
        return aggregate_.depth(side, out, max_levels);
    }
    [[nodiscard]] Price best_bid() const noexcept { return aggregate_.best_bid(); }
    [[nodiscard]] Price best_ask() const noexcept { return aggregate_.best_ask(); }
    [[nodiscard]] std::size_t open_orders() const noexcept { return open_count_; }
    [[nodiscard]] std::uint64_t aggregate_updates() const noexcept {
        return aggregate_.updates();
    }
    [[nodiscard]] L2OrderBook<MaxTicks>& aggregate() noexcept { return aggregate_; }
    [[nodiscard]] const L2OrderBook<MaxTicks>& aggregate() const noexcept { return aggregate_; }

private:
    [[nodiscard]] static constexpr std::uint64_t mix(OrderId key) noexcept {
        std::uint64_t x = key + 0x9e3779b97f4a7c15ULL;
        x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9ULL;
        x = (x ^ (x >> 27)) * 0x94d049bb133111ebULL;
        return x ^ (x >> 31);
    }

    void reset_pool() noexcept {
        hash_keys_.fill(0);
        hash_values_.fill(-1);
        for (std::size_t i = 0; i < MaxOrders; ++i) {
            orders_[i] = RestingOrder{};
            orders_[i].next_free =
                (i + 1 < MaxOrders) ? static_cast<std::int32_t>(i + 1) : -1;
        }
        free_head_ = MaxOrders == 0 ? -1 : 0;
        open_count_ = 0;
        tombstones_ = 0;
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
        for (std::size_t i = 0; i < MaxOrders; ++i) {
            if (orders_[i].live != 0) {
                hash_insert(orders_[i].id, static_cast<std::int32_t>(i));
            }
        }
    }

    [[nodiscard]] bool reduce_order(OrderId id, Quantity quantity, Nanos timestamp,
                                    bool is_execution) noexcept {
        const std::int32_t slot = find_slot(id);
        if (slot < 0) {
            return false;
        }
        auto& order = orders_[slot];
        std::int64_t reduce_raw = quantity.raw();
        if (reduce_raw <= 0) {
            reduce_raw = order.quantity.raw();
        }
        if (reduce_raw > order.quantity.raw()) {
            reduce_raw = order.quantity.raw();
        }
        if (reduce_raw <= 0) {
            return false;
        }
        const auto delta = Quantity::from_raw(-reduce_raw);
        if (!aggregate_.apply_delta(order.side, order.price, delta, timestamp)) {
            return false;
        }
        order.quantity = Quantity::from_raw(order.quantity.raw() - reduce_raw);
        if (is_execution) {
            order.filled = Quantity::from_raw(order.filled.raw() + reduce_raw);
        }
        if (order.quantity.raw() == 0) {
            hash_erase(id);
            order.live = 0;
            order.next_free = free_head_;
            free_head_ = slot;
            --open_count_;
        }
        return true;
    }

    L2OrderBook<MaxTicks> aggregate_{};
    std::array<RestingOrder, MaxOrders> orders_{};
    std::array<OrderId, kHashCapacity> hash_keys_{};
    std::array<std::int32_t, kHashCapacity> hash_values_{};
    std::int32_t free_head_{-1};
    std::size_t open_count_{0};
    std::size_t tombstones_{0};
};

using L3Book = L3OrderBook<8192, 8192>;

}  // namespace hft