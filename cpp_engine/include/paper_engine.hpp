// SPDX-License-Identifier: MIT
// Phase 2 paper-trading engine. Wires L2 depth into the seqlock order book,
// matches taker orders by walking the ladder level by level (slippage), fills
// crossed resting orders as maker, and keeps cash/fee/PnL in a paper_account.
// Single-threaded: one thread calls process_update()/submit(), readers snapshot.
#pragma once

#include <array>
#include <cstdint>

#include "latency_histogram.hpp"
#include "matching.hpp"
#include "order_manager.hpp"
#include "orderbook.hpp"
#include "paper_account.hpp"
#include "risk_guard.hpp"
#include "types.hpp"

namespace hft {

// How a resting (maker) order earns a fill.
//
// touch  — legacy model: the order fills in full at its limit price as soon as
//          the opposite touch reaches it. Fast, deterministic, and optimistic
//          in both directions: it ignores the queue in front of the order and
//          it only ever fills on the frame where the market moved through the
//          quote, so every fill it reports is adversarially selected by
//          construction.
// queue  — depth-only volume-at-price model: the order fills only from quantity
//          that actually traded at its own price after it was posted, and only
//          once the displayed depth ahead of it has been consumed. Fills are
//          partial and a quote that merely rests at the touch is not handed a
//          fill. Inferred from level decreases, so it is blind to a quote that
//          rests inside the spread (where the tape, not the book, records the
//          trade) and therefore under-reports fills.
// trade  — tape-driven model, the reference for maker economics: a resting
//          order fills when an aggressor trade prints at or through its price,
//          after the displayed queue ahead of it has been consumed. Requires
//          trade prints in the archive; without them nothing fills, which is
//          the honest answer rather than a fabricated one.
//
// Select with --fill-model; the legacy default keeps historic studies stable.
enum class fill_model : std::uint8_t { touch = 0, queue = 1, trade = 2 };

struct paper_ack {
    OrderId order_id{0};
    RejectReason reason{RejectReason::None};
    Price average_price{};
    Quantity filled{};
    Notional slippage{};

    [[nodiscard]] bool accepted() const noexcept { return reason == RejectReason::None; }
};

struct paper_metrics {
    std::uint64_t depth_events{0};
    std::uint64_t gap_events{0};
    std::uint64_t orders_submitted{0};
    std::uint64_t orders_rejected{0};
    std::uint64_t orders_cancelled{0};
    std::uint64_t fills{0};
    Quantity filled_quantity{};
    Notional traded_notional{};
    Notional slippage_total{};
    std::uint64_t p50_ns{0};
    std::uint64_t p99_ns{0};
    std::uint64_t p999_ns{0};
    std::uint32_t open_orders{0};
    Nanos last_update_ts{0};
    Nanos last_frame_latency_ns{0};
    std::uint64_t trade_events{0};
    // Grid health. ``unpriced_frames`` counts depth frames applied while the
    // book still had no two-sided top: it is the only counter that separates a
    // healthy book from a blind one, because a wide venue feed legitimately
    // carries levels far outside the window (those are counted separately as
    // ``rejected_levels`` and are not, on their own, a problem).
    // ``book_recenters`` counts how often the window had to be re-anchored.
    std::uint64_t rejected_levels{0};
    std::uint64_t book_recenters{0};
    std::uint64_t unpriced_frames{0};
    paper_account_snapshot account{};
};

class paper_engine {
public:
    static constexpr std::size_t kFillRing = 4096;
    static constexpr std::size_t kMaxCrossed = 256;

    // Fee rates accept fractional basis points (e.g. 0.5); the ledger freezes
    // them into micro-bps so the matching path remains integer-only.
    // ``model`` selects the maker fill model (see ``fill_model`` above).
    paper_engine(Price tick_size, Price center, double maker_bps = 0.0,
                 double taker_bps = 5.0,
                 Notional initial_cash = Notional{},
                 fill_model model = fill_model::touch) noexcept
        : book_(tick_size, center),
          center_(center),
          configured_center_(center),
          mark_price_(center),
          maker_bps_(maker_bps),
          taker_bps_(taker_bps),
          initial_cash_(initial_cash),
          fill_model_(model) {
        reset();
    }
    paper_engine(const paper_engine&) = delete;
    paper_engine& operator=(const paper_engine&) = delete;

    // Switching models mid-session is allowed; resting orders restart their
    // queue accounting from the book they are looking at now.
    void set_fill_model(fill_model model) noexcept { fill_model_ = model; }
    [[nodiscard]] fill_model model() const noexcept { return fill_model_; }

    void reset() noexcept {
        center_ = configured_center_;
        book_.configure(book_.tick_size(), center_);
        orders_.reset();
        account_.reset(initial_cash_, maker_bps_, taker_bps_);
        latency_.reset();
        fills_.fill(Fill{});
        fill_head_ = 0;
        fill_tail_ = 0;
        fill_count_ = 0;
        counters_ = paper_metrics{};
        last_final_update_id_ = 0;
        last_slippage_ = Notional{};
        mark_price_ = center_;
        queue_slots_.fill(queue_slot{});
    }

    void process_update(const DepthUpdate& update) noexcept {
        const Nanos start = now_ns();
        reframe_if_blind(update);
        for (std::uint32_t i = 0; i < update.bid_count; ++i) {
            if (!book_.apply_level(Side::Buy, update.bids[i].price,
                                   update.bids[i].quantity, update.exchange_ts)) {
                ++counters_.rejected_levels;
            }
        }
        for (std::uint32_t i = 0; i < update.ask_count; ++i) {
            if (!book_.apply_level(Side::Sell, update.asks[i].price,
                                   update.asks[i].quantity, update.exchange_ts)) {
                ++counters_.rejected_levels;
            }
        }
        if (update.prev_final_update_id != 0 && last_final_update_id_ != 0 &&
            update.prev_final_update_id != last_final_update_id_) {
            ++counters_.gap_events;
        }
        last_final_update_id_ = update.final_update_id;
        book_.set_sequence(update.final_update_id, update.exchange_ts, update.recv_ts);
        ++counters_.depth_events;
        counters_.last_update_ts =
            std::max(counters_.last_update_ts, update.exchange_ts);
        const Bbo top = book_.top();
        if (top.is_valid()) {
            mark_price_ = top.mid();
        } else {
            ++counters_.unpriced_frames;
        }
        account_.mark(mark_price_);
        match_resting(update.exchange_ts);
        counters_.last_frame_latency_ns = now_ns() - start;
        latency_.record(counters_.last_frame_latency_ns);
    }

    // Applies an aggressor trade print. Under ``fill_model::trade`` this is the
    // only event that can fill a resting order; under the other models the print
    // is recorded (``paper_metrics::trade_events``) and otherwise ignored.
    void process_trade(const TradeTick& trade) noexcept {
        last_trade_ = trade;
        ++counters_.trade_events;
        if (fill_model_ != fill_model::trade) {
            return;
        }
        match_trade(trade);
    }

    [[nodiscard]] paper_ack submit(const OrderRequest& request) noexcept {
        paper_ack ack;
        ++counters_.orders_submitted;
        const Bbo bbo = book_.top();
        const bool is_market = request.type == OrderType::Market;

        if (request.quantity.raw() <= 0) {
            return reject(ack, RejectReason::InvalidQuantity);
        }
        if (!is_market && request.price.raw() <= 0) {
            return reject(ack, RejectReason::InvalidPrice);
        }
        if (request.client_id != 0 && orders_.find(request.client_id).has_value()) {
            return reject(ack, RejectReason::DuplicateOrder);
        }
        if (!bbo.is_valid()) {
            return reject(ack, RejectReason::BookUnavailable);
        }

        const bool crosses =
            is_market ||
            (request.type != OrderType::PostOnly &&
             (request.side == Side::Buy ? request.price >= bbo.ask_price
                                        : request.price <= bbo.bid_price));
        if (request.type == OrderType::PostOnly && crosses) {
            return reject(ack, RejectReason::PostOnlyWouldCross);
        }

        if (!crosses) {
            const OrderId id =
                orders_.create(request, request.type == OrderType::PostOnly);
            if (id == 0) {
                return reject(ack, RejectReason::OrderPoolExhausted);
            }
            seed_queue_state(id, request.side, request.price);
            ack.order_id = id;
            if (request.time_in_force == TimeInForce::IOC) {
                Quantity cancelled;
                if (orders_.cancel(id, request.timestamp, cancelled)) {
                    ++counters_.orders_cancelled;
                }
            }
            return ack;
        }

        const Price limit = is_market ? Price{} : request.price;
        const match_result match =
            match_aggressive(book_, request.side, request.quantity, limit);
        if (match.filled.raw() == 0) {
            if (request.time_in_force == TimeInForce::FOK) {
                return reject(ack, RejectReason::FillOrKillUnavailable);
            }
            if (request.time_in_force == TimeInForce::IOC || is_market) {
                ++counters_.orders_cancelled;
                return ack;
            }
        }

        const OrderId id = orders_.create(request, false);
        if (id == 0) {
            return reject(ack, RejectReason::OrderPoolExhausted);
        }
        ack.order_id = id;

        for (std::uint32_t i = 0; i < match.leg_count; ++i) {
            const match_leg& leg = match.legs[i];
            Quantity remaining;
            (void)orders_.apply_fill(id, leg.quantity, leg.price, request.timestamp,
                                     remaining);
            const Notional fee =
                account_.apply_fill(leg.price, leg.quantity, request.side, false);
            push_fill(id, leg.price, leg.quantity, request.side, false, fee);
            ++counters_.fills;
            counters_.filled_quantity += leg.quantity;
            counters_.traded_notional += notional(leg.price, leg.quantity);
            counters_.slippage_total +=
                notional(leg.price - match.touch_price, leg.quantity).abs();
        }

        ack.filled = match.filled;
        ack.average_price = match.average_price;
        ack.slippage = match.slippage;
        last_slippage_ = match.slippage;

        if (match.remaining.raw() > 0 &&
            (is_market || request.time_in_force == TimeInForce::IOC)) {
            Quantity cancelled;
            if (orders_.cancel(id, request.timestamp, cancelled)) {
                ++counters_.orders_cancelled;
            }
        }

        account_.mark(mark_price_);
        return ack;
    }

    [[nodiscard]] bool cancel(OrderId id, Nanos timestamp = 0) noexcept {
        Quantity cancelled;
        if (!orders_.cancel(id, timestamp, cancelled)) {
            return false;
        }
        ++counters_.orders_cancelled;
        return true;
    }

    std::size_t cancel_all(Nanos timestamp = 0) noexcept {
        std::size_t count = 0;
        orders_.for_each_live([&](DefaultOrderManager::Slot& slot) {
            Quantity cancelled;
            if (orders_.cancel(slot.id, timestamp, cancelled)) {
                ++count;
            }
        });
        counters_.orders_cancelled += count;
        return count;
    }

    [[nodiscard]] bool drain_fill(Fill& out) noexcept {
        if (fill_count_ == 0) {
            return false;
        }
        out = fills_[fill_tail_ & (kFillRing - 1)];
        fill_tail_ = (fill_tail_ + 1) & (kFillRing - 1);
        --fill_count_;
        return true;
    }

    [[nodiscard]] Bbo bbo() const noexcept { return book_.top(); }
    [[nodiscard]] Price mark_price() const noexcept { return mark_price_; }
    [[nodiscard]] const L2Book& book() const noexcept { return book_; }
    [[nodiscard]] PositionSnapshot position() const noexcept {
        PositionSnapshot out;
        out.net_quantity = account_.net_quantity();
        out.average_entry_price = account_.average_entry_price();
        out.realized_pnl = account_.realized_pnl();
        out.unrealized_pnl = account_.unrealized_pnl();
        return out;
    }
    [[nodiscard]] const paper_account& account() const noexcept { return account_; }
    [[nodiscard]] Notional last_slippage() const noexcept { return last_slippage_; }

    [[nodiscard]] paper_metrics metrics() const noexcept {
        paper_metrics out = counters_;
        out.open_orders = static_cast<std::uint32_t>(orders_.open_orders());
        out.p50_ns = latency_.p50();
        out.p99_ns = latency_.p99();
        out.p999_ns = latency_.p999();
        out.account = account_.snapshot();
        return out;
    }

    [[nodiscard]] std::size_t pending_fills() const noexcept { return fill_count_; }
    [[nodiscard]] Price tick_size() const noexcept { return book_.tick_size(); }
    // The window the ladder is anchored on *now*; it follows the feed when the
    // configured centre cannot hold it (see ``reframe_if_blind``).
    [[nodiscard]] Price center() const noexcept { return center_; }

private:
    // A ladder anchored on a stale constant rejects every level of a live feed
    // and reports an empty book, which is indistinguishable from a quiet market.
    // ``reframe_anchor`` decides from the frame itself whether that is what is
    // happening; moving the window is safe here because a blind ladder holds no
    // levels to lose, and the venue's absolute levels rebuild it next frame.
    void reframe_if_blind(const DepthUpdate& update) noexcept {
        const Price anchor = reframe_anchor(book_, update);
        if (anchor.raw() <= 0) {
            return;
        }
        book_.recenter(anchor);
        center_ = anchor;
        mark_price_ = anchor;
        ++counters_.book_recenters;
    }

    // Per-order queue accounting for the volume-at-price and tape models. Kept
    // out of the order slot, which is pinned to exactly one cache line; indexed
    // by order id and validated on read, so a recycled entry degrades to
    // "freshly posted at the depth now displayed".
    struct queue_slot {
        OrderId id{0};
        std::int64_t queue_ahead{0};
        std::int64_t ref_qty{0};
    };

    // A maker fill detected during iteration and booked once the live-order
    // list is no longer being walked.
    struct staged_fill {
        OrderId id;
        Price price;
        Side side;
        Quantity quantity;
    };

    [[nodiscard]] paper_ack reject(paper_ack ack, RejectReason reason) noexcept {
        ack.reason = reason;
        ++counters_.orders_rejected;
        return ack;
    }

    void push_fill(OrderId id, Price price, Quantity quantity, Side side, bool is_maker,
                   Notional fee) noexcept {
        Fill& fill = fills_[fill_head_ & (kFillRing - 1)];
        fill.order_id = id;
        fill.price = price;
        fill.quantity = quantity;
        fill.fee = fee;
        fill.timestamp = now_ns();
        fill.side = side;
        fill.is_maker = static_cast<std::uint8_t>(is_maker ? 1 : 0);
        if (fill_count_ == kFillRing) {
            fill_tail_ = (fill_tail_ + 1) & (kFillRing - 1);
        } else {
            ++fill_count_;
        }
        fill_head_ = (fill_head_ + 1) & (kFillRing - 1);
    }

    // Seeds the queue model so a freshly posted order joins the back of the
    // displayed queue at its price instead of being granted the front of it.
    void seed_queue_state(OrderId id, Side side, Price price) noexcept {
        const std::int64_t displayed = book_.quantity_at(side, price).raw();
        queue_slot& state = queue_state(id);
        state.id = id;
        state.queue_ahead = displayed;
        state.ref_qty = displayed;
    }

    void match_resting(Nanos timestamp) noexcept {
        const Bbo bbo = book_.top();
        if (!bbo.is_valid()) {
            return;
        }
        if (fill_model_ == fill_model::queue) {
            match_resting_queue(timestamp);
            return;
        }
        if (fill_model_ != fill_model::touch) {
            // Tape model: a depth frame alone can never fill a maker order.
            return;
        }
        struct resting_cross {
            OrderId id;
            Price price;
            Side side;
            Quantity remaining;
        };
        std::array<resting_cross, kMaxCrossed> crossed{};
        std::size_t crossed_count = 0;
        orders_.for_each_live([&](DefaultOrderManager::Slot& slot) {
            if (crossed_count >= crossed.size()) {
                return;
            }
            const bool crosses = slot.side == Side::Buy
                                     ? slot.price >= bbo.ask_price
                                     : slot.price <= bbo.bid_price;
            if (!crosses) {
                return;
            }
            crossed[crossed_count++] = {slot.id, slot.price, slot.side,
                                        slot.quantity - slot.filled};
        });
        for (std::size_t i = 0; i < crossed_count; ++i) {
            if (crossed[i].remaining.raw() <= 0) {
                continue;
            }
            Quantity remaining;
            (void)orders_.apply_fill(crossed[i].id, crossed[i].remaining,
                                     crossed[i].price, timestamp, remaining);
            const Notional fee = account_.apply_fill(
                crossed[i].price, crossed[i].remaining, crossed[i].side, true);
            push_fill(crossed[i].id, crossed[i].price, crossed[i].remaining,
                      crossed[i].side, true, fee);
            ++counters_.fills;
            counters_.filled_quantity += crossed[i].remaining;
            counters_.traded_notional +=
                notional(crossed[i].price, crossed[i].remaining);
        }
        account_.mark(mark_price_);
    }

    // Volume-at-price maker fills. See ``fill_model::queue`` for the contract:
    // only quantity that traded at the order's own price can fill it, and only
    // after the depth ahead of it is consumed. A decrease in a level's displayed
    // size is read as traded volume, which is the conservative assumption when
    // the feed is L2 rather than L3 (cancels are indistinguishable from trades,
    // so this errs toward filling too often, never too rarely).
    void match_resting_queue(Nanos timestamp) noexcept {
        std::array<staged_fill, kMaxCrossed> fills{};
        std::size_t fill_count = 0;

        // Pass 1: advance queue accounting. The live list must not be mutated
        // here, so candidate fills are staged and applied afterwards.
        orders_.for_each_live([&](DefaultOrderManager::Slot& slot) {
            const std::int64_t displayed =
                book_.quantity_at(slot.side, slot.price).raw();
            queue_slot& state = queue_state(slot.id);
            if (state.id != slot.id) {
                // Never seeded, or the entry was recycled by a later order:
                // treat it as freshly posted at the depth now displayed.
                state.id = slot.id;
                state.queue_ahead = displayed;
                state.ref_qty = displayed;
                return;
            }
            const std::int64_t traded =
                state.ref_qty > displayed ? state.ref_qty - displayed : 0;
            state.ref_qty = displayed;
            if (traded <= 0) {
                return;
            }
            std::int64_t available = traded;
            if (state.queue_ahead > 0) {
                const std::int64_t consumed =
                    available < state.queue_ahead ? available : state.queue_ahead;
                state.queue_ahead -= consumed;
                available -= consumed;
            }
            if (available <= 0 || fill_count >= fills.size()) {
                return;
            }
            const std::int64_t remaining = slot.quantity.raw() - slot.filled.raw();
            if (remaining <= 0) {
                return;
            }
            const std::int64_t filled = available < remaining ? available : remaining;
            fills[fill_count++] = {slot.id, slot.price, slot.side,
                                   Quantity::from_raw(filled)};
        });

        apply_staged_fills(fills, fill_count, timestamp);
    }

    // Tape-driven maker fills. A resting buy fills on a sell aggressor printing
    // at or below its price; a resting sell fills on a buy aggressor printing at
    // or above it. The print's size is allocated in price priority, and only
    // after the displayed queue ahead of each order has been consumed.
    void match_trade(const TradeTick& trade) noexcept {
        if (trade.quantity.raw() <= 0) {
            return;
        }
        const Side resting_side =
            trade.aggressor == Side::Buy ? Side::Sell : Side::Buy;

        struct eligible {
            queue_slot* state;
            OrderId id;
            Price price;
            Quantity remaining;
        };
        std::array<eligible, kMaxCrossed> candidates{};
        std::size_t candidate_count = 0;

        orders_.for_each_live([&](DefaultOrderManager::Slot& slot) {
            if (slot.side != resting_side || candidate_count >= candidates.size()) {
                return;
            }
            const bool eligible_price =
                slot.side == Side::Buy ? trade.price.raw() <= slot.price.raw()
                                       : trade.price.raw() >= slot.price.raw();
            if (!eligible_price) {
                return;
            }
            const std::int64_t remaining = slot.quantity.raw() - slot.filled.raw();
            if (remaining <= 0) {
                return;
            }
            queue_slot& state = queue_state(slot.id);
            if (state.id != slot.id) {
                const std::int64_t displayed =
                    book_.quantity_at(slot.side, slot.price).raw();
                state.id = slot.id;
                state.queue_ahead = displayed;
                state.ref_qty = displayed;
            }
            candidates[candidate_count++] = {&state, slot.id, slot.price,
                                             Quantity::from_raw(remaining)};
        });

        // Price priority: the most aggressive resting price trades first.
        for (std::size_t i = 1; i < candidate_count; ++i) {
            eligible key = candidates[i];
            std::size_t j = i;
            while (j > 0) {
                const bool better = resting_side == Side::Buy
                                        ? key.price.raw() > candidates[j - 1].price.raw()
                                        : key.price.raw() < candidates[j - 1].price.raw();
                if (!better) {
                    break;
                }
                candidates[j] = candidates[j - 1];
                --j;
            }
            candidates[j] = key;
        }

        std::array<staged_fill, kMaxCrossed> fills{};
        std::size_t fill_count = 0;
        std::int64_t available = trade.quantity.raw();
        for (std::size_t i = 0; i < candidate_count && available > 0; ++i) {
            queue_slot& state = *candidates[i].state;
            if (state.queue_ahead > 0) {
                const std::int64_t consumed =
                    available < state.queue_ahead ? available : state.queue_ahead;
                state.queue_ahead -= consumed;
                available -= consumed;
            }
            if (available <= 0) {
                break;
            }
            const std::int64_t remaining = candidates[i].remaining.raw();
            const std::int64_t filled = available < remaining ? available : remaining;
            if (filled <= 0) {
                continue;
            }
            available -= filled;
            fills[fill_count++] = {candidates[i].id, candidates[i].price,
                                   resting_side, Quantity::from_raw(filled)};
        }

        apply_staged_fills(fills, fill_count, trade.exchange_ts);
    }

    // Books staged maker fills. Called after iteration so the live list is not
    // mutated while it is being walked.
    void apply_staged_fills(std::array<staged_fill, kMaxCrossed>& fills,
                            std::size_t fill_count, Nanos timestamp) noexcept {
        for (std::size_t i = 0; i < fill_count; ++i) {
            Quantity remaining;
            if (!orders_.apply_fill(fills[i].id, fills[i].quantity, fills[i].price,
                                    timestamp, remaining)) {
                continue;
            }
            const Notional fee = account_.apply_fill(
                fills[i].price, fills[i].quantity, fills[i].side, true);
            push_fill(fills[i].id, fills[i].price, fills[i].quantity,
                      fills[i].side, true, fee);
            ++counters_.fills;
            counters_.filled_quantity += fills[i].quantity;
            counters_.traded_notional += notional(fills[i].price, fills[i].quantity);
        }
        account_.mark(mark_price_);
    }

    static constexpr std::size_t kQueueSlots = DefaultOrderManager::kCapacity;
    static_assert((kQueueSlots & (kQueueSlots - 1)) == 0,
                  "queue table must be a power of two to mask the order id");

    [[nodiscard]] queue_slot& queue_state(OrderId id) noexcept {
        return queue_slots_[static_cast<std::size_t>(id) & (kQueueSlots - 1)];
    }

    L2Book book_;
    paper_account account_;
    DefaultOrderManager orders_;
    LatencyHistogram latency_;
    std::array<Fill, kFillRing> fills_{};
    std::size_t fill_head_{0};
    std::size_t fill_tail_{0};
    std::size_t fill_count_{0};
    paper_metrics counters_{};
    Price center_{};             // window anchor in use (follows the feed)
    Price configured_center_{};  // what the caller asked for; restored on reset
    Price mark_price_{};
    double maker_bps_{0.0};  // basis points, fractions preserved
    double taker_bps_{5.0};  // basis points, fractions preserved
    Notional initial_cash_{};
    std::uint64_t last_final_update_id_{0};
    Notional last_slippage_{};
    fill_model fill_model_{fill_model::touch};
    std::array<queue_slot, kQueueSlots> queue_slots_{};
    TradeTick last_trade_{};
};

}  // namespace hft
