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
    paper_account_snapshot account{};
};

class paper_engine {
public:
    static constexpr std::size_t kFillRing = 4096;
    static constexpr std::size_t kMaxCrossed = 256;

    // Fee rates accept fractional basis points (e.g. 0.5); the ledger freezes
    // them into micro-bps so the matching path remains integer-only.
    paper_engine(Price tick_size, Price center, double maker_bps = 0.0,
                 double taker_bps = 5.0,
                 Notional initial_cash = Notional{}) noexcept
        : book_(tick_size, center),
          center_(center),
          mark_price_(center),
          maker_bps_(maker_bps),
          taker_bps_(taker_bps),
          initial_cash_(initial_cash) {
        reset();
    }
    paper_engine(const paper_engine&) = delete;
    paper_engine& operator=(const paper_engine&) = delete;

    void reset() noexcept {
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
    }

    void process_update(const DepthUpdate& update) noexcept {
        const Nanos start = now_ns();
        for (std::uint32_t i = 0; i < update.bid_count; ++i) {
            (void)book_.apply_level(Side::Buy, update.bids[i].price,
                                    update.bids[i].quantity, update.exchange_ts);
        }
        for (std::uint32_t i = 0; i < update.ask_count; ++i) {
            (void)book_.apply_level(Side::Sell, update.asks[i].price,
                                    update.asks[i].quantity, update.exchange_ts);
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
        }
        account_.mark(mark_price_);
        match_resting(update.exchange_ts);
        counters_.last_frame_latency_ns = now_ns() - start;
        latency_.record(counters_.last_frame_latency_ns);
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
    [[nodiscard]] Price center() const noexcept { return center_; }

private:
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

    void match_resting(Nanos timestamp) noexcept {
        const Bbo bbo = book_.top();
        if (!bbo.is_valid()) {
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

    L2Book book_;
    paper_account account_;
    DefaultOrderManager orders_;
    LatencyHistogram latency_;
    std::array<Fill, kFillRing> fills_{};
    std::size_t fill_head_{0};
    std::size_t fill_tail_{0};
    std::size_t fill_count_{0};
    paper_metrics counters_{};
    Price center_{};
    Price mark_price_{};
    double maker_bps_{0.0};  // basis points, fractions preserved
    double taker_bps_{5.0};  // basis points, fractions preserved
    Notional initial_cash_{};
    std::uint64_t last_final_update_id_{0};
    Notional last_slippage_{};
};

}  // namespace hft
