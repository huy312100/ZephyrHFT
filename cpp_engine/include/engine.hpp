// SPDX-License-Identifier: MIT
// TradingEngine wires the market-data book, order manager, risk guard and
// latency telemetry into one single-writer event loop.
//
// Threading model:
//   * feed thread  : publish_depth()/publish_trade() into the SPSC queue
//   * engine thread: poll() drains the queue, updates the book, submits orders
//   * strategy     : reads BBO/depth through the seqlock (wait-free)
#pragma once

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string_view>

#include "latency_histogram.hpp"
#include "order_manager.hpp"
#include "orderbook.hpp"
#include "ring_buffer.hpp"
#include "risk_guard.hpp"
#include "risk_manager.hpp"
#include "types.hpp"

namespace hft {

enum class EngineEventType : std::uint8_t { Depth = 0, Trade = 1 };

// Feed payload carried through the SPSC queue. `DepthUpdate::symbol` views are
// not retained: the symbol is copied inline so the event owns its bytes.
struct alignas(64) EngineEvent {
    EngineEventType type{EngineEventType::Depth};
    std::uint8_t symbol_length{0};
    std::uint8_t reserved[6]{};
    std::array<char, 16> symbol{};
    DepthUpdate depth{};
    TradeTick trade{};
    Nanos enqueue_ts{0};
};

struct OrderAck {
    OrderId order_id{0};
    RejectReason reason{RejectReason::None};
    Price avg_price{};
    Quantity filled{};

    [[nodiscard]] bool accepted() const noexcept { return reason == RejectReason::None; }
};

struct EngineMetrics {
    std::uint64_t depth_events{0};
    std::uint64_t trade_events{0};
    std::uint64_t book_updates{0};
    std::uint64_t orders_submitted{0};
    std::uint64_t orders_rejected{0};
    std::uint64_t orders_cancelled{0};
    std::uint64_t fills{0};
    Quantity filled_quantity{};
    Notional traded_notional{};
    Nanos last_update_ts{0};
    std::uint64_t p50_ns{0};
    std::uint64_t p99_ns{0};
    std::uint64_t p999_ns{0};
    std::uint32_t open_orders{0};
    // Grid health: depth levels the ladder could not represent, how often the
    // price window had to be re-anchored on the feed, and how many frames left
    // the book without a two-sided top (the only one of the three that means
    // "the book is not the market").
    std::uint64_t rejected_levels{0};
    std::uint64_t book_recenters{0};
    std::uint64_t unpriced_frames{0};
};

struct EngineConfig {
    Price tick_size = Price::from_raw(1'000'000);
    Price center = Price::from_raw(3'000'000'000'000);
    Nanos stale_after_ns = 1'000'000'000;
    RiskLimits risk{};
    // Disabled by default so replay and backtests keep their unthrottled,
    // deterministic behaviour. Live deployments set `enabled = true` (the
    // remaining fields are then the production defaults).
    RiskManagerConfig risk_manager = RiskManagerConfig::disabled();
};

class TradingEngine {
public:
    static constexpr std::size_t kInboundCapacity = 2048;
    static constexpr std::size_t kFillCapacity = 8192;

    explicit TradingEngine(const EngineConfig& config = {}) noexcept
        : config_(config),
          book_(config.tick_size, config.center),
          risk_(config.risk),
          risk_manager_(config.risk_manager) {
        reset();
    }
    TradingEngine(const TradingEngine&) = delete;
    TradingEngine& operator=(const TradingEngine&) = delete;

    void reset() noexcept {
        book_.configure(config_.tick_size, config_.center);
        orders_.reset();
        inbound_.reset();
        fills_.reset();
        latency_.reset();
        position_ = PositionSnapshot{};
        last_trade_ = TradeTick{};
        mark_price_ = config_.center;
        metrics_ = EngineMetrics{};
        risk_manager_.reset(now_ns());
    }

    // ---- feed ingestion -------------------------------------------------
    [[nodiscard]] bool publish_depth(const DepthUpdate& update, Nanos recv_ts = 0) noexcept {
        EngineEvent event;
        event.type = EngineEventType::Depth;
        copy_symbol(update.symbol, event);
        event.depth = update;
        event.depth.symbol = {};
        // Queue latency uses the engine clock because caller timestamps may be
        // in a different clock domain; the receive timestamp is kept for
        // diagnostics only.
        event.depth.recv_ts = recv_ts;
        event.enqueue_ts = now_ns();
        return inbound_.try_push(event);
    }

    [[nodiscard]] bool publish_trade(const TradeTick& trade, Nanos recv_ts = 0) noexcept {
        EngineEvent event;
        event.type = EngineEventType::Trade;
        event.trade = trade;
        event.trade.recv_ts = recv_ts;
        event.enqueue_ts = now_ns();
        return inbound_.try_push(event);
    }

    // Drains up to `max_events` queued updates. Returns the number applied.
    std::size_t poll(std::size_t max_events = kInboundCapacity) noexcept {
        std::size_t processed = 0;
        while (processed < max_events) {
            EngineEvent event;
            if (!inbound_.try_pop(event)) {
                break;
            }
            if (event.enqueue_ts != 0) {
                const Nanos dequeued = now_ns();
                if (dequeued >= event.enqueue_ts) {
                    latency_.record(dequeued - event.enqueue_ts);
                }
            }
            if (event.type == EngineEventType::Depth) {
                apply_depth(event.depth);
            } else {
                apply_trade(event.trade);
            }
            ++processed;
        }
        return processed;
    }

    void apply_depth(const DepthUpdate& update) noexcept {
        reframe_if_out_of_window(update);
        for (std::uint32_t i = 0; i < update.bid_count; ++i) {
            const bool applied =
                book_.apply_level(Side::Buy, update.bids[i].price, update.bids[i].quantity,
                                  update.exchange_ts);
            if (!applied) {
                ++metrics_.rejected_levels;
            }
        }
        for (std::uint32_t i = 0; i < update.ask_count; ++i) {
            const bool applied =
                book_.apply_level(Side::Sell, update.asks[i].price, update.asks[i].quantity,
                                  update.exchange_ts);
            if (!applied) {
                ++metrics_.rejected_levels;
            }
        }
        book_.set_sequence(update.final_update_id, update.exchange_ts, update.recv_ts);
        ++metrics_.depth_events;
        metrics_.last_update_ts = std::max(metrics_.last_update_ts, update.exchange_ts);
        if (book_.best_bid().raw() > 0 && book_.best_ask().raw() > 0) {
            mark_price_ = book_.top().mid();
        } else {
            ++metrics_.unpriced_frames;
        }
        recompute_unrealized();
    }

    void apply_trade(const TradeTick& trade) noexcept {
        last_trade_ = trade;
        if (trade.price.raw() > 0) {
            mark_price_ = trade.price;
        }
        ++metrics_.trade_events;
        metrics_.last_update_ts = std::max(metrics_.last_update_ts, trade.exchange_ts);
        recompute_unrealized();
    }

    // ---- order entry ----------------------------------------------------
    [[nodiscard]] OrderAck submit(const OrderRequest& request) noexcept {
        OrderAck ack;
        ++metrics_.orders_submitted;
        if (request.client_id != 0 && orders_.find(request.client_id).has_value()) {
            ++metrics_.orders_rejected;
            ack.reason = RejectReason::DuplicateOrder;
            return ack;
        }
        const Bbo current = book_.top();
        const bool is_market = request.type == OrderType::Market;

        // Order-flow / fat-finger gate. It runs only when the book holds a
        // coherent two-sided quote: there is no mid to band against otherwise,
        // and `risk_.check()` below already reports BookUnavailable, which is
        // the more precise diagnosis. The engine is single-book, so the gate is
        // symbol-agnostic and the reject hook receives an empty symbol.
        if (risk_manager_.enabled() && current.is_valid()) {
            // Market orders carry no price; validate them at the touch, which
            // is where they would actually execute.
            const double reference =
                is_market
                    ? (request.side == Side::Buy ? current.ask_price
                                                 : current.bid_price)
                          .to_double()
                    : request.price.to_double();
            const RiskStatus risk_status = risk_manager_.validate_order(
                std::string_view{}, reference, request.quantity.to_double(),
                request.side, position_.net_quantity.to_double(),
                current.mid().to_double());
            if (!is_approved(risk_status)) {
                ++metrics_.orders_rejected;
                ack.reason = to_reject_reason(risk_status);
                return ack;
            }
        }
        const bool crosses =
            is_market ||
            (request.type != OrderType::PostOnly &&
             (request.side == Side::Buy ? request.price >= current.ask_price
                                        : request.price <= current.bid_price));
        Quantity pending_buy;
        Quantity pending_sell;
        orders_.pending_quantities(pending_buy, pending_sell);
        const RiskDecision decision =
            risk_.check(request, current, position_, orders_.open_orders(),
                        pending_buy, pending_sell);
        if (!decision.approved()) {
            ++metrics_.orders_rejected;
            ack.reason = decision.reason;
            return ack;
        }

        // FOK has no resting state: it must be fully fillable at the touch.
        if (request.time_in_force == TimeInForce::FOK) {
            const Quantity available = request.side == Side::Buy
                                           ? current.ask_quantity
                                           : current.bid_quantity;
            if (!crosses || available < request.quantity) {
                ++metrics_.orders_rejected;
                ack.reason = RejectReason::FillOrKillUnavailable;
                return ack;
            }
        }

        const OrderId id = orders_.create(request, request.type == OrderType::PostOnly);
        if (id == 0) {
            ++metrics_.orders_rejected;
            ack.reason = RejectReason::OrderPoolExhausted;
            return ack;
        }
        ack.order_id = id;

        // An IOC order that cannot trade immediately is cancelled rather than
        // left resting; this mirrors exchange semantics.
        if (!crosses) {
            if (request.time_in_force == TimeInForce::IOC) {
                Quantity cancelled;
                if (orders_.cancel(id, request.timestamp, cancelled)) {
                    ++metrics_.orders_cancelled;
                }
            }
            return ack;
        }

        const Price touch =
            request.side == Side::Buy ? current.ask_price : current.bid_price;
        const Quantity available =
            request.side == Side::Buy ? current.ask_quantity : current.bid_quantity;
        const std::int64_t fill_raw =
            std::min(request.quantity.raw(), available.raw());
        if (fill_raw > 0) {
            const Quantity fill_quantity = Quantity::from_raw(fill_raw);
            Quantity remaining;
            if (orders_.apply_fill(id, fill_quantity, touch, request.timestamp,
                                   remaining)) {
                ack.filled = fill_quantity;
                ack.avg_price = touch;
                record_fill(id, touch, fill_quantity, request.side, false,
                            request.timestamp);
                // Reflect our own execution in the local ladder until the next
                // exchange depth update restores the authoritative level.
                const bool applied = book_.apply_delta(
                    opposite(request.side), touch, -fill_quantity,
                    current.exchange_ts);
                (void)applied;
                if (book_.best_bid().raw() > 0 && book_.best_ask().raw() > 0) {
                    mark_price_ = book_.top().mid();
                }
                recompute_unrealized();
            }
            if (remaining.raw() > 0 &&
                (is_market || request.time_in_force == TimeInForce::IOC)) {
                Quantity cancelled;
                if (orders_.cancel(id, request.timestamp, cancelled)) {
                    ++metrics_.orders_cancelled;
                }
            }
        } else if (is_market || request.time_in_force == TimeInForce::IOC) {
            Quantity cancelled;
            if (orders_.cancel(id, request.timestamp, cancelled)) {
                ++metrics_.orders_cancelled;
            }
        }
        return ack;
    }

    [[nodiscard]] bool cancel(OrderId id, Nanos timestamp = 0) noexcept {
        Quantity cancelled;
        if (!orders_.cancel(id, timestamp, cancelled)) {
            return false;
        }
        ++metrics_.orders_cancelled;
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
        metrics_.orders_cancelled += count;
        return count;
    }

    [[nodiscard]] bool drain_fill(Fill& out) noexcept { return fills_.try_pop(out); }

    [[nodiscard]] Bbo bbo() const noexcept { return book_.top(); }
    [[nodiscard]] Price mark_price() const noexcept { return mark_price_; }
    [[nodiscard]] const TradeTick& last_trade() const noexcept { return last_trade_; }
    [[nodiscard]] const PositionSnapshot& position() const noexcept { return position_; }
    [[nodiscard]] const L2Book& book() const noexcept { return book_; }
    [[nodiscard]] const DefaultOrderManager& orders() const noexcept { return orders_; }
    [[nodiscard]] const LatencyHistogram& latency() const noexcept { return latency_; }
    [[nodiscard]] const EngineConfig& config() const noexcept { return config_; }

    void set_risk_limits(const RiskLimits& limits) noexcept { risk_.set_limits(limits); }
    [[nodiscard]] const RiskLimits& risk_limits() const noexcept { return risk_.limits(); }

    // Pre-trade guard for order-flow and fat-finger limits. Tunable at runtime
    // from a control thread; all of its state is atomic.
    void set_risk_manager_config(const RiskManagerConfig& config) noexcept {
        risk_manager_.configure(config);
    }
    [[nodiscard]] RiskManagerConfig risk_manager_config() const noexcept {
        return risk_manager_.config();
    }
    [[nodiscard]] RiskManager& risk_manager() noexcept { return risk_manager_; }
    [[nodiscard]] const RiskManager& risk_manager() const noexcept {
        return risk_manager_;
    }
    [[nodiscard]] RiskManagerStats risk_stats(Nanos now = 0) const noexcept {
        return risk_manager_.stats(now);
    }

    [[nodiscard]] bool is_stale(Nanos now) const noexcept {
        return metrics_.last_update_ts == 0 ||
               now > metrics_.last_update_ts + config_.stale_after_ns;
    }

    [[nodiscard]] EngineMetrics metrics() const noexcept {
        EngineMetrics out = metrics_;
        out.book_updates = book_.updates();
        out.p50_ns = latency_.p50();
        out.p99_ns = latency_.p99();
        out.p999_ns = latency_.p999();
        out.open_orders = static_cast<std::uint32_t>(orders_.open_orders());
        return out;
    }

    [[nodiscard]] std::size_t pending_events() const noexcept { return inbound_.size_approx(); }
    [[nodiscard]] std::size_t pending_fills() const noexcept { return fills_.size_approx(); }

private:
    static void copy_symbol(std::string_view symbol, EngineEvent& event) noexcept {
        const std::size_t length = std::min(symbol.size(), event.symbol.size() - 1);
        if (length != 0) {
            std::memcpy(event.symbol.data(), symbol.data(), length);
        }
        event.symbol[length] = '\0';
        event.symbol_length = static_cast<std::uint8_t>(length);
    }

    void record_fill(OrderId id, Price price, Quantity quantity, Side side,
                     bool is_maker, Nanos timestamp) noexcept {
        Fill fill;
        fill.order_id = id;
        fill.price = price;
        fill.quantity = quantity;
        fill.timestamp = timestamp != 0 ? timestamp : now_ns();
        fill.side = side;
        fill.is_maker = static_cast<std::uint8_t>(is_maker ? 1 : 0);
        const bool queued = fills_.try_push(fill);
        (void)queued;  // A full fill queue drops the audit record, never the fill.
        ++metrics_.fills;
        metrics_.filled_quantity += quantity;
        metrics_.traded_notional += notional(price, quantity);

        // Average-cost accounting. The fill either extends the current side,
        // closes part of it (realising PnL) or flips through flat.
        const std::int64_t existing = position_.net_quantity.raw();
        const std::int64_t fill_raw = quantity.raw();
        const bool buying = side == Side::Buy;
        const std::int64_t signed_fill = buying ? fill_raw : -fill_raw;
        const std::int64_t closing =
            std::min(abs_raw(existing), fill_raw);
        if (existing != 0 && buying != (existing > 0)) {
            const Price pnl_per_unit = buying
                ? position_.average_entry_price - price
                : price - position_.average_entry_price;
            position_.realized_pnl += notional(pnl_per_unit,
                                               Quantity::from_raw(closing));
        }

        const std::int64_t new_net = existing + signed_fill;
        if (existing == 0 || (existing > 0) == (signed_fill > 0)) {
            const std::int64_t old_abs = abs_raw(existing);
            const Wide total = static_cast<Wide>(old_abs) + fill_raw;
            const Wide weighted =
                static_cast<Wide>(old_abs) * position_.average_entry_price.raw() +
                static_cast<Wide>(fill_raw) * price.raw();
            position_.average_entry_price =
                Price::from_raw(static_cast<Price::rep>(weighted / total));
        } else if (new_net == 0) {
            position_.average_entry_price = Price{};
        } else if ((new_net > 0) != (existing > 0)) {
            position_.average_entry_price = price;
        }
        position_.net_quantity = Quantity::from_raw(new_net);
        recompute_unrealized();
    }

    // Marks the open position to the latest mid/last-trade price.
    void recompute_unrealized() noexcept {
        if (position_.net_quantity.raw() == 0 ||
            position_.average_entry_price.raw() == 0) {
            position_.unrealized_pnl = Notional{};
            return;
        }
        const Price diff = mark_price_ - position_.average_entry_price;
        position_.unrealized_pnl = notional(diff, position_.net_quantity);
    }

    [[nodiscard]] static std::int64_t abs_raw(std::int64_t value) noexcept {
        return value < 0 ? -value : value;
    }

    // A live book whose ladder is anchored on a stale constant is blind: it
    // rejects every level and looks like a quiet market while quoting against
    // nothing. ``reframe_anchor`` says when a frame proves the window is wrong;
    // the tree is re-based on the feed's own touch. A populated ladder is
    // reframed as well -- keeping its stale BBO is worse than losing depth --
    // and ``recenter`` carries across the levels the new window still holds.
    void reframe_if_out_of_window(const DepthUpdate& update) noexcept {
        const Price anchor = reframe_anchor(book_, update);
        if (anchor.raw() <= 0) {
            return;
        }
        book_.recenter(anchor);
        mark_price_ = anchor;
        ++metrics_.book_recenters;
    }

    EngineConfig config_{};
    L2Book book_;
    DefaultOrderManager orders_{};
    RiskGuard risk_{};
    RiskManager risk_manager_{};
    SpscRingBuffer<EngineEvent, kInboundCapacity> inbound_{};
    SpscRingBuffer<Fill, kFillCapacity> fills_{};
    LatencyHistogram latency_{};
    PositionSnapshot position_{};
    TradeTick last_trade_{};
    Price mark_price_{};
    EngineMetrics metrics_{};
};

}  // namespace hft
