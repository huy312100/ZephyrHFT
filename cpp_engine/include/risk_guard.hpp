// SPDX-License-Identifier: MIT
// Pre-trade risk checks executed synchronously before an order leaves the
// engine. All arithmetic is fixed-point; no allocation, no exceptions.
#pragma once

#include <cstddef>
#include <cstdint>

#include "types.hpp"

namespace hft {

struct PositionSnapshot {
    Quantity net_quantity{};
    Price average_entry_price{};
    Notional realized_pnl{};
    Notional unrealized_pnl{};
};

struct RiskLimits {
    Quantity max_order_quantity = Quantity::from_int(100);
    Notional max_order_notional = Notional::from_int(1'000'000);
    Notional max_position_notional = Notional::from_int(10'000'000);
    Notional max_daily_loss = Notional::from_int(250'000);
    Price max_price_deviation = Price::from_int(1'000);
    std::uint32_t max_open_orders = 1'024;
};

struct RiskDecision {
    RejectReason reason{RejectReason::None};
    [[nodiscard]] bool approved() const noexcept { return reason == RejectReason::None; }
};

class RiskGuard {
public:
    RiskGuard() noexcept = default;
    explicit RiskGuard(const RiskLimits& limits) noexcept : limits_(limits) {}

    void set_limits(const RiskLimits& limits) noexcept { limits_ = limits; }
    [[nodiscard]] const RiskLimits& limits() const noexcept { return limits_; }

    // `bbo` must be the latest coherent top-of-book view. `open_orders` is the
    // number of currently live orders owned by this strategy.
    [[nodiscard]] RiskDecision check(const OrderRequest& request, const Bbo& bbo,
                                     const PositionSnapshot& position,
                                     std::size_t open_orders,
                                     Quantity pending_buy = {},
                                     Quantity pending_sell = {}) const noexcept {
        if (request.quantity.raw() <= 0) {
            return {RejectReason::InvalidQuantity};
        }
        if (request.type != OrderType::Market && request.price.raw() <= 0) {
            return {RejectReason::InvalidPrice};
        }
        if (request.quantity > limits_.max_order_quantity) {
            return {RejectReason::InvalidQuantity};
        }
        if (open_orders >= limits_.max_open_orders) {
            return {RejectReason::OpenOrderLimit};
        }
        if ((position.realized_pnl + position.unrealized_pnl) < -limits_.max_daily_loss) {
            return {RejectReason::DailyLossLimit};
        }
        if (!bbo.is_valid()) {
            return {RejectReason::BookUnavailable};
        }

        const bool crossing_limit =
            request.type == OrderType::Limit &&
            (request.side == Side::Buy ? request.price >= bbo.ask_price
                                       : request.price <= bbo.bid_price);
        // Crossing limit orders execute at the touch, so use that price for
        // notional checks; non-crossing limits use their own limit price.
        const Price reference =
            request.type == OrderType::Market || crossing_limit
                ? (request.side == Side::Buy ? bbo.ask_price : bbo.bid_price)
                : request.price;

        // Post-only orders must rest behind the touch.
        if (request.type == OrderType::PostOnly) {
            const bool crosses = request.side == Side::Buy
                                     ? request.price >= bbo.ask_price
                                     : request.price <= bbo.bid_price;
            if (crosses) {
                return {RejectReason::PostOnlyWouldCross};
            }
        }

        if (request.type != OrderType::Market) {
            const Price deviation = abs_price(request.price - bbo.mid());
            if (deviation > limits_.max_price_deviation) {
                return {RejectReason::PriceBand};
            }
        }
        // Apply the per-order notional cap using the touch price for market
        // and crossing limit orders; resting limits use their own price.
        if (abs_notional(notional(reference, request.quantity)) >
            limits_.max_order_notional) {
            return {RejectReason::NotionalLimit};
        }

        // Worst-case exposure includes resting orders on either side, not just
        // the net position, because both books can fill independently.
        const std::int64_t worst_long =
            position.net_quantity.raw() + pending_buy.raw() +
            (request.side == Side::Buy ? request.quantity.raw() : 0);
        const std::int64_t worst_short =
            position.net_quantity.raw() - pending_sell.raw() -
            (request.side == Side::Sell ? request.quantity.raw() : 0);
        const std::int64_t worst = abs_raw(worst_long) > abs_raw(worst_short)
                                       ? worst_long
                                       : worst_short;
        const Quantity projected = Quantity::from_raw(worst);
        if (abs_notional(notional(reference, projected)) > limits_.max_position_notional) {
            return {RejectReason::PositionLimit};
        }
        return {};
    }

private:
    [[nodiscard]] static Price abs_price(Price value) noexcept {
        return Price::from_raw(value.raw() < 0 ? -value.raw() : value.raw());
    }

    [[nodiscard]] static Notional abs_notional(Notional value) noexcept {
        return Notional::from_raw(value.raw() < 0 ? -value.raw() : value.raw());
    }

    [[nodiscard]] static std::int64_t abs_raw(std::int64_t value) noexcept {
        return value < 0 ? -value : value;
    }

    RiskLimits limits_{};
};

}  // namespace hft
