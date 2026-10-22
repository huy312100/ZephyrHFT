// SPDX-License-Identifier: MIT
// Deterministic taker matching: walk the L2 ladder level by level and compute
// the volume-weighted average price, remaining quantity and slippage against
// the touch. No allocation, no mutation of the book.
#pragma once

#include <array>
#include <cstdint>

#include "orderbook.hpp"
#include "types.hpp"

namespace hft {

struct match_leg {
    Price price{};
    Quantity quantity{};
};

struct match_result {
    Quantity filled{};
    Quantity remaining{};
    Price average_price{};
    Notional notional{};
    Notional slippage{};
    Price touch_price{};
    std::uint32_t leg_count{0};
    std::array<match_leg, 32> legs{};
};

// Consumes the opposite side of `book` up to `target`, restricted to levels at
// or better than `limit_price` (a non-positive limit means "no limit").
[[nodiscard]] inline match_result match_aggressive(const L2Book& book, Side side,
                                                   Quantity target,
                                                   Price limit_price = Price{}) noexcept {
    match_result result;
    result.remaining = target;
    if (target.raw() <= 0) {
        return result;
    }

    const Side walk_side = opposite(side);
    const bool has_limit = limit_price.raw() > 0;
    std::array<BookLevel, 32> levels{};
    const std::size_t count = book.depth(walk_side, levels.data(), levels.size());
    if (count != 0) {
        result.touch_price = levels[0].price;
    }

    Wide weighted = 0;
    Wide filled_raw = 0;
    for (std::size_t i = 0; i < count && result.remaining.raw() > 0; ++i) {
        if (levels[i].quantity.raw() <= 0) {
            continue;
        }
        const bool eligible =
            !has_limit ||
            (side == Side::Buy ? levels[i].price <= limit_price
                               : levels[i].price >= limit_price);
        if (!eligible) {
            break;
        }
        const std::int64_t take = levels[i].quantity.raw() < result.remaining.raw()
                                      ? levels[i].quantity.raw()
                                      : result.remaining.raw();
        result.legs[result.leg_count] = {levels[i].price, Quantity::from_raw(take)};
        ++result.leg_count;
        weighted += static_cast<Wide>(levels[i].price.raw()) * static_cast<Wide>(take);
        filled_raw += static_cast<Wide>(take);
        result.remaining = Quantity::from_raw(result.remaining.raw() - take);
    }

    result.filled = Quantity::from_raw(static_cast<Quantity::rep>(filled_raw));
    if (filled_raw > 0) {
        result.average_price =
            Price::from_raw(static_cast<Price::rep>(weighted / filled_raw));
        result.notional = notional(result.average_price, result.filled);
        if (result.touch_price.raw() > 0) {
            result.slippage =
                notional(result.average_price - result.touch_price, result.filled);
        }
    }
    return result;
}

[[nodiscard]] inline match_result match_taker(const L2Book& book, Side side,
                                              Quantity target) noexcept {
    return match_aggressive(book, side, target, Price{});
}

[[nodiscard]] inline Price touch_price(const L2Book& book, Side side) noexcept {
    const Bbo top = book.top();
    return side == Side::Buy ? top.ask_price : top.bid_price;
}

}  // namespace hft
