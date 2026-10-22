// SPDX-License-Identifier: MIT
// Paper-trading ledger: cash, fees, position and mark-to-market PnL, all in
// signed 64-bit fixed point. Single-threaded by design: one matching thread
// mutates it and readers call snapshot().
#pragma once

#include <cstdint>

#include "types.hpp"

namespace hft {

struct paper_account_snapshot {
    Notional cash{};
    Notional fees_paid{};
    Notional realized_pnl{};
    Notional unrealized_pnl{};
    Notional gross_notional{};
    Notional equity{};
    Quantity net_quantity{};
    Price average_entry_price{};
    std::uint64_t fill_count{0};
    std::uint64_t maker_fills{0};
    std::uint64_t taker_fills{0};
};

class paper_account {
public:
    paper_account() noexcept = default;

    void reset(Notional initial_cash, std::int32_t maker_bps,
               std::int32_t taker_bps) noexcept {
        cash_ = initial_cash;
        maker_bps_ = maker_bps;
        taker_bps_ = taker_bps;
        fees_paid_ = Notional{};
        realized_pnl_ = Notional{};
        unrealized_pnl_ = Notional{};
        gross_notional_ = Notional{};
        net_quantity_ = Quantity{};
        average_entry_ = Price{};
        fill_count_ = 0;
        maker_fills_ = 0;
        taker_fills_ = 0;
    }

    void configure(std::int32_t maker_bps, std::int32_t taker_bps) noexcept {
        maker_bps_ = maker_bps;
        taker_bps_ = taker_bps;
    }

    // Applies one fill and returns the fee (negative for a maker rebate).
    [[nodiscard]] Notional apply_fill(Price price, Quantity quantity, Side side,
                                      bool is_maker) noexcept {
        const Notional fill_notional = notional(price, quantity);
        const std::int32_t fee_bps = is_maker ? maker_bps_ : taker_bps_;
        const Notional fee = fill_notional * static_cast<std::int64_t>(fee_bps) / 10'000;

        if (side == Side::Buy) {
            cash_ = cash_ - fill_notional;
        } else {
            cash_ = cash_ + fill_notional;
        }
        cash_ = cash_ - fee;
        fees_paid_ = fees_paid_ + fee;
        gross_notional_ = gross_notional_ + fill_notional;

        const std::int64_t existing = net_quantity_.raw();
        const std::int64_t fill_raw = quantity.raw();
        const std::int64_t signed_fill = side == Side::Buy ? fill_raw : -fill_raw;
        const std::int64_t closing = existing < 0 ? -existing : existing;
        const std::int64_t closing_qty = closing < fill_raw ? closing : fill_raw;

        Notional realized_delta{};
        if (existing != 0 && (side == Side::Buy) != (existing > 0)) {
            const Price pnl_per_unit =
                side == Side::Buy ? average_entry_ - price : price - average_entry_;
            realized_delta = notional(pnl_per_unit, Quantity::from_raw(closing_qty));
        }
        realized_delta = realized_delta - fee;
        realized_pnl_ = realized_pnl_ + realized_delta;

        const std::int64_t new_net = existing + signed_fill;
        if (existing == 0) {
            average_entry_ = price;
        } else if ((existing > 0) == (signed_fill > 0)) {
            const std::int64_t old_abs = existing < 0 ? -existing : existing;
            const Wide weighted = static_cast<Wide>(old_abs) * average_entry_.raw() +
                                  static_cast<Wide>(fill_raw) * price.raw();
            average_entry_ = Price::from_raw(static_cast<Price::rep>(
                weighted / static_cast<Wide>(old_abs + fill_raw)));
        } else if (new_net == 0) {
            average_entry_ = Price{};
        } else if ((new_net > 0) != (existing > 0)) {
            average_entry_ = price;
        }
        net_quantity_ = Quantity::from_raw(new_net);

        ++fill_count_;
        if (is_maker) {
            ++maker_fills_;
        } else {
            ++taker_fills_;
        }
        return fee;
    }

    void mark(Price price) noexcept {
        if (net_quantity_.raw() == 0 || average_entry_.raw() == 0) {
            unrealized_pnl_ = Notional{};
            return;
        }
        unrealized_pnl_ = notional(price - average_entry_, net_quantity_);
    }

    [[nodiscard]] Notional equity() const noexcept { return cash_ + unrealized_pnl_; }

    [[nodiscard]] paper_account_snapshot snapshot() const noexcept {
        paper_account_snapshot out;
        out.cash = cash_;
        out.fees_paid = fees_paid_;
        out.realized_pnl = realized_pnl_;
        out.unrealized_pnl = unrealized_pnl_;
        out.gross_notional = gross_notional_;
        out.equity = equity();
        out.net_quantity = net_quantity_;
        out.average_entry_price = average_entry_;
        out.fill_count = fill_count_;
        out.maker_fills = maker_fills_;
        out.taker_fills = taker_fills_;
        return out;
    }

    [[nodiscard]] Notional cash() const noexcept { return cash_; }
    [[nodiscard]] Notional fees_paid() const noexcept { return fees_paid_; }
    [[nodiscard]] Notional realized_pnl() const noexcept { return realized_pnl_; }
    [[nodiscard]] Notional unrealized_pnl() const noexcept { return unrealized_pnl_; }
    [[nodiscard]] Quantity net_quantity() const noexcept { return net_quantity_; }
    [[nodiscard]] Price average_entry_price() const noexcept { return average_entry_; }
    [[nodiscard]] std::uint64_t fill_count() const noexcept { return fill_count_; }
    [[nodiscard]] std::uint64_t maker_fills() const noexcept { return maker_fills_; }
    [[nodiscard]] std::uint64_t taker_fills() const noexcept { return taker_fills_; }

private:
    Notional cash_{};
    Notional fees_paid_{};
    Notional realized_pnl_{};
    Notional unrealized_pnl_{};
    Notional gross_notional_{};
    Quantity net_quantity_{};
    Price average_entry_{};
    std::int32_t maker_bps_{0};
    std::int32_t taker_bps_{0};
    std::uint64_t fill_count_{0};
    std::uint64_t maker_fills_{0};
    std::uint64_t taker_fills_{0};
};

}  // namespace hft
