// SPDX-License-Identifier: MIT
// Ultra-low-latency fixed-point primitives and market-data PODs.
//
// Design rules:
//   * No heap allocation, no exceptions, no RTTI in the hot path.
//   * Every market-data structure is trivially copyable so it can travel
//     through the SPSC ring buffer with a single memcpy.
//   * Fixed-point arithmetic is deterministic across machines and avoids
//     FPU latency spikes and NaN/denormal penalties.
#pragma once

#include <array>
#include <bit>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <optional>
#include <string_view>
#include <type_traits>

#include <compare>

namespace hft {

using OrderId = std::uint64_t;
using Nanos = std::uint64_t;
using Wide = __int128_t;
using UWidth = __uint128_t;

inline constexpr Nanos kNanosPerSecond = 1'000'000'000ULL;

// Monotonic wall clock in nanoseconds. Used only at feed/telemetry boundaries,
// never inside the fixed-point arithmetic itself.
[[nodiscard]] inline Nanos now_ns() noexcept {
    return static_cast<Nanos>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now().time_since_epoch())
            .count());
}
inline constexpr std::size_t kCacheLine = 64;
inline constexpr std::size_t kMaxDepthLevels = 32;

// ---------------------------------------------------------------------------
// decimal helpers
// ---------------------------------------------------------------------------
namespace detail {

[[nodiscard]] constexpr std::int64_t pow10(std::int32_t exponent) noexcept {
    std::int64_t value = 1;
    for (std::int32_t i = 0; i < exponent; ++i) {
        value *= 10;
    }
    return value;
}

[[nodiscard]] constexpr bool is_digit(char c) noexcept {
    return c >= '0' && c <= '9';
}

}  // namespace detail

struct PriceTag {};
struct QuantityTag {};
struct NotionalTag {};

// ---------------------------------------------------------------------------
// FixedPoint<Tag, Digits>
// ---------------------------------------------------------------------------
// A signed 64-bit fixed-point number with `Digits` decimal fraction digits.
// Price   : 8 decimals (~$92.2bn max, 1e-8 resolution)
// Quantity: 8 decimals (~92.2bn units, 1e-8 size resolution)
// The tag prevents accidentally adding a price to a quantity.
template <typename Tag, std::int32_t Digits>
class FixedPoint {
public:
    using rep = std::int64_t;

    static constexpr std::int32_t kDigits = Digits;
    static constexpr rep kScale = detail::pow10(Digits);
    static constexpr rep kMaxRaw = std::numeric_limits<rep>::max();
    static constexpr rep kMinRaw = std::numeric_limits<rep>::min();

    constexpr FixedPoint() noexcept = default;

    // Raw factory. This is the only constructor used on the hot path.
    [[nodiscard]] static constexpr FixedPoint from_raw(rep raw) noexcept {
        FixedPoint value;
        value.raw_ = raw;
        return value;
    }

    [[nodiscard]] static constexpr FixedPoint from_int(rep units) noexcept {
        return from_raw(units * kScale);
    }

    // Display / configuration boundary only. Never call in the hot path.
    [[nodiscard]] static FixedPoint from_double(double value) noexcept {
        const double scaled = value * static_cast<double>(kScale);
        return from_raw(static_cast<rep>(scaled >= 0.0 ? scaled + 0.5 : scaled - 0.5));
    }

    // Allocation-free decimal parser: "123.4567", "-0.0001", "+42".
    // Extra fractional digits are truncated, malformed input returns nullopt.
    [[nodiscard]] static constexpr std::optional<FixedPoint> from_string(
        std::string_view text) noexcept {
        if (text.empty()) {
            return std::nullopt;
        }
        std::size_t i = 0;
        bool negative = false;
        if (text[i] == '-' || text[i] == '+') {
            negative = text[i] == '-';
            ++i;
        }

        rep integer_part = 0;
        bool saw_digit = false;
        for (; i < text.size() && detail::is_digit(text[i]); ++i) {
            saw_digit = true;
            const rep digit = static_cast<rep>(text[i] - '0');
            if (integer_part > (kMaxRaw - digit) / 10) {
                return std::nullopt;  // overflow
            }
            integer_part = integer_part * 10 + digit;
        }

        rep fractional_part = 0;
        std::int32_t fractional_digits = 0;
        if (i < text.size() && text[i] == '.') {
            ++i;
            for (; i < text.size() && detail::is_digit(text[i]); ++i) {
                saw_digit = true;
                if (fractional_digits < Digits) {
                    fractional_part = fractional_part * 10 + (text[i] - '0');
                    ++fractional_digits;
                }
            }
        }
        if (!saw_digit || i != text.size()) {
            return std::nullopt;
        }
        for (; fractional_digits < Digits; ++fractional_digits) {
            fractional_part *= 10;
        }
        if (integer_part > (kMaxRaw - fractional_part) / kScale) {
            return std::nullopt;
        }
        const rep magnitude = integer_part * kScale + fractional_part;
        return from_raw(negative ? -magnitude : magnitude);
    }

    [[nodiscard]] constexpr rep raw() const noexcept { return raw_; }
    [[nodiscard]] constexpr double to_double() const noexcept {
        return static_cast<double>(raw_) / static_cast<double>(kScale);
    }
    [[nodiscard]] constexpr rep truncate_to_int() const noexcept { return raw_ / kScale; }
    [[nodiscard]] constexpr bool is_zero() const noexcept { return raw_ == 0; }
    [[nodiscard]] constexpr bool is_positive() const noexcept { return raw_ > 0; }
    [[nodiscard]] constexpr bool is_negative() const noexcept { return raw_ < 0; }

    [[nodiscard]] constexpr FixedPoint abs() const noexcept {
        return from_raw(raw_ < 0 ? -raw_ : raw_);
    }

    // Zero-allocation formatting into a caller-supplied buffer.
    // Returns the number of chars written, or 0 if `capacity` is too small.
    std::size_t to_chars(char* out, std::size_t capacity) const noexcept {
        constexpr std::size_t kRequired = 1 + 19 + 1 + Digits + 1;
        if (out == nullptr || capacity < kRequired) {
            return 0;
        }
        std::uint64_t magnitude = static_cast<std::uint64_t>(raw_);
        const bool negative = raw_ < 0;
        if (negative) {
            magnitude = (~magnitude) + 1U;  // INT64_MIN safe
        }
        const std::uint64_t integer_part = magnitude / static_cast<std::uint64_t>(kScale);
        std::uint64_t fractional_part = magnitude % static_cast<std::uint64_t>(kScale);

        char digits[24];
        std::size_t digit_count = 0;
        std::uint64_t value = integer_part;
        do {
            digits[digit_count++] = static_cast<char>('0' + (value % 10));
            value /= 10;
        } while (value != 0);

        std::size_t pos = 0;
        if (negative) {
            out[pos++] = '-';
        }
        while (digit_count != 0) {
            out[pos++] = digits[--digit_count];
        }
        out[pos++] = '.';
        char fraction[Digits];
        for (std::int32_t d = Digits - 1; d >= 0; --d) {
            fraction[d] = static_cast<char>('0' + (fractional_part % 10));
            fractional_part /= 10;
        }
        for (std::int32_t d = 0; d < Digits; ++d) {
            out[pos++] = fraction[d];
        }
        out[pos] = '\0';
        return pos;
    }

    constexpr FixedPoint& operator+=(FixedPoint other) noexcept {
        raw_ += other.raw_;
        return *this;
    }
    constexpr FixedPoint& operator-=(FixedPoint other) noexcept {
        raw_ -= other.raw_;
        return *this;
    }
    constexpr FixedPoint& operator*=(rep scalar) noexcept {
        raw_ *= scalar;
        return *this;
    }
    constexpr FixedPoint& operator/=(rep scalar) noexcept {
        raw_ /= scalar;
        return *this;
    }

    [[nodiscard]] constexpr FixedPoint operator-() const noexcept {
        return from_raw(-raw_);
    }

    friend constexpr FixedPoint operator+(FixedPoint lhs, FixedPoint rhs) noexcept {
        return from_raw(lhs.raw_ + rhs.raw_);
    }
    friend constexpr FixedPoint operator-(FixedPoint lhs, FixedPoint rhs) noexcept {
        return from_raw(lhs.raw_ - rhs.raw_);
    }
    friend constexpr FixedPoint operator*(FixedPoint lhs, rep scalar) noexcept {
        return from_raw(lhs.raw_ * scalar);
    }
    friend constexpr FixedPoint operator*(rep scalar, FixedPoint rhs) noexcept {
        return from_raw(rhs.raw_ * scalar);
    }
    friend constexpr FixedPoint operator/(FixedPoint lhs, rep scalar) noexcept {
        return from_raw(lhs.raw_ / scalar);
    }

    friend constexpr auto operator<=>(FixedPoint, FixedPoint) noexcept = default;

private:
    rep raw_{0};
};

using Price = FixedPoint<PriceTag, 8>;
using Quantity = FixedPoint<QuantityTag, 8>;
using Notional = FixedPoint<NotionalTag, 8>;

// Quote-currency notional with a 128-bit intermediate product.
[[nodiscard]] constexpr Notional notional(Price price, Quantity quantity) noexcept {
    const Wide product = static_cast<Wide>(price.raw()) * static_cast<Wide>(quantity.raw());
    const Wide scaled = product / static_cast<Wide>(Price::kScale);
    if (scaled > static_cast<Wide>(Notional::kMaxRaw)) {
        return Notional::from_raw(Notional::kMaxRaw);
    }
    if (scaled < static_cast<Wide>(Notional::kMinRaw)) {
        return Notional::from_raw(Notional::kMinRaw);
    }
    return Notional::from_raw(static_cast<Notional::rep>(scaled));
}

[[nodiscard]] constexpr Quantity abs_quantity(Quantity value) noexcept {
    return Quantity::from_raw(value.raw() < 0 ? -value.raw() : value.raw());
}

// ---------------------------------------------------------------------------
// Market data primitives
// ---------------------------------------------------------------------------
enum class Side : std::uint8_t { Buy = 0, Sell = 1 };
enum class OrderType : std::uint8_t { Limit = 0, Market = 1, PostOnly = 2 };
enum class TimeInForce : std::uint8_t { GTC = 0, IOC = 1, FOK = 2 };
enum class OrderStatus : std::uint8_t {
    New = 0,
    PartiallyFilled = 1,
    Filled = 2,
    Cancelled = 3,
    Rejected = 4,
};

enum class RejectReason : std::uint8_t {
    None = 0,
    InvalidPrice = 1,
    InvalidQuantity = 2,
    BookUnavailable = 3,
    PositionLimit = 4,
    NotionalLimit = 5,
    OpenOrderLimit = 6,
    RateLimit = 7,
    PriceBand = 8,
    DailyLossLimit = 9,
    PostOnlyWouldCross = 10,
    DuplicateOrder = 11,
    OrderPoolExhausted = 12,
    UnknownOrder = 13,
    FillOrKillUnavailable = 14,
};

[[nodiscard]] constexpr Side opposite(Side side) noexcept {
    return side == Side::Buy ? Side::Sell : Side::Buy;
}

struct LevelDelta {
    Price price{};
    Quantity quantity{};
};

struct alignas(64) Bbo {
    Price bid_price{};
    Price ask_price{};
    Quantity bid_quantity{};
    Quantity ask_quantity{};
    Nanos exchange_ts{0};
    Nanos recv_ts{0};
    std::uint64_t sequence{0};
    std::uint8_t valid{0};

    [[nodiscard]] bool is_valid() const noexcept { return valid != 0; }
    [[nodiscard]] Price mid() const noexcept {
        return Price::from_raw((bid_price.raw() + ask_price.raw()) / 2);
    }
    [[nodiscard]] Price spread() const noexcept {
        return Price::from_raw(ask_price.raw() - bid_price.raw());
    }
    // Derived telemetry only; engine arithmetic remains fixed-point.
    [[nodiscard]] double spread_bps() const noexcept {
        const double mid_raw = static_cast<double>(mid().raw());
        if (mid_raw <= 0.0) {
            return 0.0;
        }
        return (static_cast<double>(ask_price.raw() - bid_price.raw()) * 10'000.0) /
               mid_raw;
    }
};

struct BookLevel {
    Price price{};
    Quantity quantity{};
    std::uint32_t order_count{0};
    std::uint32_t reserved{0};
};

struct TradeTick {
    Price price{};
    Quantity quantity{};
    Nanos exchange_ts{0};
    Nanos recv_ts{0};
    Side aggressor{Side::Buy};
    std::uint8_t is_buyer_maker{0};
};

struct OrderRequest {
    OrderId client_id{0};
    Price price{};
    Quantity quantity{};
    Nanos timestamp{0};
    Side side{Side::Buy};
    OrderType type{OrderType::Limit};
    TimeInForce time_in_force{TimeInForce::GTC};
};

struct Fill {
    OrderId order_id{0};
    Price price{};
    Quantity quantity{};
    Notional fee{};
    Nanos timestamp{0};
    Side side{Side::Buy};
    std::uint8_t is_maker{0};
};

struct OrderView {
    OrderId order_id{0};
    Price price{};
    Quantity quantity{};
    Quantity filled{};
    Nanos created_ts{0};
    Nanos updated_ts{0};
    Side side{Side::Buy};
    OrderStatus status{OrderStatus::New};
    OrderType type{OrderType::Limit};
    std::uint8_t post_only{0};

    [[nodiscard]] Quantity remaining() const noexcept { return quantity - filled; }
};

struct DepthUpdate {
    std::string_view symbol{};
    Nanos exchange_ts{0};
    Nanos recv_ts{0};
    std::uint64_t first_update_id{0};
    std::uint64_t final_update_id{0};
    std::array<LevelDelta, kMaxDepthLevels> bids{};
    std::array<LevelDelta, kMaxDepthLevels> asks{};
    std::uint32_t bid_count{0};
    std::uint32_t ask_count{0};
};

// ---------------------------------------------------------------------------
// Binary layout guarantees
// ---------------------------------------------------------------------------
static_assert(sizeof(Price) == 8, "Price must stay 8 bytes");
static_assert(sizeof(Quantity) == 8, "Quantity must stay 8 bytes");
static_assert(sizeof(Notional) == 8, "Notional must stay 8 bytes");
static_assert(std::is_trivially_copyable_v<Price>);
static_assert(std::is_trivially_copyable_v<Bbo>);
static_assert(std::is_trivially_copyable_v<BookLevel>);
static_assert(std::is_trivially_copyable_v<TradeTick>);
static_assert(std::is_trivially_copyable_v<OrderRequest>);
static_assert(std::is_trivially_copyable_v<Fill>);
static_assert(std::is_trivially_copyable_v<OrderView>);
static_assert(std::is_trivially_copyable_v<DepthUpdate>);
static_assert(alignof(Bbo) == 64, "Bbo must not share a cache line");

}  // namespace hft
