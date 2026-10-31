// SPDX-License-Identifier: MIT
#include "ws_parser.hpp"

#include <algorithm>

#include "simd_scan.hpp"

namespace hft {
namespace {

struct RawValue {
    const char* begin{nullptr};
    const char* end{nullptr};
    bool quoted{false};
};

// Locates a top-level "key": value pair. Feed payloads are flat objects, so a
// single forward scan over quoted keys is sufficient and allocation-free.
[[nodiscard]] bool locate(std::string_view json, std::string_view key,
                          RawValue& out) noexcept {
    const char* p = json.data();
    const char* end = p + json.size();
    while (p < end) {
        const char* quote = simd::find_char(p, end, '"');
        if (quote == end) {
            return false;
        }
        const char* key_begin = quote + 1;
        const char* key_end = simd::find_char(key_begin, end, '"');
        if (key_end == end) {
            return false;
        }
        const char* colon = simd::skip_whitespace(key_end + 1, end);
        if (colon < end && *colon == ':' &&
            std::string_view(key_begin, static_cast<std::size_t>(key_end - key_begin)) ==
                key) {
            const char* value = simd::skip_whitespace(colon + 1, end);
            if (value == end) {
                return false;
            }
            if (*value == '"') {
                const char* close = simd::find_char(value + 1, end, '"');
                if (close == end) {
                    return false;
                }
                out = {value + 1, close, true};
                return true;
            }
            const char* stop = value;
            while (stop < end && *stop != ',' && *stop != '}' && *stop != ']') {
                ++stop;
            }
            while (stop > value && (stop[-1] == ' ' || stop[-1] == '\t' ||
                                    stop[-1] == '\n' || stop[-1] == '\r')) {
                --stop;
            }
            out = {value, stop, false};
            return true;
        }
        p = key_end + 1;
    }
    return false;
}

template <typename T>
[[nodiscard]] bool parse_decimal(const RawValue& value, T& out) noexcept {
    if (value.begin == nullptr || value.begin == value.end) {
        return false;
    }
    const auto parsed = T::from_string(
        std::string_view(value.begin, static_cast<std::size_t>(value.end - value.begin)));
    if (!parsed.has_value()) {
        return false;
    }
    out = *parsed;
    return true;
}

[[nodiscard]] bool parse_u64(const RawValue& value, std::uint64_t& out) noexcept {
    if (value.begin == nullptr) {
        return false;
    }
    return simd::parse_uint64(value.begin, value.end, out);
}

[[nodiscard]] bool parse_bool(const RawValue& value, bool& out) noexcept {
    if (value.begin == nullptr) {
        return false;
    }
    const std::string_view text(value.begin,
                                static_cast<std::size_t>(value.end - value.begin));
    if (text == "true" || text == "1") {
        out = true;
        return true;
    }
    if (text == "false" || text == "0") {
        out = false;
        return true;
    }
    return false;
}

// Index of the kept level furthest from the touch: the lowest bid, the highest ask.
[[nodiscard]] std::uint32_t furthest_slot(const LevelDelta* levels, std::uint32_t count,
                                         bool keep_highest) noexcept {
    std::uint32_t worst = 0;
    for (std::uint32_t i = 1; i < count; ++i) {
        const Price::rep candidate = levels[i].price.raw();
        const Price::rep current = levels[worst].price.raw();
        if (keep_highest ? candidate < current : candidate > current) {
            worst = i;
        }
    }
    return worst;
}

// Parses [["price","qty"],...] starting at `p` (which must point at '[').
//
// A partial-book frame can carry hundreds of levels -- one burst on this venue
// carried 440 -- while the buffer holds `kMaxDepthLevels`. Keep the levels
// nearest the touch (the highest bids, the lowest asks) rather than whichever
// arrive first: the venue promises no ordering, and a frame that lists its bids
// ascending from 30,000 would otherwise hand the engine a "best bid" the market
// never quoted. The far levels are the ones a ladder window rejects anyway.
[[nodiscard]] bool parse_levels(const char* p, const char* end, LevelDelta* levels,
                                std::uint32_t& count, bool keep_highest) noexcept {
    count = 0;
    std::uint32_t furthest = 0;
    bool have_furthest = false;
    if (p == nullptr || p == end || *p != '[') {
        return false;
    }
    ++p;
    while (p < end) {
        p = simd::skip_whitespace(p, end);
        if (p == end) {
            return false;
        }
        if (*p == ']') {
            return true;
        }
        if (*p == ',') {
            ++p;
            continue;
        }
        if (*p != '[') {
            return false;
        }
        ++p;

        const char* price_open = simd::find_char(p, end, '"');
        if (price_open == end) {
            return false;
        }
        const char* price_close = simd::find_char(price_open + 1, end, '"');
        if (price_close == end) {
            return false;
        }
        const char* quantity_open = simd::find_char(price_close + 1, end, '"');
        if (quantity_open == end) {
            return false;
        }
        const char* quantity_close = simd::find_char(quantity_open + 1, end, '"');
        if (quantity_close == end) {
            return false;
        }

        Price price;
        Quantity quantity;
        const RawValue price_value{price_open + 1, price_close, true};
        const RawValue quantity_value{quantity_open + 1, quantity_close, true};
        if (!parse_decimal(price_value, price) || !parse_decimal(quantity_value, quantity)) {
            return false;
        }
        if (count < kMaxDepthLevels) {
            levels[count] = LevelDelta{price, quantity};
            ++count;
        } else {
            // Buffer full: drop the level furthest from the touch, but only when
            // this one is nearer. ``furthest`` is recomputed after a swap and
            // never otherwise, so an over-full frame costs one compare per level.
            if (!have_furthest) {
                furthest = furthest_slot(levels, count, keep_highest);
                have_furthest = true;
            }
            const Price::rep candidate = price.raw();
            const Price::rep current = levels[furthest].price.raw();
            if (keep_highest ? candidate > current : candidate < current) {
                levels[furthest] = LevelDelta{price, quantity};
                furthest = furthest_slot(levels, count, keep_highest);
            }
        }
        const char* close = simd::find_char(quantity_close + 1, end, ']');
        if (close == end) {
            return false;
        }
        p = close + 1;
    }
    return false;
}

}  // namespace
bool JsonFeedParser::parse_depth_update(std::string_view json, DepthUpdate& out) noexcept {
    out = DepthUpdate{};
    RawValue value;
    if (locate(json, "s", value) && value.quoted) {
        out.symbol = std::string_view(value.begin,
                                      static_cast<std::size_t>(value.end - value.begin));
    }
    if (locate(json, "E", value)) {
        (void)parse_u64(value, out.exchange_ts);
    }
    if (locate(json, "u", value)) {
        (void)parse_u64(value, out.final_update_id);
    }
    if (locate(json, "U", value)) {
        (void)parse_u64(value, out.first_update_id);
    }
    if (locate(json, "pu", value)) {
        (void)parse_u64(value, out.prev_final_update_id);
    }
    RawValue bids;
    RawValue asks;
    const bool has_bids = locate(json, "b", bids);
    const bool has_asks = locate(json, "a", asks);
    if (!has_bids || !has_asks) {
        return false;
    }
    const char* end = json.data() + json.size();
    return parse_levels(bids.begin, end, out.bids.data(), out.bid_count,
                        /*keep_highest=*/true) &&
           parse_levels(asks.begin, end, out.asks.data(), out.ask_count,
                        /*keep_highest=*/false);
}

bool JsonFeedParser::parse_trade(std::string_view json, TradeTick& out) noexcept {
    out = TradeTick{};
    RawValue value;
    if (!locate(json, "p", value) || !parse_decimal(value, out.price)) {
        return false;
    }
    if (!locate(json, "q", value) || !parse_decimal(value, out.quantity)) {
        return false;
    }
    if (locate(json, "T", value) || locate(json, "E", value)) {
        (void)parse_u64(value, out.exchange_ts);
    }
    bool buyer_is_maker = false;
    if (locate(json, "m", value)) {
        (void)parse_bool(value, buyer_is_maker);
    }
    out.is_buyer_maker = static_cast<std::uint8_t>(buyer_is_maker ? 1 : 0);
    out.aggressor = buyer_is_maker ? Side::Sell : Side::Buy;
    return true;
}

FeedMessageType JsonFeedParser::classify(std::string_view json) noexcept {
    RawValue value;
    if (!locate(json, "e", value) || !value.quoted) {
        // The archived REST book snapshot has no event type of its own -- it is
        // identified by the shape the venue answers with. Recognising it here is
        // what keeps a seeded capture from being counted as a malformed line,
        // and counting it separately from the diff path is what stops an
        // absolute book being applied as a delta.
        RawValue ids, bids, asks;
        const bool snapshot = locate(json, "lastUpdateId", ids)
            && locate(json, "bids", bids) && locate(json, "asks", asks);
        return snapshot ? FeedMessageType::BookSnapshot : FeedMessageType::Unknown;
    }
    const std::string_view event(value.begin,
                                 static_cast<std::size_t>(value.end - value.begin));
    if (event == "depthUpdate" || event == "depth") {
        return FeedMessageType::DepthUpdate;
    }
    if (event == "trade" || event == "aggTrade") {
        return FeedMessageType::Trade;
    }
    return FeedMessageType::Unknown;
}

std::ptrdiff_t WebSocketFrameDecoder::decode(std::span<const std::uint8_t> data,
                                             WsFrameView& out, std::uint8_t* scratch,
                                             std::size_t scratch_capacity) noexcept {
    if (data.size() < 2) {
        return 0;
    }
    const std::uint8_t first = data[0];
    const std::uint8_t second = data[1];
    out.fin = (first & 0x80U) != 0;
    out.opcode = static_cast<WsOpcode>(first & 0x0FU);
    out.masked = (second & 0x80U) != 0;

    std::uint64_t length = second & 0x7FU;
    std::size_t offset = 2;
    if (length == 126) {
        if (data.size() < 4) {
            return 0;
        }
        length = (static_cast<std::uint64_t>(data[2]) << 8) | data[3];
        offset = 4;
    } else if (length == 127) {
        if (data.size() < 10) {
            return 0;
        }
        length = 0;
        for (std::size_t i = 0; i < 8; ++i) {
            length = (length << 8) | data[2 + i];
        }
        offset = 10;
    }
    if (length > kMaxPayload) {
        return -1;
    }
    if (out.masked) {
        if (data.size() < offset + 4) {
            return 0;
        }
        offset += 4;
    }
    if (data.size() < offset + length) {
        return 0;
    }
    const std::uint8_t* payload = data.data() + offset;
    if (out.masked) {
        if (scratch == nullptr || length > scratch_capacity) {
            return -1;
        }
        const std::uint8_t* mask = data.data() + offset - 4;
        for (std::size_t i = 0; i < length; ++i) {
            scratch[i] = static_cast<std::uint8_t>(payload[i] ^ mask[i & 3U]);
        }
        payload = scratch;
    }
    out.payload = payload;
    out.payload_length = static_cast<std::size_t>(length);
    return static_cast<std::ptrdiff_t>(offset + length);
}
}  // namespace hft
