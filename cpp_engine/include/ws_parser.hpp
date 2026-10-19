// SPDX-License-Identifier: MIT
// Allocation-free WebSocket frame decoder + SIMD JSON feed parser.
//
// Transport is deliberately not baked in: the same parser is driven by an
// io_uring / epoll / Asio read loop in production and by a replay loop in the
// backtester. This file only handles framing and decoding.
#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <span>
#include <string_view>

#include "types.hpp"

namespace hft {

enum class WsOpcode : std::uint8_t {
    Continuation = 0x0,
    Text = 0x1,
    Binary = 0x2,
    Close = 0x8,
    Ping = 0x9,
    Pong = 0xA,
};

struct WsFrameView {
    WsOpcode opcode{WsOpcode::Text};
    bool fin{true};
    bool masked{false};
    const std::uint8_t* payload{nullptr};
    std::size_t payload_length{0};
};

class WebSocketFrameDecoder {
public:
    static constexpr std::size_t kMaxPayload = 256 * 1024;

    // Decodes exactly one frame from `data`.
    // Returns bytes consumed, 0 when the frame is incomplete, -1 on a protocol
    // error. Masked payloads are copied (and unmasked) into `scratch`.
    [[nodiscard]] static std::ptrdiff_t decode(std::span<const std::uint8_t> data,
                                               WsFrameView& out, std::uint8_t* scratch,
                                               std::size_t scratch_capacity) noexcept;
};

enum class FeedMessageType : std::uint8_t { Unknown = 0, DepthUpdate = 1, Trade = 2 };

class JsonFeedParser {
public:
    // Both parsers are non-throwing, non-allocating and operate on a borrowed
    // JSON view. `DepthUpdate::symbol` points into `json`.
    [[nodiscard]] static bool parse_depth_update(std::string_view json,
                                                 DepthUpdate& out) noexcept;
    [[nodiscard]] static bool parse_trade(std::string_view json, TradeTick& out) noexcept;
    [[nodiscard]] static FeedMessageType classify(std::string_view json) noexcept;
};

}  // namespace hft
