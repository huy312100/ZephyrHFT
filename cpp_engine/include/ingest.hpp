// SPDX-License-Identifier: MIT
// Bulk Phase 1 NDJSON -> paper_engine bridge. Classifies, parses and applies
// each line in C++ so no per-level Python object is ever constructed.
#pragma once

#include <string>
#include <string_view>
#include <vector>

#include "paper_engine.hpp"
#include "ws_parser.hpp"

namespace hft {

struct ingest_stats {
    std::uint64_t lines{0};
    std::uint64_t depth_events{0};
    std::uint64_t trade_events{0};
    std::uint64_t malformed{0};
    std::uint64_t gaps{0};
};

[[nodiscard]] inline bool ingest_json(paper_engine& engine, std::string_view json,
                                      ingest_stats& stats) noexcept {
    ++stats.lines;
    const FeedMessageType kind = JsonFeedParser::classify(json);
    if (kind == FeedMessageType::DepthUpdate) {
        DepthUpdate update;
        if (!JsonFeedParser::parse_depth_update(json, update)) {
            ++stats.malformed;
            return false;
        }
        engine.process_update(update);
        ++stats.depth_events;
        return true;
    }
    if (kind == FeedMessageType::Trade) {
        TradeTick trade;
        if (JsonFeedParser::parse_trade(json, trade)) {
            // Trade prints carry the fills under the tape-driven fill model;
            // dropping them here made every maker fill a book-crossing guess.
            engine.process_trade(trade);
            ++stats.trade_events;
        } else {
            ++stats.malformed;
        }
        return true;
    }
    ++stats.malformed;
    return false;
}

[[nodiscard]] inline ingest_stats ingest_lines(
    paper_engine& engine, const std::vector<std::string>& lines) noexcept {
    ingest_stats stats;
    for (const std::string& line : lines) {
        (void)ingest_json(engine, line, stats);
    }
    stats.gaps = engine.metrics().gap_events;
    return stats;
}

}  // namespace hft
