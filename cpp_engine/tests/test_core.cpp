// SPDX-License-Identifier: MIT
// Self-contained unit tests for the C++ HFT core (no external framework).
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdio>
#include <cstring>
#include <limits>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "engine.hpp"
#include "ingest.hpp"
#include "latency_histogram.hpp"
#include "matching.hpp"
#include "order_manager.hpp"
#include "orderbook.hpp"
#include "paper_account.hpp"
#include "paper_engine.hpp"
#include "ring_buffer.hpp"
#include "risk/heartbeat.hpp"
#include "risk/kill_switch.hpp"
#include "risk_guard.hpp"
#include "risk_manager.hpp"
#include "types.hpp"
#include "ws_parser.hpp"

namespace {

int g_failures = 0;

#define CHECK(condition)                                                       \
    do {                                                                       \
        if (!(condition)) {                                                    \
            std::fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__,      \
                         #condition);                                          \
            ++g_failures;                                                      \
        }                                                                      \
    } while (false)

hft::Price P(const char* text) {
    const auto value = hft::Price::from_string(text);
    return value.value_or(hft::Price{});
}

hft::Quantity Q(const char* text) {
    const auto value = hft::Quantity::from_string(text);
    return value.value_or(hft::Quantity{});
}

hft::Notional N(const char* text) {
    const auto value = hft::Notional::from_string(text);
    return value.value_or(hft::Notional{});
}

void test_fixed_point() {
    CHECK(P("29999.50") == hft::Price::from_raw(2'999'950'000'000LL));
    CHECK(P("-0.00000001").raw() == -1);
    CHECK(Q("0.5") + Q("0.25") == Q("0.75"));
    CHECK(Q("1.0") - Q("0.25") == Q("0.75"));
    CHECK(hft::notional(P("30000"), Q("0.5")) == N("15000"));
    CHECK(hft::Price::from_string("1.2.3") == std::nullopt);
    CHECK(hft::Price::from_string("") == std::nullopt);
    CHECK(hft::Price::from_string("abc") == std::nullopt);
    CHECK(hft::Price::from_double(1.5) == P("1.5"));
    CHECK(P("0.000000019").raw() == 1);  // truncated, never rounded up
}

void test_ring_buffer() {
    hft::SpscRingBuffer<std::uint64_t, 1024> ring;
    CHECK(ring.empty());
    for (std::uint64_t i = 0; i < 1024; ++i) {
        CHECK(ring.try_push(i));
    }
    CHECK(ring.full());
    CHECK(!ring.try_push(1024));
    for (std::uint64_t i = 0; i < 1024; ++i) {
        std::uint64_t out = 0;
        CHECK(ring.try_pop(out));
        CHECK(out == i);
    }
    CHECK(ring.empty());

    constexpr std::uint64_t kMessages = 250'000;
    std::atomic<int> errors{0};
    std::atomic<bool> producer_done{false};
    std::thread producer([&] {
        for (std::uint64_t i = 1; i <= kMessages; ++i) {
            while (!ring.try_push(i)) {
                std::this_thread::yield();
            }
        }
        producer_done.store(true, std::memory_order_release);
    });
    std::uint64_t expected = 1;
    while (expected <= kMessages) {
        std::uint64_t out = 0;
        if (ring.try_pop(out)) {
            if (out != expected) {
                errors.fetch_add(1, std::memory_order_relaxed);
            }
            ++expected;
        } else if (producer_done.load(std::memory_order_acquire) && ring.empty()) {
            break;
        } else {
            std::this_thread::yield();
        }
    }
    producer.join();
    CHECK(errors.load() == 0);
    CHECK(expected == kMessages + 1);
    CHECK(ring.empty());
}

void test_l2_book() {
    hft::L2Book book{hft::Price::from_int(1), hft::Price::from_int(30'000)};
    CHECK(book.apply_level(hft::Side::Buy, hft::Price::from_int(29'999),
                           hft::Quantity::from_int(5)));
    CHECK(book.apply_level(hft::Side::Buy, hft::Price::from_int(29'998),
                           hft::Quantity::from_int(2)));
    CHECK(book.apply_level(hft::Side::Sell, hft::Price::from_int(30'001),
                           hft::Quantity::from_int(4)));
    CHECK(book.apply_level(hft::Side::Sell, hft::Price::from_int(30'002),
                           hft::Quantity::from_int(1)));

    const hft::Bbo bbo = book.top();
    CHECK(bbo.is_valid());
    CHECK(bbo.bid_price == hft::Price::from_int(29'999));
    CHECK(bbo.ask_price == hft::Price::from_int(30'001));
    CHECK(bbo.bid_quantity == hft::Quantity::from_int(5));
    CHECK(bbo.mid() == hft::Price::from_int(30'000));
    CHECK(bbo.spread() == hft::Price::from_int(2));

    hft::BookSnapshot snapshot;
    CHECK(book.snapshot(snapshot));
    CHECK(snapshot.bid_count == 2);
    CHECK(snapshot.ask_count == 2);
    CHECK(snapshot.bids[0].price == hft::Price::from_int(29'999));
    CHECK(snapshot.bids[1].price == hft::Price::from_int(29'998));
    CHECK(snapshot.asks[0].price == hft::Price::from_int(30'001));
    CHECK(snapshot.asks[1].price == hft::Price::from_int(30'002));

    // Off-grid and out-of-range prices are rejected, not rounded.
    CHECK(!book.apply_level(hft::Side::Buy, hft::Price::from_double(29'998.5),
                            hft::Quantity::from_int(1)));
    CHECK(!book.apply_level(hft::Side::Buy, hft::Price::from_int(50'000),
                            hft::Quantity::from_int(1)));

    CHECK(book.apply_level(hft::Side::Buy, hft::Price::from_int(29'999),
                           hft::Quantity{}));
    CHECK(book.best_bid() == hft::Price::from_int(29'998));
    CHECK(book.live_levels(hft::Side::Buy) == 1);
    CHECK(book.updates() >= 5);
}

void test_l3_book() {
    hft::L3Book book{hft::Price::from_int(1), hft::Price::from_int(30'000)};
    CHECK(book.add_order(1, hft::Side::Buy, hft::Price::from_int(29'999),
                         hft::Quantity::from_int(2)));
    CHECK(book.add_order(2, hft::Side::Buy, hft::Price::from_int(29'999),
                         hft::Quantity::from_int(3)));
    CHECK(book.add_order(3, hft::Side::Buy, hft::Price::from_int(29'998),
                         hft::Quantity::from_int(1)));
    CHECK(book.add_order(4, hft::Side::Sell, hft::Price::from_int(30'001),
                         hft::Quantity::from_int(4)));
    CHECK(book.open_orders() == 4);
    CHECK(!book.add_order(2, hft::Side::Buy, hft::Price::from_int(29'999),
                          hft::Quantity::from_int(1)));  // duplicate id
    CHECK(!book.add_order(5, hft::Side::Buy, hft::Price::from_int(29'999),
                          hft::Quantity{}));  // zero quantity

    CHECK(book.best_bid() == hft::Price::from_int(29'999));
    CHECK(book.best_ask() == hft::Price::from_int(30'001));

    hft::BookLevel levels[4]{};
    CHECK(book.depth(hft::Side::Buy, levels, 4) == 2);
    CHECK(levels[0].price == hft::Price::from_int(29'999));
    CHECK(levels[0].quantity == hft::Quantity::from_int(5));
    CHECK(levels[1].quantity == hft::Quantity::from_int(1));

    CHECK(book.execute_order(1, Q("0.5"), 7));
    const auto view = book.find(1);
    CHECK(view.has_value());
    CHECK(view->filled == Q("0.5"));
    CHECK(view->remaining() == Q("1.5"));
    CHECK(book.depth(hft::Side::Buy, levels, 4) == 2);
    CHECK(levels[0].quantity == Q("4.5"));

    // A partial cancel removes liquidity without counting as executed volume.
    CHECK(book.cancel_order(1, Q("0.5")));
    const auto after_cancel = book.find(1);
    CHECK(after_cancel.has_value());
    CHECK(after_cancel->filled == Q("0.5"));
    CHECK(after_cancel->remaining() == Q("1.0"));
    CHECK(book.depth(hft::Side::Buy, levels, 4) == 2);
    CHECK(levels[0].quantity == Q("4.0"));

    CHECK(book.cancel_order(1));
    CHECK(!book.find(1).has_value());
    CHECK(book.open_orders() == 3);
    CHECK(book.depth(hft::Side::Buy, levels, 4) == 2);
    CHECK(levels[0].quantity == hft::Quantity::from_int(3));

    CHECK(book.execute_order(3, hft::Quantity::from_int(5)));
    CHECK(!book.find(3).has_value());
    CHECK(book.open_orders() == 2);

    book.configure(hft::Price::from_int(1), hft::Price::from_int(30'000));
    CHECK(book.open_orders() == 0);
    CHECK(!book.top().is_valid());
}

void test_order_manager() {
    hft::DefaultOrderManager manager;
    hft::OrderRequest request;
    request.client_id = 101;
    request.price = hft::Price::from_int(29'999);
    request.quantity = hft::Quantity::from_int(2);
    request.side = hft::Side::Buy;
    request.type = hft::OrderType::Limit;

    CHECK(manager.create(request) == 101);
    CHECK(manager.open_orders() == 1);
    hft::Quantity pending_buy;
    hft::Quantity pending_sell;
    manager.pending_quantities(pending_buy, pending_sell);
    CHECK(pending_buy == hft::Quantity::from_int(2));
    CHECK(pending_sell.raw() == 0);
    CHECK(manager.create(request) == 0);  // duplicate live client id

    hft::Quantity remaining;
    CHECK(manager.apply_fill(101, Q("0.5"),
                             hft::Price::from_int(29'999), 10, remaining));
    CHECK(remaining == Q("1.5"));
    manager.pending_quantities(pending_buy, pending_sell);
    CHECK(pending_buy == Q("1.5"));
    const auto partial = manager.find(101);
    CHECK(partial.has_value());
    CHECK(partial->status == hft::OrderStatus::PartiallyFilled);
    CHECK(partial->remaining() == Q("1.5"));

    CHECK(manager.apply_fill(101, hft::Quantity::from_int(99),
                             hft::Price::from_int(29'999), 11, remaining));
    CHECK(remaining.raw() == 0);
    CHECK(manager.open_orders() == 0);
    CHECK(!manager.find(101).has_value());

    hft::OrderRequest resting = request;
    resting.client_id = 202;
    CHECK(manager.create(resting) == 202);
    hft::Quantity cancelled;
    CHECK(!manager.cancel(999, 12, cancelled));
    CHECK(manager.cancel(202, 12, cancelled));
    CHECK(cancelled == hft::Quantity::from_int(2));
    CHECK(manager.empty());

    // Slots are recycled: the same client id can be reused after termination.
    CHECK(manager.create(request) == 101);
    CHECK(manager.open_orders() == 1);
}

void test_risk_guard() {
    hft::RiskGuard guard;
    hft::Bbo bbo;
    bbo.bid_price = hft::Price::from_int(29'999);
    bbo.ask_price = hft::Price::from_int(30'001);
    bbo.bid_quantity = hft::Quantity::from_int(5);
    bbo.ask_quantity = hft::Quantity::from_int(5);
    bbo.valid = 1;

    hft::OrderRequest request;
    request.price = hft::Price::from_int(30'000);
    request.quantity = hft::Quantity::from_int(1);
    request.side = hft::Side::Buy;
    request.type = hft::OrderType::Limit;
    hft::PositionSnapshot flat;

    CHECK(guard.check(request, bbo, flat, 0).approved());

    hft::OrderRequest invalid = request;
    invalid.quantity = hft::Quantity{};
    CHECK(guard.check(invalid, bbo, flat, 0).reason ==
          hft::RejectReason::InvalidQuantity);

    invalid = request;
    invalid.price = hft::Price{};
    CHECK(guard.check(invalid, bbo, flat, 0).reason == hft::RejectReason::InvalidPrice);

    invalid = request;
    invalid.quantity = hft::Quantity::from_int(101);
    CHECK(guard.check(invalid, bbo, flat, 0).reason ==
          hft::RejectReason::InvalidQuantity);

    invalid = request;
    invalid.price = hft::Price::from_int(40'000);
    CHECK(guard.check(invalid, bbo, flat, 0).reason == hft::RejectReason::PriceBand);

    invalid = request;
    invalid.type = hft::OrderType::PostOnly;
    invalid.price = bbo.ask_price;
    CHECK(guard.check(invalid, bbo, flat, 0).reason ==
          hft::RejectReason::PostOnlyWouldCross);

    hft::Bbo unavailable;
    CHECK(guard.check(request, unavailable, flat, 0).reason ==
          hft::RejectReason::BookUnavailable);

    hft::RiskLimits tight;
    tight.max_position_notional = hft::Notional::from_int(1'000);
    guard.set_limits(tight);
    CHECK(guard.check(request, bbo, flat, 0).reason ==
          hft::RejectReason::PositionLimit);

    hft::RiskLimits order_cap;
    order_cap.max_order_notional = hft::Notional::from_int(1'000);
    order_cap.max_position_notional = hft::Notional::from_int(10'000'000);
    guard.set_limits(order_cap);
    hft::OrderRequest market = request;
    market.type = hft::OrderType::Market;
    CHECK(guard.check(market, bbo, flat, 0).reason ==
          hft::RejectReason::NotionalLimit);

    hft::RiskLimits pending_cap;
    pending_cap.max_position_notional = hft::Notional::from_int(100'000);
    guard.set_limits(pending_cap);
    CHECK(guard.check(request, bbo, flat, 0).approved());
    const hft::RiskDecision pending_decision =
        guard.check(request, bbo, flat, 0, Q("10"), hft::Quantity{});
    CHECK(pending_decision.reason == hft::RejectReason::PositionLimit);

    hft::RiskLimits loss_limit;
    loss_limit.max_daily_loss = N("50");
    guard.set_limits(loss_limit);
    hft::PositionSnapshot losing;
    losing.realized_pnl = N("-75");
    CHECK(guard.check(request, bbo, losing, 0).reason ==
          hft::RejectReason::DailyLossLimit);
}

void test_latency_histogram() {
    hft::LatencyHistogram histogram;
    CHECK(histogram.count() == 0);
    CHECK(histogram.percentile(0.99) == 0);
    histogram.record(50);
    histogram.record(150);
    histogram.record(500);
    histogram.record(5'000);
    CHECK(histogram.count() == 4);
    CHECK(histogram.max_ns() == 5'000);
    CHECK(histogram.mean_ns() == 1'425);
    CHECK(histogram.p50() == 200);
    CHECK(histogram.p99() == 5'100);  // upper edge of the 5000 ns bucket
    histogram.reset();
    CHECK(histogram.count() == 0);
}

void test_json_parser() {
    const std::string json =
        R"({"e":"depthUpdate","E":1700000000000,"U":41,"u":42,"s":"BTCUSDT",)"
        R"("b":[["29999.50","1.5"],["29999.00","2.0"]],)"
        R"("a":[["30000.50","2.5"],["30001.00","1.0"]]})";
    hft::DepthUpdate update;
    CHECK(hft::JsonFeedParser::parse_depth_update(json, update));
    CHECK(update.symbol == "BTCUSDT");
    CHECK(update.exchange_ts == 1'700'000'000'000ULL);
    CHECK(update.first_update_id == 41);
    CHECK(update.final_update_id == 42);
    CHECK(update.bid_count == 2);
    CHECK(update.ask_count == 2);
    CHECK(update.bids[0].price == P("29999.5"));
    CHECK(update.bids[0].quantity == Q("1.5"));
    CHECK(update.asks[1].price == P("30001"));
    CHECK(hft::JsonFeedParser::classify(json) == hft::FeedMessageType::DepthUpdate);

    const std::string trade_json =
        R"({"e":"trade","E":1700000000001,"T":1700000000002,"p":"30000.25",)"
        R"("q":"0.75","m":true})";
    hft::TradeTick trade;
    CHECK(hft::JsonFeedParser::parse_trade(trade_json, trade));
    CHECK(trade.price == P("30000.25"));
    CHECK(trade.quantity == Q("0.75"));
    CHECK(trade.exchange_ts == 1'700'000'000'002ULL);
    CHECK(trade.is_buyer_maker == 1);
    CHECK(trade.aggressor == hft::Side::Sell);
    CHECK(hft::JsonFeedParser::classify(trade_json) == hft::FeedMessageType::Trade);
    CHECK(hft::JsonFeedParser::classify(R"({"e":"heartbeat"})") ==
          hft::FeedMessageType::Unknown);

    hft::DepthUpdate malformed;
    CHECK(!hft::JsonFeedParser::parse_depth_update(R"({"e":"depthUpdate"})", malformed));
    hft::TradeTick bad_trade;
    CHECK(!hft::JsonFeedParser::parse_trade(R"({"e":"trade","p":"oops","q":"1"})",
                                            bad_trade));
}

void test_json_parser_keeps_the_levels_nearest_the_touch() {
    // A partial-book burst carries more levels than the update buffer holds, and
    // the venue promises no ordering. Here the bids are listed ascending from
    // 30,000, so keeping the first come-first-served would make the engine's
    // "best bid" the lowest one in the frame rather than the real touch.
    std::string json = "{\"e\":\"depthUpdate\",\"E\":1700000000000,\"U\":1,\"u\":2,";
    json += "\"s\":\"BTCUSDT\",\"b\":[";
    for (int i = 0; i < 40; ++i) {
        json += (i == 0 ? "" : ",");
        json += "[\"" + std::to_string(30000 + i) + ".0\",\"1.0\"]";
    }
    json += "],\"a\":[";
    for (int i = 0; i < 40; ++i) {
        json += (i == 0 ? "" : ",");
        json += "[\"" + std::to_string(40000 + i) + ".0\",\"1.0\"]";
    }
    json += "]}";

    hft::DepthUpdate update;
    CHECK(hft::JsonFeedParser::parse_depth_update(json, update));
    CHECK(update.bid_count == hft::kMaxDepthLevels);
    CHECK(update.ask_count == hft::kMaxDepthLevels);
    hft::Price::rep best_bid = 0;
    for (std::uint32_t i = 0; i < update.bid_count; ++i) {
        best_bid = std::max(best_bid, update.bids[i].price.raw());
    }
    hft::Price::rep best_ask = update.asks[0].price.raw();
    for (std::uint32_t i = 1; i < update.ask_count; ++i) {
        best_ask = std::min(best_ask, update.asks[i].price.raw());
    }
    CHECK(best_bid == P("30039").raw());  // the highest bid, not the first
    CHECK(best_ask == P("40000").raw());  // the lowest ask, not the last
    CHECK(update.bids[0].quantity == Q("1.0"));
}

void test_websocket_decoder() {
    std::uint8_t scratch[64]{};
    hft::WsFrameView view;

    const std::uint8_t mask[4] = {0x01, 0x02, 0x03, 0x04};
    std::uint8_t masked[11] = {0x81, 0x80 | 5, mask[0], mask[1], mask[2], mask[3]};
    const char message[] = "hello";
    for (std::size_t i = 0; i < 5; ++i) {
        masked[6 + i] = static_cast<std::uint8_t>(message[i]) ^ mask[i % 4];
    }
    CHECK(hft::WebSocketFrameDecoder::decode(masked, view, scratch, sizeof(scratch)) == 11);
    CHECK(view.fin);
    CHECK(view.masked);
    CHECK(view.opcode == hft::WsOpcode::Text);
    CHECK(view.payload_length == 5);
    CHECK(std::memcmp(view.payload, "hello", 5) == 0);

    const std::uint8_t extended[] = {0x81, 126, 0, 5, 'w', 'o', 'r', 'l', 'd'};
    CHECK(hft::WebSocketFrameDecoder::decode(extended, view, scratch, sizeof(scratch)) == 9);
    CHECK(view.payload_length == 5);
    CHECK(std::memcmp(view.payload, "world", 5) == 0);
    CHECK(!view.masked);

    const std::uint8_t incomplete[] = {0x81, 5, 'h', 'e'};
    CHECK(hft::WebSocketFrameDecoder::decode(incomplete, view, scratch, sizeof(scratch)) == 0);

    const std::uint8_t too_large[] = {0x82, 127, 0, 0, 0, 0, 0, 0x10, 0, 0};
    CHECK(hft::WebSocketFrameDecoder::decode(too_large, view, scratch, sizeof(scratch)) == -1);
}

void test_crossing_limit_fill_preserves_price() {
    hft::EngineConfig config;
    config.tick_size = hft::Price::from_double(0.5);
    config.center = hft::Price::from_int(30'000);
    auto engine = std::make_unique<hft::TradingEngine>(config);

    hft::DepthUpdate depth;
    depth.exchange_ts = 100;
    depth.bids[0] = {P("29999.5"), Q("5")};
    depth.asks[0] = {P("30000.5"), Q("1")};
    depth.bid_count = 1;
    depth.ask_count = 1;
    engine->apply_depth(depth);

    hft::OrderRequest buy;
    buy.client_id = 811;
    buy.side = hft::Side::Buy;
    buy.type = hft::OrderType::Limit;
    buy.price = P("30001");
    buy.quantity = Q("2");
    const hft::OrderAck ack = engine->submit(buy);
    CHECK(ack.accepted());
    CHECK(ack.filled == Q("1"));
    CHECK(ack.avg_price == P("30000.5"));

    const auto remaining = engine->orders().find(ack.order_id);
    CHECK(remaining.has_value());
    CHECK(remaining->price == P("30001"));
    CHECK(remaining->remaining() == Q("1"));
    CHECK(remaining->status == hft::OrderStatus::PartiallyFilled);
}

void test_l2_clear_resets_timestamps() {
    hft::L2Book book{hft::Price::from_int(1), hft::Price::from_int(30000)};
    CHECK(book.apply_level(hft::Side::Buy, hft::Price::from_int(29999), Q("1"), 123));
    CHECK(book.apply_level(hft::Side::Sell, hft::Price::from_int(30001), Q("1"), 123));
    book.set_sequence(7, 456, 789);
    CHECK(book.top().exchange_ts == 456);
    CHECK(book.top().recv_ts == 789);

    book.clear();
    const hft::Bbo cleared = book.top();
    CHECK(cleared.is_valid() == false);
    CHECK(cleared.exchange_ts == 0);
    CHECK(cleared.recv_ts == 0);
    CHECK(cleared.sequence == 0);
    CHECK(book.last_update_ts() == 0);
}

void test_risk_guard_uses_touch_for_crossing_limit() {
    hft::RiskGuard guard;
    hft::Bbo bbo;
    bbo.bid_price = hft::Price::from_int(30000);
    bbo.ask_price = hft::Price::from_int(30001);
    bbo.bid_quantity = hft::Quantity::from_int(5);
    bbo.ask_quantity = hft::Quantity::from_int(5);
    bbo.valid = 1;

    hft::RiskLimits limits;
    limits.max_order_notional = N("29999.5");
    limits.max_position_notional = N("1000000");
    guard.set_limits(limits);

    hft::OrderRequest sell;
    sell.price = hft::Price::from_int(29999);
    sell.quantity = hft::Quantity::from_int(1);
    sell.side = hft::Side::Sell;
    sell.type = hft::OrderType::Limit;
    hft::PositionSnapshot flat;
    CHECK(guard.check(sell, bbo, flat, 0).reason == hft::RejectReason::NotionalLimit);
}

void test_zero_exchange_ts_preserves_timestamps() {
    hft::EngineConfig config;
    config.tick_size = hft::Price::from_double(0.5);
    config.center = hft::Price::from_int(30000);
    auto engine = std::make_unique<hft::TradingEngine>(config);

    hft::DepthUpdate depth;
    depth.exchange_ts = 1000;
    depth.recv_ts = 500;
    depth.final_update_id = 42;
    depth.bids[0] = {P("29999.5"), Q("1")};
    depth.asks[0] = {P("30000.5"), Q("1")};
    depth.bid_count = 1;
    depth.ask_count = 1;
    engine->apply_depth(depth);
    CHECK(engine->bbo().exchange_ts == 1000);
    CHECK(engine->bbo().recv_ts == 500);

    hft::DepthUpdate zero_ts;
    zero_ts.final_update_id = 43;
    zero_ts.bids[0] = {P("29999.5"), Q("2")};
    zero_ts.bid_count = 1;
    engine->apply_depth(zero_ts);
    CHECK(engine->bbo().bid_quantity == Q("2"));
    CHECK(engine->bbo().exchange_ts == 1000);
    CHECK(engine->bbo().recv_ts == 500);
    CHECK(engine->bbo().sequence == 43);
    CHECK(engine->book().last_update_ts() == 1000);
}

void test_position_accounting() {
    hft::EngineConfig config;
    config.tick_size = hft::Price::from_double(0.5);
    config.center = hft::Price::from_int(30'000);
    auto engine_ptr = std::make_unique<hft::TradingEngine>(config);
    hft::TradingEngine& engine = *engine_ptr;

    hft::DepthUpdate depth;
    depth.exchange_ts = 1'000;
    depth.bids[0] = {P("29999.5"), Q("10")};
    depth.asks[0] = {P("30000.5"), Q("10")};
    depth.bid_count = 1;
    depth.ask_count = 1;
    engine.apply_depth(depth);

    auto submit_market = [&](hft::Side side, const char* qty, hft::Nanos timestamp) {
        hft::OrderRequest request;
        request.side = side;
        request.type = hft::OrderType::Market;
        request.quantity = Q(qty);
        request.timestamp = timestamp;
        const hft::OrderAck ack = engine.submit(request);
        CHECK(ack.accepted());
        return ack;
    };

    // Open a long at the ask; the book mid (30000) marks it 0.5 lower.
    submit_market(hft::Side::Buy, "1.0", 10);
    CHECK(engine.position().net_quantity == Q("1.0"));
    CHECK(engine.position().average_entry_price == P("30000.5"));
    CHECK(engine.position().unrealized_pnl == N("-0.5"));

    // Close 0.4 at the bid: realise 0.4 * (29999.5 - 30000.5).
    submit_market(hft::Side::Sell, "0.4", 11);
    CHECK(engine.position().net_quantity == Q("0.6"));
    CHECK(engine.position().average_entry_price == P("30000.5"));
    CHECK(engine.position().realized_pnl == N("-0.4"));
    CHECK(engine.position().unrealized_pnl == N("-0.3"));

    // Extending at the same price keeps the average unchanged.
    submit_market(hft::Side::Buy, "0.2", 12);
    CHECK(engine.position().net_quantity == Q("0.8"));
    CHECK(engine.position().average_entry_price == P("30000.5"));
    CHECK(engine.position().unrealized_pnl == N("-0.4"));

    // Sell through flat: realise the long, open a short at the fill price.
    submit_market(hft::Side::Sell, "1.0", 13);
    CHECK(engine.position().net_quantity == Q("-0.2"));
    CHECK(engine.position().average_entry_price == P("29999.5"));
    CHECK(engine.position().realized_pnl == N("-1.2"));
    CHECK(engine.position().unrealized_pnl == N("-0.1"));

    hft::Fill fill;
    CHECK(engine.drain_fill(fill));
    CHECK(fill.timestamp == 10);
}

void test_time_in_force() {
    hft::EngineConfig config;
    config.tick_size = hft::Price::from_double(0.5);
    config.center = hft::Price::from_int(30'000);
    auto engine_ptr = std::make_unique<hft::TradingEngine>(config);
    hft::TradingEngine& engine = *engine_ptr;

    hft::DepthUpdate depth;
    depth.bids[0] = {P("29999.5"), Q("1")};
    depth.asks[0] = {P("30000.5"), Q("2")};
    depth.bid_count = 1;
    depth.ask_count = 1;
    engine.apply_depth(depth);

    hft::OrderRequest fok;
    fok.client_id = 1;
    fok.side = hft::Side::Buy;
    fok.type = hft::OrderType::Limit;
    fok.time_in_force = hft::TimeInForce::FOK;
    fok.price = P("30000.5");
    fok.quantity = Q("3.0");
    CHECK(engine.submit(fok).reason == hft::RejectReason::FillOrKillUnavailable);
    CHECK(engine.metrics().open_orders == 0);

    fok.client_id = 2;
    fok.quantity = Q("1.0");
    const hft::OrderAck filled = engine.submit(fok);
    CHECK(filled.accepted());
    CHECK(filled.filled == Q("1.0"));
    CHECK(engine.position().net_quantity == Q("1.0"));

    // An IOC that cannot cross is accepted, then cancelled without resting.
    hft::OrderRequest ioc;
    ioc.client_id = 3;
    ioc.side = hft::Side::Sell;
    ioc.type = hft::OrderType::Limit;
    ioc.time_in_force = hft::TimeInForce::IOC;
    ioc.price = P("30000");
    ioc.quantity = Q("0.5");
    const hft::OrderAck cancelled = engine.submit(ioc);
    CHECK(cancelled.accepted() && cancelled.filled.raw() == 0);
    CHECK(engine.metrics().open_orders == 0);
    CHECK(engine.metrics().orders_cancelled == 1);
}

void test_engine() {
    hft::EngineConfig config;
    config.tick_size = hft::Price::from_double(0.5);
    config.center = hft::Price::from_int(30'000);
    auto engine = std::make_unique<hft::TradingEngine>(config);

    hft::DepthUpdate depth;
    depth.exchange_ts = 1'000;
    depth.recv_ts = 500;
    depth.bids[0] = {P("29999.5"), Q("1.5")};
    depth.asks[0] = {P("30000.5"), Q("2.0")};
    depth.bid_count = 1;
    depth.ask_count = 1;
    depth.final_update_id = 42;
    engine->apply_depth(depth);

    const hft::Bbo bbo = engine->bbo();
    CHECK(bbo.is_valid());
    CHECK(bbo.bid_price == P("29999.5"));
    CHECK(bbo.ask_price == P("30000.5"));
    CHECK(bbo.exchange_ts == 1'000);
    CHECK(bbo.recv_ts == 500);
    CHECK(bbo.sequence == 42);
    CHECK(engine->mark_price() == P("30000"));

    hft::OrderRequest market;
    market.client_id = 1;
    market.side = hft::Side::Buy;
    market.type = hft::OrderType::Market;
    market.quantity = Q("0.5");
    const hft::OrderAck ack = engine->submit(market);
    CHECK(ack.accepted());
    CHECK(ack.filled == Q("0.5"));
    CHECK(ack.avg_price == P("30000.5"));
    CHECK(engine->position().net_quantity == Q("0.5"));
    CHECK(engine->bbo().ask_quantity == Q("1.5"));

    hft::Fill fill;
    CHECK(engine->drain_fill(fill));
    CHECK(fill.order_id == ack.order_id);
    CHECK(fill.quantity == Q("0.5"));
    CHECK(!engine->drain_fill(fill));

    hft::OrderRequest post_only;
    post_only.client_id = 2;
    post_only.side = hft::Side::Buy;
    post_only.type = hft::OrderType::PostOnly;
    post_only.price = P("30000.5");
    post_only.quantity = Q("0.1");
    CHECK(engine->submit(post_only).reason == hft::RejectReason::PostOnlyWouldCross);

    hft::OrderRequest resting;
    resting.client_id = 3;
    resting.side = hft::Side::Buy;
    resting.type = hft::OrderType::Limit;
    resting.price = P("29999");
    resting.quantity = Q("0.25");
    const hft::OrderAck rest_ack = engine->submit(resting);
    CHECK(rest_ack.accepted());
    CHECK(rest_ack.filled.raw() == 0);
    CHECK(engine->metrics().open_orders == 1);
    CHECK(engine->submit(resting).reason == hft::RejectReason::DuplicateOrder);
    CHECK(engine->cancel(rest_ack.order_id));
    CHECK(!engine->cancel(rest_ack.order_id));

    hft::TradeTick trade;
    trade.price = P("30000.25");
    trade.quantity = Q("0.1");
    trade.exchange_ts = 1'001;
    CHECK(engine->publish_trade(trade));
    CHECK(engine->pending_events() == 1);
    CHECK(engine->poll() == 1);
    CHECK(engine->last_trade().price == P("30000.25"));

    const hft::EngineMetrics metrics = engine->metrics();
    CHECK(metrics.depth_events == 1);
    CHECK(metrics.trade_events == 1);
    CHECK(metrics.fills == 1);
    CHECK(metrics.filled_quantity == Q("0.5"));
    CHECK(metrics.orders_submitted == 4);
    CHECK(metrics.orders_rejected == 2);
    CHECK(metrics.p50_ns > 0);

    CHECK(!engine->is_stale(metrics.last_update_ts + 1));
    CHECK(engine->is_stale(metrics.last_update_ts + config.stale_after_ns + 1));
    engine->reset();
    CHECK(engine->metrics().fills == 0);
    CHECK(!engine->bbo().is_valid());
}

// ---------------------------------------------------------------------------
// RiskManager
// ---------------------------------------------------------------------------
struct RejectProbe {
    int count = 0;
    hft::RiskStatus last = hft::RiskStatus::APPROVED;
    std::size_t symbol_length = 0;
};

void probe_hook(void* context, hft::RiskStatus status,
                std::string_view symbol) noexcept {
    auto* probe = static_cast<RejectProbe*>(context);
    ++probe->count;
    probe->last = status;
    probe->symbol_length = symbol.size();
}

hft::RiskManagerConfig base_risk_config() {
    hft::RiskManagerConfig config;
    config.enabled = true;
    config.max_position = 10.0;
    config.max_order_size = 5.0;
    config.max_price_deviation_pct = 0.01;  // 1% band
    config.order_rate_limit = 0.0;          // tests opt in to rate limiting
    config.order_burst = 0.0;
    return config;
}

void test_token_bucket() {
    const hft::Nanos t0 = 5'000'000'000;

    hft::TokenBucket bucket(10.0, 10.0);  // 10 orders/s, burst of 10
    CHECK(bucket.enabled());
    CHECK(bucket.rate_per_sec() == 10.0);
    CHECK(bucket.burst() == 10.0);

    // The burst is admitted back to back...
    for (int i = 0; i < 10; ++i) {
        CHECK(bucket.try_acquire(t0));
    }
    // ...and the eleventh order at the same instant is refused.
    CHECK(!bucket.try_acquire(t0));
    CHECK(std::fabs(bucket.level(t0)) < 1e-9);

    // One interval later exactly one token is available again.
    CHECK(bucket.try_acquire(t0 + 100'000'000));
    CHECK(!bucket.try_acquire(t0 + 100'000'000));

    // Long idleness refills to capacity, never beyond it.
    CHECK(std::fabs(bucket.level(t0 + 5'000'000'000) - 10.0) < 1e-9);

    // A rate of zero disables the limiter entirely.
    hft::TokenBucket off(0.0, 0.0);
    CHECK(!off.enabled());
    for (int i = 0; i < 1'000; ++i) {
        CHECK(off.try_acquire(t0));
    }

    // reset() restarts the window.
    hft::TokenBucket restarted(1.0, 2.0);
    CHECK(restarted.try_acquire(t0));
    CHECK(restarted.try_acquire(t0));
    CHECK(!restarted.try_acquire(t0 + 500'000'000));
    restarted.reset(t0 + 500'000'000);
    CHECK(restarted.try_acquire(t0 + 500'000'000));
}

void test_risk_manager_guards() {
    hft::RiskManager risk(base_risk_config());
    const hft::Nanos t0 = 1'000'000'000;
    const double mid = 30'000.0;
    const std::string_view symbol = "BTCUSDT";

    auto check = [&](double price, double quantity, hft::Side side,
                     double position, double market_mid) {
        return risk.validate_order_at(t0, symbol, price, quantity, side, position,
                                      market_mid);
    };

    // ---- happy path ------------------------------------------------------
    CHECK(check(30'000.0, 1.0, hft::Side::Buy, 0.0, mid) ==
          hft::RiskStatus::APPROVED);
    // A sell that reduces an existing long is always inside the position cap.
    CHECK(check(30'000.0, 4.0, hft::Side::Sell, 9.0, mid) ==
          hft::RiskStatus::APPROVED);

    // ---- max order size --------------------------------------------------
    CHECK(check(30'000.0, 5.0, hft::Side::Buy, 0.0, mid) ==
          hft::RiskStatus::APPROVED);  // boundary is inclusive
    CHECK(check(30'000.0, 5.0001, hft::Side::Buy, 0.0, mid) ==
          hft::RiskStatus::REJECTED_MAX_ORDER_SIZE);

    // ---- position limit --------------------------------------------------
    CHECK(check(30'000.0, 1.0, hft::Side::Buy, 9.0, mid) ==
          hft::RiskStatus::APPROVED);  // 9 + 1 == 10, exactly at the cap
    CHECK(check(30'000.0, 1.0, hft::Side::Buy, 9.5, mid) ==
          hft::RiskStatus::REJECTED_POSITION_LIMIT);
    CHECK(check(30'000.0, 1.0, hft::Side::Sell, -9.5, mid) ==
          hft::RiskStatus::REJECTED_POSITION_LIMIT);  // short side too
    CHECK(check(30'000.0, 4.0, hft::Side::Sell, 1.0, mid) ==
          hft::RiskStatus::APPROVED);  // 1 - 4 == -3, flips through flat
    CHECK(check(30'000.0, 15.0, hft::Side::Sell, 1.0, mid) ==
          hft::RiskStatus::REJECTED_MAX_ORDER_SIZE);  // size guard fires first

    // ---- fat finger ------------------------------------------------------
    CHECK(check(30'300.0, 1.0, hft::Side::Buy, 0.0, mid) ==
          hft::RiskStatus::APPROVED);  // exactly +1% is inside the band
    CHECK(check(30'301.0, 1.0, hft::Side::Buy, 0.0, mid) ==
          hft::RiskStatus::REJECTED_FAT_FINGER);
    CHECK(check(29'699.0, 1.0, hft::Side::Sell, 0.0, mid) ==
          hft::RiskStatus::REJECTED_FAT_FINGER);
    // No usable reference price: fail closed rather than guess.
    CHECK(check(30'000.0, 1.0, hft::Side::Buy, 0.0, 0.0) ==
          hft::RiskStatus::REJECTED_FAT_FINGER);

    // ---- invalid input: NaN must never slip through ----------------------
    const double nan = std::numeric_limits<double>::quiet_NaN();
    const double inf = std::numeric_limits<double>::infinity();
    CHECK(check(0.0, 1.0, hft::Side::Buy, 0.0, mid) ==
          hft::RiskStatus::REJECTED_INVALID_ORDER);
    CHECK(check(-1.0, 1.0, hft::Side::Buy, 0.0, mid) ==
          hft::RiskStatus::REJECTED_INVALID_ORDER);
    CHECK(check(30'000.0, 0.0, hft::Side::Buy, 0.0, mid) ==
          hft::RiskStatus::REJECTED_INVALID_ORDER);
    CHECK(check(30'000.0, -1.0, hft::Side::Buy, 0.0, mid) ==
          hft::RiskStatus::REJECTED_INVALID_ORDER);
    CHECK(check(nan, 1.0, hft::Side::Buy, 0.0, mid) ==
          hft::RiskStatus::REJECTED_INVALID_ORDER);
    CHECK(check(30'000.0, nan, hft::Side::Buy, 0.0, mid) ==
          hft::RiskStatus::REJECTED_INVALID_ORDER);
    CHECK(check(inf, 1.0, hft::Side::Buy, 0.0, mid) ==
          hft::RiskStatus::REJECTED_INVALID_ORDER);
    CHECK(check(30'000.0, 1.0, hft::Side::Buy, nan, mid) ==
          hft::RiskStatus::REJECTED_INVALID_ORDER);
}

void test_risk_manager_telemetry() {
    hft::RiskManager risk(base_risk_config());
    const hft::Nanos t0 = 1'000'000'000;
    const std::string_view symbol = "BTCUSDT";
    const double mid = 30'000.0;

    CHECK(std::string_view(hft::risk_status_name(hft::RiskStatus::APPROVED)) ==
          "APPROVED");
    CHECK(std::string_view(
              hft::risk_status_name(hft::RiskStatus::REJECTED_RATE_LIMIT)) ==
          "REJECTED_RATE_LIMIT");
    CHECK(hft::is_approved(hft::RiskStatus::APPROVED));
    CHECK(!hft::is_approved(hft::RiskStatus::REJECTED_FAT_FINGER));
    CHECK(hft::to_reject_reason(hft::RiskStatus::REJECTED_RATE_LIMIT) ==
          hft::RejectReason::RateLimit);
    CHECK(hft::to_reject_reason(hft::RiskStatus::REJECTED_FAT_FINGER) ==
          hft::RejectReason::PriceBand);
    CHECK(hft::to_reject_reason(hft::RiskStatus::REJECTED_POSITION_LIMIT) ==
          hft::RejectReason::PositionLimit);
    CHECK(hft::to_reject_reason(hft::RiskStatus::REJECTED_MAX_ORDER_SIZE) ==
          hft::RejectReason::InvalidQuantity);

    // The hook fires on rejection only, and sees the caller's symbol.
    RejectProbe probe;
    risk.set_reject_hook(&probe_hook, &probe);
    (void)risk.validate_order_at(t0, symbol, 45'000.0, 1.0, hft::Side::Buy, 0.0,
                                 mid);
    CHECK(probe.count == 1);
    CHECK(probe.last == hft::RiskStatus::REJECTED_FAT_FINGER);
    CHECK(probe.symbol_length == symbol.size());

    risk.reset(t0);
    CHECK(risk.validate_order_at(t0, symbol, 30'000.0, 1.0, hft::Side::Buy, 0.0,
                                 mid) == hft::RiskStatus::APPROVED);
    CHECK(probe.count == 1);  // approvals never fire the hook
    const hft::RiskManagerStats stats = risk.stats(t0);
    CHECK(stats.evaluated == 1);
    CHECK(stats.approved == 1);
    CHECK(stats.rejected == 0);
    CHECK(stats.count(hft::RiskStatus::APPROVED) == 1);

    // Live tuning takes effect immediately and is readable back.
    risk.set_max_order_size(2.0);
    CHECK(risk.max_order_size() == 2.0);
    CHECK(risk.config().max_order_size == 2.0);
    CHECK(risk.validate_order_at(t0, symbol, 30'000.0, 3.0, hft::Side::Buy, 0.0,
                                 mid) == hft::RiskStatus::REJECTED_MAX_ORDER_SIZE);
    // A non-finite limit disables that guard instead of poisoning every check.
    risk.set_max_order_size(std::numeric_limits<double>::quiet_NaN());
    CHECK(risk.max_order_size() == 0.0);
    CHECK(risk.validate_order_at(t0, symbol, 30'000.0, 3.0, hft::Side::Buy, 0.0,
                                 mid) == hft::RiskStatus::APPROVED);

    // A dormant manager approves everything and records nothing.
    hft::RiskManager dormant(hft::RiskManagerConfig::disabled());
    CHECK(!dormant.enabled());
    CHECK(dormant.validate_order_at(t0, symbol, 1.0, 1.0e6, hft::Side::Buy, 1.0e9,
                                    0.0) == hft::RiskStatus::APPROVED);
    CHECK(dormant.stats(t0).evaluated == 0);
}

void test_risk_manager_rate_limit() {
    hft::RiskManagerConfig config = base_risk_config();
    config.max_position = 0.0;           // isolate the limiter
    config.max_order_size = 0.0;
    config.max_price_deviation_pct = 0.0;
    config.order_rate_limit = 50.0;
    config.order_burst = 50.0;
    hft::RiskManager risk(config);
    const hft::Nanos t0 = 1'000'000'000;
    const std::string_view symbol = "BTCUSDT";

    auto submit = [&](hft::Nanos now) {
        return risk.validate_order_at(now, symbol, 30'000.0, 1.0, hft::Side::Buy,
                                      0.0, 30'000.0);
    };

    // Exactly `burst` orders pass at one instant; the 51st does not.
    int approved = 0;
    for (int i = 0; i < 50; ++i) {
        if (submit(t0) == hft::RiskStatus::APPROVED) {
            ++approved;
        }
    }
    CHECK(approved == 50);
    CHECK(submit(t0) == hft::RiskStatus::REJECTED_RATE_LIMIT);

    // 50 orders/s means one token every 20 ms, no more.
    CHECK(submit(t0 + 20'000'000) == hft::RiskStatus::APPROVED);
    CHECK(submit(t0 + 20'000'000) == hft::RiskStatus::REJECTED_RATE_LIMIT);
    CHECK(submit(t0 + 39'999'999) == hft::RiskStatus::REJECTED_RATE_LIMIT);
    CHECK(submit(t0 + 40'000'000) == hft::RiskStatus::APPROVED);

    // Sustained throughput over five seconds matches the configured rate.
    risk.reset(t0);
    std::size_t admitted = 0;
    for (hft::Nanos now = t0; now < t0 + 5'000'000'000; now += 500'000) {
        if (submit(now) == hft::RiskStatus::APPROVED) {
            ++admitted;
        }
    }
    CHECK(admitted >= 290 && admitted <= 300);  // 50 burst + 5 s * 50/s

    // An order refused by another guard must not burn exchange budget: only
    // orders that would actually be sent count against the API limit.
    hft::RiskManagerConfig mixed = config;
    mixed.max_order_size = 1.0;
    mixed.order_rate_limit = 1.0;
    mixed.order_burst = 1.0;
    hft::RiskManager gated(mixed);
    const hft::Nanos t1 = 2'000'000'000;
    CHECK(gated.validate_order_at(t1, symbol, 30'000.0, 50.0, hft::Side::Buy, 0.0,
                                  30'000.0) == hft::RiskStatus::REJECTED_MAX_ORDER_SIZE);
    CHECK(gated.validate_order_at(t1, symbol, 30'000.0, 1.0, hft::Side::Buy, 0.0,
                                  30'000.0) == hft::RiskStatus::APPROVED);
    CHECK(gated.validate_order_at(t1, symbol, 30'000.0, 1.0, hft::Side::Buy, 0.0,
                                  30'000.0) == hft::RiskStatus::REJECTED_RATE_LIMIT);
}

void test_risk_manager_concurrent_burst() {
    hft::RiskManagerConfig config = base_risk_config();
    config.max_position = 0.0;
    config.max_order_size = 0.0;
    config.max_price_deviation_pct = 0.0;
    config.order_rate_limit = 1'000.0;
    config.order_burst = 50.0;
    hft::RiskManager risk(config);
    const hft::Nanos t0 = 7'000'000'000;
    risk.reset(t0);

    // Four threads hammering one bucket at the same instant must be admitted
    // exactly `burst` times: the CAS loop is the only arbiter, so a torn or
    // double-applied refill shows up here as an off-by-N immediately.
    constexpr int kThreads = 4;
    constexpr int kAttempts = 5'000;
    std::atomic<int> admitted{0};
    std::vector<std::thread> workers;
    workers.reserve(kThreads);
    for (int thread = 0; thread < kThreads; ++thread) {
        workers.emplace_back([&risk, &admitted] {
            for (int i = 0; i < kAttempts; ++i) {
                if (risk.validate_order_at(t0, "BTCUSDT", 30'000.0, 0.25,
                                           hft::Side::Buy, 0.0,
                                           30'000.0) == hft::RiskStatus::APPROVED) {
                    admitted.fetch_add(1, std::memory_order_relaxed);
                }
            }
        });
    }
    for (auto& worker : workers) {
        worker.join();
    }
    CHECK(admitted.load() == 50);

    const hft::RiskManagerStats stats = risk.stats(t0);
    CHECK(stats.evaluated ==
          static_cast<std::uint64_t>(kThreads) * kAttempts);
    CHECK(stats.approved == 50);
    CHECK(stats.count(hft::RiskStatus::REJECTED_RATE_LIMIT) ==
          stats.evaluated - 50);
}

void test_risk_manager_in_engine() {
    hft::EngineConfig config;
    config.tick_size = hft::Price::from_double(0.5);
    config.center = hft::Price::from_int(30'000);
    config.risk_manager = base_risk_config();  // 5-unit order cap, 1% band
    hft::TradingEngine engine(config);
    CHECK(engine.risk_manager().enabled());

    hft::DepthUpdate depth;
    depth.exchange_ts = 1'000;
    depth.bids[0] = {P("29999.5"), Q("50")};
    depth.asks[0] = {P("30000.5"), Q("50")};
    depth.bid_count = 1;
    depth.ask_count = 1;
    engine.apply_depth(depth);

    // Passive bid one tick behind the touch: inside every limit.
    hft::OrderRequest passive;
    passive.side = hft::Side::Buy;
    passive.type = hft::OrderType::Limit;
    passive.price = P("29999.0");

    hft::OrderRequest oversized = passive;
    oversized.client_id = 2;
    oversized.quantity = Q("6");
    const hft::OrderAck too_big = engine.submit(oversized);
    CHECK(!too_big.accepted());
    CHECK(too_big.reason == hft::RejectReason::InvalidQuantity);
    CHECK(engine.metrics().orders_rejected == 1);

    hft::OrderRequest mispriced = passive;
    mispriced.client_id = 3;
    mispriced.quantity = Q("1");
    mispriced.price = P("31000");  // 3.3% away from a 30 000 mid
    const hft::OrderAck fat = engine.submit(mispriced);
    CHECK(!fat.accepted());
    CHECK(fat.reason == hft::RejectReason::PriceBand);

    hft::OrderRequest good = passive;
    good.client_id = 4;
    good.quantity = Q("1");
    const hft::OrderAck accepted = engine.submit(good);
    CHECK(accepted.accepted());
    CHECK(accepted.order_id != 0);

    const hft::RiskManagerStats stats = engine.risk_stats();
    CHECK(stats.evaluated == 3);
    CHECK(stats.approved == 1);
    CHECK(stats.rejected == 2);

    // The gate is dormant by default so replay keeps its previous behaviour.
    hft::TradingEngine plain{hft::EngineConfig{}};
    CHECK(!plain.risk_manager().enabled());
    CHECK(!plain.risk_manager_config().enabled);
}

void test_kill_switch_basic() {
    hft::KillSwitch kill;
    CHECK(!kill.is_tripped());
    CHECK(kill.get_state() == hft::SystemState::NORMAL);
    CHECK(kill.check_order_allowed());
    CHECK(kill.get_stats().trip_count == 0);
    CHECK(kill.get_stats().last_trip_reason.empty());
    CHECK(hft::system_state_name(hft::SystemState::RECOVERING) == "RECOVERING");

    kill.trip("manual: ops halt");
    CHECK(kill.is_tripped());
    CHECK(kill.get_state() == hft::SystemState::TRIGGERED);
    CHECK(!kill.check_order_allowed());

    const hft::KillSwitchStats tripped = kill.get_stats();
    CHECK(tripped.trip_count == 1);
    CHECK(tripped.last_trip_reason == "manual: ops halt");
    CHECK(tripped.last_trip_timestamp_ns > 0);
    CHECK(tripped.blocked_orders == 1);
    CHECK(tripped.state == hft::SystemState::TRIGGERED);

    // Recovery keeps blocking until an operator explicitly clears the halt.
    CHECK(kill.begin_recovery());
    CHECK(kill.get_state() == hft::SystemState::RECOVERING);
    CHECK(!kill.check_order_allowed());
    CHECK(!kill.begin_recovery());

    kill.reset();
    CHECK(kill.get_state() == hft::SystemState::NORMAL);
    CHECK(kill.check_order_allowed());
    const hft::KillSwitchStats reset = kill.get_stats();
    CHECK(reset.reset_count == 1);
    CHECK(reset.trip_count == 1);
    CHECK(reset.last_trip_reason == "manual: ops halt");
    CHECK(reset.consecutive_rejects == 0);

    // An empty reason still leaves a usable audit trail.
    hft::KillSwitch bare;
    bare.trip("");
    CHECK(bare.get_stats().last_trip_reason == "unspecified");
}

void test_kill_switch_guards() {
    hft::KillSwitchConfig config;
    config.max_drawdown_limit = 1'000.0;
    config.max_consecutive_rejects = 3;
    hft::KillSwitch kill(config);

    CHECK(!kill.check_drawdown(999.99));
    CHECK(!kill.is_tripped());
    CHECK(kill.check_drawdown(1'000.0));
    CHECK(kill.is_tripped());
    CHECK(kill.get_stats().last_trip_reason.find("drawdown=") == 0);
    CHECK(!kill.check_drawdown(5'000.0));  // no second transition

    kill.reset();
    CHECK(!kill.record_order_rejected());
    CHECK(!kill.record_order_rejected());
    kill.record_order_accepted();  // streak resets
    CHECK(kill.get_stats().consecutive_rejects == 0);
    CHECK(!kill.record_order_rejected());
    CHECK(!kill.record_order_rejected());
    CHECK(kill.record_order_rejected());
    CHECK(kill.get_state() == hft::SystemState::TRIGGERED);
    CHECK(kill.get_stats().consecutive_rejects == 0);

    // NaN must fail closed rather than slip through the comparison.
    hft::KillSwitch nan_guard(config);
    CHECK(nan_guard.check_drawdown(std::nan("")));
    CHECK(nan_guard.is_tripped());

    // A disabled switch keeps the automation inert but still stops on demand.
    hft::KillSwitchConfig disarmed;
    disarmed.enabled = false;
    disarmed.max_drawdown_limit = 1.0;
    disarmed.max_consecutive_rejects = 1;
    hft::KillSwitch manual_only(disarmed);
    CHECK(!manual_only.enabled());
    CHECK(!manual_only.check_drawdown(1e9));
    CHECK(!manual_only.record_order_rejected());
    CHECK(!manual_only.is_tripped());
    manual_only.trip("operator override");
    CHECK(manual_only.is_tripped());
    CHECK(!manual_only.check_order_allowed());

    // Zero thresholds disable the guards without touching the state machine.
    hft::KillSwitchConfig off;
    hft::KillSwitch dormant(off);
    CHECK(!dormant.check_drawdown(1e12));
    CHECK(!dormant.record_order_rejected());
    CHECK(!dormant.is_tripped());
}

void test_kill_switch_hooks_and_threads() {
    hft::KillSwitch kill;
    int cancels = 0;
    int flattens = 0;
    int logs = 0;
    std::string logged_reason;
    std::string logged_state;

    kill.set_cancel_all_hook([&cancels] { ++cancels; });
    kill.set_flatten_hook([&flattens] { ++flattens; });
    kill.set_trip_hook([&](hft::SystemState state, std::string_view reason) {
        ++logs;
        logged_reason.assign(reason);
        logged_state.assign(hft::system_state_name(state));
    });

    kill.trip("halt");
    CHECK(cancels == 1);
    CHECK(flattens == 1);
    CHECK(logs == 1);
    CHECK(logged_reason == "halt");
    CHECK(logged_state == "TRIGGERED");

    kill.clear_hooks();
    kill.trip("halt again");
    CHECK(cancels == 1);
    CHECK(logs == 1);

    // A throwing remediation hook must never unwind the trip itself.
    hft::KillSwitch guarded;
    guarded.set_cancel_all_hook([] { throw std::runtime_error("cancel failed"); });
    guarded.set_flatten_hook([] { throw std::runtime_error("flatten failed"); });
    guarded.trip("halt");
    CHECK(guarded.is_tripped());
    CHECK(guarded.get_stats().trip_count == 1);

    // Concurrent trips must all be counted and converge on TRIGGERED.
    hft::KillSwitch concurrent;
    constexpr int kThreads = 4;
    constexpr int kTrips = 250;
    std::atomic<int> torn{0};
    std::vector<std::thread> workers;
    workers.reserve(kThreads);
    for (int thread = 0; thread < kThreads; ++thread) {
        workers.emplace_back([&concurrent, &torn] {
            for (int i = 0; i < kTrips; ++i) {
                concurrent.trip("burst");
                if (!concurrent.is_tripped()) {
                    torn.fetch_add(1, std::memory_order_relaxed);
                }
            }
        });
    }
    for (auto& worker : workers) {
        worker.join();
    }
    CHECK(torn.load() == 0);
    CHECK(concurrent.get_state() == hft::SystemState::TRIGGERED);
    CHECK(concurrent.get_stats().trip_count ==
          static_cast<std::uint64_t>(kThreads) * kTrips);
    concurrent.reset();
    CHECK(concurrent.check_order_allowed());
}

// The dead-man's switch: a silent feed must trip the kill switch exactly once
// per expiry window, and a single beat must re-arm it for the next outage.
void test_heartbeat_monitor() {
    constexpr std::int64_t kMs = 1'000'000;
    hft::HeartbeatConfig config;
    config.timeout_ns = 500 * kMs;

    hft::HeartbeatMonitor monitor(config);
    CHECK(monitor.timeout_ns() == 500 * kMs);
    CHECK(!monitor.armed());
    CHECK(monitor.stats(0).beats == 0);
    CHECK(monitor.last_beat_ns() == 0);

    // Monotonic timestamps make the state machine deterministic without sleeps.
    const std::int64_t t0 = 10'000 * kMs;
    monitor.beat(t0);
    CHECK(monitor.last_beat_ns() == t0);
    CHECK(monitor.stats(t0 + 100 * kMs).beats == 1);
    CHECK(monitor.silence_ns(t0 + 100 * kMs) == 100 * kMs);
    CHECK(!monitor.is_expired(t0 + 499 * kMs));

    // At the deadline the monitor expires, but only the first poll reports it.
    CHECK(monitor.is_expired(t0 + 500 * kMs));
    CHECK(monitor.poll(t0 + 500 * kMs));
    CHECK(!monitor.poll(t0 + 900 * kMs));
    CHECK(monitor.stats(t0 + 900 * kMs).timeouts == 1);
    CHECK(monitor.stats(t0 + 900 * kMs).max_silence_ns >= 900 * kMs);

    // An unarmed monitor still reports expiry; it just has nothing to trip.
    CHECK(!monitor.armed());

    // A beat re-arms the window so the next outage is detected again.
    monitor.beat(t0 + 1'000 * kMs);
    CHECK(!monitor.is_expired(t0 + 1'200 * kMs));
    CHECK(!monitor.poll(t0 + 1'200 * kMs));
    CHECK(monitor.poll(t0 + 1'500 * kMs));
    CHECK(monitor.stats(t0 + 1'500 * kMs).timeouts == 2);

    // Armed: the trip runs through the kill switch, hooks included.
    hft::KillSwitch kill;
    int cancels = 0;
    int flattens = 0;
    kill.set_cancel_all_hook([&cancels] { ++cancels; });
    kill.set_flatten_hook([&flattens] { ++flattens; });
    monitor.arm(kill);
    CHECK(monitor.armed());
    CHECK(monitor.stats(t0 + 1'500 * kMs).armed);
    CHECK(!kill.is_tripped());

    CHECK(!monitor.poll(t0 + 1'500 * kMs));  // same window: latched, no new trip
    CHECK(!kill.is_tripped());

    monitor.beat(t0 + 2'000 * kMs);
    CHECK(monitor.poll(t0 + 2'500 * kMs));
    CHECK(kill.get_state() == hft::SystemState::TRIGGERED);
    CHECK(!kill.check_order_allowed());
    CHECK(cancels == 1);
    CHECK(flattens == 1);
    CHECK(kill.get_stats().last_trip_reason.find("heartbeat_timeout") == 0);

    // `begin_recovery` keeps orders blocked while remediation is in flight.
    CHECK(kill.begin_recovery());
    CHECK(kill.get_state() == hft::SystemState::RECOVERING);
    CHECK(!kill.check_order_allowed());

    // `reset()` clears the switch and restarts the heartbeat window, but it
    // keeps the telemetry (beats/timeouts survive, like KillSwitchStats).
    kill.reset();
    monitor.reset(t0 + 3'000 * kMs);
    CHECK(kill.get_state() == hft::SystemState::NORMAL);
    CHECK(monitor.armed());
    CHECK(monitor.stats(t0 + 3'000 * kMs).beats == 3);
    CHECK(monitor.stats(t0 + 3'000 * kMs).timeouts == 3);
    CHECK(!monitor.is_expired(t0 + 3'100 * kMs));

    // `timeout_ns == 0` disables expiry entirely (pure latency gauge).
    monitor.set_timeout_ns(0);
    CHECK(!monitor.is_expired(t0 + 10'000 * kMs));
    CHECK(!monitor.poll(t0 + 10'000 * kMs));
    CHECK(monitor.stats(t0 + 10'000 * kMs).max_silence_ns > 0);

    // A vanishing window: a beat before expiry must prevent the trip.
    hft::HeartbeatMonitor steady(config);
    hft::KillSwitch steady_kill;
    steady.arm(steady_kill);
    for (int i = 0; i < 10; ++i) {
        steady.beat(t0 + i * 100 * kMs);
        CHECK(!steady.poll(t0 + i * 100 * kMs + 99 * kMs));
    }
    CHECK(!steady_kill.is_tripped());
    CHECK(steady.stats().timeouts == 0);

    steady.detach();
    CHECK(!steady.armed());
}

// Beats come from the ingestion thread while the watchdog polls: the state must
// stay coherent with no locks on either side.
void test_heartbeat_concurrency() {
    hft::HeartbeatConfig config;
    config.timeout_ns = 50'000'000;  // 50 ms
    hft::HeartbeatMonitor monitor(config);
    hft::KillSwitch kill;
    monitor.arm(kill);

    std::atomic<bool> stop{false};
    std::atomic<int> torn{0};
    std::vector<std::thread> threads;
    threads.emplace_back([&] {
        while (!stop.load(std::memory_order_relaxed)) {
            monitor.beat();
            if (monitor.last_beat_ns() <= 0) {
                torn.fetch_add(1, std::memory_order_relaxed);
            }
        }
    });
    threads.emplace_back([&] {
        while (!stop.load(std::memory_order_relaxed)) {
            (void)monitor.poll();
            const hft::HeartbeatStats stats = monitor.stats();
            if (stats.timeout_ns != 50'000'000) {
                torn.fetch_add(1, std::memory_order_relaxed);
            }
        }
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(60));
    stop.store(true);
    for (auto& worker : threads) {
        worker.join();
    }

    CHECK(torn.load() == 0);
    CHECK(monitor.stats().beats > 0);
    CHECK(monitor.stats().max_silence_ns >= 0);
    // Whether an expiry happened depends on scheduling; the switch must simply
    // be in a legal state and still block orders if it tripped.
    const hft::SystemState state = kill.get_state();
    CHECK(state == hft::SystemState::NORMAL || state == hft::SystemState::TRIGGERED);
    CHECK(kill.is_tripped() == !kill.check_order_allowed());

    monitor.detach();
    CHECK(!monitor.armed());
}


void test_paper_account() {
    hft::paper_account account;
    account.reset(N("100000"), -10, 0);
    (void)account.apply_fill(P("30000"), Q("2"), hft::Side::Buy, false);
    CHECK(account.cash() == N("40000"));
    CHECK(account.net_quantity() == Q("2"));
    CHECK(account.average_entry_price() == P("30000"));
    (void)account.apply_fill(P("30100"), Q("1"), hft::Side::Sell, false);
    CHECK(account.cash() == N("70100"));
    CHECK(account.realized_pnl() == N("100"));
    CHECK(account.net_quantity() == Q("1"));
    (void)account.apply_fill(P("30050"), Q("1"), hft::Side::Sell, true);
    CHECK(account.cash() == N("100180.05"));
    CHECK(account.fees_paid() == N("-30.05"));
    CHECK(account.realized_pnl() == N("180.05"));
    CHECK(account.net_quantity() == Q("0"));
    CHECK(account.average_entry_price() == P("0"));
    CHECK(account.fill_count() == 3);
    CHECK(account.maker_fills() == 1);
    CHECK(account.taker_fills() == 2);
    CHECK(account.equity() == N("100180.05"));

    // Equity is mark-to-market: opening a position *at the mark* must not move
    // it, and marking the book revalues the inventory. Regression guard for the
    // cost-basis term in `equity()` (an open position used to report its whole
    // entry notional as a loss, which poisoned drawdown/Sharpe curves).
    hft::paper_account marked;
    marked.reset(N("100000"), 0.0, 0.0);
    (void)marked.apply_fill(P("30000"), Q("2"), hft::Side::Buy, true);
    marked.mark(P("30000"));
    CHECK(marked.equity() == N("100000"));
    CHECK(marked.unrealized_pnl() == N("0"));
    marked.mark(P("30100"));
    CHECK(marked.equity() == N("100200"));
    CHECK(marked.realized_pnl() == N("0"));

    // ... and symmetrically for a short: a lower mark is a profit.
    hft::paper_account short_account;
    short_account.reset(N("100000"), 0.0, 0.0);
    (void)short_account.apply_fill(P("30000"), Q("2"), hft::Side::Sell, true);
    short_account.mark(P("29900"));
    CHECK(short_account.equity() == N("100200"));
}

void test_paper_account_fractional_bps() {
    // 0.5 bps must not be truncated to 0: 60'000 notional * 0.5 bps = 3.0.
    hft::paper_account taker_account;
    taker_account.reset(N("100000"), 0.0, 0.5);
    CHECK(taker_account.taker_bps() == 0.5);
    (void)taker_account.apply_fill(P("30000"), Q("2"), hft::Side::Buy, false);
    CHECK(taker_account.fees_paid() == N("3"));
    CHECK(taker_account.cash() == N("39997"));
    CHECK(taker_account.realized_pnl() == N("-3"));

    // Fractional maker rebate: -0.25 bps of 60'000 = -1.5 (credited to cash).
    hft::paper_account maker_account;
    maker_account.reset(N("100000"), -0.25, 5.0);
    CHECK(maker_account.maker_bps() == -0.25);
    (void)maker_account.apply_fill(P("30000"), Q("2"), hft::Side::Sell, true);
    CHECK(maker_account.fees_paid() == N("-1.5"));
    CHECK(maker_account.cash() == N("160001.5"));
    CHECK(maker_account.realized_pnl() == N("1.5"));

    // Whole-basis-point rates keep the historical integer result bit-for-bit.
    hft::paper_account legacy_account;
    legacy_account.reset(N("100000"), -1, 5);
    (void)legacy_account.apply_fill(P("29999.5"), Q("1"), hft::Side::Sell, true);
    CHECK(legacy_account.fees_paid() == N("-2.99995"));
}

void test_matching_slippage() {
    hft::L2Book book{P("0.5"), P("30000")};
    CHECK(book.apply_level(hft::Side::Sell, P("30000.5"), Q("2"), 1));
    CHECK(book.apply_level(hft::Side::Sell, P("30001.0"), Q("3"), 1));
    CHECK(book.apply_level(hft::Side::Sell, P("30001.5"), Q("10"), 1));
    CHECK(book.apply_level(hft::Side::Buy, P("30000.0"), Q("5"), 1));
    const hft::match_result match = hft::match_taker(book, hft::Side::Buy, Q("4"));
    CHECK(match.filled == Q("4"));
    CHECK(match.remaining == Q("0"));
    CHECK(match.leg_count == 2);
    CHECK(match.touch_price == P("30000.5"));
    CHECK(match.average_price == P("30000.75"));
    CHECK(match.slippage == N("1"));
}

void test_paper_engine_taker() {
    hft::paper_engine engine{P("0.5"), P("30000"), 0, 5, N("100000")};
    hft::DepthUpdate depth;
    depth.exchange_ts = 1000;
    depth.bids[0] = {P("30000.0"), Q("10")};
    depth.asks[0] = {P("30000.5"), Q("2")};
    depth.asks[1] = {P("30001.0"), Q("3")};
    depth.bid_count = 1;
    depth.ask_count = 2;
    depth.first_update_id = 1;
    depth.final_update_id = 2;
    engine.process_update(depth);
    CHECK(engine.bbo().is_valid());

    hft::OrderRequest request;
    request.side = hft::Side::Buy;
    request.type = hft::OrderType::Market;
    request.quantity = Q("4");
    const hft::paper_ack ack = engine.submit(request);
    CHECK(ack.accepted());
    CHECK(ack.filled == Q("4"));
    CHECK(ack.average_price == P("30000.75"));
    CHECK(ack.slippage == N("1"));
    CHECK(engine.account().net_quantity() == Q("4"));
    CHECK(engine.account().average_entry_price() == P("30000.75"));
    CHECK(engine.account().cash() == N("-20063.0015"));
    CHECK(engine.metrics().fills == 2);
    CHECK(engine.metrics().slippage_total == N("1"));

    hft::Fill fill;
    CHECK(engine.drain_fill(fill));
    CHECK(fill.price == P("30000.5"));
    CHECK(fill.quantity == Q("2"));
    CHECK(fill.is_maker == 0);
    CHECK(engine.drain_fill(fill));
    CHECK(fill.price == P("30001.0"));
    CHECK(fill.quantity == Q("2"));
    CHECK(!engine.drain_fill(fill));
}

void test_paper_engine_gap_detection() {
    hft::paper_engine engine{P("0.5"), P("30000")};
    hft::DepthUpdate first;
    first.exchange_ts = 1;
    first.bids[0] = {P("30000"), Q("1")};
    first.asks[0] = {P("30001"), Q("1")};
    first.bid_count = 1;
    first.ask_count = 1;
    first.final_update_id = 10;
    engine.process_update(first);
    CHECK(engine.metrics().gap_events == 0);

    hft::DepthUpdate second = first;
    second.final_update_id = 11;
    second.prev_final_update_id = 9;
    engine.process_update(second);
    CHECK(engine.metrics().gap_events == 1);
}

void test_ingest_lines() {
    hft::paper_engine engine{P("0.5"), P("30000")};
    std::vector<std::string> lines;
    lines.push_back(
        R"({"e":"depthUpdate","E":1000,"U":1,"u":2,"pu":0,"b":[["30000.00","1"]],"a":[["30001.00","1"]]})");
    lines.push_back(
        R"({"e":"depthUpdate","E":1100,"U":3,"u":4,"pu":2,"b":[["30000.00","2"]],"a":[["30001.00","1"]]})");
    lines.push_back("not json");
    const hft::ingest_stats stats = hft::ingest_lines(engine, lines);
    CHECK(stats.lines == 3);
    CHECK(stats.depth_events == 2);
    CHECK(stats.malformed == 1);
    CHECK(stats.gaps == 0);
}

}  // namespace

int main() {
    test_fixed_point();
    test_ring_buffer();
    test_l2_book();
    test_l3_book();
    test_order_manager();
    test_risk_guard();
    test_latency_histogram();
    test_json_parser();
    test_json_parser_keeps_the_levels_nearest_the_touch();
    test_websocket_decoder();
    test_l2_clear_resets_timestamps();
    test_crossing_limit_fill_preserves_price();
    test_risk_guard_uses_touch_for_crossing_limit();
    test_zero_exchange_ts_preserves_timestamps();
    test_position_accounting();
    test_time_in_force();
    test_engine();
    test_token_bucket();
    test_risk_manager_guards();
    test_risk_manager_telemetry();
    test_risk_manager_rate_limit();
    test_risk_manager_concurrent_burst();
    test_risk_manager_in_engine();
    test_kill_switch_basic();
    test_kill_switch_guards();
    test_kill_switch_hooks_and_threads();
    test_heartbeat_monitor();
    test_heartbeat_concurrency();
    test_paper_account();
    test_paper_account_fractional_bps();
    test_matching_slippage();
    test_paper_engine_taker();
    test_paper_engine_gap_detection();
    test_ingest_lines();
    if (g_failures != 0) {
        std::fprintf(stderr, "%d test assertion(s) failed\n", g_failures);
        return 1;
    }
    std::printf("all C++ core tests passed\n");
    return 0;
}



