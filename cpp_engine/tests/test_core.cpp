// SPDX-License-Identifier: MIT
// Self-contained unit tests for the C++ HFT core (no external framework).
#include <atomic>
#include <cstdio>
#include <cstring>
#include <memory>
#include <thread>
#include <vector>

#include "engine.hpp"
#include "latency_histogram.hpp"
#include "order_manager.hpp"
#include "orderbook.hpp"
#include "ring_buffer.hpp"
#include "risk_guard.hpp"
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
    test_websocket_decoder();
    test_l2_clear_resets_timestamps();
    test_crossing_limit_fill_preserves_price();
    test_risk_guard_uses_touch_for_crossing_limit();
    test_zero_exchange_ts_preserves_timestamps();
    test_position_accounting();
    test_time_in_force();
    test_engine();
    if (g_failures != 0) {
        std::fprintf(stderr, "%d test assertion(s) failed\n", g_failures);
        return 1;
    }
    std::printf("all C++ core tests passed\n");
    return 0;
}



