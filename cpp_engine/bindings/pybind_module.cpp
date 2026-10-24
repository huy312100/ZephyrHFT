// SPDX-License-Identifier: MIT
// pybind11 surface for the C++ HFT core.
//
// Python owns strategy logic and research; every latency-sensitive operation
// (book maintenance, risk checks, order lifecycle) runs in `hft::TradingEngine`.
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "engine.hpp"
#include "ingest.hpp"
#include "matching.hpp"
#include "orderbook.hpp"
#include "paper_account.hpp"
#include "paper_engine.hpp"
#include "ring_buffer.hpp"
#include "risk/heartbeat.hpp"
#include "risk/kill_switch.hpp"
#include "risk_manager.hpp"
#include "types.hpp"
#include "ws_parser.hpp"

namespace py = pybind11;
using namespace hft;

namespace {

template <typename T>
std::string fixed_to_string(const T& value) {
    char buffer[48];
    const std::size_t length = value.to_chars(buffer, sizeof(buffer));
    return std::string(buffer, length);
}

template <typename T>
T fixed_from_string(const std::string& text) {
    const std::optional<T> parsed = T::from_string(text);
    if (!parsed.has_value()) {
        throw py::value_error("invalid fixed-point decimal: " + text);
    }
    return *parsed;
}

template <typename T>
void bind_fixed(py::module_& module, const char* name) {
    py::class_<T>(module, name)
        .def(py::init<>())
        .def_static("from_raw", &T::from_raw, py::arg("raw"))
        .def_static("from_int", &T::from_int, py::arg("value"))
        .def_static("from_string", &fixed_from_string<T>, py::arg("text"))
        .def("raw", &T::raw)
        .def("to_string", &fixed_to_string<T>)
        .def("__str__", &fixed_to_string<T>)
        .def("__repr__", [name](const T& value) {
            return std::string(name) + "(" + fixed_to_string(value) + ")";
        })
        .def("__float__", [](const T& value) {
            return static_cast<double>(value.raw()) / static_cast<double>(T::kScale);
        })
        .def("__eq__", [](const T& lhs, const T& rhs) { return lhs.raw() == rhs.raw(); })
        .def("__lt__", [](const T& lhs, const T& rhs) { return lhs.raw() < rhs.raw(); })
        .def("__le__", [](const T& lhs, const T& rhs) { return lhs.raw() <= rhs.raw(); })
        .def("__add__", [](const T& lhs, const T& rhs) { return lhs + rhs; })
        .def("__sub__", [](const T& lhs, const T& rhs) { return lhs - rhs; })
        .def("__neg__", [](const T& value) { return -value; });
}

std::uint32_t fill_levels(const std::vector<std::pair<std::string, std::string>>& source,
                          LevelDelta* destination) {
    if (source.size() > kMaxDepthLevels) {
        throw py::value_error("too many depth levels (max " +
                              std::to_string(kMaxDepthLevels) + ")");
    }
    std::uint32_t count = 0;
    for (const auto& [price, quantity] : source) {
        destination[count].price = fixed_from_string<Price>(price);
        destination[count].quantity = fixed_from_string<Quantity>(quantity);
        ++count;
    }
    return count;
}

std::vector<BookLevel> snapshot_levels(
    const std::array<BookLevel, BookSnapshot::kMaxLevels>& levels, std::uint32_t count) {
    return std::vector<BookLevel>(levels.begin(), levels.begin() + count);
}

std::vector<std::pair<std::string, std::string>> level_pairs(
    const std::array<LevelDelta, kMaxDepthLevels>& levels, std::uint32_t count) {
    std::vector<std::pair<std::string, std::string>> out;
    out.reserve(count);
    for (std::uint32_t i = 0; i < count; ++i) {
        out.emplace_back(fixed_to_string(levels[i].price),
                         fixed_to_string(levels[i].quantity));
    }
    return out;
}

}  // namespace

PYBIND11_MODULE(hft_native, m) {
    m.doc() = "Zero-allocation C++23 core for the hybrid crypto HFT stack";

    py::enum_<Side>(m, "Side").value("BUY", Side::Buy).value("SELL", Side::Sell);
    py::enum_<OrderType>(m, "OrderType")
        .value("LIMIT", OrderType::Limit)
        .value("MARKET", OrderType::Market)
        .value("POST_ONLY", OrderType::PostOnly);
    py::enum_<TimeInForce>(m, "TimeInForce")
        .value("GTC", TimeInForce::GTC)
        .value("IOC", TimeInForce::IOC)
        .value("FOK", TimeInForce::FOK);
    py::enum_<OrderStatus>(m, "OrderStatus")
        .value("NEW", OrderStatus::New)
        .value("PARTIALLY_FILLED", OrderStatus::PartiallyFilled)
        .value("FILLED", OrderStatus::Filled)
        .value("CANCELLED", OrderStatus::Cancelled)
        .value("REJECTED", OrderStatus::Rejected);
    py::enum_<RejectReason>(m, "RejectReason")
        .value("NONE", RejectReason::None)
        .value("INVALID_PRICE", RejectReason::InvalidPrice)
        .value("INVALID_QUANTITY", RejectReason::InvalidQuantity)
        .value("BOOK_UNAVAILABLE", RejectReason::BookUnavailable)
        .value("POSITION_LIMIT", RejectReason::PositionLimit)
        .value("NOTIONAL_LIMIT", RejectReason::NotionalLimit)
        .value("OPEN_ORDER_LIMIT", RejectReason::OpenOrderLimit)
        .value("RATE_LIMIT", RejectReason::RateLimit)
        .value("PRICE_BAND", RejectReason::PriceBand)
        .value("DAILY_LOSS_LIMIT", RejectReason::DailyLossLimit)
        .value("POST_ONLY_WOULD_CROSS", RejectReason::PostOnlyWouldCross)
        .value("DUPLICATE_ORDER", RejectReason::DuplicateOrder)
        .value("ORDER_POOL_EXHAUSTED", RejectReason::OrderPoolExhausted)
        .value("UNKNOWN_ORDER", RejectReason::UnknownOrder)
        .value("FILL_OR_KILL_UNAVAILABLE", RejectReason::FillOrKillUnavailable);
    py::enum_<RiskStatus>(m, "RiskStatus")
        .value("APPROVED", RiskStatus::APPROVED)
        .value("REJECTED_POSITION_LIMIT", RiskStatus::REJECTED_POSITION_LIMIT)
        .value("REJECTED_RATE_LIMIT", RiskStatus::REJECTED_RATE_LIMIT)
        .value("REJECTED_FAT_FINGER", RiskStatus::REJECTED_FAT_FINGER)
        .value("REJECTED_MAX_ORDER_SIZE", RiskStatus::REJECTED_MAX_ORDER_SIZE)
        .value("REJECTED_INVALID_ORDER", RiskStatus::REJECTED_INVALID_ORDER);
    py::enum_<FeedMessageType>(m, "FeedMessageType")
        .value("UNKNOWN", FeedMessageType::Unknown)
        .value("DEPTH_UPDATE", FeedMessageType::DepthUpdate)
        .value("TRADE", FeedMessageType::Trade);
    py::enum_<SystemState>(m, "SystemState")
        .value("NORMAL", SystemState::NORMAL)
        .value("TRIGGERED", SystemState::TRIGGERED)
        .value("RECOVERING", SystemState::RECOVERING);

    bind_fixed<Price>(m, "Price");
    bind_fixed<Quantity>(m, "Quantity");
    bind_fixed<Notional>(m, "Notional");

    py::class_<BookLevel>(m, "BookLevel")
        .def(py::init<>())
        .def_readwrite("price", &BookLevel::price)
        .def_readwrite("quantity", &BookLevel::quantity)
        .def_readwrite("order_count", &BookLevel::order_count)
        .def("__repr__", [](const BookLevel& level) {
            return "BookLevel(" + fixed_to_string(level.price) + ", " +
                   fixed_to_string(level.quantity) + ", orders=" +
                   std::to_string(level.order_count) + ")";
        });

    py::class_<Bbo>(m, "Bbo")
        .def(py::init<>())
        .def_readonly("bid_price", &Bbo::bid_price)
        .def_readonly("ask_price", &Bbo::ask_price)
        .def_readonly("bid_quantity", &Bbo::bid_quantity)
        .def_readonly("ask_quantity", &Bbo::ask_quantity)
        .def_readonly("exchange_ts", &Bbo::exchange_ts)
        .def_readonly("recv_ts", &Bbo::recv_ts)
        .def_readonly("sequence", &Bbo::sequence)
        .def_readonly("valid", &Bbo::valid)
        .def("is_valid", &Bbo::is_valid)
        .def("mid", &Bbo::mid)
        .def("spread", &Bbo::spread)
        .def("spread_bps", &Bbo::spread_bps)
        .def("__repr__", [](const Bbo& bbo) {
            return "Bbo(bid=" + fixed_to_string(bbo.bid_price) + " x " +
                   fixed_to_string(bbo.bid_quantity) + ", ask=" +
                   fixed_to_string(bbo.ask_price) + " x " +
                   fixed_to_string(bbo.ask_quantity) + ", seq=" +
                   std::to_string(bbo.sequence) + ")";
        });

    py::class_<BookSnapshot>(m, "BookSnapshot")
        .def(py::init<>())
        .def_readonly("bid_count", &BookSnapshot::bid_count)
        .def_readonly("ask_count", &BookSnapshot::ask_count)
        .def_readonly("sequence", &BookSnapshot::sequence)
        .def_readonly("bbo", &BookSnapshot::bbo)
        .def_property_readonly("bids", [](const BookSnapshot& snapshot) {
            return snapshot_levels(snapshot.bids, snapshot.bid_count);
        })
        .def_property_readonly("asks", [](const BookSnapshot& snapshot) {
            return snapshot_levels(snapshot.asks, snapshot.ask_count);
        });

    py::class_<TradeTick>(m, "TradeTick")
        .def(py::init<>())
        .def_readwrite("price", &TradeTick::price)
        .def_readwrite("quantity", &TradeTick::quantity)
        .def_readwrite("exchange_ts", &TradeTick::exchange_ts)
        .def_readwrite("recv_ts", &TradeTick::recv_ts)
        .def_readwrite("is_buyer_maker", &TradeTick::is_buyer_maker)
        .def_readwrite("aggressor", &TradeTick::aggressor);

    py::class_<DepthUpdate>(m, "DepthUpdate")
        .def(py::init([](const std::vector<std::pair<std::string, std::string>>& bids,
                         const std::vector<std::pair<std::string, std::string>>& asks,
                         std::uint64_t exchange_ts, std::uint64_t first_update_id,
                         std::uint64_t final_update_id,
                         std::uint64_t prev_final_update_id) {
                 DepthUpdate update;
                 update.exchange_ts = exchange_ts;
                 update.first_update_id = first_update_id;
                 update.final_update_id = final_update_id;
                 update.prev_final_update_id = prev_final_update_id;
                 update.bid_count = fill_levels(bids, update.bids.data());
                 update.ask_count = fill_levels(asks, update.asks.data());
                 return update;
             }),
             py::arg("bids") = std::vector<std::pair<std::string, std::string>>{},
             py::arg("asks") = std::vector<std::pair<std::string, std::string>>{},
             py::arg("exchange_ts") = 0, py::arg("first_update_id") = 0,
             py::arg("final_update_id") = 0, py::arg("prev_final_update_id") = 0)
        .def_property_readonly("bids", [](const DepthUpdate& update) {
            return level_pairs(update.bids, update.bid_count);
        })
        .def_property_readonly("asks", [](const DepthUpdate& update) {
            return level_pairs(update.asks, update.ask_count);
        })
        .def_readwrite("exchange_ts", &DepthUpdate::exchange_ts)
        .def_readwrite("recv_ts", &DepthUpdate::recv_ts)
        .def_readwrite("first_update_id", &DepthUpdate::first_update_id)
        .def_readwrite("final_update_id", &DepthUpdate::final_update_id)
        .def_readwrite("prev_final_update_id", &DepthUpdate::prev_final_update_id);

    py::class_<OrderRequest>(m, "OrderRequest")
        .def(py::init<>())
        .def_readwrite("client_id", &OrderRequest::client_id)
        .def_readwrite("price", &OrderRequest::price)
        .def_readwrite("quantity", &OrderRequest::quantity)
        .def_readwrite("timestamp", &OrderRequest::timestamp)
        .def_readwrite("side", &OrderRequest::side)
        .def_readwrite("type", &OrderRequest::type)
        .def_readwrite("time_in_force", &OrderRequest::time_in_force)
        .def("__repr__", [](const OrderRequest& request) {
            return "OrderRequest(id=" + std::to_string(request.client_id) + ", " +
                   (request.side == Side::Buy ? "BUY " : "SELL ") +
                   fixed_to_string(request.quantity) + " @ " +
                   fixed_to_string(request.price) + ")";
        });

    py::class_<Fill>(m, "Fill")
        .def(py::init<>())
        .def_readonly("order_id", &Fill::order_id)
        .def_readonly("price", &Fill::price)
        .def_readonly("quantity", &Fill::quantity)
        .def_readonly("fee", &Fill::fee)
        .def_readonly("timestamp", &Fill::timestamp)
        .def_readonly("side", &Fill::side)
        .def_readonly("is_maker", &Fill::is_maker);

    py::class_<OrderAck>(m, "OrderAck")
        .def(py::init<>())
        .def_readonly("order_id", &OrderAck::order_id)
        .def_readonly("reason", &OrderAck::reason)
        .def_readonly("avg_price", &OrderAck::avg_price)
        .def_readonly("filled", &OrderAck::filled)
        .def("accepted", &OrderAck::accepted);

    py::class_<PositionSnapshot>(m, "PositionSnapshot")
        .def(py::init<>())
        .def_readonly("net_quantity", &PositionSnapshot::net_quantity)
        .def_readonly("average_entry_price", &PositionSnapshot::average_entry_price)
        .def_readonly("realized_pnl", &PositionSnapshot::realized_pnl)
        .def_readonly("unrealized_pnl", &PositionSnapshot::unrealized_pnl)
        .def("__repr__", [](const PositionSnapshot& position) {
            return "PositionSnapshot(net=" + fixed_to_string(position.net_quantity) +
                   ", avg_entry=" + fixed_to_string(position.average_entry_price) +
                   ", realized=" + fixed_to_string(position.realized_pnl) +
                   ", unrealized=" + fixed_to_string(position.unrealized_pnl) + ")";
        });

    py::class_<RiskLimits>(m, "RiskLimits")
        .def(py::init<>())
        .def_readwrite("max_order_quantity", &RiskLimits::max_order_quantity)
        .def_readwrite("max_order_notional", &RiskLimits::max_order_notional)
        .def_readwrite("max_position_notional", &RiskLimits::max_position_notional)
        .def_readwrite("max_daily_loss", &RiskLimits::max_daily_loss)
        .def_readwrite("max_price_deviation", &RiskLimits::max_price_deviation)
        .def_readwrite("max_open_orders", &RiskLimits::max_open_orders);

    // ---- pre-trade risk manager -----------------------------------------
    py::class_<TokenBucket>(m, "TokenBucket")
        .def("enabled", &TokenBucket::enabled)
        .def("rate_per_sec", &TokenBucket::rate_per_sec)
        .def("burst", &TokenBucket::burst)
        .def("level", &TokenBucket::level, py::arg("now_ns"));

    py::class_<RiskManagerConfig>(m, "RiskManagerConfig")
        .def(py::init<>())
        .def_readwrite("enabled", &RiskManagerConfig::enabled)
        .def_readwrite("max_position", &RiskManagerConfig::max_position)
        .def_readwrite("max_order_size", &RiskManagerConfig::max_order_size)
        .def_readwrite("max_price_deviation_pct",
                       &RiskManagerConfig::max_price_deviation_pct)
        .def_readwrite("order_rate_limit", &RiskManagerConfig::order_rate_limit)
        .def_readwrite("order_burst", &RiskManagerConfig::order_burst)
        .def_static("disabled", &RiskManagerConfig::disabled)
        .def("__repr__", [](const RiskManagerConfig& config) {
            return "RiskManagerConfig(enabled=" + std::string(config.enabled ? "True" : "False") +
                   ", max_position=" + std::to_string(config.max_position) +
                   ", max_order_size=" + std::to_string(config.max_order_size) +
                   ", max_price_deviation_pct=" +
                   std::to_string(config.max_price_deviation_pct) +
                   ", order_rate_limit=" + std::to_string(config.order_rate_limit) +
                   ", order_burst=" + std::to_string(config.order_burst) + ")";
        });

    py::class_<RiskManagerStats>(m, "RiskManagerStats")
        .def(py::init<>())
        .def_readonly("evaluated", &RiskManagerStats::evaluated)
        .def_readonly("approved", &RiskManagerStats::approved)
        .def_readonly("rejected", &RiskManagerStats::rejected)
        .def_readonly("by_status", &RiskManagerStats::by_status)
        .def_readonly("tokens_available", &RiskManagerStats::tokens_available)
        .def("count", &RiskManagerStats::count, py::arg("status"))
        .def("__repr__", [](const RiskManagerStats& stats) {
            return "RiskManagerStats(evaluated=" + std::to_string(stats.evaluated) +
                   ", approved=" + std::to_string(stats.approved) +
                   ", rejected=" + std::to_string(stats.rejected) +
                   ", tokens=" + std::to_string(stats.tokens_available) + ")";
        });

    py::class_<RiskManager>(m, "RiskManager")
        .def(py::init<>())
        .def(py::init<const RiskManagerConfig&>(), py::arg("config"))
        .def("configure", &RiskManager::configure, py::arg("config"))
        .def("config", &RiskManager::config)
        .def_property("enabled", &RiskManager::enabled, &RiskManager::set_enabled)
        .def_property("max_position", &RiskManager::max_position,
                      &RiskManager::set_max_position)
        .def_property("max_order_size", &RiskManager::max_order_size,
                      &RiskManager::set_max_order_size)
        .def_property("max_price_deviation_pct",
                      &RiskManager::max_price_deviation_pct,
                      &RiskManager::set_max_price_deviation_pct)
        .def("set_order_rate_limit", &RiskManager::set_order_rate_limit,
             py::arg("rate_per_sec"), py::arg("burst") = 1.0)
        .def("validate_order", &RiskManager::validate_order, py::arg("symbol"),
             py::arg("price"), py::arg("quantity"), py::arg("side"),
             py::arg("current_position"), py::arg("current_mid_price"))
        .def("validate_order_at", &RiskManager::validate_order_at, py::arg("now_ns"),
             py::arg("symbol"), py::arg("price"), py::arg("quantity"), py::arg("side"),
             py::arg("current_position"), py::arg("current_mid_price"))
        .def("tokens_available", [](const RiskManager& risk) {
            return risk.tokens_available();
        })
        .def("tokens_available",
             [](const RiskManager& risk, Nanos now_ns) {
                 return risk.tokens_available(now_ns);
             },
             py::arg("now_ns"))
        .def("stats", [](const RiskManager& risk) { return risk.stats(); })
        .def("stats", [](const RiskManager& risk, Nanos now_ns) { return risk.stats(now_ns); },
             py::arg("now_ns"))
        .def("reset", &RiskManager::reset, py::arg("now_ns"))
        .def("rate_limiter", &RiskManager::rate_limiter,
             py::return_value_policy::reference_internal);

    m.def("risk_status_name", &risk_status_name, py::arg("status"));
    m.def("is_approved", &is_approved, py::arg("status"));

    // ---- emergency kill switch -------------------------------------------
    py::class_<KillSwitchConfig>(m, "KillSwitchConfig")
        .def(py::init<>())
        .def_readwrite("enabled", &KillSwitchConfig::enabled)
        .def_readwrite("max_drawdown_limit", &KillSwitchConfig::max_drawdown_limit)
        .def_readwrite("max_consecutive_rejects",
                       &KillSwitchConfig::max_consecutive_rejects)
        .def("__repr__", [](const KillSwitchConfig& config) {
            return "KillSwitchConfig(enabled=" +
                   std::string(config.enabled ? "True" : "False") +
                   ", max_drawdown_limit=" + std::to_string(config.max_drawdown_limit) +
                   ", max_consecutive_rejects=" +
                   std::to_string(config.max_consecutive_rejects) + ")";
        });

    py::class_<KillSwitchStats>(m, "KillSwitchStats")
        .def(py::init<>())
        .def_readonly("trip_count", &KillSwitchStats::trip_count)
        .def_readonly("last_trip_timestamp_ns", &KillSwitchStats::last_trip_timestamp_ns)
        .def_readonly("last_trip_reason", &KillSwitchStats::last_trip_reason)
        .def_readonly("state", &KillSwitchStats::state)
        .def_readonly("consecutive_rejects", &KillSwitchStats::consecutive_rejects)
        .def_readonly("blocked_orders", &KillSwitchStats::blocked_orders)
        .def_readonly("reset_count", &KillSwitchStats::reset_count)
        .def("__repr__", [](const KillSwitchStats& stats) {
            return "KillSwitchStats(state=" +
                   std::string(system_state_name(stats.state)) +
                   ", trip_count=" + std::to_string(stats.trip_count) +
                   ", last_trip_reason=" + stats.last_trip_reason + ")";
        });

    py::class_<KillSwitch>(m, "KillSwitch")
        .def(py::init<>())
        .def(py::init<const KillSwitchConfig&>(), py::arg("config"))
        .def("is_tripped", &KillSwitch::is_tripped)
        .def("get_state", &KillSwitch::get_state)
        .def("check_order_allowed", &KillSwitch::check_order_allowed)
        .def("trip", &KillSwitch::trip, py::arg("reason") = "manual")
        .def("reset", &KillSwitch::reset)
        .def("begin_recovery", &KillSwitch::begin_recovery)
        .def("get_stats", &KillSwitch::get_stats)
        .def("check_drawdown", &KillSwitch::check_drawdown, py::arg("drawdown"))
        .def("record_order_rejected", &KillSwitch::record_order_rejected)
        .def("record_order_accepted", &KillSwitch::record_order_accepted)
        .def("config", &KillSwitch::config)
        .def("set_config", &KillSwitch::set_config, py::arg("config"))
        .def_property("enabled", &KillSwitch::enabled, &KillSwitch::set_enabled)
        // Python callbacks acquire the GIL themselves: a trip can be raised
        // from any C++/worker thread that does not hold it.
        .def("set_trip_hook", [](KillSwitch& self, py::function callback) {
            self.set_trip_hook([callback](SystemState state, std::string_view reason) {
                py::gil_scoped_acquire gil;
                callback(py::str(system_state_name(state)),
                         py::str(reason.data(), reason.size()));
            });
        }, py::arg("callback"))
        .def("set_cancel_all_hook", [](KillSwitch& self, py::function callback) {
            self.set_cancel_all_hook([callback]() {
                py::gil_scoped_acquire gil;
                callback();
            });
        }, py::arg("callback"))
        .def("set_flatten_hook", [](KillSwitch& self, py::function callback) {
            self.set_flatten_hook([callback]() {
                py::gil_scoped_acquire gil;
                callback();
            });
        }, py::arg("callback"))
        .def("clear_hooks", &KillSwitch::clear_hooks)
        .def("__repr__", [](const KillSwitch& self) {
            return "KillSwitch(state=" +
                   std::string(system_state_name(self.get_state())) + ")";
        });

    m.def("system_state_name", &system_state_name, py::arg("state"));

    // ---- feed dead-man's switch ------------------------------------------
    py::class_<HeartbeatConfig>(m, "HeartbeatConfig")
        .def(py::init<>())
        .def_readwrite("timeout_ns", &HeartbeatConfig::timeout_ns)
        .def_property_readonly("expiry_enabled", &HeartbeatConfig::expiry_enabled)
        .def("__repr__", [](const HeartbeatConfig& config) {
            return "HeartbeatConfig(timeout_ns=" + std::to_string(config.timeout_ns) +
                   ", expiry_enabled=" + (config.expiry_enabled() ? "True" : "False") + ")";
        });

    py::class_<HeartbeatStats>(m, "HeartbeatStats")
        .def(py::init<>())
        .def_readonly("beats", &HeartbeatStats::beats)
        .def_readonly("timeouts", &HeartbeatStats::timeouts)
        .def_readonly("last_beat_ns", &HeartbeatStats::last_beat_ns)
        .def_readonly("silence_ns", &HeartbeatStats::silence_ns)
        .def_readonly("max_silence_ns", &HeartbeatStats::max_silence_ns)
        .def_readonly("timeout_ns", &HeartbeatStats::timeout_ns)
        .def_readonly("expired", &HeartbeatStats::expired)
        .def_readonly("armed", &HeartbeatStats::armed)
        .def_readonly("state", &HeartbeatStats::state)
        .def("__repr__", [](const HeartbeatStats& stats) {
            return "HeartbeatStats(beats=" + std::to_string(stats.beats) +
                   ", timeouts=" + std::to_string(stats.timeouts) +
                   ", silence_ms=" + std::to_string(stats.silence_ns / 1'000'000) +
                   ", expired=" + (stats.expired ? "True" : "False") + ")";
        });

    py::class_<HeartbeatMonitor>(m, "HeartbeatMonitor")
        .def(py::init<>())
        .def(py::init<const HeartbeatConfig&>(), py::arg("config"))
        .def_static("now_ns", &HeartbeatMonitor::now_ns)
        // `0` means "sample the monotonic clock"; tests pass explicit stamps.
        .def("beat", [](HeartbeatMonitor& self) { self.beat(); })
        .def("beat", [](HeartbeatMonitor& self, std::int64_t timestamp_ns) {
            self.beat(timestamp_ns);
        }, py::arg("timestamp_ns"))
        .def("last_beat_ns", &HeartbeatMonitor::last_beat_ns)
        .def("silence_ns", [](const HeartbeatMonitor& self) { return self.silence_ns(); })
        .def("silence_ns", [](const HeartbeatMonitor& self, std::int64_t now) {
            return self.silence_ns(now);
        }, py::arg("now_ns"))
        .def("is_expired", [](const HeartbeatMonitor& self) { return self.is_expired(); })
        .def("is_expired", [](const HeartbeatMonitor& self, std::int64_t now) {
            return self.is_expired(now);
        }, py::arg("now_ns"))
        .def("poll", [](HeartbeatMonitor& self) { return self.poll(); })
        .def("poll", [](HeartbeatMonitor& self, std::int64_t now) {
            return self.poll(now);
        }, py::arg("now_ns"))
        .def("stats", [](const HeartbeatMonitor& self) { return self.stats(); })
        .def("stats", [](const HeartbeatMonitor& self, std::int64_t now) {
            return self.stats(now);
        }, py::arg("now_ns"))
        .def("arm", &HeartbeatMonitor::arm, py::arg("kill_switch"),
             py::keep_alive<1, 2>())
        .def("detach", &HeartbeatMonitor::detach)
        .def("armed", &HeartbeatMonitor::armed)
        .def("timeout_ns", &HeartbeatMonitor::timeout_ns)
        .def("set_timeout_ns", &HeartbeatMonitor::set_timeout_ns, py::arg("timeout_ns"))
        .def("reset", [](HeartbeatMonitor& self) { self.reset(); })
        .def("reset", [](HeartbeatMonitor& self, std::int64_t now) {
            self.reset(now);
        }, py::arg("now_ns"))
        .def("__repr__", [](const HeartbeatMonitor& self) {
            return "HeartbeatMonitor(armed=" + std::string(self.armed() ? "True" : "False") +
                   ", timeout_ms=" + std::to_string(self.timeout_ns() / 1'000'000) + ")";
        });

    py::class_<EngineConfig>(m, "EngineConfig")
        .def(py::init<>())
        .def_readwrite("tick_size", &EngineConfig::tick_size)
        .def_readwrite("center", &EngineConfig::center)
        .def_readwrite("stale_after_ns", &EngineConfig::stale_after_ns)
        .def_readwrite("risk", &EngineConfig::risk)
        .def_readwrite("risk_manager", &EngineConfig::risk_manager);

    py::class_<EngineMetrics>(m, "EngineMetrics")
        .def(py::init<>())
        .def_readonly("depth_events", &EngineMetrics::depth_events)
        .def_readonly("trade_events", &EngineMetrics::trade_events)
        .def_readonly("book_updates", &EngineMetrics::book_updates)
        .def_readonly("orders_submitted", &EngineMetrics::orders_submitted)
        .def_readonly("orders_rejected", &EngineMetrics::orders_rejected)
        .def_readonly("orders_cancelled", &EngineMetrics::orders_cancelled)
        .def_readonly("fills", &EngineMetrics::fills)
        .def_readonly("filled_quantity", &EngineMetrics::filled_quantity)
        .def_readonly("traded_notional", &EngineMetrics::traded_notional)
        .def_readonly("last_update_ts", &EngineMetrics::last_update_ts)
        .def_readonly("p50_ns", &EngineMetrics::p50_ns)
        .def_readonly("p99_ns", &EngineMetrics::p99_ns)
        .def_readonly("p999_ns", &EngineMetrics::p999_ns)
        .def_readonly("open_orders", &EngineMetrics::open_orders);

    py::class_<OrderView>(m, "OrderView")
        .def(py::init<>())
        .def_readonly("order_id", &OrderView::order_id)
        .def_readonly("price", &OrderView::price)
        .def_readonly("quantity", &OrderView::quantity)
        .def_readonly("filled", &OrderView::filled)
        .def_readonly("created_ts", &OrderView::created_ts)
        .def_readonly("updated_ts", &OrderView::updated_ts)
        .def_readonly("side", &OrderView::side)
        .def_readonly("status", &OrderView::status)
        .def_readonly("type", &OrderView::type)
        .def("remaining", &OrderView::remaining);

    py::class_<L2Book>(m, "L2Book")
        .def(py::init<>())
        .def(py::init<Price, Price>(), py::arg("tick_size"), py::arg("center"))
        .def("configure", &L2Book::configure, py::arg("tick_size"), py::arg("center"))
        .def("apply_level", &L2Book::apply_level, py::arg("side"), py::arg("price"),
             py::arg("quantity"), py::arg("timestamp") = 0)
        .def("apply_delta", &L2Book::apply_delta, py::arg("side"), py::arg("price"),
             py::arg("delta"), py::arg("timestamp") = 0)
        .def("set_sequence", &L2Book::set_sequence, py::arg("sequence"),
             py::arg("exchange_ts") = 0, py::arg("recv_ts") = 0)
        .def("clear", &L2Book::clear)
        .def("top", &L2Book::top)
        .def("snapshot", [](const L2Book& book) {
            BookSnapshot snapshot;
            if (!book.snapshot(snapshot)) {
                throw std::runtime_error("book snapshot lost the seqlock race");
            }
            return snapshot;
        })
        .def("depth", [](const L2Book& book, Side side, std::size_t levels) {
            std::vector<BookLevel> out(levels);
            const std::size_t count = book.depth(side, out.data(), levels);
            out.resize(count);
            return out;
        }, py::arg("side"), py::arg("levels") = 10)
        .def("best_bid", &L2Book::best_bid)
        .def("best_ask", &L2Book::best_ask)
        .def("live_levels", &L2Book::live_levels, py::arg("side"))
        .def("updates", &L2Book::updates)
        .def("last_update_ts", &L2Book::last_update_ts)
        .def("tick_size", &L2Book::tick_size)
        .def("center_price", &L2Book::center_price);

    py::class_<L3Book>(m, "L3Book")
        .def(py::init<>())
        .def(py::init<Price, Price>(), py::arg("tick_size"), py::arg("center"))
        .def("configure", &L3Book::configure, py::arg("tick_size"), py::arg("center"))
        .def("add_order", &L3Book::add_order, py::arg("order_id"), py::arg("side"),
             py::arg("price"), py::arg("quantity"), py::arg("timestamp") = 0)
        .def("cancel_order", &L3Book::cancel_order, py::arg("order_id"),
             py::arg("quantity") = Quantity{})
        .def("execute_order", &L3Book::execute_order, py::arg("order_id"),
             py::arg("quantity"), py::arg("timestamp") = 0)
        .def("find", &L3Book::find, py::arg("order_id"))
        .def("top", &L3Book::top)
        .def("snapshot", [](const L3Book& book) {
            BookSnapshot snapshot;
            if (!book.snapshot(snapshot)) {
                throw std::runtime_error("book snapshot lost the seqlock race");
            }
            return snapshot;
        })
        .def("depth", [](const L3Book& book, Side side, std::size_t levels) {
            std::vector<BookLevel> out(levels);
            const std::size_t count = book.depth(side, out.data(), levels);
            out.resize(count);
            return out;
        }, py::arg("side"), py::arg("levels") = 10)
        .def("best_bid", &L3Book::best_bid)
        .def("best_ask", &L3Book::best_ask)
        .def("open_orders", &L3Book::open_orders)
        .def("aggregate_updates", &L3Book::aggregate_updates);

    py::class_<TradingEngine>(m, "TradingEngine")
        .def(py::init<const EngineConfig&>(), py::arg("config") = EngineConfig{})
        .def("reset", &TradingEngine::reset)
        .def("publish_depth", &TradingEngine::publish_depth, py::arg("update"),
             py::arg("recv_ts") = 0)
        .def("publish_trade", &TradingEngine::publish_trade, py::arg("trade"),
             py::arg("recv_ts") = 0)
        .def("poll", &TradingEngine::poll, py::arg("max_events") = TradingEngine::kInboundCapacity)
        .def("apply_depth", &TradingEngine::apply_depth, py::arg("update"))
        .def("apply_trade", &TradingEngine::apply_trade, py::arg("trade"))
        .def("submit", &TradingEngine::submit, py::arg("request"))
        .def("cancel", &TradingEngine::cancel, py::arg("order_id"), py::arg("timestamp") = 0)
        .def("cancel_all", &TradingEngine::cancel_all, py::arg("timestamp") = 0)
        .def("drain_fill", [](TradingEngine& engine) -> std::optional<Fill> {
            Fill fill;
            if (!engine.drain_fill(fill)) {
                return std::nullopt;
            }
            return fill;
        })
        .def("bbo", &TradingEngine::bbo)
        .def("mark_price", &TradingEngine::mark_price)
        .def("last_trade", &TradingEngine::last_trade,
             py::return_value_policy::copy)
        .def("position", &TradingEngine::position, py::return_value_policy::reference_internal)
        .def("book", &TradingEngine::book, py::return_value_policy::reference_internal)
        .def("metrics", &TradingEngine::metrics)
        .def("pending_events", &TradingEngine::pending_events)
        .def("pending_fills", &TradingEngine::pending_fills)
        .def("set_risk_limits", &TradingEngine::set_risk_limits, py::arg("limits"))
        .def("risk_limits", &TradingEngine::risk_limits,
             py::return_value_policy::copy)
        .def("set_risk_manager_config", &TradingEngine::set_risk_manager_config,
             py::arg("config"))
        .def("risk_manager_config", &TradingEngine::risk_manager_config)
        .def("risk_manager",
             static_cast<RiskManager& (TradingEngine::*)() noexcept>(
                 &TradingEngine::risk_manager),
             py::return_value_policy::reference_internal)
        .def("risk_stats", &TradingEngine::risk_stats, py::arg("now") = 0)
        .def("is_stale", &TradingEngine::is_stale, py::arg("now_ns"))
        .def("config", &TradingEngine::config,
             py::return_value_policy::copy)
        .def_property_readonly_static("INBOUND_CAPACITY",
                                      [](py::object) { return TradingEngine::kInboundCapacity; });

    // ---- feed parsing ----------------------------------------------------
    m.def("parse_depth_json", [](const std::string& json) {
        DepthUpdate update;
        if (!JsonFeedParser::parse_depth_update(json, update)) {
            throw std::runtime_error("malformed depth update JSON");
        }
        update.symbol = {};  // would dangle once `json` goes out of scope
        return update;
    }, py::arg("json"));

    m.def("parse_trade_json", [](const std::string& json) {
        TradeTick trade;
        if (!JsonFeedParser::parse_trade(json, trade)) {
            throw std::runtime_error("malformed trade JSON");
        }
        return trade;
    }, py::arg("json"));

    m.def("classify_json", [](const std::string& json) {
        return JsonFeedParser::classify(json);
    }, py::arg("json"));

    m.def("decode_ws_frame", [](const py::bytes& frame_bytes) {
        const std::string frame = frame_bytes;
        const auto* data = reinterpret_cast<const std::uint8_t*>(frame.data());
        std::vector<std::uint8_t> scratch(WebSocketFrameDecoder::kMaxPayload);
        WsFrameView view;
        const std::ptrdiff_t consumed = WebSocketFrameDecoder::decode(
            std::span<const std::uint8_t>(data, frame.size()), view, scratch.data(),
            scratch.size());
        if (consumed < 0) {
            throw std::runtime_error("WebSocket protocol error");
        }
        py::bytes payload;
        if (consumed > 0) {
            payload = py::bytes(reinterpret_cast<const char*>(view.payload),
                                view.payload_length);
        }
        return py::make_tuple(static_cast<std::int64_t>(consumed),
                              static_cast<int>(view.opcode), payload, view.fin, view.masked);
    }, py::arg("frame"));

    // ---- diagnostics -----------------------------------------------------
    m.def("ring_buffer_smoke", [](std::size_t messages) {
        SpscRingBuffer<std::uint64_t, 1024> ring;
        std::uint64_t consumed = 0;
        for (std::size_t i = 0; i < messages; ++i) {
            const std::uint64_t value = i + 1;
            if (!ring.try_push(value)) {
                return false;
            }
            std::uint64_t out = 0;
            if (!ring.try_pop(out) || out != value) {
                return false;
            }
            ++consumed;
        }
        return consumed == messages && ring.empty();
    }, py::arg("messages") = 100000);

    m.def("footprint", []() {
        return py::make_tuple(sizeof(TradingEngine), sizeof(EngineEvent),
                              sizeof(DepthUpdate), sizeof(L2Book), sizeof(L3Book));
    });

    py::class_<paper_account_snapshot>(m, "PaperAccountSnapshot")
        .def(py::init<>())
        .def_readonly("cash", &paper_account_snapshot::cash)
        .def_readonly("fees_paid", &paper_account_snapshot::fees_paid)
        .def_readonly("realized_pnl", &paper_account_snapshot::realized_pnl)
        .def_readonly("unrealized_pnl", &paper_account_snapshot::unrealized_pnl)
        .def_readonly("gross_notional", &paper_account_snapshot::gross_notional)
        .def_readonly("equity", &paper_account_snapshot::equity)
        .def_readonly("net_quantity", &paper_account_snapshot::net_quantity)
        .def_readonly("average_entry_price",
                      &paper_account_snapshot::average_entry_price)
        .def_readonly("fill_count", &paper_account_snapshot::fill_count)
        .def_readonly("maker_fills", &paper_account_snapshot::maker_fills)
        .def_readonly("taker_fills", &paper_account_snapshot::taker_fills);

    py::class_<paper_account>(m, "PaperAccount")
        .def("cash", &paper_account::cash)
        .def("fees_paid", &paper_account::fees_paid)
        .def("realized_pnl", &paper_account::realized_pnl)
        .def("unrealized_pnl", &paper_account::unrealized_pnl)
        .def("equity", &paper_account::equity)
        .def("net_quantity", &paper_account::net_quantity)
        .def("average_entry_price", &paper_account::average_entry_price)
        .def("maker_bps", &paper_account::maker_bps,
             "effective maker rate in bps (fractional values preserved)")
        .def("taker_bps", &paper_account::taker_bps,
             "effective taker rate in bps (fractional values preserved)")
        .def("snapshot", &paper_account::snapshot);

    py::class_<match_leg>(m, "MatchLeg")
        .def(py::init<>())
        .def_readonly("price", &match_leg::price)
        .def_readonly("quantity", &match_leg::quantity);

    py::class_<match_result>(m, "MatchResult")
        .def(py::init<>())
        .def_readonly("filled", &match_result::filled)
        .def_readonly("remaining", &match_result::remaining)
        .def_readonly("average_price", &match_result::average_price)
        .def_readonly("notional", &match_result::notional)
        .def_readonly("slippage", &match_result::slippage)
        .def_readonly("touch_price", &match_result::touch_price)
        .def_readonly("leg_count", &match_result::leg_count)
        .def_property_readonly("legs", [](const match_result& result) {
            return std::vector<match_leg>(result.legs.begin(),
                                          result.legs.begin() + result.leg_count);
        });

    py::class_<paper_ack>(m, "PaperAck")
        .def(py::init<>())
        .def_readonly("order_id", &paper_ack::order_id)
        .def_readonly("reason", &paper_ack::reason)
        .def_readonly("average_price", &paper_ack::average_price)
        .def_readonly("filled", &paper_ack::filled)
        .def_readonly("slippage", &paper_ack::slippage)
        .def("accepted", &paper_ack::accepted);

    py::class_<paper_metrics>(m, "PaperMetrics")
        .def(py::init<>())
        .def_readonly("depth_events", &paper_metrics::depth_events)
        .def_readonly("gap_events", &paper_metrics::gap_events)
        .def_readonly("orders_submitted", &paper_metrics::orders_submitted)
        .def_readonly("orders_rejected", &paper_metrics::orders_rejected)
        .def_readonly("orders_cancelled", &paper_metrics::orders_cancelled)
        .def_readonly("fills", &paper_metrics::fills)
        .def_readonly("filled_quantity", &paper_metrics::filled_quantity)
        .def_readonly("traded_notional", &paper_metrics::traded_notional)
        .def_readonly("slippage_total", &paper_metrics::slippage_total)
        .def_readonly("p50_ns", &paper_metrics::p50_ns)
        .def_readonly("p99_ns", &paper_metrics::p99_ns)
        .def_readonly("p999_ns", &paper_metrics::p999_ns)
        .def_readonly("open_orders", &paper_metrics::open_orders)
        .def_readonly("last_update_ts", &paper_metrics::last_update_ts)
        .def_readonly("last_frame_latency_ns", &paper_metrics::last_frame_latency_ns)
        .def_readonly("account", &paper_metrics::account);

    py::class_<paper_engine>(m, "PaperEngine")
        .def(py::init<Price, Price, double, double, Notional>(),
             py::arg("tick_size"), py::arg("center"), py::arg("maker_bps") = 0.0,
             py::arg("taker_bps") = 5.0, py::arg("initial_cash") = Notional{})
        .def("reset", &paper_engine::reset)
        .def("process_update", &paper_engine::process_update, py::arg("update"))
        .def("submit", &paper_engine::submit, py::arg("request"))
        .def("cancel", &paper_engine::cancel, py::arg("order_id"),
             py::arg("timestamp") = 0)
        .def("cancel_all", &paper_engine::cancel_all, py::arg("timestamp") = 0)
        .def("drain_fill", [](paper_engine& engine) -> std::optional<Fill> {
            Fill fill;
            if (!engine.drain_fill(fill)) {
                return std::nullopt;
            }
            return fill;
        })
        .def("bbo", &paper_engine::bbo)
        .def("mark_price", &paper_engine::mark_price)
        .def("position", &paper_engine::position, py::return_value_policy::copy)
        .def("account", &paper_engine::account,
             py::return_value_policy::reference_internal)
        .def("book", &paper_engine::book, py::return_value_policy::reference_internal)
        .def("metrics", &paper_engine::metrics)
        .def("pending_fills", &paper_engine::pending_fills)
        .def("tick_size", &paper_engine::tick_size)
        .def("center", &paper_engine::center)
        .def("last_slippage", &paper_engine::last_slippage);

    py::class_<ingest_stats>(m, "IngestStats")
        .def(py::init<>())
        .def_readonly("lines", &ingest_stats::lines)
        .def_readonly("depth_events", &ingest_stats::depth_events)
        .def_readonly("trade_events", &ingest_stats::trade_events)
        .def_readonly("malformed", &ingest_stats::malformed)
        .def_readonly("gaps", &ingest_stats::gaps);

    m.def("match_taker", [](const L2Book& book, Side side, Quantity quantity) {
        return match_taker(book, side, quantity);
    }, py::arg("book"), py::arg("side"), py::arg("quantity"));

    m.def("match_aggressive",
          [](const L2Book& book, Side side, Quantity quantity, Price limit_price) {
              return match_aggressive(book, side, quantity, limit_price);
          },
          py::arg("book"), py::arg("side"), py::arg("quantity"),
          py::arg("limit_price"));

    m.def("ingest_lines",
          [](paper_engine& engine, const std::vector<std::string>& lines) {
              return ingest_lines(engine, lines);
          },
          py::arg("engine"), py::arg("lines"));

    m.attr("__version__") = "0.2.0";
}





