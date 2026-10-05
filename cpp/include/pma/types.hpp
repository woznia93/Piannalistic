#pragma once
#include <cstdint>

namespace pma {

// Aggressor side of a trade: who crossed the spread.
enum class Side : std::int8_t { Sell = -1, Unknown = 0, Buy = 1 };

struct Trade {
    double ts = 0.0;     // seconds since epoch (exchange time)
    double price = 0.0;
    double size = 0.0;
    Side side = Side::Unknown;
};

struct Quote {
    double ts = 0.0;
    double bid = 0.0;
    double ask = 0.0;
    double bid_size = 0.0;
    double ask_size = 0.0;
};

// Point-in-time view of every metric the engine maintains.
struct Snapshot {
    double ts = 0.0;
    double last_price = 0.0;
    double mid = 0.0;
    double spread_bps = 0.0;       // (ask - bid) / mid * 1e4
    double quote_imbalance = 0.0;  // (bid_sz - ask_sz) / (bid_sz + ask_sz), in [-1, 1]
    double vwap = 0.0;             // over the rolling window
    double volume = 0.0;           // over the rolling window
    double trade_imbalance = 0.0;  // signed volume / volume, in [-1, 1]
    double volatility = 0.0;       // stddev of trade-to-trade log returns in window
    std::uint64_t trades_in_window = 0;
    std::uint64_t total_trades = 0;
};

}  // namespace pma
