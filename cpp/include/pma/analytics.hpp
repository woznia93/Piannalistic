#pragma once
#include <algorithm>
#include <cmath>
#include <cstdint>
#include <deque>

#include "pma/types.hpp"

namespace pma {

// Streaming analytics over a rolling time window.
//
// Every metric is kept as a running sum that is updated when a trade enters
// the window and again when it expires, so on_trade/on_quote/snapshot are all
// O(1) amortised regardless of window length.
//
// Not thread-safe: drive it from a single thread (see SpscRingBuffer for
// handing events over from a network thread).
class AnalyticsEngine {
public:
    explicit AnalyticsEngine(double window_seconds = 60.0)
        : window_(window_seconds) {}

    void on_trade(const Trade& t) {
        if (!(t.price > 0.0) || !(t.size > 0.0)) return;  // also drops NaNs

        Entry e;
        e.ts = t.ts;
        e.notional = t.price * t.size;
        e.size = t.size;
        e.signed_size = static_cast<double>(static_cast<int>(t.side)) * t.size;
        if (last_price_ > 0.0) {
            e.ret = std::log(t.price / last_price_);
            e.has_ret = true;
        }

        sum_notional_ += e.notional;
        sum_size_ += e.size;
        sum_signed_ += e.signed_size;
        if (e.has_ret) {
            sum_ret_ += e.ret;
            sum_ret_sq_ += e.ret * e.ret;
            ++n_ret_;
        }
        window_trades_.push_back(e);

        last_price_ = t.price;
        now_ = std::max(now_, t.ts);
        ++total_trades_;
        expire();
    }

    void on_quote(const Quote& q) {
        if (!(q.bid > 0.0) || !(q.ask > 0.0)) return;
        quote_ = q;
        has_quote_ = true;
        now_ = std::max(now_, q.ts);
        expire();
    }

    Snapshot snapshot() const {
        Snapshot s;
        s.ts = now_;
        s.last_price = last_price_;
        s.total_trades = total_trades_;
        s.trades_in_window = window_trades_.size();

        if (has_quote_) {
            s.mid = 0.5 * (quote_.bid + quote_.ask);
            s.spread_bps = (quote_.ask - quote_.bid) / s.mid * 1e4;
            const double depth = quote_.bid_size + quote_.ask_size;
            if (depth > 0.0)
                s.quote_imbalance = (quote_.bid_size - quote_.ask_size) / depth;
        }

        // Running sums pick up tiny float error as entries are added and
        // removed, so guard the divisions and clamp where a bound is known.
        if (!window_trades_.empty() && sum_size_ > 0.0) {
            s.volume = sum_size_;
            s.vwap = sum_notional_ / sum_size_;
            s.trade_imbalance = std::clamp(sum_signed_ / sum_size_, -1.0, 1.0);
        }
        if (n_ret_ >= 2) {
            const double n = static_cast<double>(n_ret_);
            const double mean = sum_ret_ / n;
            const double var = (sum_ret_sq_ - n * mean * mean) / (n - 1.0);
            s.volatility = std::sqrt(std::max(var, 0.0));
        }
        return s;
    }

    void reset() { *this = AnalyticsEngine(window_); }

    double window_seconds() const { return window_; }

private:
    struct Entry {
        double ts = 0.0;
        double notional = 0.0;
        double size = 0.0;
        double signed_size = 0.0;
        double ret = 0.0;
        bool has_ret = false;
    };

    void expire() {
        const double cutoff = now_ - window_;
        while (!window_trades_.empty() && window_trades_.front().ts < cutoff) {
            const Entry& e = window_trades_.front();
            sum_notional_ -= e.notional;
            sum_size_ -= e.size;
            sum_signed_ -= e.signed_size;
            if (e.has_ret) {
                sum_ret_ -= e.ret;
                sum_ret_sq_ -= e.ret * e.ret;
                --n_ret_;
            }
            window_trades_.pop_front();
        }
        if (window_trades_.empty()) {
            // Good moment to wipe accumulated float error.
            sum_notional_ = sum_size_ = sum_signed_ = 0.0;
            sum_ret_ = sum_ret_sq_ = 0.0;
            n_ret_ = 0;
        }
    }

    double window_;
    // TODO(perf): std::deque allocates in chunks. Replace with a preallocated
    // circular buffer to get allocation off the hot path, and measure the
    // difference with pma_bench.
    std::deque<Entry> window_trades_;

    double sum_notional_ = 0.0;
    double sum_size_ = 0.0;
    double sum_signed_ = 0.0;
    double sum_ret_ = 0.0;
    double sum_ret_sq_ = 0.0;
    std::uint64_t n_ret_ = 0;

    double last_price_ = 0.0;
    double now_ = 0.0;
    std::uint64_t total_trades_ = 0;
    Quote quote_{};
    bool has_quote_ = false;
};

}  // namespace pma
