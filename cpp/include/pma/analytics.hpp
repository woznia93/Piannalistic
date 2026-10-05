#pragma once
#include <algorithm>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <vector>

#include "pma/types.hpp"
#include "pma/vpin.hpp"

namespace pma {

// Streaming analytics over a rolling time window.
//
// Every windowed metric is a running sum that is updated when a trade enters
// the window and again when it expires, so on_trade/on_quote/snapshot are all
// O(1) amortised regardless of window length. Window entries live in a
// preallocated circular buffer, so the steady state does no heap allocation.
//
// Not thread-safe: drive it from a single thread (see SpscRingBuffer for
// handing events over from a network thread).
class AnalyticsEngine {
public:
    // vpin_bucket_volume <= 0 means auto-calibrate: once one full window of
    // trading has been seen, the bucket is set to a tenth of that window's
    // volume, so a bucket fills roughly every window/10 seconds.
    explicit AnalyticsEngine(double window_seconds = 60.0,
                             double vpin_bucket_volume = 0.0,
                             std::size_t vpin_num_buckets = 50,
                             std::size_t initial_capacity = 1 << 14)
        : window_(window_seconds),
          vpin_(vpin_bucket_volume, vpin_num_buckets),
          vpin_cfg_bucket_(vpin_bucket_volume),
          vpin_cfg_n_(vpin_num_buckets),
          buf_(round_up_pow2(initial_capacity)) {}

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
        push(e);

        if (total_trades_ == 0) first_trade_ts_ = t.ts;
        last_price_ = t.price;
        now_ = std::max(now_, t.ts);
        ++total_trades_;

        if (vpin_.has_bucket_volume()) {
            vpin_.add(t.size, t.side);
        } else {
            calib_volume_ += t.size;
            if (t.ts - first_trade_ts_ >= window_)
                vpin_.set_bucket_volume(calib_volume_ / 10.0);
        }
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
        s.trades_in_window = count_;
        s.vpin = vpin_.value();
        s.vpin_bucket_volume = vpin_.bucket_volume();
        s.vpin_buckets = vpin_.completed_buckets();

        if (has_quote_) {
            s.mid = 0.5 * (quote_.bid + quote_.ask);
            s.spread_bps = (quote_.ask - quote_.bid) / s.mid * 1e4;
            const double depth = quote_.bid_size + quote_.ask_size;
            if (depth > 0.0)
                s.quote_imbalance = (quote_.bid_size - quote_.ask_size) / depth;
        }

        // Running sums pick up tiny float error as entries are added and
        // removed, so guard the divisions and clamp where a bound is known.
        if (count_ > 0 && sum_size_ > 0.0) {
            s.volume = sum_size_;
            s.vwap = sum_notional_ / sum_size_;
            s.trade_imbalance = std::clamp(sum_signed_ / sum_size_, -1.0, 1.0);
            // Until a full window has elapsed, divide by the time actually seen.
            const double seen = std::clamp(now_ - first_trade_ts_, 1.0, window_);
            s.trade_rate = static_cast<double>(count_) / seen;
        }
        if (n_ret_ >= 2) {
            const double n = static_cast<double>(n_ret_);
            const double mean = sum_ret_ / n;
            const double var = (sum_ret_sq_ - n * mean * mean) / (n - 1.0);
            s.volatility = std::sqrt(std::max(var, 0.0));
        }
        return s;
    }

    void reset() {
        *this = AnalyticsEngine(window_, vpin_cfg_bucket_, vpin_cfg_n_, buf_.size());
    }

    double window_seconds() const { return window_; }
    std::size_t capacity() const { return buf_.size(); }

private:
    struct Entry {
        double ts = 0.0;
        double notional = 0.0;
        double size = 0.0;
        double signed_size = 0.0;
        double ret = 0.0;
        bool has_ret = false;
    };

    static std::size_t round_up_pow2(std::size_t n) {
        std::size_t p = 16;
        while (p < n) p <<= 1;
        return p;
    }

    void push(const Entry& e) {
        if (count_ == buf_.size()) grow();  // rare: window busier than capacity
        buf_[(head_ + count_) & (buf_.size() - 1)] = e;
        ++count_;
    }

    void grow() {
        std::vector<Entry> bigger(buf_.size() * 2);
        for (std::size_t i = 0; i < count_; ++i)
            bigger[i] = buf_[(head_ + i) & (buf_.size() - 1)];
        buf_.swap(bigger);
        head_ = 0;
    }

    void expire() {
        const double cutoff = now_ - window_;
        const std::size_t mask = buf_.size() - 1;
        while (count_ > 0 && buf_[head_].ts < cutoff) {
            const Entry& e = buf_[head_];
            sum_notional_ -= e.notional;
            sum_size_ -= e.size;
            sum_signed_ -= e.signed_size;
            if (e.has_ret) {
                sum_ret_ -= e.ret;
                sum_ret_sq_ -= e.ret * e.ret;
                --n_ret_;
            }
            head_ = (head_ + 1) & mask;
            --count_;
        }
        if (count_ == 0) {
            // Good moment to wipe accumulated float error.
            sum_notional_ = sum_size_ = sum_signed_ = 0.0;
            sum_ret_ = sum_ret_sq_ = 0.0;
            n_ret_ = 0;
        }
    }

    double window_;
    Vpin vpin_;
    double vpin_cfg_bucket_;
    std::size_t vpin_cfg_n_;

    std::vector<Entry> buf_;  // circular; size is a power of two
    std::size_t head_ = 0;
    std::size_t count_ = 0;

    double sum_notional_ = 0.0;
    double sum_size_ = 0.0;
    double sum_signed_ = 0.0;
    double sum_ret_ = 0.0;
    double sum_ret_sq_ = 0.0;
    std::uint64_t n_ret_ = 0;

    double last_price_ = 0.0;
    double now_ = 0.0;
    double first_trade_ts_ = 0.0;
    double calib_volume_ = 0.0;
    std::uint64_t total_trades_ = 0;
    Quote quote_{};
    bool has_quote_ = false;
};

}  // namespace pma
