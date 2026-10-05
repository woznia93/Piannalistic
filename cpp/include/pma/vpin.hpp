#pragma once
#include <algorithm>
#include <cmath>
#include <cstddef>
#include <vector>

#include "pma/types.hpp"

namespace pma {

// Volume-synchronised probability of informed trading (Easley, Lopez de Prado,
// O'Hara). Trades are poured into buckets of equal *volume* rather than equal
// time; each closed bucket records |buy volume - sell volume|, and
//
//     VPIN = sum(|buy - sell|) / (num_buckets * bucket_volume)
//
// over the most recent `num_buckets` buckets. 0 means perfectly two-sided
// flow, 1 means every bucket was entirely one-sided.
//
// Note: the original paper infers buy/sell split from price changes ("bulk
// volume classification"). This uses the exchange-reported aggressor side,
// which is exact when the feed provides it.
class Vpin {
public:
    explicit Vpin(double bucket_volume = 0.0, std::size_t num_buckets = 50)
        : bucket_(std::max(bucket_volume, 0.0)),
          ring_(std::max<std::size_t>(num_buckets, 1), 0.0) {}

    bool has_bucket_volume() const { return bucket_ > 0.0; }
    double bucket_volume() const { return bucket_; }
    std::size_t completed_buckets() const { return count_; }

    // Set once (e.g. after auto-calibration). Ignored if already set.
    void set_bucket_volume(double v) {
        if (bucket_ <= 0.0 && v > 0.0) bucket_ = v;
    }

    void add(double size, Side side) {
        if (bucket_ <= 0.0 || !(size > 0.0)) return;
        double remaining = size;

        // A single trade far larger than the whole lookback would otherwise
        // spin the loop below. Buckets beyond the last `n` would be overwritten
        // by identical ones anyway, so drop them up front (keeping one extra
        // to finish the bucket currently in progress). The cheap comparison
        // keeps the division and floor off the normal path.
        const double n = static_cast<double>(ring_.size());
        if (remaining > bucket_ * (2.0 * n + 2.0)) {
            const double whole = std::floor(remaining / bucket_);
            remaining -= (whole - n - 1.0) * bucket_;
        }

        // +1 / 0 / -1. Multiplying instead of branching on the side matters:
        // buy vs sell is close to a coin flip, so a branch here mispredicts
        // about half the time.
        const double sign = static_cast<double>(static_cast<int>(side));

        while (remaining > 0.0) {
            const double take = std::min(remaining, bucket_ - filled_);
            signed_ += sign * take;  // buy volume minus sell volume
            filled_ += take;
            remaining -= take;
            if (filled_ >= bucket_ * (1.0 - 1e-12)) close_bucket();
        }
    }

    double value() const {
        if (count_ == 0) return 0.0;
        const double v = sum_ / (static_cast<double>(count_) * bucket_);
        return std::clamp(v, 0.0, 1.0);
    }

private:
    void close_bucket() {
        const double imbalance = std::fabs(signed_);
        if (count_ == ring_.size()) sum_ -= ring_[next_];
        else ++count_;
        ring_[next_] = imbalance;
        sum_ += imbalance;
        next_ = (next_ + 1) % ring_.size();
        filled_ = signed_ = 0.0;
    }

    double bucket_;
    std::vector<double> ring_;  // imbalance of each completed bucket
    std::size_t next_ = 0;
    std::size_t count_ = 0;
    double sum_ = 0.0;
    double filled_ = 0.0, signed_ = 0.0;  // bucket in progress
};

}  // namespace pma
