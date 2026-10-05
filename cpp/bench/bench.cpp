// Pushes synthetic trades from a "network" thread through the SPSC queue to an
// analytics thread and reports throughput plus queue-to-processed latency.
//
//   ./build/pma_bench [num_messages] [messages_per_second]
//
// messages_per_second = 0 (default) means unthrottled: that measures peak
// throughput, and latency then mostly reflects queueing. Pass a rate (e.g.
// 50000) to measure per-message latency under a realistic load.
#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <random>
#include <thread>
#include <vector>

#include "pma/analytics.hpp"
#include "pma/ring_buffer.hpp"

using Clock = std::chrono::steady_clock;

struct Msg {
    pma::Trade trade;
    std::int64_t enqueue_ns;
};

static std::int64_t now_ns() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               Clock::now().time_since_epoch())
        .count();
}

int main(int argc, char** argv) {
    const std::size_t n = argc > 1 ? std::strtoull(argv[1], nullptr, 10) : 2'000'000;
    const double rate = argc > 2 ? std::atof(argv[2]) : 0.0;
    if (n == 0) {
        std::fprintf(stderr, "num_messages must be > 0\n");
        return 1;
    }

    // Pre-generate so the producer loop does nothing but timestamp and push.
    std::vector<pma::Trade> trades(n);
    std::mt19937_64 rng(42);
    std::normal_distribution<double> step(0.0, 0.0002);
    std::exponential_distribution<double> size(2.0);
    double price = 50'000.0;
    for (std::size_t i = 0; i < n; ++i) {
        price *= 1.0 + step(rng);
        trades[i].ts = static_cast<double>(i) * 1e-4;  // 10k trades per simulated second
        trades[i].price = price;
        trades[i].size = size(rng) + 1e-6;
        trades[i].side = (rng() & 1) ? pma::Side::Buy : pma::Side::Sell;
    }

    pma::SpscRingBuffer<Msg> queue(1 << 16);
    pma::AnalyticsEngine engine(60.0);
    std::vector<std::int64_t> latency(n);

    std::thread consumer([&] {
        Msg m;
        std::size_t done = 0;
        while (done < n) {
            if (queue.try_pop(m)) {
                engine.on_trade(m.trade);
                latency[done++] = now_ns() - m.enqueue_ns;
            }
        }
    });

    const std::int64_t gap_ns = rate > 0.0 ? static_cast<std::int64_t>(1e9 / rate) : 0;
    const std::int64_t start = now_ns();
    for (std::size_t i = 0; i < n; ++i) {
        if (gap_ns > 0) {
            const std::int64_t due = start + static_cast<std::int64_t>(i) * gap_ns;
            while (now_ns() < due) { /* busy-wait for precise pacing */ }
        }
        Msg m{trades[i], now_ns()};
        while (!queue.try_push(m)) { /* queue full: spin */ }
    }
    consumer.join();
    const double elapsed_s = static_cast<double>(now_ns() - start) / 1e9;

    std::sort(latency.begin(), latency.end());
    auto pct = [&](double p) {
        return latency[std::min(n - 1, static_cast<std::size_t>(p * static_cast<double>(n)))];
    };

    const pma::Snapshot s = engine.snapshot();
    std::printf("messages     : %zu\n", n);
    std::printf("throughput   : %.0f msgs/sec\n", static_cast<double>(n) / elapsed_s);
    std::printf("latency (ns) : p50=%lld  p99=%lld  p99.9=%lld  max=%lld\n",
                static_cast<long long>(pct(0.50)), static_cast<long long>(pct(0.99)),
                static_cast<long long>(pct(0.999)), static_cast<long long>(latency.back()));
    std::printf("final vwap   : %.2f over %llu trades in window\n", s.vwap,
                static_cast<unsigned long long>(s.trades_in_window));
    return 0;
}
