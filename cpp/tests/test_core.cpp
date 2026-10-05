// Minimal dependency-free tests. Run with `ctest` or ./build/pma_tests.
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <thread>

#include "pma/analytics.hpp"
#include "pma/ring_buffer.hpp"

static int g_failures = 0;

#define CHECK(cond)                                                        \
    do {                                                                   \
        if (!(cond)) {                                                     \
            std::printf("FAIL %s:%d  %s\n", __FILE__, __LINE__, #cond);    \
            ++g_failures;                                                  \
        }                                                                  \
    } while (0)

static bool near(double a, double b, double tol = 1e-9) {
    return std::fabs(a - b) <= tol;
}

using namespace pma;

static void test_vwap_and_imbalance() {
    AnalyticsEngine e(60.0);
    e.on_trade({1.0, 100.0, 2.0, Side::Buy});
    e.on_trade({2.0, 110.0, 1.0, Side::Sell});
    e.on_trade({3.0, 120.0, 1.0, Side::Buy});
    const Snapshot s = e.snapshot();
    CHECK(near(s.vwap, (200.0 + 110.0 + 120.0) / 4.0));
    CHECK(near(s.volume, 4.0));
    CHECK(near(s.trade_imbalance, (2.0 - 1.0 + 1.0) / 4.0));
    CHECK(s.trades_in_window == 3);
    CHECK(near(s.last_price, 120.0));
}

static void test_window_expiry() {
    AnalyticsEngine e(10.0);
    e.on_trade({0.0, 100.0, 5.0, Side::Buy});
    e.on_trade({5.0, 200.0, 1.0, Side::Sell});
    e.on_trade({20.0, 300.0, 2.0, Side::Sell});  // first two are now stale
    const Snapshot s = e.snapshot();
    CHECK(s.trades_in_window == 1);
    CHECK(s.total_trades == 3);
    CHECK(near(s.vwap, 300.0));
    CHECK(near(s.trade_imbalance, -1.0));

    // A quote alone advances the clock and can empty the window.
    e.on_quote({100.0, 299.0, 301.0, 1.0, 1.0});
    const Snapshot s2 = e.snapshot();
    CHECK(s2.trades_in_window == 0);
    CHECK(near(s2.volume, 0.0));
    CHECK(near(s2.vwap, 0.0));
}

static void test_volatility() {
    AnalyticsEngine e(60.0);
    const double prices[] = {100.0, 101.0, 99.0, 102.0};
    for (int i = 0; i < 4; ++i)
        e.on_trade({static_cast<double>(i), prices[i], 1.0, Side::Unknown});
    const double r[] = {std::log(101.0 / 100.0), std::log(99.0 / 101.0),
                        std::log(102.0 / 99.0)};
    const double mean = (r[0] + r[1] + r[2]) / 3.0;
    double var = 0.0;
    for (double x : r) var += (x - mean) * (x - mean);
    var /= 2.0;
    CHECK(near(e.snapshot().volatility, std::sqrt(var), 1e-12));
}

static void test_quote_metrics() {
    AnalyticsEngine e;
    e.on_quote({1.0, 99.0, 101.0, 3.0, 1.0});
    const Snapshot s = e.snapshot();
    CHECK(near(s.mid, 100.0));
    CHECK(near(s.spread_bps, 200.0));
    CHECK(near(s.quote_imbalance, 0.5));
}

static void test_bad_input_ignored() {
    AnalyticsEngine e;
    e.on_trade({1.0, 0.0, 1.0, Side::Buy});
    e.on_trade({1.0, 100.0, -1.0, Side::Buy});
    e.on_trade({1.0, std::nan(""), 1.0, Side::Buy});
    CHECK(e.snapshot().total_trades == 0);
}

static void test_ring_basic() {
    SpscRingBuffer<int> q(4);
    CHECK(q.capacity() == 4);
    int v = 0;
    CHECK(!q.try_pop(v));
    for (int i = 0; i < 4; ++i) CHECK(q.try_push(i));
    CHECK(!q.try_push(99));  // full
    for (int i = 0; i < 4; ++i) {
        CHECK(q.try_pop(v));
        CHECK(v == i);
    }
    CHECK(!q.try_pop(v));
    CHECK(SpscRingBuffer<int>(5).capacity() == 8);
}

static void test_ring_threaded() {
    constexpr std::uint64_t N = 2'000'000;
    SpscRingBuffer<std::uint64_t> q(1024);
    std::uint64_t sum = 0, count = 0;
    bool in_order = true;

    std::thread consumer([&] {
        std::uint64_t v = 0, expect = 0;
        while (count < N) {
            if (q.try_pop(v)) {
                in_order = in_order && (v == expect++);
                sum += v;
                ++count;
            } else {
                std::this_thread::yield();
            }
        }
    });
    for (std::uint64_t i = 0; i < N; ++i)
        while (!q.try_push(i)) std::this_thread::yield();
    consumer.join();

    CHECK(count == N);
    CHECK(in_order);
    CHECK(sum == N * (N - 1) / 2);
}

int main() {
    test_vwap_and_imbalance();
    test_window_expiry();
    test_volatility();
    test_quote_metrics();
    test_bad_input_ignored();
    test_ring_basic();
    test_ring_threaded();
    if (g_failures == 0) std::printf("all tests passed\n");
    return g_failures == 0 ? 0 : 1;
}
