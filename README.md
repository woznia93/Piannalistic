# pi-market-analytics

Streaming market analytics on a Raspberry Pi. A header-only C++17 core computes
rolling metrics; Python handles the data feed, recording, and replay.

```
feed (Python, asyncio)  ->  AnalyticsEngine (C++ via pybind11)  ->  console / CSV
  coinbase | synthetic | replay
```

## Metrics (rolling time window, O(1) per event)

| Field | Meaning |
|---|---|
| `vwap`, `volume` | volume-weighted average price and traded size in the window |
| `trade_imbalance` | signed aggressor volume / total volume, -1 (all selling) to +1 (all buying) |
| `volatility` | standard deviation of trade-to-trade log returns in the window |
| `mid`, `spread_bps` | top-of-book midpoint and spread in basis points |
| `quote_imbalance` | (bid size - ask size) / (bid size + ask size) at the top of book |

## Setup (Raspberry Pi OS / Debian / Ubuntu)

```bash
sudo apt install -y build-essential cmake python3-dev python3-venv
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

cmake -S . -B build -Dpybind11_DIR=$(python3 -m pybind11 --cmakedir)
cmake --build build -j
ctest --test-dir build --output-on-failure
```

The build drops `pma_core.*.so` into `python/`.

## Run

```bash
# offline random-walk market, no network needed
python python/run.py --source synthetic

# live Coinbase data (public feed, no API key), recording raw events
python python/run.py --source coinbase --product BTC-USD --record data/btc.csv

# replay a recording at full speed and write metrics to CSV
python python/run.py --source replay --file data/btc.csv --out data/btc_metrics.csv --quiet
```

Snapshots are emitted on event time, so replaying a recording gives identical
output every run.

## Benchmark

```bash
./build/pma_bench                  # peak throughput, unthrottled
./build/pma_bench 500000 50000     # latency at 50k msgs/sec
```

The benchmark uses two busy-spinning threads, so it needs two free cores to
give meaningful latency numbers. On the Pi, pin them:
`taskset -c 2,3 ./build/pma_bench`.

## Layout

```
cpp/include/pma/types.hpp        Trade, Quote, Snapshot
cpp/include/pma/analytics.hpp    AnalyticsEngine (rolling metrics)
cpp/include/pma/ring_buffer.hpp  lock-free SPSC queue
cpp/src/bindings.cpp             pybind11 module `pma_core`
cpp/tests/test_core.cpp          unit tests
cpp/bench/bench.cpp              throughput / latency benchmark
python/feeds.py                  coinbase, synthetic, replay sources
python/run.py                    CLI entry point
```

## Next steps

- Replace the `std::deque` window in `AnalyticsEngine` with a preallocated
  circular buffer; compare `pma_bench` before and after.
- Move the feed handler into C++ (Boost.Beast + simdjson) and hand events to
  the engine through `SpscRingBuffer`.
- Add a limit order book fed by Coinbase `level2` (needs an API key).
- Add a dashboard (Grafana, or a small web page reading the metrics CSV).
