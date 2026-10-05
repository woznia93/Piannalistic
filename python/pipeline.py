"""Shared plumbing for run.py and dashboard.py: one C++ engine per product,
snapshots emitted on event time, optional recording."""
from __future__ import annotations

import argparse
import csv
import sys
from typing import Optional, Tuple

try:
    import pma_core
except ImportError:
    sys.exit("pma_core not found. Build it first:\n"
             "  cmake -S . -B build -Dpybind11_DIR=$(python3 -m pybind11 --cmakedir)\n"
             "  cmake --build build -j")

import feeds

FIELDS = ["ts", "last_price", "mid", "spread_bps", "quote_imbalance", "vwap", "volume",
          "trade_imbalance", "volatility", "trade_rate", "vpin", "vpin_bucket_volume",
          "vpin_buckets", "trades_in_window", "total_trades"]


class Pipeline:
    """Routes events to per-product engines.

    Snapshots are emitted on *event time*, not wall-clock time, so a replay
    produces exactly the same output as the live session it was recorded from.
    """

    def __init__(self, window: float = 60.0, interval: float = 1.0,
                 vpin_bucket: float = 0.0, vpin_buckets: int = 50,
                 record_path: Optional[str] = None):
        self.window = window
        self.interval = interval
        self.vpin_bucket = vpin_bucket
        self.vpin_buckets = vpin_buckets
        self.engines = {}
        self._next_emit = {}
        self.events = 0
        self._record_f = open(record_path, "w", newline="") if record_path else None
        self._recorder = csv.writer(self._record_f) if self._record_f else None
        if self._recorder:
            self._recorder.writerow(feeds.RECORD_HEADER)

    def process(self, ev) -> Optional[Tuple[str, "pma_core.Snapshot"]]:
        """Feed one event. Returns (product, snapshot) when a snapshot is due."""
        self.events += 1
        product, ts = ev[1], ev[2]
        engine = self.engines.get(product)
        if engine is None:
            engine = self.engines[product] = pma_core.AnalyticsEngine(
                self.window, self.vpin_bucket, self.vpin_buckets)
            self._next_emit[product] = ts + self.interval
        if ev[0] == "trade":
            engine.on_trade(ts, ev[3], ev[4], ev[5])
        else:
            engine.on_quote(ts, ev[3], ev[4], ev[5], ev[6])
        if self._recorder:
            self._recorder.writerow(ev)

        due = self._next_emit[product]
        if ts < due:
            return None
        while due <= ts:
            due += self.interval
        self._next_emit[product] = due
        return product, engine.snapshot()

    def close(self) -> None:
        if self._record_f:
            self._record_f.close()
            self._record_f = None


def add_common_args(p: argparse.ArgumentParser) -> None:
    p.add_argument("--source", choices=["synthetic", "coinbase", "replay"], default="synthetic")
    p.add_argument("--products", nargs="+", default=["BTC-USD"], metavar="ID",
                   help="one or more product ids, e.g. BTC-USD ETH-USD SOL-USD")
    p.add_argument("--window", type=float, default=60.0, help="rolling window, seconds")
    p.add_argument("--interval", type=float, default=1.0, help="seconds between snapshots")
    p.add_argument("--vpin-bucket", type=float, default=0.0,
                   help="VPIN bucket volume; 0 = calibrate from the first window")
    p.add_argument("--vpin-buckets", type=int, default=50, help="buckets averaged for VPIN")
    p.add_argument("--rate", type=float, default=20.0, help="synthetic: trades/sec per product")
    p.add_argument("--fast", action="store_true",
                   help="synthetic: simulated clock, no sleeping (pair with --max-events)")
    p.add_argument("--file", help="replay: recording to read")
    p.add_argument("--speed", type=float, default=None,
                   help="replay speed; 0 = as fast as possible")
    p.add_argument("--record", help="write raw events to this CSV")
    p.add_argument("--max-events", type=int, default=0, help="stop after N events (0 = no limit)")


def make_source(args, default_replay_speed: float):
    if args.source == "coinbase":
        return feeds.coinbase(args.products)
    if args.source == "replay":
        if not args.file:
            sys.exit("--source replay needs --file")
        speed = default_replay_speed if args.speed is None else args.speed
        return feeds.replay(args.file, speed)
    return feeds.synthetic(args.products, args.rate, fast=args.fast)


def make_pipeline(args) -> Pipeline:
    return Pipeline(args.window, args.interval, args.vpin_bucket, args.vpin_buckets, args.record)
