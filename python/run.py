#!/usr/bin/env python3
"""Entry point: wire a data source into the C++ analytics engine.

    python python/run.py --source synthetic
    python python/run.py --source coinbase --product ETH-USD --record data/eth.csv
    python python/run.py --source replay --file data/eth.csv --out data/eth_metrics.csv
"""
from __future__ import annotations

import argparse
import asyncio
import csv
import sys
import time

try:
    import pma_core
except ImportError:
    sys.exit("pma_core not found. Build it first:\n"
             "  cmake -S . -B build -Dpybind11_DIR=$(python3 -m pybind11 --cmakedir)\n"
             "  cmake --build build -j")

import feeds

FIELDS = ["ts", "last_price", "mid", "spread_bps", "quote_imbalance", "vwap",
          "volume", "trade_imbalance", "volatility", "trades_in_window", "total_trades"]


def format_line(s) -> str:
    clock = time.strftime("%H:%M:%S", time.gmtime(s.ts))
    return (f"{clock}  last={s.last_price:>11.2f}  vwap={s.vwap:>11.2f}  "
            f"spread={s.spread_bps:5.2f}bp  vol={s.volatility * 1e4:6.2f}bp  "
            f"flow={s.trade_imbalance:+.2f}  book={s.quote_imbalance:+.2f}  "
            f"n={s.trades_in_window}")


async def main(args) -> None:
    if args.source == "coinbase":
        source = feeds.coinbase(args.product)
    elif args.source == "replay":
        if not args.file:
            sys.exit("--source replay needs --file")
        source = feeds.replay(args.file, args.speed)
    else:
        source = feeds.synthetic(args.rate)

    engine = pma_core.AnalyticsEngine(args.window)
    record_f = open(args.record, "w", newline="") if args.record else None
    out_f = open(args.out, "w", newline="") if args.out else None
    recorder = csv.writer(record_f) if record_f else None
    out = csv.DictWriter(out_f, fieldnames=FIELDS) if out_f else None
    if recorder:
        recorder.writerow(["type", "ts", "a", "b", "c", "d"])
    if out:
        out.writeheader()

    # Snapshots are emitted on *event time*, not wall-clock time, so a replay
    # produces exactly the same output as the live session it was recorded from.
    next_emit = None
    events = 0
    try:
        async for ev in source:
            events += 1
            if ev[0] == "trade":
                engine.on_trade(ev[1], ev[2], ev[3], ev[4])
            else:
                engine.on_quote(ev[1], ev[2], ev[3], ev[4], ev[5])
            if recorder:
                recorder.writerow(ev)

            ts = ev[1]
            if next_emit is None:
                next_emit = ts + args.interval
            elif ts >= next_emit:
                snap = engine.snapshot()
                if not args.quiet:
                    print(format_line(snap), flush=True)
                if out:
                    out.writerow(snap.as_dict())
                while next_emit <= ts:
                    next_emit += args.interval
            if args.max_events and events >= args.max_events:
                break
    finally:
        for f in (record_f, out_f):
            if f:
                f.close()
    print(f"[run] processed {events} events")


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--source", choices=["synthetic", "coinbase", "replay"], default="synthetic")
    p.add_argument("--product", default="BTC-USD", help="Coinbase product id")
    p.add_argument("--window", type=float, default=60.0, help="rolling window, seconds")
    p.add_argument("--interval", type=float, default=1.0, help="seconds between snapshots")
    p.add_argument("--rate", type=float, default=200.0, help="synthetic events/sec")
    p.add_argument("--file", help="CSV to replay")
    p.add_argument("--speed", type=float, default=0.0, help="replay speed; 0 = max")
    p.add_argument("--record", help="write raw events to this CSV")
    p.add_argument("--out", help="write metric snapshots to this CSV")
    p.add_argument("--max-events", type=int, default=0, help="stop after N events (0 = run forever)")
    p.add_argument("--quiet", action="store_true", help="don't print snapshots")
    try:
        asyncio.run(main(p.parse_args()))
    except KeyboardInterrupt:
        print("\n[run] stopped")
