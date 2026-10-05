#!/usr/bin/env python3
"""Console runner: print metric snapshots, record events, write metrics to CSV.

    python python/run.py --source synthetic
    python python/run.py --source coinbase --products BTC-USD ETH-USD --record data/session.csv
    python python/run.py --source replay --file data/session.csv --out data/metrics.csv --quiet

For the live web view, use dashboard.py instead.
"""
from __future__ import annotations

import argparse
import asyncio
import csv
import time

from pipeline import FIELDS, add_common_args, make_pipeline, make_source


def format_line(product: str, s) -> str:
    clock = time.strftime("%H:%M:%S", time.gmtime(s.ts))
    vpin = f"{s.vpin:.2f}" if s.vpin_buckets else " -- "
    return (f"{clock} {product:<9} last={s.last_price:>11.2f}  vwap={s.vwap:>11.2f}  "
            f"spread={s.spread_bps:5.2f}bp  vol={s.volatility * 1e4:6.2f}bp  "
            f"flow={s.trade_imbalance:+.2f}  book={s.quote_imbalance:+.2f}  "
            f"vpin={vpin}  {s.trade_rate:5.1f} trades/s")


async def main(args) -> None:
    source = make_source(args, default_replay_speed=0.0)
    pipe = make_pipeline(args)
    out_f = open(args.out, "w", newline="") if args.out else None
    out = csv.DictWriter(out_f, fieldnames=["product"] + FIELDS) if out_f else None
    if out:
        out.writeheader()
    try:
        async for ev in source:
            result = pipe.process(ev)
            if result:
                product, snap = result
                if not args.quiet:
                    print(format_line(product, snap), flush=True)
                if out:
                    out.writerow({"product": product, **snap.as_dict()})
            if args.max_events and pipe.events >= args.max_events:
                break
    finally:
        pipe.close()
        if out_f:
            out_f.close()
    print(f"[run] processed {pipe.events} events")


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    add_common_args(p)
    p.add_argument("--out", help="write metric snapshots to this CSV")
    p.add_argument("--quiet", action="store_true", help="don't print snapshots")
    try:
        asyncio.run(main(p.parse_args()))
    except KeyboardInterrupt:
        print("\n[run] stopped")
