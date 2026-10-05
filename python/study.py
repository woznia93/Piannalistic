#!/usr/bin/env python3
"""Signal study: do the metrics say anything about what happens next?

Replays a recording through the engine, samples the metrics once per interval,
and compares each sample with the move in the mid price over the following
few seconds.

    python python/study.py data/session.csv
    python python/study.py data/session.csv --horizons 1 5 15 60 --csv data/features.csv

Two questions are asked:

  Direction   Do order flow and book pressure (both signed, -1..+1) line up
              with the *signed* forward return?
  Turbulence  Do VPIN and volatility (both unsigned) line up with the *size*
              of the forward move, whichever way it goes?

Columns:
  IC        correlation between the metric now and the outcome later
            (0 = no relationship; in real markets 0.02-0.10 is already notable)
  t         rough t-statistic for IC, deflated because forward windows overlap
  hit       direction only: how often the metric's sign matched the move's sign
  top-bot   mean outcome when the metric is in its top fifth minus its bottom
            fifth, in basis points
"""
from __future__ import annotations

import argparse
import csv
import math
from collections import defaultdict

from pipeline import Pipeline
import feeds

DIRECTIONAL = [("flow", "order flow"), ("book", "book pressure")]
UNSIGNED = [("vpin", "VPIN"), ("vol", "volatility")]


def collect(path: str, window: float, interval: float, vpin_bucket: float, vpin_buckets: int):
    """Replay the recording; return {product: [row, ...]} sampled every interval."""
    pipe = Pipeline(window, interval, vpin_bucket, vpin_buckets)
    rows = defaultdict(list)
    first_ts = {}
    for ev in feeds.read_events(path):
        first_ts.setdefault(ev[1], ev[2])
        result = pipe.process(ev)
        if not result:
            continue
        product, s = result
        if s.mid <= 0 or s.ts - first_ts[product] < window:
            continue  # no quote yet, or the rolling window is still filling
        rows[product].append({
            "ts": s.ts, "mid": s.mid,
            "flow": s.trade_imbalance, "book": s.quote_imbalance,
            "vpin": s.vpin if s.vpin_buckets else None,
            "vol": s.volatility * 1e4,
        })
    return rows


def forward_returns(rows, horizon: float, interval: float):
    """Forward mid return in bp for each row, or None where there is no sample
    close enough to `horizon` seconds ahead (end of file, or a gap in the data)."""
    out = [None] * len(rows)
    j = 0
    for i, r in enumerate(rows):
        target = r["ts"] + horizon
        j = max(j, i)
        while j < len(rows) and rows[j]["ts"] < target:
            j += 1
        if j < len(rows) and rows[j]["ts"] - target < 2 * interval:
            out[i] = (rows[j]["mid"] / r["mid"] - 1.0) * 1e4
    return out


def pearson(xs, ys):
    n = len(xs)
    if n < 3:
        return float("nan")
    mx, my = sum(xs) / n, sum(ys) / n
    sxx = sum((x - mx) ** 2 for x in xs)
    syy = sum((y - my) ** 2 for y in ys)
    if sxx <= 0 or syy <= 0:
        return float("nan")
    return sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / math.sqrt(sxx * syy)


def evaluate(xs, ys, horizon: float, interval: float, directional: bool):
    n = len(xs)
    ic = pearson(xs, ys)
    # Consecutive forward windows share most of their data, so the number of
    # independent observations is closer to n / (samples per horizon).
    n_eff = n / max(1.0, horizon / interval)
    t = float("nan")
    if n_eff > 3 and not math.isnan(ic) and abs(ic) < 1:
        t = ic * math.sqrt((n_eff - 2) / (1 - ic * ic))
    hit = float("nan")
    if directional:
        pairs = [(x, y) for x, y in zip(xs, ys) if x != 0 and y != 0]
        if pairs:
            hit = sum((x > 0) == (y > 0) for x, y in pairs) / len(pairs)
    spread = float("nan")
    if n >= 10:
        order = sorted(range(n), key=lambda k: xs[k])
        q = n // 5
        bottom = sum(ys[k] for k in order[:q]) / q
        top = sum(ys[k] for k in order[-q:]) / q
        spread = top - bottom
    return n, ic, t, hit, spread


def fmt(v, spec):
    return "   n/a" if v is None or (isinstance(v, float) and math.isnan(v)) else format(v, spec)


def report(product, rows, horizons, interval):
    span = rows[-1]["ts"] - rows[0]["ts"] if rows else 0
    print(f"\n{product}: {len(rows)} samples over {span / 60:.1f} minutes")
    if len(rows) < 50:
        print("  Not enough data. Record at least 10-15 minutes; an hour or more is better.")
        return
    fwd = {h: forward_returns(rows, h, interval) for h in horizons}
    for title, metrics, directional in (
            ("Direction: metric vs signed forward return", DIRECTIONAL, True),
            ("Turbulence: metric vs size of forward move", UNSIGNED, False)):
        print(f"\n  {title}")
        print(f"  {'metric':<14}{'horizon':>8}{'n':>8}{'IC':>9}{'t':>8}{'hit':>8}{'top-bot bp':>12}")
        for key, label in metrics:
            for h in horizons:
                xs, ys = [], []
                for r, y in zip(rows, fwd[h]):
                    if y is None or r[key] is None:
                        continue
                    xs.append(r[key])
                    ys.append(y if directional else abs(y))
                n, ic, t, hit, spread = evaluate(xs, ys, h, interval, directional)
                hit_s = fmt(hit * 100 if not math.isnan(hit) else hit, "5.1f") + "%" if directional else "     -"
                print(f"  {label:<14}{h:>7g}s{n:>8}{fmt(ic, '+9.3f')}{fmt(t, '+8.1f')}"
                      f"{hit_s:>8}{fmt(spread, '+12.2f')}")


def main():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("file", help="recording made with --record")
    p.add_argument("--horizons", type=float, nargs="+", default=[1, 5, 10, 30],
                   help="look-ahead times in seconds")
    p.add_argument("--window", type=float, default=60.0)
    p.add_argument("--interval", type=float, default=1.0, help="sampling interval, seconds")
    p.add_argument("--vpin-bucket", type=float, default=0.0)
    p.add_argument("--vpin-buckets", type=int, default=50)
    p.add_argument("--csv", help="also write the sampled features and forward returns here")
    args = p.parse_args()

    data = collect(args.file, args.window, args.interval, args.vpin_bucket, args.vpin_buckets)
    if not data:
        raise SystemExit("No usable samples in that file.")
    for product, rows in data.items():
        report(product, rows, args.horizons, args.interval)

    if args.csv:
        with open(args.csv, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["product", "ts", "mid", "flow", "book", "vpin", "vol_bp"] +
                       [f"fwd_{h:g}s_bp" for h in args.horizons])
            for product, rows in data.items():
                fwd = [forward_returns(rows, h, args.interval) for h in args.horizons]
                for i, r in enumerate(rows):
                    w.writerow([product, r["ts"], r["mid"], r["flow"], r["book"],
                                "" if r["vpin"] is None else r["vpin"], r["vol"]] +
                               ["" if col[i] is None else round(col[i], 4) for col in fwd])
        print(f"\nWrote {args.csv}")

    print("\nReading this: one session is one sample of market conditions. Treat a result "
          "as real only if it\nholds up across several recordings made on different days.")


if __name__ == "__main__":
    main()
