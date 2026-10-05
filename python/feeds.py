"""Market data sources. Each one is an async generator yielding plain tuples:

    ("trade", product, ts, price, size, side)        side: +1 buy aggressor, -1 sell
    ("quote", product, ts, bid, ask, bid_size, ask_size)

Keeping events as tuples (not objects) keeps the Python hot loop cheap on a Pi.
"""
from __future__ import annotations

import asyncio
import csv
import json
import math
import random
import time
from datetime import datetime
from typing import AsyncIterator, Iterator, Sequence, Tuple

Event = Tuple  # ("trade", ...) or ("quote", ...)

COINBASE_WS = "wss://ws-feed.exchange.coinbase.com"
RECORD_HEADER = ["type", "product", "ts", "a", "b", "c", "d"]


def _parse_time(s: str) -> float:
    # e.g. "2026-10-05T14:03:11.123456Z"
    return datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp()


async def coinbase(products: Sequence[str] = ("BTC-USD",)) -> AsyncIterator[Event]:
    """Live trades and top-of-book from Coinbase Exchange's public feed.

    No API key needed for the `matches` and `ticker` channels. Reconnects with
    backoff if the socket drops.
    """
    import websockets  # imported here so synthetic/replay work without it

    subscribe = json.dumps({
        "type": "subscribe",
        "product_ids": list(products),
        "channels": ["matches", "ticker"],
    })
    backoff = 1.0
    while True:
        try:
            async with websockets.connect(COINBASE_WS, ping_interval=20) as ws:
                await ws.send(subscribe)
                backoff = 1.0
                async for raw in ws:
                    msg = json.loads(raw)
                    kind = msg.get("type")
                    if kind == "match":
                        # Coinbase reports the MAKER side; the aggressor is the opposite.
                        side = 1 if msg["side"] == "sell" else -1
                        yield ("trade", msg["product_id"], _parse_time(msg["time"]),
                               float(msg["price"]), float(msg["size"]), side)
                    elif kind == "ticker" and "best_bid" in msg and "time" in msg:
                        yield ("quote", msg["product_id"], _parse_time(msg["time"]),
                               float(msg["best_bid"]), float(msg["best_ask"]),
                               float(msg.get("best_bid_size", 0) or 0),
                               float(msg.get("best_ask_size", 0) or 0))
                    elif kind == "error":
                        raise RuntimeError(f"feed error: {msg.get('message')} {msg.get('reason', '')}")
        except asyncio.CancelledError:
            raise
        except RuntimeError:
            raise
        except Exception as exc:  # network hiccup: reconnect
            print(f"[feed] disconnected ({exc!r}); retrying in {backoff:.0f}s")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30.0)


_SYNTH_START = {"BTC-USD": 62_000.0, "ETH-USD": 2_450.0, "SOL-USD": 148.0}


async def synthetic(products: Sequence[str] = ("BTC-USD",), rate: float = 20.0,
                    seed: int | None = None, fast: bool = False,
                    impact: float = 0.000006) -> AsyncIterator[Event]:
    """Simulated market for offline development.

    `rate` is trades per second per product. Order flow is persistent (buying
    pressure drifts slowly between buyer- and seller-dominated), each trade
    nudges the price in its own direction by `impact`, and volatility wanders
    between calm and choppy spells. That plants a real,
    known relationship between flow and future price, which makes this a
    positive control for study.py: the study should find it here. Set
    impact=0 for a pure random walk, where it should find nothing.

    fast=True uses a simulated clock and never sleeps, for generating hours of
    test data in seconds.
    """
    rng = random.Random(seed)
    gap = 1.0 / rate
    state = {}
    for p in products:
        start = _SYNTH_START.get(p, 100.0)
        state[p] = {"price": start, "pressure": 0.0, "heat": 0.0, "unit": 30.0 / start}
    ts = time.time()
    theta, sigma = 0.05, 0.45  # pressure mean-reverts over ~20 s
    n = 0
    while True:
        ts = ts + gap if fast else time.time()
        for p in products:
            st = state[p]
            st["pressure"] += -theta * st["pressure"] * gap + sigma * math.sqrt(gap) * rng.gauss(0, 1)
            tilt = math.tanh(st["pressure"])           # -1 sellers .. +1 buyers
            side = 1 if rng.random() < 0.5 + 0.4 * tilt else -1
            size = (rng.expovariate(1.0) + 0.02) * st["unit"]
            # "heat" wanders slowly so the market has calm and choppy spells.
            st["heat"] += -0.01 * st["heat"] * gap + 0.25 * math.sqrt(gap) * rng.gauss(0, 1)
            choppy = math.exp(st["heat"])
            st["price"] *= 1.0 + side * impact * (size / st["unit"]) + rng.gauss(0, 0.00002) * choppy
            half = st["price"] * 0.00005 * (1.0 + abs(tilt)) * min(choppy, 4.0)
            yield ("quote", p, ts, st["price"] - half, st["price"] + half,
                   rng.expovariate(1.0) * (1.0 + 0.8 * tilt) * st["unit"] * 4,
                   rng.expovariate(1.0) * (1.0 - 0.8 * tilt) * st["unit"] * 4)
            yield ("trade", p, ts, st["price"] + side * half, size, side)
        n += 1
        if fast:
            if n % 2000 == 0:
                await asyncio.sleep(0)
        else:
            await asyncio.sleep(gap)


def read_events(path: str) -> Iterator[Event]:
    """Read a CSV written with --record, in order."""
    with open(path, newline="") as f:
        for row in csv.reader(f):
            if not row or row[0] == "type":
                continue
            if row[0] == "trade":
                yield ("trade", row[1], float(row[2]), float(row[3]), float(row[4]), int(row[5]))
            elif row[0] == "quote":
                yield ("quote", row[1], float(row[2]), float(row[3]), float(row[4]),
                       float(row[5]), float(row[6]))


async def replay(path: str, speed: float = 0.0) -> AsyncIterator[Event]:
    """Replay a recording.

    speed=0 replays as fast as possible (deterministic, good for backtests);
    speed=1 replays in real time; speed=10 at 10x, and so on.
    """
    prev_ts = None
    for i, ev in enumerate(read_events(path)):
        ts = ev[2]
        if speed > 0 and prev_ts is not None and ts > prev_ts:
            await asyncio.sleep((ts - prev_ts) / speed)
        prev_ts = ts
        yield ev
        if speed == 0 and i % 5000 == 0:
            await asyncio.sleep(0)  # let other tasks run
