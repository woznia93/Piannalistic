"""Market data sources. Each one is an async generator yielding plain tuples:

    ("trade", ts, price, size, side)             side: +1 buy aggressor, -1 sell
    ("quote", ts, bid, ask, bid_size, ask_size)

Keeping events as tuples (not objects) keeps the Python hot loop cheap on a Pi.
"""
from __future__ import annotations

import asyncio
import csv
import json
import random
import time
from datetime import datetime
from typing import AsyncIterator, Tuple

Event = Tuple  # ("trade", ...) or ("quote", ...)

COINBASE_WS = "wss://ws-feed.exchange.coinbase.com"


def _parse_time(s: str) -> float:
    # e.g. "2026-10-05T14:03:11.123456Z"
    return datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp()


async def coinbase(product: str = "BTC-USD") -> AsyncIterator[Event]:
    """Live trades and top-of-book from Coinbase Exchange's public feed.

    No API key needed for the `matches` and `ticker` channels. Reconnects with
    backoff if the socket drops.
    """
    import websockets  # imported here so synthetic/replay work without it

    subscribe = json.dumps({
        "type": "subscribe",
        "product_ids": [product],
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
                        yield ("trade", _parse_time(msg["time"]),
                               float(msg["price"]), float(msg["size"]), side)
                    elif kind == "ticker" and "best_bid" in msg and "time" in msg:
                        yield ("quote", _parse_time(msg["time"]),
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


async def synthetic(rate: float = 200.0, start_price: float = 50_000.0,
                    seed: int | None = None) -> AsyncIterator[Event]:
    """Random-walk market for offline development. `rate` is events per second."""
    rng = random.Random(seed)
    price = start_price
    gap = 1.0 / rate
    while True:
        price *= 1.0 + rng.gauss(0.0, 0.0002)
        ts = time.time()
        half_spread = price * 0.00005
        yield ("quote", ts, price - half_spread, price + half_spread,
               rng.expovariate(1.0), rng.expovariate(1.0))
        side = 1 if rng.random() < 0.5 else -1
        yield ("trade", ts, price + side * half_spread, rng.expovariate(2.0) + 1e-6, side)
        await asyncio.sleep(gap)


async def replay(path: str, speed: float = 0.0) -> AsyncIterator[Event]:
    """Replay a CSV written by `run.py --record`.

    speed=0 replays as fast as possible (deterministic, good for backtests);
    speed=1 replays in real time; speed=10 at 10x, and so on.
    """
    prev_ts = None
    with open(path, newline="") as f:
        for i, row in enumerate(csv.reader(f)):
            if not row or row[0] == "type":
                continue
            ts = float(row[1])
            if speed > 0 and prev_ts is not None and ts > prev_ts:
                await asyncio.sleep((ts - prev_ts) / speed)
            prev_ts = ts
            if row[0] == "trade":
                yield ("trade", ts, float(row[2]), float(row[3]), int(row[4]))
            elif row[0] == "quote":
                yield ("quote", ts, float(row[2]), float(row[3]), float(row[4]), float(row[5]))
            if speed == 0 and i % 5000 == 0:
                await asyncio.sleep(0)  # let other tasks run
