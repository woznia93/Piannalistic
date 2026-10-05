#!/usr/bin/env python3
"""Live web dashboard. Runs the feed and the analytics engine, and pushes a
snapshot per product per interval to every connected browser over a WebSocket.

    python python/dashboard.py --source synthetic --products BTC-USD ETH-USD
    python python/dashboard.py --source coinbase --products BTC-USD ETH-USD SOL-USD
    python python/dashboard.py --source replay --file data/session.csv --speed 5

Then open http://<pi-address>:8080 from any device on the same network.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import socket
import time
from collections import deque
from pathlib import Path

from aiohttp import WSMsgType, web

from pipeline import add_common_args, make_pipeline, make_source

STATIC = Path(__file__).parent / "static"


def compact(snap, lag_ms=None) -> dict:
    """Snapshot as a small JSON-friendly dict (rounded to keep messages short)."""
    d = {
        "t": round(snap.ts, 3),
        "last": snap.last_price,
        "mid": round(snap.mid, 8),
        "vwap": round(snap.vwap, 8),
        "spread": round(snap.spread_bps, 3),
        "vol": round(snap.volatility * 1e4, 4),       # basis points
        "flow": round(snap.trade_imbalance, 4),
        "book": round(snap.quote_imbalance, 4),
        "vpin": round(snap.vpin, 4),
        "vpin_n": snap.vpin_buckets,
        "vpin_bucket": snap.vpin_bucket_volume,
        "rate": round(snap.trade_rate, 2),
        "volume": round(snap.volume, 6),
    }
    if lag_ms is not None:
        d["lag"] = round(lag_ms)
    return d


class Hub:
    def __init__(self, args):
        self.args = args
        self.pipe = make_pipeline(args)
        self.history = {}            # product -> deque of compact snapshots
        self.clients = set()
        self.ended = False
        self.error = None
        # Feed delay only means something when event times are wall-clock times.
        self.live = args.source == "coinbase" or (args.source == "synthetic" and not args.fast)

    def hello(self) -> str:
        return json.dumps({
            "type": "hello",
            "source": self.args.source,
            "window": self.args.window,
            "interval": self.args.interval,
            "vpin_buckets": self.args.vpin_buckets,
            "host": socket.gethostname(),
            "ended": self.ended,
            "error": self.error,
            "history": {p: list(h) for p, h in self.history.items()},
        })

    async def broadcast(self, message: str) -> None:
        for ws in list(self.clients):
            try:
                await asyncio.wait_for(ws.send_str(message), timeout=2.0)
            except Exception:
                self.clients.discard(ws)  # slow or gone; it will reconnect

    async def run_feed(self) -> None:
        args = self.args
        source = make_source(args, default_replay_speed=1.0)
        try:
            async for ev in source:
                result = self.pipe.process(ev)
                if result:
                    product, snap = result
                    lag = (time.time() - snap.ts) * 1000.0 if self.live else None
                    item = compact(snap, lag)
                    hist = self.history.get(product)
                    if hist is None:
                        hist = self.history[product] = deque(maxlen=args.history)
                    hist.append(item)
                    if self.clients:
                        await self.broadcast(json.dumps(
                            {"type": "snap", "product": product, "s": item}))
                if args.max_events and self.pipe.events >= args.max_events:
                    break
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self.error = str(exc) or repr(exc)
            print(f"[feed] stopped: {self.error}")
        finally:
            self.pipe.close()
        # Keep serving so the charts stay viewable after a replay finishes.
        self.ended = True
        await self.broadcast(json.dumps({"type": "ended", "error": self.error}))
        print(f"[feed] ended after {self.pipe.events} events; dashboard still being served")


async def index(_request):
    return web.FileResponse(STATIC / "index.html", headers={"Cache-Control": "no-cache"})


async def ws_handler(request):
    hub: Hub = request.app["hub"]
    ws = web.WebSocketResponse(heartbeat=20)
    await ws.prepare(request)
    await ws.send_str(hub.hello())
    hub.clients.add(ws)
    try:
        async for msg in ws:  # the page never sends anything; just wait for close
            if msg.type in (WSMsgType.ERROR, WSMsgType.CLOSE):
                break
    finally:
        hub.clients.discard(ws)
    return ws


async def on_startup(app):
    app["feed"] = asyncio.create_task(app["hub"].run_feed())


async def on_cleanup(app):
    app["feed"].cancel()
    try:
        await app["feed"]
    except asyncio.CancelledError:
        pass


def lan_address() -> str:
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("10.255.255.255", 1))  # no packet is sent; just picks the outbound interface
        return s.getsockname()[0]
    except OSError:
        return "127.0.0.1"
    finally:
        s.close()


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    add_common_args(p)
    p.add_argument("--host", default="0.0.0.0",
                   help="address to listen on; 0.0.0.0 = every device on your network")
    p.add_argument("--port", type=int, default=8080)
    p.add_argument("--history", type=int, default=1800,
                   help="snapshots kept per product for newly opened pages")
    args = p.parse_args()

    app = web.Application()
    app["hub"] = Hub(args)
    app.router.add_get("/", index)
    app.router.add_get("/ws", ws_handler)
    app.on_startup.append(on_startup)
    app.on_cleanup.append(on_cleanup)

    print(f"Dashboard: http://{lan_address()}:{args.port}   "
          f"(or http://{socket.gethostname()}.local:{args.port})")
    web.run_app(app, host=args.host, port=args.port, print=None)


if __name__ == "__main__":
    main()
