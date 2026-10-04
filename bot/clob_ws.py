"""Real-time Polymarket CLOB order-book stream.

Polymarket's website shows each market's price as the ORDER-BOOK MIDPOINT, updated
live over its CLOB WebSocket. The REST `/price` and `/midpoint` endpoints lag badly
(observed /price=0.97 while the live book mid was 0.986), which is why the dashboard
"Polymarket Odds" drifted from what you see on the site.

This stream subscribes to the CLOB market channel for the current Up/Down token ids
and keeps each token's best bid / best ask / midpoint fresh in real time. The main
loop reads `get(token_id)` for the displayed odds (mid) and the executable ask.

Token ids change every 15m window; call `set_assets([up, down])` each tick — when the
ids actually change the socket resubscribes automatically.
"""

import asyncio
import json
import time
from typing import Dict, List, Optional

import aiohttp

from .net_utils import get_proxy_url_for

CLOB_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"


def _f(x) -> Optional[float]:
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


class ClobOrderBookStream:
    def __init__(self, ws_url: str = CLOB_WS_URL):
        self.ws_url = ws_url
        self.closed = False
        self.assets: List[str] = []
        # token_id -> {"bids": {price: size}, "asks": {price: size}}
        self._books: Dict[str, Dict[str, Dict[float, float]]] = {}
        # token_id -> {"bid","ask","mid","ts"}
        self.best: Dict[str, Dict[str, float]] = {}
        self._ws = None
        self._subscribed: List[str] = []

    # ── public API ──────────────────────────────────────────────────────────
    def set_assets(self, assets: List[str]) -> None:
        """Point the stream at the current Up/Down token ids. Resubscribes when the
        set actually changes (i.e. when the 15m window rolls to a new market)."""
        clean = [str(a) for a in assets if a]
        if sorted(clean) == sorted(self.assets):
            return
        self.assets = clean
        # Force the live socket to drop so the connect loop resubscribes with the
        # new asset ids.
        ws = self._ws
        if ws is not None and not ws.closed:
            try:
                asyncio.get_event_loop().create_task(ws.close())
            except Exception:
                pass

    def get(self, token_id: Optional[str], max_age_s: float = 15.0) -> Dict[str, Optional[float]]:
        """Latest {bid, ask, mid} for a token, or Nones if stale/absent."""
        if not token_id:
            return {"bid": None, "ask": None, "mid": None}
        b = self.best.get(str(token_id))
        if not b or (time.time() - b.get("ts", 0)) > max_age_s:
            return {"bid": None, "ask": None, "mid": None}
        return {"bid": b.get("bid"), "ask": b.get("ask"), "mid": b.get("mid")}

    # ── book maintenance ────────────────────────────────────────────────────
    def _apply_book(self, asset: str, bids: list, asks: list) -> None:
        book = {"bids": {}, "asks": {}}
        for lvl in bids or []:
            p, s = _f(lvl.get("price")), _f(lvl.get("size"))
            if p is not None and s is not None and s > 0:
                book["bids"][p] = s
        for lvl in asks or []:
            p, s = _f(lvl.get("price")), _f(lvl.get("size"))
            if p is not None and s is not None and s > 0:
                book["asks"][p] = s
        self._books[asset] = book
        self._recompute(asset)

    def _apply_change(self, asset: str, price: Optional[float], side: str, size: Optional[float]) -> None:
        if price is None or size is None:
            return
        book = self._books.setdefault(asset, {"bids": {}, "asks": {}})
        side_key = "bids" if str(side).upper() in ("BUY", "BID") else "asks"
        if size <= 0:
            book[side_key].pop(price, None)
        else:
            book[side_key][price] = size
        self._recompute(asset)

    def _recompute(self, asset: str) -> None:
        book = self._books.get(asset)
        if not book:
            return
        best_bid = max(book["bids"], default=None) if book["bids"] else None
        best_ask = min(book["asks"], default=None) if book["asks"] else None
        mid = None
        if best_bid is not None and best_ask is not None:
            mid = (best_bid + best_ask) / 2.0
        elif best_bid is not None:
            mid = best_bid
        elif best_ask is not None:
            mid = best_ask
        self.best[asset] = {"bid": best_bid, "ask": best_ask, "mid": mid, "ts": time.time()}

    def _handle(self, m) -> None:
        if isinstance(m, list):
            for item in m:
                self._handle(item)
            return
        if not isinstance(m, dict):
            return
        et = m.get("event_type") or m.get("type")
        asset = str(m.get("asset_id") or m.get("assetId") or "")
        if not asset:
            return
        if et == "book":
            self._apply_book(asset, m.get("bids", []), m.get("asks", []))
        elif et == "price_change":
            changes = m.get("changes")
            if isinstance(changes, list):
                for c in changes:
                    self._apply_change(asset, _f(c.get("price")), c.get("side", ""), _f(c.get("size")))
            else:
                self._apply_change(asset, _f(m.get("price")), m.get("side", ""), _f(m.get("size")))

    # ── connection loop ───────────────────────────────────────────────────────
    async def start(self) -> None:
        while not self.closed:
            if not self.assets:
                await asyncio.sleep(1)
                continue
            assets = list(self.assets)
            try:
                proxy = get_proxy_url_for(self.ws_url)
                async with aiohttp.ClientSession() as session:
                    async with session.ws_connect(self.ws_url, proxy=proxy if proxy else None,
                                                  heartbeat=10.0, timeout=15.0) as ws:
                        self._ws = ws
                        self._subscribed = assets
                        await ws.send_str(json.dumps({"assets_ids": assets, "type": "market"}))
                        print(f"[clob_ws] subscribed to {len(assets)} assets")
                        async for msg in ws:
                            if self.closed or sorted(self.assets) != sorted(assets):
                                break
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                if msg.data in ("PONG", "PING"):
                                    continue
                                try:
                                    self._handle(json.loads(msg.data))
                                except (ValueError, TypeError):
                                    pass
                            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                                break
            except Exception as e:
                print(f"[clob_ws] error: {e}")
            finally:
                self._ws = None
            if not self.closed:
                await asyncio.sleep(1.5)

    def close(self) -> None:
        self.closed = True
