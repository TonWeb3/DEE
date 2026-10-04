import asyncio
import json
import aiohttp
import time
from typing import Optional, Callable, Dict, List
from .config import settings
from .net_utils import get_proxy_url_for

class BinanceTradeStream:
    """Fast spot-price feed from Binance @trade — the leading signal for fair_prob."""
    def __init__(self, symbol: str, on_update: Optional[Callable] = None):
        self.symbol = symbol.lower()
        self.on_update = on_update
        self.last_price = None
        self.last_ts = None
        self.closed = False

    async def start(self):
        url = f"wss://stream.binance.com:9443/ws/{self.symbol}@trade"

        while not self.closed:
            try:
                proxy = get_proxy_url_for(url)
                async with aiohttp.ClientSession() as session:
                    async with session.ws_connect(url, proxy=proxy if proxy else None) as ws:
                        print(f"Connected to Binance WS: {self.symbol}")
                        while not self.closed:
                            msg = await ws.receive()
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                data_msg = json.loads(msg.data)
                                self._process_trade(float(data_msg.get("p")))
                            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                                break
            except Exception as e:
                print(f"Binance trade WS failed: {e}")
                if not self.closed:
                    await asyncio.sleep(2)

    def _process_trade(self, p: float):
        self.last_price = p
        self.last_ts = time.time()

        if self.on_update:
            asyncio.create_task(self.on_update({"price": self.last_price, "ts": self.last_ts}))

    def get_last(self):
        return {"price": self.last_price, "ts": self.last_ts}

    def close(self):
        self.closed = True

class BinanceKlineStream:
    def __init__(self, symbol: str, interval: str, limit: int = 240):
        self.symbol = symbol.lower()
        self.interval = interval
        self.limit = limit
        self.candles = []
        self.closed = False

    async def start(self):
        url = f"wss://stream.binance.com:9443/ws/{self.symbol}@kline_{self.interval}"

        while not self.closed:
            try:
                proxy = get_proxy_url_for(url)
                async with aiohttp.ClientSession() as session:
                    async with session.ws_connect(url, proxy=proxy if proxy else None) as ws:
                        print(f"Connected to Binance Kline WS: {self.symbol} {self.interval}")
                        while not self.closed:
                            msg = await ws.receive()
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                data_msg = json.loads(msg.data)
                                k = data_msg.get("k", {})
                                candle = {
                                    "openTime": int(k.get("t")),
                                    "open": float(k.get("o")),
                                    "high": float(k.get("h")),
                                    "low": float(k.get("l")),
                                    "close": float(k.get("c")),
                                    "volume": float(k.get("v")),
                                    "closeTime": int(k.get("T")),
                                    "isClosed": k.get("x")
                                }
                                self._update_candle(candle)
                            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                                break
            except Exception as e:
                print(f"Binance Kline WS failed: {e}")
                if not self.closed:
                    await asyncio.sleep(5)

    def _update_candle(self, candle: Dict):
        if not self.candles:
            self.candles.append(candle)
        else:
            if candle["openTime"] == self.candles[-1]["openTime"]:
                self.candles[-1] = candle
            else:
                self.candles.append(candle)
        if len(self.candles) > self.limit:
            self.candles.pop(0)

    def set_candles(self, candles: List[Dict]):
        self.candles = candles[-self.limit:]

    def get_candles(self):
        return self.candles

    def close(self):
        self.closed = True

class PolymarketChainlinkStream:
    def __init__(self, ws_url: str, symbol_includes: str = "btc", on_update: Optional[Callable] = None):
        self.ws_url = ws_url
        self.symbol_includes = symbol_includes.lower()
        self.on_update = on_update
        self.last_price = None
        self.last_updated_at = None
        self.closed = False

    async def start(self):
        if not self.ws_url:
            return

        async def ping_loop(ws):
            while not self.closed:
                try:
                    await ws.send_str("PING")
                    await asyncio.sleep(5)
                except:
                    break

        while not self.closed:
            try:
                proxy = get_proxy_url_for(self.ws_url)
                headers = {
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
                }
                async with aiohttp.ClientSession(headers=headers) as session:
                    print(f"Connecting to Polymarket WS: {self.ws_url}")
                    async with session.ws_connect(self.ws_url, proxy=proxy if proxy else None) as ws:
                        print(f"Connected to Polymarket WS. Subscribing to topics...")

                        # Comprehensive topic subscription for maximum compatibility
                        topics = ["crypto_prices_chainlink", "price_chainlink", "settlement_prices"]
                        for t in topics:
                            await ws.send_json({"action": "subscribe", "topic": t})

                        asyncio.create_task(ping_loop(ws))

                        while not self.closed:
                            msg = await ws.receive()
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                data_text = msg.data
                                if data_text in ("PONG", "OK", "PONG\n"):
                                    continue

                                try:
                                    data_msg = json.loads(data_text)
                                except:
                                    continue

                                topic = data_msg.get("topic")
                                if topic not in ("crypto_prices_chainlink", "price_chainlink", "settlement_prices"):
                                    continue

                                payload = data_msg.get("payload", {})
                                if isinstance(payload, str):
                                    try:
                                        payload = json.loads(payload)
                                    except:
                                        continue

                                updates = payload if isinstance(payload, list) else [payload]

                                for update in updates:
                                    if not isinstance(update, dict): continue

                                    sym = str(update.get("symbol") or update.get("pair") or update.get("ticker") or update.get("asset") or "").lower()
                                    # Normalize symbol btc-usd, btc/usd, bitcoin
                                    if self.symbol_includes:
                                        target = self.symbol_includes.lower()
                                        if target not in sym and not (target == "btc" and "bitcoin" in sym):
                                            continue

                                    try:
                                        price_val = update.get("price") or update.get("value") or update.get("current")
                                        if price_val is not None:
                                            self.last_price = float(price_val)
                                            ts_val = update.get("timestamp") or update.get("updated_at") or time.time()
                                            updated_at = float(ts_val)
                                            if updated_at < 10000000000: updated_at *= 1000
                                            self.last_updated_at = updated_at

                                            if self.on_update:
                                                await self.on_update({"price": self.last_price, "updatedAt": self.last_updated_at, "source": "polymarket_ws"})
                                    except:
                                        continue

                            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                                break
            except Exception as e:
                print(f"WS Error (Polymarket): {e}")
                if not self.closed:
                    await asyncio.sleep(2)

    def get_last(self):
        return {"price": self.last_price, "updatedAt": self.last_updated_at, "source": "polymarket_ws"}

    def close(self):
        self.closed = True

class ChainlinkPriceStream:
    def __init__(self, aggregator: str, decimals: int = 8, on_update: Optional[Callable] = None):
        self.aggregator = aggregator
        self.decimals = decimals
        self.on_update = on_update
        self.last_price = None
        self.last_updated_at = None
        self.closed = False
        self.wss_urls = settings.POLYGON_WSS_URLS + ([settings.POLYGON_WSS_URL] if settings.POLYGON_WSS_URL else [])

    async def start(self):
        if not self.wss_urls or not self.aggregator:
            return

        url_idx = 0
        while not self.closed:
            url = self.wss_urls[url_idx % len(self.wss_urls)]
            url_idx += 1
            try:
                proxy = get_proxy_url_for(url)
                async with aiohttp.ClientSession() as session:
                    async with session.ws_connect(url, proxy=proxy if proxy else None) as ws:
                        print(f"Connected to Chainlink RPC WS: {url}")
                        sub_msg = {
                            "jsonrpc": "2.0",
                            "id": 1,
                            "method": "eth_subscribe",
                            "params": [
                                "logs",
                                {
                                    "address": self.aggregator,
                                    "topics": ["0x05598845ccd9c46647361c770d3023029a3514781ca1029c91d84f2913e79435"] # AnswerUpdated topic
                                }
                            ]
                        }
                        await ws.send_json(sub_msg)

                        while not self.closed:
                            msg = await ws.receive()
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                data_res = json.loads(msg.data)
                                if data_res.get("method") == "eth_subscription":
                                    log = data_res.get("params", {}).get("result", {})
                                    topics = log.get("topics", [])
                                    if len(topics) >= 2:
                                        answer = int(topics[1], 16)
                                        if answer >= 2**255:
                                            answer -= 2**256

                                        self.last_price = answer / (10 ** self.decimals)
                                        data_hex = log.get("data", "0x")
                                        if len(data_hex) >= 66:
                                            self.last_updated_at = int(data_hex[2:66], 16) * 1000

                                        if self.on_update:
                                            await self.on_update({"price": self.last_price, "updatedAt": self.last_updated_at, "source": "chainlink_ws"})
                            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                                break
            except Exception as e:
                print(f"WS Error (Chainlink RPC): {e}")
                if not self.closed:
                    await asyncio.sleep(2)

    def get_last(self):
        return {"price": self.last_price, "updatedAt": self.last_updated_at, "source": "chainlink_ws"}

    def close(self):
        self.closed = True
