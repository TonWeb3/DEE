import asyncio
import time
import json
import os
import math
from datetime import datetime
from typing import Dict, Any, Optional

from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, WebSocket
from fastapi.responses import HTMLResponse, FileResponse, JSONResponse
from fastapi.templating import Jinja2Templates

from bot.config import settings
import bot.data as data
import bot.ws_data as ws_data
import bot.chainlink as chainlink
import bot.indicators as indicators
import bot.engines as engines
import bot.utils as utils
from bot.clob_trader import clob_trader

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Load previous state
    load_state()
    load_telegram_subscribers()

    # Initial seeding
    await seed_kline_buffers()

    # Start all background tasks
    tasks = [
        asyncio.create_task(binance_stream.start()),
        asyncio.create_task(binance_kline_1m.start()),
        asyncio.create_task(binance_kline_5m.start()),
        asyncio.create_task(polymarket_ws_stream.start()),
        asyncio.create_task(polymarket_clob_ws.start()),
        asyncio.create_task(chainlink_ws_stream.start()),
        asyncio.create_task(telegram_poller()),   # auto-subscribe: collect everyone who starts the bot
        asyncio.create_task(entry_watcher()),     # event-driven entries, off the 1 Hz clock
        asyncio.create_task(update_loop())
    ]

    yield

    # Shutdown cleanup
    for task in tasks:
        task.cancel()

    binance_stream.close()
    binance_kline_1m.close()
    binance_kline_5m.close()
    polymarket_ws_stream.close()
    polymarket_clob_ws.close()
    chainlink_ws_stream.close()

app = FastAPI(title="Polymarket BTC 15m Assistant", lifespan=lifespan)
templates = Jinja2Templates(directory="templates")

# Global state to store the latest data
state = {
    "latest_data": {},
    "last_update_ts": 0,
    "trading_mode": settings.MODE,
    "paper_balance": settings.PAPER_BALANCE_USD,
    "active_trades": [],
    "trade_history": [],
    "logs": [],
    "last_trade_side": None,
    "last_balance_refresh": 0,
    # Trading is OFF until the user presses Start on the dashboard. Data/prices still
    # stream; this flag only gates ENTRIES (open positions always settle to expiry).
    "running": False,
    # Per-window marked opens, keyed by the market's own eventStartTime (ms):
    #   {start_ms: {"chainlink": float|None, "binance": float|None,
    #               "close": float|None, "genuine": bool}}
    # "chainlink" is the SETTLEMENT strike; "binance" is the model's reference open.
    "market_opens": {},
    "last_window_start": None,
    "last_seen_price": None,
    # ── Auto-withdrawal (capital extractor) state machine ────────────────────────
    #   ARMED -> (equity >= trigger) WAITING_FLAT -> (no open trades) WITHDRAWING
    #         -> WITHDRAW_SUBMITTED -> ARMED (+ resume)
    "withdraw_state": "ARMED",
    "last_withdrawal": None,         # {"amount","tx","to","time"} of the most recent one
    "withdraw_flat_since": None,     # when the account went flat, so the sell can settle
    "withdraw_submitted_at": 0,      # for the "confirmed" resume mode's timeout
    "withdraw_locked_market": None,  # after a withdrawal, wait for the NEXT market
    "telegram_subscribers": {},      # {chat_id: {name,type,added}} — auto-collected
    # Published each housekeeping tick, consumed by the event-driven entry path.
    "trade_ctx": {},
    "event_exec": None,              # an event-path entry's reason, for the next CSV row
    "log_seq": 0,                    # bumped per log line; lets a push say "new logs"
}

def save_state():
    try:
        data_to_save = {
            "paper_balance": state["paper_balance"],
            "active_trades": state["active_trades"],
            "trade_history": state["trade_history"],
            "last_trade_side": state["last_trade_side"]
        }
        with open("state_data.json", "w") as f:
            json.dump(data_to_save, f, indent=2)
            
        if os.path.exists("config.json"):
            with open("config.json", "r") as f:
                cfg = json.load(f)
            cfg["paper_balance_usd"] = state["paper_balance"]
            with open("config.json", "w") as f:
                json.dump(cfg, f, indent=2)
    except Exception as e:
        print(f"Error saving state: {e}")

def load_state():
    try:
        if os.path.exists("state_data.json"):
            with open("state_data.json", "r") as f:
                loaded = json.load(f)
                state["paper_balance"] = loaded.get("paper_balance", settings.PAPER_BALANCE_USD)
                state["active_trades"] = loaded.get("active_trades", [])
                state["trade_history"] = loaded.get("trade_history", [])
                state["last_trade_side"] = loaded.get("last_trade_side")
                log_message("State loaded from state_data.json")
    except Exception as e:
        print(f"Error loading state: {e}")

def log_message(msg: str):
    timestamp = datetime.now().strftime("%H:%M:%S")
    formatted = f"[{timestamp}] {msg}"
    print(formatted)
    state["logs"].append(formatted)
    if len(state["logs"]) > 100:
        state["logs"].pop(0)
    # Bumped so a pushed snapshot can tell the dashboard "there are new log lines"
    # without carrying the whole 100-line buffer in every frame.
    state["log_seq"] += 1


# ── Dashboard push ───────────────────────────────────────────────────────────
# The dashboard used to poll /api/latest once a second, on top of the loop's own
# 1 Hz rebuild — so a value could be up to ~2s old on screen. Now the server pushes
# the snapshot the moment it is rebuilt, and again immediately after any discrete
# event (an entry, Start/Stop), so those appear at once rather than on the next poll.
_ws_clients = set()


async def broadcast_state():
    """Push the current snapshot to every connected dashboard. Best-effort: a client
    that errors is dropped, and a failure here must never disturb trading."""
    if not _ws_clients or not state["latest_data"]:
        return
    # Stamped at SEND time, not at rebuild time: a push triggered between ticks (an
    # entry, Start/Stop) carries the snapshot the loop last built, whose log_seq would
    # otherwise be stale — and the client uses it to decide whether to refetch logs.
    state["latest_data"]["log_seq"] = state["log_seq"]
    dead = []
    for ws in list(_ws_clients):
        try:
            await ws.send_json(state["latest_data"])
        except Exception:
            dead.append(ws)
    for ws in dead:
        _ws_clients.discard(ws)

SUBSCRIBERS_FILE = "telegram_subscribers.json"

def load_telegram_subscribers():
    """Load the saved subscribers (everyone who has started the bot) from disk."""
    try:
        if os.path.exists(SUBSCRIBERS_FILE):
            with open(SUBSCRIBERS_FILE, "r") as f:
                subs = json.load(f)
            if isinstance(subs, dict):
                state["telegram_subscribers"] = {str(k): v for k, v in subs.items()}
    except Exception as e:
        print(f"Error loading telegram subscribers: {e}")

def save_telegram_subscribers():
    try:
        with open(SUBSCRIBERS_FILE, "w") as f:
            json.dump(state["telegram_subscribers"], f, indent=2)
    except Exception as e:
        print(f"Error saving telegram subscribers: {e}")

def add_telegram_subscriber(chat: dict) -> bool:
    """Add a chat (user/group/channel) to the subscriber list. Returns True if new."""
    cid = str(chat.get("id"))
    if not cid or cid == "None":
        return False
    if cid in state["telegram_subscribers"]:
        return False
    state["telegram_subscribers"][cid] = {
        "name": chat.get("username") or chat.get("title") or chat.get("first_name") or cid,
        "type": chat.get("type"),
        "added": datetime.now().isoformat(),
    }
    save_telegram_subscribers()
    return True

def remove_telegram_subscriber(chat_id: str) -> bool:
    if str(chat_id) in state["telegram_subscribers"]:
        del state["telegram_subscribers"][str(chat_id)]
        save_telegram_subscribers()
        return True
    return False

async def send_telegram(text: str):
    """Best-effort Telegram alert, BROADCAST to every saved subscriber. No-op unless
    enabled, a bot token is set, and there is at least one subscriber. Never raises —
    a failed alert must not affect trading."""
    ids = list(state["telegram_subscribers"].keys())
    if not (settings.TELEGRAM_ENABLED and settings.TELEGRAM_BOT_TOKEN and ids):
        return
    url = f"https://api.telegram.org/bot{settings.TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        import httpx
        from bot.net_utils import get_proxy_url_for
        proxy = get_proxy_url_for(url)
        async with httpx.AsyncClient(proxy=proxy if proxy else None, timeout=10.0) as client:
            for cid in ids:
                try:
                    resp = await client.post(url, json={
                        "chat_id": cid,
                        "text": text,
                        "parse_mode": "HTML",
                        "disable_web_page_preview": True,
                    })
                    if resp.status_code != 200:
                        log_message(f"Telegram alert to {cid} failed: HTTP {resp.status_code} {resp.text[:100]}")
                except Exception as e:
                    log_message(f"Telegram alert to {cid} error: {e}")
    except Exception as e:
        log_message(f"Telegram alert error: {e}")

async def send_telegram_to(chat_id: str, text: str):
    """Send one message to a single chat id (used for subscribe/unsubscribe replies)."""
    if not settings.TELEGRAM_BOT_TOKEN:
        return
    url = f"https://api.telegram.org/bot{settings.TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        import httpx
        from bot.net_utils import get_proxy_url_for
        proxy = get_proxy_url_for(url)
        async with httpx.AsyncClient(proxy=proxy if proxy else None, timeout=10.0) as client:
            await client.post(url, json={"chat_id": chat_id, "text": text, "parse_mode": "HTML"})
    except Exception:
        pass

async def telegram_poller():
    """Continuously read the bot's incoming messages (long-poll getUpdates) and
    auto-subscribe anyone who messages it. `/stop` (or `/unsubscribe`) removes them.
    Runs for the whole app lifetime; idle while Telegram is disabled or has no token."""
    offset = None
    while True:
        try:
            if not (settings.TELEGRAM_ENABLED and settings.TELEGRAM_BOT_TOKEN):
                await asyncio.sleep(5)
                continue
            import httpx
            from bot.net_utils import get_proxy_url_for
            url = f"https://api.telegram.org/bot{settings.TELEGRAM_BOT_TOKEN}/getUpdates"
            proxy = get_proxy_url_for(url)
            params = {"timeout": 25, "allowed_updates": json.dumps(["message", "my_chat_member", "channel_post"])}
            if offset is not None:
                params["offset"] = offset
            async with httpx.AsyncClient(proxy=proxy if proxy else None, timeout=35.0) as client:
                data = (await client.get(url, params=params)).json()
            if not data.get("ok"):
                await asyncio.sleep(5)
                continue
            for u in data.get("result", []):
                offset = u["update_id"] + 1
                obj = u.get("message") or u.get("channel_post") or u.get("my_chat_member") or {}
                chat = obj.get("chat") or {}
                if chat.get("id") is None:
                    continue
                text = (obj.get("text") or "").strip().lower()
                if text in ("/stop", "/unsubscribe"):
                    if remove_telegram_subscriber(str(chat["id"])):
                        log_message(f"Telegram: {chat.get('username') or chat['id']} unsubscribed")
                        await send_telegram_to(str(chat["id"]), "🔕 You've unsubscribed from withdrawal alerts.")
                else:
                    if add_telegram_subscriber(chat):
                        log_message(f"Telegram: new subscriber {chat.get('username') or chat.get('title') or chat['id']} ({chat.get('type')})")
                        await send_telegram_to(str(chat["id"]), "🔔 Subscribed — you'll receive withdrawal alerts here. Send /stop to unsubscribe.")
        except Exception as e:
            log_message(f"Telegram poller error: {e}")
            await asyncio.sleep(5)

def get_ws_symbol_filter(symbol: str) -> str:
    s = symbol.upper()
    if s.endswith("USDT"):
        return s[:-4].lower()
    return s.lower()

# ── Event-driven entry primitives ────────────────────────────────────────────
# Defined before the streams because they are handed to them as callbacks.
# The path that uses them lives further down, next to update_loop.
MIN_EVAL_INTERVAL_S = 0.05   # coalesce bursts; 20 evaluations/sec is plenty
CTX_MAX_AGE_S = 5.0          # housekeeping context older than this is not tradable

_market_event = asyncio.Event()
_entry_lock = asyncio.Lock()   # execute_trade is not re-entrant: the slot check and
                               # the append must not interleave between the two paths
_last_eval_ts = 0.0


def _wake_entry(_payload=None):
    """Synchronous hook handed to both streams. Deliberately does nothing but set a
    flag — the work happens in entry_watcher, so a burst of ticks collapses into one
    evaluation instead of spawning a task each."""
    _market_event.set()


# Background task instances
binance_stream = ws_data.BinanceTradeStream(symbol=settings.SYMBOL, on_update=_wake_entry)
binance_kline_1m = ws_data.BinanceKlineStream(symbol=settings.SYMBOL, interval="1m", limit=240)
binance_kline_5m = ws_data.BinanceKlineStream(symbol=settings.SYMBOL, interval="5m", limit=200)

polymarket_ws_stream = ws_data.PolymarketChainlinkStream(
    ws_url=settings.POLYMARKET_LIVE_DATA_WS_URL,
    symbol_includes=get_ws_symbol_filter(settings.SYMBOL)
)
chainlink_ws_stream = ws_data.ChainlinkPriceStream(aggregator=settings.get_aggregator(settings.SYMBOL))
# Live CLOB order books for the active window's two tokens. Subscribed per market by
# fetch_polymarket_snapshot(), so it needs no restart when the symbol/series changes.
polymarket_clob_ws = ws_data.PolymarketClobBookStream(ws_url=settings.POLYMARKET_CLOB_WS_URL,
                                                     on_update=_wake_entry)

def get_candle_window_timing(window_minutes: int) -> Dict[str, float]:
    now_ms = time.time() * 1000
    window_ms = window_minutes * 60_000
    start_ms = (now_ms // window_ms) * window_ms
    end_ms = start_ms + window_ms
    elapsed_ms = now_ms - start_ms
    remaining_ms = end_ms - now_ms
    return {
        "startMs": start_ms,
        "endMs": end_ms,
        "elapsedMs": elapsed_ms,
        "remainingMs": remaining_ms,
        "elapsedMinutes": elapsed_ms / 60_000,
        "remainingMinutes": remaining_ms / 60_000
    }

async def fetch_polymarket_snapshot() -> Dict[str, Any]:
    market = None
    if settings.POLYMARKET_SLUG:
        market = await data.fetch_market_by_slug(settings.POLYMARKET_SLUG)
    elif settings.POLYMARKET_AUTO_SELECT_LATEST:
        events = await data.fetch_live_events_by_series_id(settings.POLYMARKET_SERIES_ID)
        markets = data.flatten_event_markets(events)

        now = time.time() * 1000
        live_markets = [m for m in markets if m.get("endDate") and datetime.fromisoformat(m["endDate"].replace('Z', '+00:00')).timestamp() * 1000 > now]
        if live_markets:
            live_markets.sort(key=lambda x: x["endDate"])
            market = live_markets[0]

    if not market:
        return {"ok": False, "reason": "market_not_found"}

    outcomes = market.get("outcomes", [])
    if isinstance(outcomes, str):
        outcomes = json.loads(outcomes)

    clob_token_ids = market.get("clobTokenIds", [])
    if isinstance(clob_token_ids, str):
        clob_token_ids = json.loads(clob_token_ids)

    outcome_prices = market.get("outcomePrices", [])
    if isinstance(outcome_prices, str):
        outcome_prices = json.loads(outcome_prices)

    up_token_id = None
    down_token_id = None

    for i, outcome in enumerate(outcomes):
        token_id = clob_token_ids[i] if i < len(clob_token_ids) else None
        if not token_id: continue
        if outcome.lower() == settings.POLYMARKET_UP_LABEL.lower():
            up_token_id = token_id
        elif outcome.lower() == settings.POLYMARKET_DOWN_LABEL.lower():
            down_token_id = token_id

    up_index = next((i for i, x in enumerate(outcomes) if x.lower() == settings.POLYMARKET_UP_LABEL.lower()), -1)
    down_index = next((i for i, x in enumerate(outcomes) if x.lower() == settings.POLYMARKET_DOWN_LABEL.lower()), -1)

    gamma_yes = float(outcome_prices[up_index]) if up_index >= 0 and up_index < len(outcome_prices) else None
    gamma_no = float(outcome_prices[down_index]) if down_index >= 0 and down_index < len(outcome_prices) else None

    if not up_token_id or not down_token_id:
        return {"ok": False, "reason": "missing_token_ids"}

    # ── The price we must pay to BUY is the best ASK ─────────────────────────────
    # `/price?side=X` quotes the best resting order ON side X of the book, it does NOT
    # mean "the price to X at". Measured against the live book, 36/36 simultaneous
    # samples: side=buy == bestBid, side=sell == bestAsk. Quoting side=buy therefore
    # returned the BID — the price we would SELL at — which overstated every EV by the
    # full spread and, on a wide book, recorded fills that were never obtainable (a
    # 0.11 "entry" while the ask stood at 0.89 credits 9x the shares actually buyable).
    # Take bestAsk from the book we already fetch, and fall back to side=sell.
    # ── The book: websocket first, REST only as a fallback ───────────────────────
    # The book is the half of the edge calculation this strategy is racing, so polling
    # it at 1 Hz while the spot feed is pushed measures the race on the slower clock.
    # `get_summary` returns None when the socket has no usable book for a token —
    # unsubscribed, never delivered, or STALE past MAX_BOOK_AGE_S — and only then do we
    # pay for a REST round trip. The fallback has to be able to fire: a stale-but-present
    # book that still answered here would pin the bot to a frozen order book forever.
    polymarket_clob_ws.update_assets([up_token_id, down_token_id])
    max_age = settings.MAX_BOOK_AGE_S
    up_book_summary = polymarket_clob_ws.get_summary(up_token_id, max_age_s=max_age) if max_age else None
    down_book_summary = polymarket_clob_ws.get_summary(down_token_id, max_age_s=max_age) if max_age else None
    # Which side came from the socket is decided BEFORE the fallback fills the gaps —
    # afterwards both are non-None and the distinction is gone.
    ws_up, ws_down = up_book_summary is not None, down_book_summary is not None
    book_source = "ws" if (ws_up and ws_down) else ("mixed" if (ws_up or ws_down) else "rest")

    up_sell = down_sell = None
    if not (ws_up and ws_down):
        try:
            up_sell, down_sell, up_book, down_book = await asyncio.gather(
                data.fetch_clob_price(up_token_id, "sell"),
                data.fetch_clob_price(down_token_id, "sell"),
                data.fetch_order_book(up_token_id) if up_book_summary is None else asyncio.sleep(0, result=None),
                data.fetch_order_book(down_token_id) if down_book_summary is None else asyncio.sleep(0, result=None)
            )
            if up_book_summary is None and up_book is not None:
                up_book_summary = data.summarize_order_book(up_book)
            if down_book_summary is None and down_book is not None:
                down_book_summary = data.summarize_order_book(down_book)
        except Exception:
            up_sell = None
            down_sell = None
        _empty = {"bestBid": None, "bestAsk": None, "spread": None,
                  "bidLiquidity": None, "askLiquidity": None, "askLevels": [], "bidLevels": []}
        if up_book_summary is None:
            up_book_summary = dict(_empty)
        if down_book_summary is None:
            down_book_summary = dict(_empty)

    # bestAsk first (same tick as the liquidity we size against), then side=sell, then
    # Gamma's last price. Gamma is a LAST-TRADE mark, not an executable offer, so it is
    # a display fallback only and is deliberately last.
    up_ask = up_book_summary.get("bestAsk") or up_sell or gamma_yes
    down_ask = down_book_summary.get("bestAsk") or down_sell or gamma_no

    return {
        "ok": True,
        "market": market,
        "prices": {
            "up": up_ask,
            "down": down_ask
        },
        "token_ids": {
            "up": up_token_id,
            "down": down_token_id
        },
        "orderbook": {
            "up": up_book_summary,
            "down": down_book_summary
        },
        # "ws" | "rest" | "mixed" — surfaced so a silent fall back to polling is
        # visible on the dashboard rather than something you only find in a latency post-mortem.
        "book_source": book_source
    }

async def execute_trade(decision: Dict[str, Any], market_prices: Dict[str, Any], market: Dict[str, Any], strike_open: Optional[float], token_ids: Dict[str, Any], orderbook: Optional[Dict[str, Any]] = None,
                        strike_source: str = "chainlink_ws", window_start_ms: Optional[int] = None,
                        open_reason: str = "ev_entry"):
    # Regular entry from decision engine. Returns a short reason string describing
    # the outcome (entered / which gate vetoed it) for diagnostic logging.
    #
    # `strike_open` is the SETTLEMENT strike — the Chainlink price latched at the
    # market's eventStartTime. It is what update_trades() scores the close against, so
    # it must come from the same feed as the close, NOT from the model's Binance open.
    if decision["action"] != "ENTER":
        return decision.get("reason", "no_trade")

    # CONSTRAINT: Only one position at a time
    if state["active_trades"]:
        return "slot_busy"

    # No authoritative strike (the Chainlink open at this market's eventStartTime was
    # never captured) => no trade. Scoring against a guessed open is worse than sitting
    # the window out, so there is deliberately no Binance/spot fallback here.
    if strike_open is None:
        return "no_strike"

    side = decision["side"]

    price = market_prices["up"] if side == "UP" else market_prices["down"]
    if price is None:
        return "no_price"

    # ── Risk per trade ──────────────────────────────────────────────────────────
    # No flat price cap (EV already governs reward/risk). RISK_TYPE selects how the
    # stake (the dollars put at risk) is sized:
    #   "percent" -> RISK_VALUE% of the current balance
    #   "fixed"   -> RISK_VALUE dollars, flat
    balance = state["paper_balance"]
    risk_type = (settings.RISK_TYPE or "percent").lower()
    if risk_type == "fixed":
        amount_to_risk = float(settings.RISK_VALUE)
    else:  # "percent" (default)
        amount_to_risk = (float(settings.RISK_VALUE) / 100.0) * balance

    if amount_to_risk <= 0:
        return "stake_zero"

    # Liquidity: never outsize what the ask side of the book can absorb. The levels are
    # now ordered from the touch outward, so this is depth we could actually hit.
    ob = (orderbook or {}).get("up" if side == "UP" else "down") or {}
    ask_levels = ob.get("askLevels") or []
    ask_liq_usd = sum(p * s for p, s in ask_levels)
    if ask_levels:
        if ask_liq_usd < settings.MIN_BOOK_LIQUIDITY_USD:
            log_message(f"Skip {side}: thin book (${ask_liq_usd:.2f} ask liquidity)")
            return "thin_book"
        amount_to_risk = min(amount_to_risk, ask_liq_usd)  # don't outsize the book

    if balance < amount_to_risk or amount_to_risk <= 0:
        print(f"Insufficient paper balance ({balance}) or invalid risk amount ({amount_to_risk})")
        return "insufficient_balance"

    end_date_str = market.get("endDate")
    end_ts = 0
    if end_date_str:
        try:
            end_ts = datetime.fromisoformat(end_date_str.replace('Z', '+00:00')).timestamp()
        except: pass
    # Fallback so a trade always has a definite expiry even if endDate is missing/unparseable
    if not end_ts:
        end_ts = time.time() + settings.CANDLE_WINDOW_MINUTES * 60

    trade = {
        "market_id": market["id"],
        "market_slug": market.get("slug"),
        "side": side,
        "entry_price": price,
        "amount": amount_to_risk,
        "shares": amount_to_risk / price,
        "entry_time": datetime.now().isoformat(),
        "status": "OPEN",
        "settlement_price": None,
        "profit_loss": None,
        "strike_price": strike_open,       # SETTLEMENT strike: Chainlink @ eventStartTime
        "strike_source": strike_source,
        "window_start_ms": int(window_start_ms) if window_start_ms is not None else None,
        "open_reason": open_reason,        # "ev_entry" | "flip_entry"
        "close_price": None,               # frozen once, the instant the window expires
        "end_ts": end_ts,
        "mode": state["trading_mode"]
    }

    if state["trading_mode"] == "paper":
        state["paper_balance"] -= amount_to_risk
        state["active_trades"].append(trade)
        state["last_trade_side"] = side
        save_state()

        log_message(f"Executed PAPER trade: {side} @ {price} for {market.get('slug')} (Amount: ${amount_to_risk:.2f})")
        return "entered"
    else:
        # LIVE: place a real Fill-Or-Kill market BUY on the Polymarket CLOB
        token_id = token_ids.get("up") if side == "UP" else token_ids.get("down")
        if not token_id:
            log_message(f"LIVE trade aborted: missing token_id for side {side}")
            return "missing_token_id"

        result = await asyncio.to_thread(clob_trader.place_market_buy, token_id, amount_to_risk, price)
        if result.get("ok"):
            trade["order_id"] = result.get("order_id")
            trade["order_response"] = result.get("response") or {}
            trade["token_id"] = token_id
            # Record what ACTUALLY filled, not what we asked for. A Fill-Or-Kill can
            # fill anywhere up to the limit price, so shares != amount / quote. Using
            # the estimate here would misprice every live position and its P/L.
            fill_size = result.get("fill_size")
            fill_price = result.get("fill_price")
            fill_usd = result.get("fill_usd")
            if fill_size and fill_price:
                trade["shares"] = float(fill_size)
                trade["entry_price"] = float(fill_price)
                trade["amount"] = float(fill_usd if fill_usd else fill_size * fill_price)
                trade["quoted_price"] = price          # what we saw before sending
                trade["slippage"] = float(fill_price) - float(price) if price else None
            state["active_trades"].append(trade)
            state["last_trade_side"] = side
            save_state()
            log_message(
                f"Executed LIVE trade: {side} ${trade['amount']:.2f} on {market.get('slug')} "
                f"— {trade['shares']:.2f} shares @ {trade['entry_price']:.4f} "
                f"(quote {price}, order {trade['order_id']})")
            return "entered"
        else:
            # A KILLED Fill-Or-Kill lands here and opens NO position — which is the
            # point: previously an unfilled order was recorded as a live trade.
            log_message(f"LIVE trade FAILED ({side}): {result.get('error')}")
            return "live_order_failed"

async def maybe_flip_position(decision: Dict[str, Any], poly_snapshot: Dict[str, Any], time_left_min: Optional[float]):
    """Close the open position early and flip when a STRONG opposite signal appears.

    Opt-in (FLIP_ENABLED). Guards: the new side must clear FLIP_MIN_CONVICTION and at
    least FLIP_MIN_MINUTES_LEFT must remain, and we only flip within the same market.
    After closing here, execute_trade() opens the new side (slot is now free).
    """
    if not settings.FLIP_ENABLED:
        return
    if decision.get("action") != "ENTER" or not state["active_trades"]:
        return

    new_side = decision["side"]
    new_prob = decision.get("prob", 0) or 0
    if new_prob < settings.FLIP_MIN_CONVICTION:
        return
    if time_left_min is not None and time_left_min < settings.FLIP_MIN_MINUTES_LEFT:
        return

    market = poly_snapshot["market"]
    prices = poly_snapshot["prices"]
    token_ids = poly_snapshot.get("token_ids", {})
    orderbook = poly_snapshot.get("orderbook", {})

    trade = state["active_trades"][0]
    if trade["side"] == new_side:
        return  # already on the signalled side
    if str(trade.get("market_id")) != str(market.get("id")):
        return  # different market — let the old one settle on its own

    held_key = "up" if trade["side"] == "UP" else "down"
    ob = orderbook.get(held_key) or {}
    exit_price = ob.get("bestBid") or prices.get(held_key)
    if not exit_price or exit_price <= 0:
        log_message(f"FLIP aborted: no exit price for {trade['side']}")
        return

    if state["trading_mode"] == "live":
        token_id = token_ids.get(held_key)
        result = await asyncio.to_thread(clob_trader.place_market_sell, token_id, trade["shares"], exit_price)
        if not result.get("ok"):
            # Killed FOK => we still HOLD the position. Leave it open and let it settle
            # at expiry rather than recording a close that never happened.
            log_message(f"FLIP sell FAILED ({trade['side']}): {result.get('error')} — position kept")
            return
        # Price the exit off the REAL fill, not the quote we aimed at.
        if result.get("fill_price"):
            exit_price = float(result["fill_price"])
        trade["exit_order_id"] = result.get("order_id")
        # live balance is refreshed from chain elsewhere
    else:
        state["paper_balance"] += trade["shares"] * exit_price  # proceeds from selling out

    trade["status"] = "CLOSED"
    trade["exit_time"] = datetime.now().isoformat()
    trade["exit_reason"] = "flip"
    trade["resolution"] = "flip_exit"
    trade["settlement_price_at_expiry"] = exit_price
    # A flip exits on the BOOK, not at expiry — so there is no window close price.
    # Record the marked open and the BTC price we bailed at, for the history table.
    trade["open_price"] = trade.get("strike_price")
    trade["close_price"] = state.get("last_seen_price")
    trade["profit_loss"] = (trade["shares"] * exit_price) - trade["amount"]
    state["trade_history"].append(_archive(trade))
    state["active_trades"] = [t for t in state["active_trades"] if t is not trade]
    state["last_trade_side"] = None
    save_state()
    log_message(f"FLIP: closed {trade['side']} @ {exit_price:.2f} (P/L ${trade['profit_loss']:.2f}); opening {new_side}")
    return new_side   # truthy => the entry that follows is a flip entry, not a fresh EV entry

MARK_CAPTURE_WINDOW_MS = 20_000   # how late after eventStartTime a latch still counts

# How old a feed value may be before it stops counting as "the price right now".
# Polymarket's crypto_prices_chainlink stream ticks about once a second, so a value
# older than this means the socket is dead, not that the market is quiet. The on-chain
# sources report the aggregator's ROUND timestamp, which is legitimately minutes old
# between posts, so theirs is only a liveness check.
POLY_WS_MAX_AGE_MS = 10_000
ONCHAIN_MAX_AGE_MS = 15 * 60_000


def mark_window_open(start_ms: int, window_ms: int, current_price: Optional[float],
                     spot_price: Optional[float], price_source: Optional[str],
                     price_is_fresh: bool = True) -> Dict[str, Any]:
    """Latch this window's OPEN at the market's own eventStartTime and return its record.

    Two values are captured at the SAME instant:
      "chainlink" -> the SETTLEMENT strike, the price Polymarket resolves against
      "binance"   -> the MODEL's reference open, so fair_prob keeps measuring the
                     Binance move since the open exactly as it always has
    Mixing the feeds (Binance spot vs a Chainlink open) would inject a constant
    ~0.13% offset straight into the model, which on a 15m window is the same order
    of magnitude as the move being predicted.

    A window is only marked when `genuine` — i.e. we were already running through the
    immediately-preceding window, so the first price we see in this one really is its
    open. Otherwise "chainlink" stays None and the caller opens no trade: no API can
    fetch a past open, and scoring against a guess is worse than sitting the window out.
    """
    opens = state["market_opens"]
    prev_ws = state.get("last_window_start")

    # On rollover, freeze the PRIOR window's close = the last price seen inside it.
    if prev_ws is not None and prev_ws != start_ms and prev_ws in opens:
        if opens[prev_ws].get("close") is None and state.get("last_seen_price"):
            opens[prev_ws]["close"] = state["last_seen_price"]
    if current_price:
        state["last_seen_price"] = current_price

    observed_prev = prev_ws is not None and abs((start_ms - window_ms) - prev_ws) < 2000
    if start_ms not in opens:
        opens[start_ms] = {"chainlink": None, "binance": None,
                           "close": None, "genuine": observed_prev}
        for k in list(opens.keys()):           # prune old windows
            if k < start_ms - 4 * window_ms:
                del opens[k]

    win = opens[start_ms]
    # `since_start` must be within [0, MARK_CAPTURE_WINDOW_MS): too late and the price
    # is no longer the open; NEGATIVE means eventStartTime is still in the future (the
    # market hasn't begun) and latching would freeze a pre-open price as the strike.
    since_start = time.time() * 1000 - start_ms
    # `price_is_fresh` is the point of the whole guard: being inside the capture window
    # says OUR CLOCK is near the open, it says nothing about how old the PRICE is. A
    # stalled socket returns its last value forever, and latching that freezes a strike
    # from minutes earlier - measured at a median of 22s before the window open across
    # 113 live trades, worst case $86 off the true tick.
    if (win["chainlink"] is None and win["genuine"] and current_price and price_is_fresh
            and 0 <= since_start < MARK_CAPTURE_WINDOW_MS):
        win["chainlink"] = current_price
        win["binance"] = spot_price
        log_message(f"Window open marked @ eventStartTime: Chainlink {current_price:.2f} "
                    f"({price_source}) / Binance {spot_price if spot_price else '-'}")
    state["last_window_start"] = start_ms
    return win


async def _redeem_win(trade: Dict[str, Any], market: Optional[Dict[str, Any]],
                      up_index: int, down_index: int, winning_index: int):
    """Redeem a winning LIVE position into pUSD.

    Winning outcome tokens are CTF conditional tokens worth $1 each; they only become
    spendable collateral once redeemed. `get_usdc_balance()` reads pUSD and cannot see
    them, so without this a live win never shows up in the balance.

    Best-effort: the result is recorded on the trade either way, and a failure is
    logged rather than raised — settlement must never be blocked by a redeem problem.
    """
    condition_id = (market or {}).get("conditionId") or (market or {}).get("condition_id")
    if not condition_id:
        trade["redeem"] = {"ok": False, "error": "missing_condition_id"}
        log_message(f"REDEEM skipped for {trade['market_slug']}: no conditionId on the market")
        return

    # The CTF expects one amount per outcome, in index order; the losing leg is 0.
    amounts = [0.0, 0.0]
    idx = up_index if winning_index == up_index else down_index
    if 0 <= idx < len(amounts):
        amounts[idx] = float(trade.get("shares") or 0.0)

    neg_risk = bool((market or {}).get("negRisk") or (market or {}).get("neg_risk") or False)
    try:
        res = await asyncio.to_thread(clob_trader.redeem, condition_id, amounts, neg_risk)
    except Exception as e:
        res = {"ok": False, "error": f"{type(e).__name__}: {e}"}

    trade["redeem"] = res
    if res.get("ok"):
        log_message(f"REDEEM ok for {trade['market_slug']}: {amounts[idx]:.2f} shares (tx {res.get('tx')})")
    else:
        log_message(f"REDEEM FAILED for {trade['market_slug']}: {res.get('error')} "
                    f"— redeem manually on Polymarket to free the capital")


def _archive(trade: Dict[str, Any]) -> Dict[str, Any]:
    """Strip bulky/internal scratch keys before a trade goes into trade_history.

    `_market` caches a whole Gamma market payload while the trade is open; writing
    that into state_data.json every settle would bloat the file for no benefit.
    """
    for k in ("_market", "_market_closed", "order_response"):
        trade.pop(k, None)
    return trade


def exit_price_for(poly_snapshot: Dict[str, Any], side: str, shares: float):
    """The all-in price selling `shares` would fetch on the bid side, by walking the
    book rather than valuing the whole position at the touch. Returns
    (avg_price, shares_sellable, proceeds)."""
    key = "up" if side == "UP" else "down"
    ob = (poly_snapshot.get("orderbook") or {}).get(key) or {}
    levels = ob.get("bidLevels") or []
    avg, sold, proceeds = data.sweep_sell(levels, shares)
    if avg is None:
        return ob.get("bestBid"), 0.0, 0.0
    return avg, sold, proceeds


async def close_open_position(poly_snapshot: Dict[str, Any], reason: str):
    """Sell the open position into the bid side and book the realized P/L. Used by the
    auto-withdrawal to go flat before extracting funds. Returns
    {"side","exit_price","pl"} on success, else None.

    Live sells are RETRY-BOUNDED (`EXIT_MAX_RETRIES`): without a bound, a rejected FOK
    becomes a fresh sell order on every tick, chasing the book down. When the retries
    are spent the position simply settles at expiry as it normally would.
    """
    if not state["active_trades"] or not poly_snapshot.get("ok"):
        return None
    trade = state["active_trades"][0]
    market = poly_snapshot["market"]
    if str(trade.get("market_id")) != str(market.get("id")):
        return None  # position is in a prior market — let it settle on its own

    attempts = trade.get("exit_attempts", {}).get(reason, 0)
    if settings.EXIT_MAX_RETRIES > 0 and attempts >= settings.EXIT_MAX_RETRIES:
        return None  # give up on this exit; the position settles at expiry

    token_ids = poly_snapshot.get("token_ids", {})
    held_key = "up" if trade["side"] == "UP" else "down"
    exit_price, sellable, _ = exit_price_for(poly_snapshot, trade["side"], trade["shares"])
    if not exit_price or exit_price <= 0:
        return None
    # A Fill-Or-Kill sell of the whole position is KILLED outright if the bid side
    # cannot absorb it, so don't pretend otherwise — in paper that would credit
    # proceeds the book could never pay, and in live it just burns a retry.
    if sellable < trade["shares"] * 0.999:
        return None

    if state["trading_mode"] == "live":
        token_id = token_ids.get(held_key)
        result = await asyncio.to_thread(clob_trader.place_market_sell, token_id,
                                         trade["shares"], exit_price)
        if not result.get("ok"):
            trade.setdefault("exit_attempts", {})[reason] = attempts + 1
            left = max(0, settings.EXIT_MAX_RETRIES - (attempts + 1))
            log_message(f"{reason} sell FAILED ({trade['side']}): {result.get('error')} "
                        f"— {left} attempt(s) left")
            if left == 0:
                log_message(f"{reason}: giving up on the early exit; holding {trade['side']} to expiry")
            save_state()
            return None
        # Book the REAL fill, not the quote we aimed at.
        if result.get("fill_price") and result.get("fill_size"):
            exit_price = float(result["fill_price"])
            proceeds = result.get("fill_usd") or (result["fill_size"] * exit_price)
        else:
            proceeds = trade["shares"] * exit_price
        trade["exit_order_id"] = result.get("order_id")
        state["last_balance_refresh"] = 0   # re-read the on-chain balance next tick
    else:
        proceeds = trade["shares"] * exit_price
        state["paper_balance"] += proceeds

    pl = proceeds - trade["amount"]
    side = trade["side"]
    trade["status"] = "CLOSED"
    trade["exit_time"] = datetime.now().isoformat()
    trade["exit_reason"] = reason
    trade["resolution"] = "early_exit"
    trade["settlement_price_at_expiry"] = exit_price
    trade["profit_loss"] = pl
    trade["exit_proceeds"] = proceeds
    # An early exit books its P/L on the SELL, not on the window's outcome. Record the
    # marked open and the BTC price we bailed at, for the history table.
    trade["open_price"] = trade.get("strike_price")
    trade["close_price"] = state.get("last_seen_price")
    state["trade_history"].append(_archive(trade))
    state["active_trades"] = [t for t in state["active_trades"] if t is not trade]
    state["last_trade_side"] = None
    save_state()
    return {"side": side, "exit_price": exit_price, "pl": pl}


async def maybe_auto_withdraw(equity: Optional[float], poly_snapshot: Dict[str, Any]):
    """Auto-withdrawal (capital extractor) state machine — LIVE mode only.

        ARMED --(EQUITY >= trigger)--> WAITING_FLAT --(close any open trade, go flat)-->
        WITHDRAWING --(submitted)--> WITHDRAW_SUBMITTED --> ARMED

    The trigger uses **equity** (cash + the value of any open position), so a running
    trade still counts toward the threshold. If a trade is open when the trigger fires
    it is CLOSED IMMEDIATELY (sold at the bid) so the balance settles into cash.

    After the withdrawal:
      - auto_resume ON  -> trading resumes at the NEXT 15m market (this one is locked).
      - auto_resume OFF -> the bot is STOPPED entirely.

    Paper mode never withdraws — there is nothing to withdraw — and the state machine
    is held disarmed so it can never pause entries in a mode it does not apply to.
    """
    if state["trading_mode"] != "live" or not settings.AUTO_WITHDRAW_ENABLED:
        if state["withdraw_state"] != "ARMED":   # disabled -> never keep entries paused
            state["withdraw_state"] = "ARMED"
        return

    st = state["withdraw_state"]
    cash = state["paper_balance"]  # the live pUSD balance is mirrored here

    if st == "ARMED":
        # Trigger on EQUITY, not just cash — so an open trade counts toward it.
        if equity is not None and equity >= settings.WITHDRAW_TRIGGER_BALANCE:
            state["withdraw_state"] = "WAITING_FLAT"
            state["withdraw_flat_since"] = None
            log_message(f"Auto-withdraw: equity ${equity:.2f} >= ${settings.WITHDRAW_TRIGGER_BALANCE:.2f} "
                        f"-> pausing entries and closing any open trade")

    elif st == "WAITING_FLAT":
        # Close the open position immediately so the funds settle into cash.
        if state["active_trades"]:
            res = await close_open_position(poly_snapshot, "withdraw_close")
            if res:
                log_message(f"Auto-withdraw: closed {res['side']} @ {res['exit_price']:.2f} "
                            f"(P/L ${res['pl']:.2f}) to go flat")
                state["withdraw_flat_since"] = time.time()
                state["last_balance_refresh"] = 0   # re-read the on-chain balance next tick
            return
        # Flat — give the sell a moment to settle on-chain before reading the balance.
        if state.get("withdraw_flat_since") is None:
            state["withdraw_flat_since"] = time.time()
            state["last_balance_refresh"] = 0
            return
        if time.time() - state["withdraw_flat_since"] < 5:
            return
        state["withdraw_flat_since"] = None
        state["withdraw_state"] = "WITHDRAWING"
        log_message("Auto-withdraw: account is flat -> withdrawing")

    elif st == "WITHDRAWING":
        # Destination: the user-set address, or your own wallet (the EOA derived from
        # the key/seed) when left blank.
        recipient = settings.WITHDRAW_ADDRESS or clob_trader.get_eoa_address()
        if not recipient:
            log_message("Auto-withdraw aborted: no wallet/key available. Disarming.")
            state["withdraw_state"] = "ARMED"
            return
        amount = min(float(settings.WITHDRAW_AMOUNT), float(cash or 0))
        if amount <= 0:
            log_message("Auto-withdraw aborted: no cash balance to withdraw. Disarming.")
            state["withdraw_state"] = "ARMED"
            return
        result = await asyncio.to_thread(clob_trader.withdraw_pusd, recipient, amount)
        if result.get("ok"):
            when = datetime.now()
            tx = result.get("tx")
            state["last_withdrawal"] = {"amount": result.get("amount"), "tx": tx,
                                        "to": result.get("recipient"), "time": when.isoformat()}
            state["withdraw_state"] = "WITHDRAW_SUBMITTED"
            state["withdraw_submitted_at"] = time.time()
            log_message(f"Auto-withdraw: submitted ${amount:.2f} -> {recipient} (tx {tx})")
            # Telegram alert: a withdrawal happened — time + amount.
            await send_telegram(
                "💸 <b>Withdrawal completed</b>\n"
                f"Amount: <b>${amount:.2f}</b>\n"
                f"Time: {when.strftime('%Y-%m-%d %H:%M:%S')}\n"
                f"To: <code>{recipient}</code>"
                + (f"\nTx: <code>{tx}</code>" if tx else "")
            )
        else:
            log_message(f"Auto-withdraw FAILED: {result.get('error')}. Disarming.")
            state["withdraw_state"] = "ARMED"

    elif st == "WITHDRAW_SUBMITTED":
        state["last_balance_refresh"] = 0  # fresh balance read so a stale value can't re-trigger
        if str(settings.WITHDRAW_RESUME_AFTER).lower() == "confirmed":
            tx = (state.get("last_withdrawal") or {}).get("tx")
            waited = time.time() - state.get("withdraw_submitted_at", time.time())
            confirmed = await asyncio.to_thread(clob_trader.is_tx_confirmed, tx) if tx else None
            if confirmed is not True and waited < 180:
                return   # keep waiting for the receipt (entries stay paused)
            if confirmed is True:
                log_message(f"Auto-withdraw: tx confirmed on-chain ({tx})")
            else:
                log_message(f"Auto-withdraw: no confirmation after {waited:.0f}s; resuming anyway")
        if not settings.WITHDRAW_AUTO_RESUME:
            state["running"] = False
            _reflect_running_now()
            log_message("Auto-withdraw complete; auto-resume OFF -> bot STOPPED.")
        else:
            # Resume, but not in the market we just exited — wait for the next 15m one.
            mkt_id = str(poly_snapshot["market"].get("id")) if poly_snapshot.get("ok") else None
            if mkt_id:
                state["withdraw_locked_market"] = mkt_id
            log_message("Auto-withdraw complete; trading resumes at the next 15m market.")
        state["withdraw_state"] = "ARMED"


async def update_trades(current_prices: Dict[str, Any]):
    remaining_active = []
    trades_changed = False
    now_ts = time.time()

    # Freshest price to settle against — the CLOSE price. Polymarket settles on
    # Chainlink, and the strike (open) is now the Chainlink WS value too, so prefer
    # Chainlink here: open and close then come from the SAME feed and no cross-feed
    # offset can flip a near-the-money result. Binance spot is a last resort only.
    cur_price = current_prices.get("chainlink") or current_prices.get("spot")
    SETTLEMENT_GRACE_SECONDS = 300  # if still unresolvable this long past expiry, void it

    for trade in state["active_trades"]:
        # Keep a rolling price snapshot so settlement always has a recent value,
        # even if the feed drops out exactly at expiry.
        if cur_price:
            trade["last_price"] = cur_price

        # Effective window end. If endDate was missing at entry (end_ts == 0), derive
        # it from entry_time + window so a trade can never wait forever.
        end_ts = trade.get("end_ts", 0)
        if not end_ts:
            try:
                end_ts = datetime.fromisoformat(trade["entry_time"]).timestamp() + settings.CANDLE_WINDOW_MINUTES * 60
            except Exception:
                end_ts = now_ts
        expired = now_ts >= end_ts

        # Freeze the CLOSE the instant the window ends. Polymarket settles on the
        # Chainlink value AT the close time — not whenever we happen to get around to
        # resolving (which can lag by many seconds). Capturing it once here stops
        # post-expiry drift from flipping a near-the-money win/loss.
        if expired and trade.get("close_price") is None:
            frozen_close = cur_price or trade.get("last_price")
            if frozen_close:
                trade["close_price"] = frozen_close

        # Poll the market for the AUTHORITATIVE Polymarket resolution. Before expiry
        # this is a cheap ~30s heartbeat; ONCE EXPIRED we poll every 3s, because the
        # official outcome is what we actually want to settle on.
        #
        # This used to be a flat 15s poll with `market` reset to None each tick, which
        # meant that on the very tick where `expired` first became true `market` was
        # almost always None -> no outcome prices -> the close-vs-open fallback below
        # resolved and CLOSED the trade immediately. The "authoritative first" priority
        # was therefore dead code, and every trade got scored against our own captured
        # strike rather than Polymarket's published outcome. Caching the last fetched
        # market on the trade and polling fast after expiry fixes that.
        market = trade.get("_market")
        poll_every = 3.0 if expired else 30.0
        if trade.get("last_api_check", 0) < now_ts - poll_every:
            try:
                fetched = await data.fetch_market_by_slug(trade["market_slug"])
            except Exception:
                fetched = None
            trade["last_api_check"] = now_ts
            if fetched is not None:
                market = fetched
                trade["_market"] = fetched
                trade["_market_closed"] = bool(fetched.get("closed"))
        market_closed = trade.get("_market_closed", False)

        # Still live: window running and market still open -> keep waiting.
        if not expired and not market_closed:
            remaining_active.append(trade)
            continue

        # ---- Determine the winning outcome ----
        outcomes = []
        outcome_prices = []
        if market:
            outcomes = market.get("outcomes", [])
            if isinstance(outcomes, str): outcomes = json.loads(outcomes)
            outcome_prices = market.get("outcomePrices", [])
            if isinstance(outcome_prices, str): outcome_prices = json.loads(outcome_prices)
        if not outcomes:
            outcomes = [settings.POLYMARKET_UP_LABEL, settings.POLYMARKET_DOWN_LABEL]

        up_index = next((i for i, x in enumerate(outcomes) if x.lower() == settings.POLYMARKET_UP_LABEL.lower()), 0)
        down_index = next((i for i, x in enumerate(outcomes) if x.lower() == settings.POLYMARKET_DOWN_LABEL.lower()), 1)

        winning_index = -1
        resolution = None
        # 1) Authoritative: a RESOLVED Polymarket outcome is marked at $1.
        #
        # `outcomePrices` on an OPEN market is the last traded price, not a result. The
        # cached market can be up to 30s old (the pre-expiry poll interval), so on the
        # first expired tick this used to read a still-trading book — and a live
        # favourite above 90c is not a resolution. It loses often enough to matter
        # (~1 market in 15 at these odds), and three trades in a 113-trade paper run
        # were settled against the wrong side that way, crediting $4,163 that was never
        # won. Require the market to actually be closed, and require a resolved mark
        # (>= 0.99) rather than merely a strong favourite.
        resolved = market_closed or str(
            (market or {}).get("umaResolutionStatus", "")).lower() == "resolved"
        for i, p in enumerate(outcome_prices if resolved else []):
            try:
                if float(p) >= 0.99:
                    winning_index = i
                    resolution = "polymarket_settled"
                    break
            except Exception:
                pass

        # 2) Fallback once the window/market is over: frozen CLOSE vs STRIKE (open).
        # Both are Chainlink values now, so this mirrors how Polymarket resolves —
        # did the close finish above or below the open?
        #
        # Only used AFTER giving Polymarket AUTHORITATIVE_SETTLE_WAIT_S to publish its
        # own outcome: our strike is a best-effort snapshot, theirs is the truth.
        strike = trade.get("strike_price")   # the marked OPEN
        settlement_price = (trade.get("close_price") or trade.get("settlement_price_at_expiry")
                            or trade.get("last_price") or cur_price)   # the frozen CLOSE
        if winning_index == -1 and (expired or market_closed):
            if trade.get("expired_at") is None:
                trade["expired_at"] = now_ts
            waited = now_ts - trade["expired_at"]
            # Wait the full window on TIME alone. `closed` can flip before UMA publishes
            # the final $1/$0 marks, so the old `and not market_closed` shortcut could
            # skip the wait in exactly the seconds the official result was about to
            # land, and score the trade against our own strike snapshot instead.
            #
            # Measured over 2,879 markets (2026-07-21..08-19), resolution lands a median
            # of 52s after expiry, p90 87s, p99 151s. So the 90s default already misses
            # the official result on ~2.8% of markets and falls back to close_vs_open;
            # 180s covers 99.2%. Raise `authoritative_settle_wait_s` if you would rather
            # hold the trade slot longer than score against our own strike snapshot.
            if waited < settings.AUTHORITATIVE_SETTLE_WAIT_S:
                remaining_active.append(trade)   # keep waiting for the official result
                continue
            if strike and settlement_price:
                trade["settlement_price_at_expiry"] = settlement_price
                winning_index = up_index if settlement_price > strike else down_index
                resolution = "close_vs_open"
                trade["settle_wait_s"] = round(waited, 1)

        # ---- Could not resolve yet ----
        if winning_index == -1:
            first_seen = trade.get("unresolved_since")
            if first_seen is None:
                trade["unresolved_since"] = now_ts
                remaining_active.append(trade)
                continue
            if now_ts - first_seen < SETTLEMENT_GRACE_SECONDS:
                remaining_active.append(trade)
                continue
            # Grace exhausted — void so a single bad trade can't block forever.
            trade["status"] = "VOID"
            trade["exit_reason"] = "void"
            trade["exit_time"] = datetime.now().isoformat()
            trade["profit_loss"] = 0.0
            if trade.get("mode", "paper") == "paper":
                state["paper_balance"] += trade["amount"]  # refund the stake
            state["trade_history"].append(_archive(trade))
            trades_changed = True
            log_message(f"VOID: Trade for {trade['market_slug']} unresolved past grace; stake refunded (paper).")
            continue

        # ---- Settle WIN / LOSS ----
        won = ((trade["side"] == "UP" and winning_index == up_index) or
               (trade["side"] == "DOWN" and winning_index == down_index))

        # Open/close context — recorded and shown in the log so the direction (and why
        # that side won) is always visible after the fact.
        open_px = strike
        close_px = trade.get("close_price") or settlement_price
        trade["open_price"] = open_px
        trade["close_price"] = close_px
        trade["resolution"] = resolution or "unknown"
        if open_px and close_px:
            move_side = "UP" if close_px > open_px else "DOWN"
            dir_txt = f"open {open_px:.2f} -> close {close_px:.2f} ({move_side} by {abs(close_px - open_px):.2f})"
        else:
            dir_txt = f"open {open_px} -> close {close_px}"

        if won:
            payout = trade["shares"] * 1.0
            # Paper credits the simulated balance; live balance comes from the
            # on-chain USDC refresh in the main loop, not credited here.
            if trade.get("mode", "paper") == "paper":
                state["paper_balance"] += payout
            trade["profit_loss"] = payout - trade["amount"]
            log_message(f"WIN: {trade['side']} on {trade['market_slug']}: {dir_txt} "
                        f"[{trade['resolution']}]. Profit: ${trade['profit_loss']:.2f}")
            # A winning LIVE position is still a CTF token worth $1 — it does not
            # become spendable pUSD on its own. Redeem it, or the balance silently
            # under-reports and the capital strands.
            if trade.get("mode") == "live":
                await _redeem_win(trade, market, up_index, down_index, winning_index)
        else:
            trade["profit_loss"] = -trade["amount"]
            log_message(f"LOSS: {trade['side']} on {trade['market_slug']}: {dir_txt} "
                        f"[{trade['resolution']}]. Loss: ${trade['profit_loss']:.2f}")

        trade["status"] = "CLOSED"
        trade["exit_reason"] = trade.get("exit_reason") or "settled"
        trade["exit_time"] = datetime.now().isoformat()
        trade["settlement_price_at_expiry"] = trade.get("settlement_price_at_expiry") or settlement_price
        trade["winning_outcome"] = outcomes[winning_index] if 0 <= winning_index < len(outcomes) else None
        state["trade_history"].append(_archive(trade))
        trades_changed = True

    state["active_trades"] = remaining_active
    if trades_changed:
        save_state()

async def seed_kline_buffers():
    try:
        k1m, k5m = await asyncio.gather(
            data.fetch_klines(settings.SYMBOL, "1m", 240),
            data.fetch_klines(settings.SYMBOL, "5m", 200)
        )
        binance_kline_1m.set_candles(k1m)
        binance_kline_5m.set_candles(k5m)
        log_message(f"Seeded Binance kline buffers (1m/5m) for {settings.SYMBOL}")
    except Exception as e:
        log_message(f"Failed to seed kline buffers: {e}")

# ─────────────────────────────────────────────────────────────────────────────
#  Event-driven entry path
#
#  The 1 Hz loop stays the HOUSEKEEPING clock: indicators, strike marking,
#  settlement, the withdrawal machine, logging, and publishing the dashboard
#  snapshot. Those are either time-triggered (a window expires because time
#  passed, not because a message arrived) or cheap enough not to matter.
#
#  The ENTRY DECISION does not wait for it. The whole thesis is acting on a gap
#  between Binance spot and the Polymarket book, so the decision re-runs the
#  moment either side of that gap moves — a Binance trade tick or a CLOB book
#  update — instead of on a timer that could sit on a live edge for ~1s.
#
#  Both sides of the edge are already pushed, so this path does NO network I/O:
#  it reads the in-memory spot and book, recomputes the fair probability against
#  model parameters cached by the last housekeeping tick, and enters.
# ─────────────────────────────────────────────────────────────────────────────

def _live_prices_from_ws(ctx) -> Optional[Dict[str, Any]]:
    """Best asks + book summaries straight from the socket, or None if either side is
    unusable. The event path never falls back to REST: a REST round trip here would
    reintroduce exactly the latency this exists to remove, and the 1 Hz loop is already
    covering the degraded case."""
    tids = ctx.get("token_ids") or {}
    up_id, down_id = tids.get("up"), tids.get("down")
    if not up_id or not down_id:
        return None
    max_age = settings.MAX_BOOK_AGE_S
    if not max_age:
        return None
    up = polymarket_clob_ws.get_summary(up_id, max_age_s=max_age)
    down = polymarket_clob_ws.get_summary(down_id, max_age_s=max_age)
    if not up or not down:
        return None
    if up.get("bestAsk") is None or down.get("bestAsk") is None:
        return None
    return {"prices": {"up": up["bestAsk"], "down": down["bestAsk"]},
            "orderbook": {"up": up, "down": down}}


async def evaluate_entry(trigger: str):
    """Re-run the entry decision against live spot + live book. Entries only — a flip
    is an exit decision on a position we already hold, and stays on the 1 Hz loop."""
    global _last_eval_ts
    ctx = state.get("trade_ctx") or {}
    if not ctx:
        return
    now = time.time()
    if now - _last_eval_ts < MIN_EVAL_INTERVAL_S:
        return
    if now - ctx.get("ts", 0) > CTX_MAX_AGE_S:
        return                      # the housekeeping loop has stalled; don't trade blind
    # Same gates the 1 Hz loop applies, checked before doing any work.
    if not state["running"] or state["active_trades"]:
        return
    if state["withdraw_state"] != "ARMED":
        return
    if state.get("withdraw_locked_market") and \
            state["withdraw_locked_market"] == str((ctx.get("market") or {}).get("id")):
        return
    if ctx.get("strike_open") is None or ctx.get("target_open") is None:
        return

    live = _live_prices_from_ws(ctx)
    if not live:
        return

    spot = (binance_stream.get_last() or {}).get("price")
    if not spot:
        return

    settlement_ms = ctx.get("settlement_ms")
    time_left_min = ((settlement_ms - now * 1000) / 60_000) if settlement_ms else None
    if time_left_min is not None and time_left_min <= 0:
        return
    steps = max(1, math.ceil((time_left_min if time_left_min is not None else 15) / 5))

    fair_up = indicators.fair_prob_up(spot, ctx["target_open"], steps,
                                      ctx.get("sigma_5m"), drift_per_step=ctx.get("drift_5m") or 0.0)

    decision = engines.decide_ev({
        "mcProbUp": fair_up,
        "priceUp": live["prices"]["up"],
        "priceDown": live["prices"]["down"],
        "minProb": settings.MIN_PROB_EV,
        "evThreshold": settings.EV_THRESHOLD,
        "rsi": ctx.get("rsi"),
        "haExhaustedGreen": ctx.get("ha_green"),
        "haExhaustedRed": ctx.get("ha_red"),
    })
    _last_eval_ts = now
    if decision["action"] != "ENTER":
        return

    async with _entry_lock:
        if state["active_trades"] or not state["running"]:
            return                  # won between the check above and the lock
        result = await execute_trade(
            decision, live["prices"], ctx["market"], ctx["strike_open"],
            ctx.get("token_ids", {}), live["orderbook"],
            strike_source=ctx.get("strike_source", "chainlink_ws"),
            window_start_ms=ctx.get("window_start_ms"), open_reason="ev_entry")
    if result == "entered":
        # Hand the reason to the next CSV row so `signals.csv` still explains every
        # entry — otherwise an between-ticks entry shows up only as `slot_busy`.
        state["event_exec"] = f"entered_on_{trigger}"
        log_message(f"Entered on {trigger} tick (event-driven, "
                    f"{(now - ctx['ts']) * 1000:.0f}ms after the last housekeeping tick)")
        # Reflect the new position on the dashboard now, not on the next tick — an
        # entry taken between ticks is exactly the thing worth seeing immediately.
        ts = state["latest_data"].get("trading_state")
        if isinstance(ts, dict):
            ts["active_trades"] = state["active_trades"]
            ts["balance"] = state["paper_balance"]
        await broadcast_state()
    elif result not in (None, "slot_busy", "no_trade"):
        state["event_exec"] = f"{result}_on_{trigger}"


async def entry_watcher():
    """One consumer for both streams. Waiting on an Event collapses a burst of trade
    ticks and book frames into a single evaluation, so the cost is bounded no matter
    how fast the feeds run."""
    while True:
        try:
            await _market_event.wait()
            _market_event.clear()
            await evaluate_entry("book" if polymarket_clob_ws.connected else "spot")
        except asyncio.CancelledError:
            raise
        except Exception as e:
            print(f"Entry watcher error: {e}")
        await asyncio.sleep(MIN_EVAL_INTERVAL_S)


async def update_loop():
    csv_header = [
        "timestamp", "entry_minute", "time_left_min", "signal",
        "model_up", "model_down", "mkt_up", "mkt_down", "edge_up", "edge_down",
        "recommendation", "reason", "exec_result"
    ]

    while True:
        try:
            timing = get_candle_window_timing(settings.CANDLE_WINDOW_MINUTES)

            binance_ws = binance_stream.get_last()
            if not binance_ws.get("price"):
                poly_ws_last = polymarket_ws_stream.get_last()
                cl_ws_last = chainlink_ws_stream.get_last()
                binance_ws["price"] = poly_ws_last.get("price") or cl_ws_last.get("price")
            poly_ws = polymarket_ws_stream.get_last()
            cl_ws = chainlink_ws_stream.get_last()

            results = await asyncio.gather(
                data.fetch_last_price(settings.SYMBOL),
                chainlink.chainlink_fetcher.fetch_chainlink_btc_usd(),
                fetch_polymarket_snapshot(),
                return_exceptions=True
            )

            last_price = results[0] if not isinstance(results[0], Exception) else None
            chainlink_data = results[1] if not isinstance(results[1], Exception) else {}
            poly_snapshot = results[2] if not isinstance(results[2], Exception) else {"ok": False}

            klines_1m = binance_kline_1m.get_candles()
            klines_5m = binance_kline_5m.get_candles()

            spot_price = binance_ws.get("price") if binance_ws and binance_ws.get("price") else last_price

            mc_steps = max(1, math.ceil(timing["remainingMinutes"] / 5))

            # ── The settlement feed ──────────────────────────────────────────────
            # Prefer Polymarket's OWN Chainlink WS: it is the exact stream Polymarket
            # settles on, so marking the open and the close from it matches the market
            # most faithfully. Fall back to the direct Chainlink RPC WS, then REST.
            # A stream's get_last() returns whatever it saw LAST, which after a dropped
            # connection or a quiet feed can be minutes old. Preferring it purely on
            # "has a price" latched stale values as the strike: measured against
            # Polymarket's own recorded oracle series over 113 live trades, the marked
            # open was never the tick at eventStartTime — median $8.30 out (1.05 bps),
            # worst $86 — and best-matched a tick a median of 22s BEFORE the window
            # opened. Each source carries `updatedAt`; honour it and fall through to the
            # next source when the value is stale.
            # The two feeds mean different things by `updatedAt`, so they get different
            # limits. Polymarket's stream carries the tick's own time and ticks ~1/s, so
            # anything older than POLY_WS_MAX_AGE_MS means the socket is dead. The RPC
            # sources report the on-chain ROUND timestamp, which is legitimately old
            # between aggregator posts — there the limit is only a liveness check.
            now_ms_feed = time.time() * 1000

            def _age_ok(snap, max_age_ms):
                px = (snap or {}).get("price")
                if not px:
                    return None
                ts = (snap or {}).get("updatedAt")
                if ts and (now_ms_feed - float(ts)) > max_age_ms:
                    return None
                return px

            sources = ((poly_ws, "Polymarket WS", POLY_WS_MAX_AGE_MS),
                       (cl_ws, "Chainlink RPC WS", ONCHAIN_MAX_AGE_MS),
                       (chainlink_data, "Chainlink RPC REST", ONCHAIN_MAX_AGE_MS))

            current_price = None
            price_source = None
            price_is_fresh = False
            for snap, label, max_age in sources:
                px = _age_ok(snap, max_age)
                if px:
                    current_price, price_source, price_is_fresh = px, label, True
                    break
            if current_price is None:
                # Every feed is stale. Carry the freshest value we have so the dashboard
                # still shows something, but flag it so no strike is latched from it.
                for snap, label, _ in sources:
                    if (snap or {}).get("price"):
                        current_price, price_source = snap["price"], label + " (STALE)"
                        break

            # ── Authoritative window start = the market's own eventStartTime ──────
            # NOT the local aligned clock. eventStartTime is the exact second the
            # contract's "Price to Beat" is fixed, so a feed value captured at that
            # instant matches Polymarket's settlement. Fall back to endDate - window,
            # then the local boundary. (Polymarket exposes no numeric strike field —
            # the strike IS the Chainlink price at eventStartTime.)
            window_ms = settings.CANDLE_WINDOW_MINUTES * 60_000
            event_start_ms = None
            if poly_snapshot.get("ok"):
                _mkt = poly_snapshot["market"]
                _esr = _mkt.get("eventStartTime") or _mkt.get("gameStartTime")
                if _esr:
                    try:
                        event_start_ms = int(datetime.fromisoformat(str(_esr).replace('Z', '+00:00')).timestamp() * 1000)
                    except Exception:
                        event_start_ms = None
                if event_start_ms is None and _mkt.get("endDate"):
                    try:
                        event_start_ms = int(datetime.fromisoformat(_mkt["endDate"].replace('Z', '+00:00')).timestamp() * 1000) - window_ms
                    except Exception:
                        event_start_ms = None
            if event_start_ms is None:
                event_start_ms = int(timing["startMs"])

            # ── Mark this window's OPEN at eventStartTime ─────────────────────────
            # Two values are captured at the same instant:
            #   "chainlink" -> the SETTLEMENT strike (what Polymarket resolves against)
            #   "binance"   -> the MODEL's reference open, so fair_prob keeps measuring
            #                  the Binance move since the open exactly as it always has.
            # Mixing the two feeds would inject a constant ~0.13% offset into the model.
            start_ms = event_start_ms
            win = mark_window_open(start_ms, window_ms, current_price, spot_price,
                                   price_source, price_is_fresh)

            # Settlement strike for a trade entered now: ONLY the Chainlink open latched
            # at eventStartTime. None until/unless captured => no trade this window.
            strike_open = win["chainlink"]
            strike_source = "chainlink_ws"

            # Model reference open. Prefer the 5m candle that opens EXACTLY at the
            # window start (strict equality — a `<=` scan silently picks the previous
            # candle's open in the first seconds of a window, before the new one has
            # arrived over the WS). Fall back to the Binance spot latched at the mark.
            model_open = None
            for c in reversed(klines_5m):
                if c["openTime"] == start_ms:
                    model_open = c["open"]
                    break
                if c["openTime"] < start_ms:
                    break
            model_open = model_open or win.get("binance")
            target_open = model_open if strike_open is not None else None

            # Fast closed-form fair probability (replaces 1000-sim Monte Carlo —
            # backtest-verified equivalent, ~1000x cheaper, which a latency play needs).
            drift_5m, sigma_5m = indicators.realized_drift_vol(klines_5m, lookback=300)
            fair_up = indicators.fair_prob_up(spot_price or 0, target_open or 0, mc_steps, sigma_5m, drift_per_step=drift_5m or 0.0)
            fair_data = {
                "prob_up": fair_up,
                "prob_down": 1.0 - fair_up,
                "bias": "BULLISH" if fair_up > 0.6 else "BEARISH" if fair_up < 0.4 else "NEUTRAL",
                "steps": mc_steps,
                "sigma_5m": sigma_5m,
            }

            settlement_ms = None
            if poly_snapshot["ok"] and poly_snapshot["market"].get("endDate"):
                settlement_ms = datetime.fromisoformat(poly_snapshot["market"]["endDate"].replace('Z', '+00:00')).timestamp() * 1000

            time_left_min = (settlement_ms - time.time() * 1000) / 60_000 if settlement_ms else timing["remainingMinutes"]

            closes = [c["close"] for c in klines_1m]
            rsi_now = indicators.compute_rsi(closes, settings.RSI_PERIOD)

            # Heiken-Ashi streaks (1m & 5m) — the exhaustion veto.
            consec = indicators.count_consecutive(indicators.compute_heiken_ashi(klines_1m))
            consec_5m = {"color": None, "count": 0}
            if len(klines_5m) >= 20:
                consec_5m = indicators.count_consecutive(indicators.compute_heiken_ashi(klines_5m))

            market_up = poly_snapshot["prices"]["up"] if poly_snapshot["ok"] else None
            market_down = poly_snapshot["prices"]["down"] if poly_snapshot["ok"] else None

            # ── LATENCY EDGE ─────────────────────────────────────────────────────
            # Our fast Binance-derived fair prob vs the market's (possibly stale)
            # implied prob. A positive edge = the book hasn't repriced the move yet.
            market_implied_up = None
            if market_up is not None and market_down is not None and (market_up + market_down) > 0:
                market_implied_up = market_up / (market_up + market_down)
            edge = {
                "marketUp": market_implied_up,
                "marketDown": (1 - market_implied_up) if market_implied_up is not None else None,
                "edgeUp": (fair_up - market_implied_up) if market_implied_up is not None else None,
                "edgeDown": ((1 - fair_up) - (1 - market_implied_up)) if market_implied_up is not None else None,
            }
            prob_view = {"adjustedUp": fair_up, "adjustedDown": 1 - fair_up}

            # Heiken-Ashi exhaustion veto (>=6 bars in one direction = don't chase).
            EB = engines.EXHAUSTION_BARS

            def _is(color, count, want):
                return color == want and (count or 0) >= EB

            ha_exhausted_green = _is(consec["color"], consec["count"], "green") or _is(consec_5m["color"], consec_5m["count"], "green")
            ha_exhausted_red = _is(consec["color"], consec["count"], "red") or _is(consec_5m["color"], consec_5m["count"], "red")

            decision = engines.decide_ev({
                "mcProbUp": fair_up,
                "priceUp": market_up,
                "priceDown": market_down,
                "minProb": settings.MIN_PROB_EV,
                "evThreshold": settings.EV_THRESHOLD,
                "rsi": rsi_now,
                "haExhaustedGreen": ha_exhausted_green,
                "haExhaustedRed": ha_exhausted_red,
            })

            current_prices_dict = {"spot": spot_price, "chainlink": current_price}

            # Clear the post-withdrawal market lock once the window has rolled on.
            cur_market_id = str(poly_snapshot["market"].get("id")) if poly_snapshot["ok"] else None
            if state.get("withdraw_locked_market") and cur_market_id and \
                    state["withdraw_locked_market"] != cur_market_id:
                state["withdraw_locked_market"] = None

            # Entries and flips run ONLY when the user has pressed Start, no withdrawal
            # is in flight, and this isn't the market a withdrawal just exited. Feeds,
            # the model and settlement all keep running either way, so an open position
            # always settles to expiry and can never get stranded by a Stop.
            withdraw_locked = (state.get("withdraw_locked_market") is not None
                               and state["withdraw_locked_market"] == cur_market_id)
            entries_allowed = (state["running"] and state["withdraw_state"] == "ARMED"
                               and not withdraw_locked)

            # Publish everything the event path needs to decide without any network I/O.
            # The model parameters (sigma/drift/RSI/HA) change on candle boundaries, not
            # on ticks, so caching them here and recomputing only the fast-moving parts
            # (spot, book, time left) is exact rather than an approximation.
            if poly_snapshot["ok"]:
                state["trade_ctx"] = {
                    "ts": time.time(),
                    "market": poly_snapshot["market"],
                    "token_ids": poly_snapshot.get("token_ids", {}),
                    "strike_open": strike_open,
                    "strike_source": strike_source,
                    "target_open": target_open,
                    "window_start_ms": start_ms,
                    "settlement_ms": settlement_ms,
                    "sigma_5m": sigma_5m,
                    "drift_5m": drift_5m,
                    "rsi": rsi_now,
                    "ha_green": ha_exhausted_green,
                    "ha_red": ha_exhausted_red,
                }
            else:
                state["trade_ctx"] = {}

            exec_result = None
            if poly_snapshot["ok"] and entries_allowed:
                flipped = await maybe_flip_position(decision, poly_snapshot, time_left_min)
                # NOTE: the trade is stamped with `strike_open` (Chainlink @ eventStartTime),
                # NOT `target_open` (the model's Binance reference). Settlement compares a
                # Chainlink close to this, so both sides must come from the same feed.
                # Same lock the event path takes — otherwise the slot check and the
                # append can interleave between the two and open two positions.
                async with _entry_lock:
                    exec_result = await execute_trade(
                        decision, poly_snapshot["prices"], poly_snapshot["market"], strike_open,
                        poly_snapshot.get("token_ids", {}), poly_snapshot.get("orderbook", {}),
                        strike_source=strike_source, window_start_ms=start_ms,
                        open_reason="flip_entry" if flipped else "ev_entry")
            elif not state["running"]:
                exec_result = "stopped"
            elif state["withdraw_state"] != "ARMED":
                exec_result = f"withdraw_{state['withdraw_state'].lower()}"
            elif withdraw_locked:
                exec_result = "withdraw_locked"

            await update_trades(current_prices_dict)

            # Mark open positions to market so the dashboard can show live P/L.
            open_value = 0.0
            for t in state["active_trades"]:
                mark = None
                if poly_snapshot["ok"] and str(t.get("market_id")) == str(poly_snapshot["market"].get("id")):
                    ob = (poly_snapshot.get("orderbook") or {}).get("up" if t["side"] == "UP" else "down") or {}
                    mark = ob.get("bestBid") or (market_up if t["side"] == "UP" else market_down)
                if mark:
                    t["mark_price"] = mark
                    t["unrealized_pl"] = (t["shares"] * mark) - t["amount"]
                    open_value += t["shares"] * mark
                else:
                    t["unrealized_pl"] = None
                    open_value += t["amount"]   # no quote — carry at cost

            # In live mode, reflect the real on-chain USDC balance in the dashboard
            if state["trading_mode"] == "live":
                now_ts = time.time()
                if now_ts - state.get("last_balance_refresh", 0) > 30:
                    real_bal = await asyncio.to_thread(clob_trader.get_usdc_balance)
                    if real_bal is not None:
                        state["paper_balance"] = real_bal
                    state["last_balance_refresh"] = now_ts

            # Auto-withdrawal (capital extractor) — triggers on EQUITY (cash + open
            # position value), force-closes any open trade, withdraws, then resumes at
            # the next window. Runs AFTER the balance refresh so it decides on a
            # current number, and after settlement so a just-closed win is counted.
            equity = state["paper_balance"] + open_value
            await maybe_auto_withdraw(equity, poly_snapshot)
            # A withdrawal may have just sold the position out. `open_value` was marked
            # before that, so drop it rather than showing the equity tile a position
            # that no longer exists for one tick.
            if not state["active_trades"]:
                open_value = 0.0

            # An entry that happened BETWEEN ticks would otherwise show up here only as
            # `slot_busy`, with nothing saying why. The event path leaves its reason for
            # this row so signals.csv keeps explaining every entry.
            if state.get("event_exec"):
                exec_result = state["event_exec"]
                state["event_exec"] = None

            signal_label = f"BUY {decision['side']}" if decision["action"] == "ENTER" else "NO TRADE"
            utils.append_csv_row("./logs/signals.csv", csv_header, [
                datetime.now().isoformat(), timing["elapsedMinutes"], time_left_min,
                signal_label, fair_up, 1 - fair_up, market_up, market_down,
                edge["edgeUp"], edge["edgeDown"], f"{decision['side']}:{decision['phase']}:{decision['strength']}" if decision["action"] == "ENTER" else "NO_TRADE",
                decision.get("reason", ""), exec_result or ""
            ])

            state["latest_data"] = {
                "timestamp": datetime.now().isoformat(),
                "log_seq": state["log_seq"],
                "timing": timing,
                "market": poly_snapshot.get("market") if poly_snapshot["ok"] else None,
                "trading_state": {
                    "mode": state["trading_mode"],
                    "running": state["running"],
                    "balance": state["paper_balance"],
                    "equity": state["paper_balance"] + open_value,
                    "open_value": open_value,
                    "active_trades": state["active_trades"],
                    "history_count": len(state["trade_history"]),
                    "risk": {"type": settings.RISK_TYPE, "value": settings.RISK_VALUE},
                    "symbol": settings.SYMBOL,
                    "withdraw": {
                        "enabled": settings.AUTO_WITHDRAW_ENABLED,
                        "state": state["withdraw_state"],
                        "trigger_balance": settings.WITHDRAW_TRIGGER_BALANCE,
                        "amount": settings.WITHDRAW_AMOUNT,
                        "last": state["last_withdrawal"],
                    }
                },
                "prices": {
                    "spot": spot_price,
                    "chainlink": current_price,
                    "chainlink_source": price_source,
                    "poly_up": market_up,
                    "poly_down": market_down,
                    "window_open": strike_open,          # strike: Chainlink @ eventStartTime
                    "window_open_source": strike_source,
                    "model_open": model_open,            # the model's Binance reference open
                    "window_start_ms": start_ms,
                    "book_source": poly_snapshot.get("book_source") if poly_snapshot["ok"] else None
                },
                "indicators": {
                    "rsi": rsi_now,
                    "heiken": consec,
                    "heiken_5m": consec_5m,
                    "fair": fair_data
                },
                "analysis": {
                    "probability": prob_view, "edge": edge, "decision": decision
                }
            }
            state["last_update_ts"] = time.time()
            await broadcast_state()

        except Exception as e:
            print(f"Error in update loop: {e}")

        await asyncio.sleep(settings.POLL_INTERVAL_MS / 1000)


@app.get("/", response_class=HTMLResponse)
async def get_dashboard(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@app.get("/settings", response_class=HTMLResponse)
async def get_settings_page(request: Request):
    return templates.TemplateResponse("settings.html", {"request": request})

@app.get("/api/latest")
async def get_latest():
    return state["latest_data"]


@app.websocket("/ws")
async def dashboard_ws(ws: WebSocket):
    """Pushes the dashboard snapshot: on connect, on every rebuilt tick, and
    immediately after a discrete event (an entry, Start/Stop).

    `/api/latest` is unchanged and still works — the page falls back to polling it
    whenever this socket is unavailable, so the dashboard degrades rather than dies.
    """
    await ws.accept()
    try:
        # Sent BEFORE joining the broadcast set: two coroutines writing the same
        # websocket concurrently can interleave frames. The cost is that a snapshot
        # rebuilt in this instant is missed, which the next tick corrects.
        if state["latest_data"]:
            await ws.send_json(state["latest_data"])
        _ws_clients.add(ws)
        while True:
            # We expect nothing from the client; this is how a disconnect surfaces.
            await ws.receive_text()
    except Exception:
        pass
    finally:
        _ws_clients.discard(ws)

@app.get("/api/logs")
async def get_logs():
    return state["logs"]

# Data files the dashboard is allowed to download. An explicit whitelist, NOT a path
# join on user input — this endpoint is reachable by anyone who can reach the
# dashboard, so it must not be able to serve arbitrary files (private_key lives in
# config.json).
DOWNLOADABLE = {
    "signals": ("logs/signals.csv", "text/csv"),
    "trades": ("state_data.json", "application/json"),
}


@app.get("/api/files")
async def list_files():
    """Which data files exist, how big, and when they last changed."""
    out = []
    for key, (path, _) in DOWNLOADABLE.items():
        exists = os.path.exists(path)
        out.append({
            "key": key,
            "name": os.path.basename(path),
            "exists": exists,
            "size": os.path.getsize(path) if exists else 0,
            "rows": (max(0, sum(1 for _ in open(path, encoding="utf-8", errors="ignore")) - 1)
                     if exists and path.endswith(".csv") else None),
            "modified": (datetime.fromtimestamp(os.path.getmtime(path)).isoformat()
                         if exists else None),
        })
    return out


@app.get("/api/download/{key}")
async def download_file(key: str):
    entry = DOWNLOADABLE.get(key)
    if not entry:
        return JSONResponse({"error": "unknown_file"}, status_code=404)
    path, media = entry
    if not os.path.exists(path):
        return JSONResponse({"error": "not_generated_yet", "path": path}, status_code=404)
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    base, ext = os.path.splitext(os.path.basename(path))
    return FileResponse(path, media_type=media, filename=f"15m-{base}-{stamp}{ext}")


def _reflect_running_now():
    """Mirror the running flag into latest_data immediately so /api/latest is in sync
    on the very next poll (the update loop would otherwise lag ~1s, flickering the UI)."""
    ts = state["latest_data"].get("trading_state")
    if isinstance(ts, dict):
        ts["running"] = state["running"]


@app.post("/api/start")
async def start_trading():
    """Begin trading. Data/prices stream continuously; this flips the gate so the
    engine may enter/flip trades."""
    state["running"] = True
    _reflect_running_now()
    log_message("Trading STARTED by user")
    await broadcast_state()
    return {"ok": True, "running": True}


@app.post("/api/stop")
async def stop_trading():
    """Stop all trading. New entries and flips halt immediately; any open position
    keeps settling to expiry so it can't get stuck."""
    state["running"] = False
    _reflect_running_now()
    log_message("Trading STOPPED by user")
    await broadcast_state()
    return {"ok": True, "running": False}


@app.get("/api/available-series")
async def get_available_series():
    return await data.fetch_available_15m_series()

@app.get("/api/settings")
async def get_settings():
    def mask(v: str) -> str:
        return v[:6] + "..." + v[-4:] if v and len(v) > 10 else v

    masked_pk = mask(settings.PRIVATE_KEY)

    return {
        "mode": settings.MODE,
        "paper_balance_usd": settings.PAPER_BALANCE_USD,
        "private_key": masked_pk,
        "live": {
            # signature_type / funder are AUTO-DETECTED from the key under CLOB V2 —
            # they are reported for visibility, not for editing.
            "relayer_api_key": mask(settings.RELAYER_API_KEY),
            "alchemy_api_key": mask(settings.ALCHEMY_API_KEY),
            "max_slippage": settings.CLOB_MAX_SLIPPAGE
        },
        "polymarket": {
            "series_id": settings.POLYMARKET_SERIES_ID,
            "gamma_base_url": settings.GAMMA_BASE_URL,
            "clob_base_url": settings.CLOB_BASE_URL,
            "live_ws_url": settings.POLYMARKET_LIVE_DATA_WS_URL,
            "up_label": settings.POLYMARKET_UP_LABEL,
            "down_label": settings.POLYMARKET_DOWN_LABEL
        },
        "trading": {
            "symbol": settings.SYMBOL,
            "risk_type": settings.RISK_TYPE,
            "risk_value": settings.RISK_VALUE
        },
        "ev": {
            "ev_threshold": settings.EV_THRESHOLD,
            "min_prob": settings.MIN_PROB_EV,
            "min_book_liquidity_usd": settings.MIN_BOOK_LIQUIDITY_USD
        },
        "flip": {
            "enabled": settings.FLIP_ENABLED,
            "min_conviction": settings.FLIP_MIN_CONVICTION,
            "min_minutes_left": settings.FLIP_MIN_MINUTES_LEFT
        },
        "capital_extractor": {
            "enabled": settings.AUTO_WITHDRAW_ENABLED,
            "trigger_balance": settings.WITHDRAW_TRIGGER_BALANCE,
            "withdraw_amount": settings.WITHDRAW_AMOUNT,
            "withdraw_address": settings.WITHDRAW_ADDRESS,
            "auto_resume_after_withdrawal": settings.WITHDRAW_AUTO_RESUME,
            "resume_after": settings.WITHDRAW_RESUME_AFTER
        },
        "telegram": {
            "enabled": settings.TELEGRAM_ENABLED,
            # The token is never echoed back — the form shows "set" and POSTing that
            # sentinel unchanged leaves the stored token alone.
            "bot_token": "set" if settings.TELEGRAM_BOT_TOKEN else ""
        }
    }

@app.post("/api/settings")
async def post_settings(new_settings: Dict[str, Any]):
    global binance_stream, polymarket_ws_stream, chainlink_ws_stream, binance_kline_1m, binance_kline_5m
    old_symbol = settings.SYMBOL

    # "set" is the placeholder GET /api/settings returns for a stored bot token — it is
    # not a token. Drop it so the deep-merge below can't overwrite the real one with it.
    if isinstance(new_settings.get("telegram"), dict) and new_settings["telegram"].get("bot_token") == "set":
        new_settings["telegram"].pop("bot_token", None)

    new_pk = new_settings.get("private_key")
    if new_pk and "..." in new_pk:
        new_settings["private_key"] = settings.PRIVATE_KEY
    elif new_pk:
        # Accepts a hex key OR a 12/24-word seed phrase; stored as hex either way.
        from bot.config import normalize_private_key
        try:
            settings.PRIVATE_KEY = normalize_private_key(new_pk)
            new_settings["private_key"] = settings.PRIVATE_KEY
        except Exception as e:
            return {"status": "error", "error": f"invalid_private_key: {e}"}

    # Deep-merge into the existing config so keys not present in the settings form
    # (chainlink, binance_base_url, poll_interval_ms, etc.) are preserved.
    existing_cfg = {}
    if os.path.exists("config.json"):
        try:
            with open("config.json", "r") as f:
                existing_cfg = json.load(f)
        except Exception:
            existing_cfg = {}

    def deep_merge(base, override):
        for k, v in override.items():
            if isinstance(v, dict) and isinstance(base.get(k), dict):
                deep_merge(base[k], v)
            else:
                base[k] = v
        return base

    merged_cfg = deep_merge(existing_cfg, new_settings)
    with open("config.json", "w") as f:
        json.dump(merged_cfg, f, indent=2)

    settings.MODE = new_settings.get("mode", settings.MODE)
    settings.PAPER_BALANCE_USD = float(new_settings.get("paper_balance_usd", settings.PAPER_BALANCE_USD))

    if "trading" in new_settings:
        t = new_settings["trading"]
        settings.SYMBOL = t.get("symbol", settings.SYMBOL)
        settings.RISK_TYPE = t.get("risk_type", settings.RISK_TYPE)
        settings.RISK_VALUE = float(t.get("risk_value", settings.RISK_VALUE))

    if "ev" in new_settings:
        e = new_settings["ev"]
        settings.EV_THRESHOLD = float(e.get("ev_threshold", settings.EV_THRESHOLD))
        settings.MIN_PROB_EV = float(e.get("min_prob", settings.MIN_PROB_EV))
        settings.MIN_BOOK_LIQUIDITY_USD = float(e.get("min_book_liquidity_usd", settings.MIN_BOOK_LIQUIDITY_USD))

    if "flip" in new_settings:
        f = new_settings["flip"]
        if "enabled" in f:
            settings.FLIP_ENABLED = bool(f["enabled"])
        settings.FLIP_MIN_CONVICTION = float(f.get("min_conviction", settings.FLIP_MIN_CONVICTION))
        settings.FLIP_MIN_MINUTES_LEFT = float(f.get("min_minutes_left", settings.FLIP_MIN_MINUTES_LEFT))

    if "capital_extractor" in new_settings:
        ce = new_settings["capital_extractor"]
        if "enabled" in ce: settings.AUTO_WITHDRAW_ENABLED = bool(ce["enabled"])
        if "trigger_balance" in ce: settings.WITHDRAW_TRIGGER_BALANCE = float(ce["trigger_balance"])
        if "withdraw_amount" in ce: settings.WITHDRAW_AMOUNT = float(ce["withdraw_amount"])
        if "withdraw_address" in ce: settings.WITHDRAW_ADDRESS = ce["withdraw_address"]
        if "auto_resume_after_withdrawal" in ce: settings.WITHDRAW_AUTO_RESUME = bool(ce["auto_resume_after_withdrawal"])
        if "resume_after" in ce: settings.WITHDRAW_RESUME_AFTER = ce["resume_after"]

    if "telegram" in new_settings:
        tg = new_settings["telegram"]
        if "enabled" in tg: settings.TELEGRAM_ENABLED = bool(tg["enabled"])
        if "bot_token" in tg: settings.TELEGRAM_BOT_TOKEN = tg["bot_token"]

    if "polymarket" in new_settings:
        p = new_settings["polymarket"]
        settings.POLYMARKET_SERIES_ID = p.get("series_id", settings.POLYMARKET_SERIES_ID)
        settings.POLYMARKET_UP_LABEL = p.get("up_label", settings.POLYMARKET_UP_LABEL)
        settings.POLYMARKET_DOWN_LABEL = p.get("down_label", settings.POLYMARKET_DOWN_LABEL)

    if "live" in new_settings:
        lv = new_settings["live"]
        if "max_slippage" in lv:
            settings.CLOB_MAX_SLIPPAGE = float(lv["max_slippage"])
        # A value still showing the "abc123...wxyz" mask was not edited — keep the real
        # one rather than overwriting the secret with its own mask.
        rk = lv.get("relayer_api_key")
        if rk and "..." not in rk:
            settings.RELAYER_API_KEY = rk
            new_settings.setdefault("relayer", {})["api_key"] = rk
        elif rk:
            lv["relayer_api_key"] = settings.RELAYER_API_KEY
        ak = lv.get("alchemy_api_key")
        if ak and "..." not in ak:
            settings.ALCHEMY_API_KEY = ak
            new_settings.setdefault("chainlink", {})["alchemy_api_key"] = ak
        elif ak:
            lv["alchemy_api_key"] = settings.ALCHEMY_API_KEY

    # Credentials/signature may have changed — drop the cached CLOB client so the
    # next live order re-initialises with the new key/signature/funder.
    clob_trader.reset()

    state["trading_mode"] = settings.MODE
    state["paper_balance"] = settings.PAPER_BALANCE_USD

    if settings.SYMBOL != old_symbol:
        binance_stream.close()
        binance_stream = ws_data.BinanceTradeStream(symbol=settings.SYMBOL, on_update=_wake_entry)
        asyncio.create_task(binance_stream.start())

        binance_kline_1m.close()
        binance_kline_1m = ws_data.BinanceKlineStream(symbol=settings.SYMBOL, interval="1m", limit=240)
        asyncio.create_task(binance_kline_1m.start())

        binance_kline_5m.close()
        binance_kline_5m = ws_data.BinanceKlineStream(symbol=settings.SYMBOL, interval="5m", limit=200)
        asyncio.create_task(binance_kline_5m.start())

        await seed_kline_buffers()

        polymarket_ws_stream.close()
        polymarket_ws_stream = ws_data.PolymarketChainlinkStream(
            ws_url=settings.POLYMARKET_LIVE_DATA_WS_URL,
            symbol_includes=get_ws_symbol_filter(settings.SYMBOL)
        )
        asyncio.create_task(polymarket_ws_stream.start())

        chainlink_ws_stream.close()
        chainlink_ws_stream = ws_data.ChainlinkPriceStream(aggregator=settings.get_aggregator(settings.SYMBOL))
        asyncio.create_task(chainlink_ws_stream.start())

    return {"status": "ok"}

@app.post("/api/setup-wallet")
async def setup_wallet():
    """One-time gasless on-chain setup for the deposit wallet: deploy it if needed and
    set the token approvals, sponsored by the relayer key. Replaces the old manual EOA
    allowance flow — under CLOB V2 you never pay gas for this."""
    try:
        result = await asyncio.to_thread(clob_trader.ensure_setup)
        if result.get("ok"):
            if result.get("skipped"):
                log_message("Wallet setup: already done this session")
            else:
                log_message(f"Wallet setup complete ({result.get('approvals', 0)} approvals)")
        else:
            log_message(f"Wallet setup failed: {result.get('error')}")
        return result
    except Exception as e:
        log_message(f"Wallet setup error: {e}")
        return {"ok": False, "error": str(e)}


@app.post("/api/test-connection")
async def test_connection():
    """Read-only diagnostic: derive the EOA from the key/seed, list every candidate
    wallet (deposit / proxy / safe) with its pUSD balance, and report which one will
    actually be traded from. Needs no relayer key."""
    try:
        result = await asyncio.to_thread(clob_trader.test_connection)
        if result.get("ok"):
            log_message(f"Connection OK — EOA {result.get('eoa')}, trading from "
                        f"{result.get('funder')} (sig type {result.get('chosen_signature_type')})")
        else:
            log_message(f"Connection test failed: {result.get('error')}")
        return result
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.post("/api/enable-auto-redeem")
async def enable_auto_redeem():
    """Ask Polymarket to auto-redeem resolved positions, so winning outcome tokens
    turn back into pUSD without the bot doing it per-trade."""
    try:
        result = await asyncio.to_thread(clob_trader.enable_auto_redeem)
        log_message("Auto-redeem enabled" if result.get("ok")
                    else f"Auto-redeem failed: {result.get('error')}")
        return result
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.get("/api/telegram-subscribers")
async def get_telegram_subscribers():
    """Everyone currently receiving withdrawal alerts. They add themselves by sending
    the bot /start — there are no chat IDs to copy by hand."""
    subs = [{"chat_id": cid, **info} for cid, info in state["telegram_subscribers"].items()]
    return {"count": len(subs), "subscribers": subs}


@app.post("/api/telegram-unsubscribe")
async def telegram_unsubscribe(body: Dict[str, Any]):
    """Remove a subscriber from the list (they'll no longer get alerts)."""
    cid = str(body.get("chat_id", ""))
    removed = remove_telegram_subscriber(cid)
    if removed:
        log_message(f"Telegram subscriber removed by user: {cid}")
    return {"ok": removed, "count": len(state["telegram_subscribers"])}


@app.post("/api/test-telegram")
async def test_telegram():
    """Broadcast a test message to every subscriber, to verify the setup works."""
    if not settings.TELEGRAM_BOT_TOKEN:
        return {"ok": False, "error": "missing_bot_token"}
    if not settings.TELEGRAM_ENABLED:
        return {"ok": False, "error": "telegram_alerts_disabled"}
    if not state["telegram_subscribers"]:
        return {"ok": False, "error": "no_subscribers_yet — send /start to your bot first"}
    await send_telegram("✅ <b>Test alert</b>\nThis chat will receive withdrawal alerts from your "
                        "Polymarket BTC 15m bot.")
    log_message(f"Telegram test alert broadcast to {len(state['telegram_subscribers'])} subscriber(s)")
    return {"ok": True, "count": len(state["telegram_subscribers"])}


@app.get("/health")
async def health():
    return {"status": "ok", "last_update": state["last_update_ts"], "mode": state["trading_mode"],
            "running": state["running"]}

@app.get("/history")
async def get_history():
    return state["trade_history"]

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8010)
