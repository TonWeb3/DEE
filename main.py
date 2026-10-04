import asyncio
import time
import json
import os
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, List, Optional, Tuple

from contextlib import asynccontextmanager
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from bot.config import settings
import bot.data as data
import bot.ws_data as ws_data
import bot.clob_ws as clob_ws
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
        asyncio.create_task(chainlink_ws_stream.start()),
        asyncio.create_task(clob_ob_stream.start()),   # real-time CLOB order-book (Polymarket odds)
        asyncio.create_task(update_loop()),
        asyncio.create_task(telegram_poller()),   # auto-subscribe: collect everyone who starts the bot
    ]

    yield

    # Shutdown cleanup
    for task in tasks:
        task.cancel()

    binance_stream.close()
    binance_kline_1m.close()
    binance_kline_5m.close()
    polymarket_ws_stream.close()
    chainlink_ws_stream.close()
    clob_ob_stream.close()

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
    "running": False,   # trading is OFF until the user presses Start on the dashboard
    # Auto-withdrawal (capital extractor) state machine:
    #   ARMED -> (balance>=trigger) WAITING_FLAT -> (no open trades) WITHDRAWING
    #         -> WITHDRAW_SUBMITTED -> ARMED (+ resume)
    "withdraw_state": "ARMED",
    "last_withdrawal": None,   # {"amount","tx","time"} of the most recent withdrawal
    "telegram_subscribers": {},      # {chat_id: {name,type,added}} — auto-collected
    "withdraw_flat_since": None,     # when the account went flat, so the sell can settle
    "withdraw_locked_market": None,  # after a withdrawal, wait for the NEXT market
    # After a take-profit / stop-loss close, lock this market id so we don't re-enter
    # until the next 15m window starts.
    "tp_sl_locked_market": None,
    # Per-window Chainlink OPEN prices, keyed by window start ms:
    #   {start_ms: {"chainlink": float|None, "binance": float|None, "genuine": bool}}
    # Used to settle trades the way Polymarket does (Chainlink close vs open).
    "market_opens": {},
    "last_window_start": None
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

def get_ws_symbol_filter(symbol: str) -> str:
    s = symbol.upper()
    if s.endswith("USDT"):
        return s[:-4].lower()
    return s.lower()

# Background task instances
binance_stream = ws_data.BinanceTradeStream(symbol=settings.SYMBOL)
binance_kline_1m = ws_data.BinanceKlineStream(symbol=settings.SYMBOL, interval="1m", limit=240)
binance_kline_5m = ws_data.BinanceKlineStream(symbol=settings.SYMBOL, interval="5m", limit=200)

polymarket_ws_stream = ws_data.PolymarketChainlinkStream(
    ws_url=settings.POLYMARKET_LIVE_DATA_WS_URL,
    symbol_includes=get_ws_symbol_filter(settings.SYMBOL)
)
chainlink_ws_stream = ws_data.ChainlinkPriceStream(aggregator=settings.get_aggregator(settings.SYMBOL))

# Real-time Polymarket CLOB order-book stream — drives the displayed "Polymarket
# Odds" (book midpoint) so they match the site live, instead of the stale REST /price.
clob_ob_stream = clob_ws.ClobOrderBookStream()

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

    # Point the real-time CLOB stream at this window's tokens (resubscribes on roll).
    clob_ob_stream.set_assets([up_token_id, down_token_id])

    try:
        up_buy, down_buy, up_book, down_book = await asyncio.gather(
            data.fetch_clob_price(up_token_id, "buy"),
            data.fetch_clob_price(down_token_id, "buy"),
            data.fetch_order_book(up_token_id),
            data.fetch_order_book(down_token_id)
        )
        up_book_summary = data.summarize_order_book(up_book)
        down_book_summary = data.summarize_order_book(down_book)
    except:
        up_buy = None
        down_buy = None
        up_book_summary = {"bestBid": None, "bestAsk": None, "mid": None, "spread": None, "bidLiquidity": None, "askLiquidity": None}
        down_book_summary = {"bestBid": None, "bestAsk": None, "mid": None, "spread": None, "bidLiquidity": None, "askLiquidity": None}

    # ── Displayed "Polymarket Odds" = the ORDER-BOOK MIDPOINT, matching the site. ──
    # Prefer the real-time CLOB WebSocket book; fall back to the REST book mid, then
    # gamma's outcomePrices. This is the number shown on the dashboard — NOT the price
    # we transact at.
    up_ws = clob_ob_stream.get(up_token_id)
    down_ws = clob_ob_stream.get(down_token_id)
    up_odds = up_ws["mid"] if up_ws["mid"] is not None else (up_book_summary.get("mid") if up_book_summary.get("mid") is not None else gamma_yes)
    down_odds = down_ws["mid"] if down_ws["mid"] is not None else (down_book_summary.get("mid") if down_book_summary.get("mid") is not None else gamma_no)
    # Normalise so Up + Down = 1 (Polymarket's displayed chances sum to 100%).
    if up_odds is not None and down_odds is not None and (up_odds + down_odds) > 0:
        tot = up_odds + down_odds
        up_odds, down_odds = up_odds / tot, down_odds / tot

    # Executable BUY price = the ASK (what you actually pay). Prefer the live WS ask,
    # then the REST book ask, then the /price(buy), then gamma. Used for EV / execution.
    up_exec = up_ws["ask"] if up_ws["ask"] is not None else (up_book_summary.get("bestAsk") or up_buy or gamma_yes)
    down_exec = down_ws["ask"] if down_ws["ask"] is not None else (down_book_summary.get("bestAsk") or down_buy or gamma_no)

    return {
        "ok": True,
        "market": market,
        "prices": {          # executable ask — what a BUY costs (EV / execution)
            "up": up_exec,
            "down": down_exec
        },
        "odds": {            # book midpoint — the number shown on Polymarket (display)
            "up": up_odds,
            "down": down_odds
        },
        "token_ids": {
            "up": up_token_id,
            "down": down_token_id
        },
        "orderbook": {
            "up": up_book_summary,
            "down": down_book_summary
        }
    }

async def execute_trade(decision: Dict[str, Any], market_prices: Dict[str, Any], market: Dict[str, Any], target_open: float, token_ids: Dict[str, Any], orderbook: Optional[Dict[str, Any]] = None, strike_source: str = "chainlink_ws"):
    # Regular entry from decision engine. Returns a short reason string describing
    # the outcome (entered / which gate vetoed it) for diagnostic logging.
    if decision["action"] != "ENTER":
        return decision.get("reason", "no_trade")

    # CONSTRAINT: Only one position at a time
    if state["active_trades"]:
        return "slot_busy"

    # No Polymarket/Chainlink open for this window → do NOT open. We can't score the
    # trade against a real open, so we wait for the next window where we mark it.
    if target_open is None:
        return "no_open_price"

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

    # Liquidity: never outsize what the ask side of the book can absorb.
    ob = (orderbook or {}).get("up" if side == "UP" else "down") or {}
    ask_liq_shares = ob.get("askLiquidity")
    if ask_liq_shares is not None and price > 0:
        ask_liq_usd = ask_liq_shares * price
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
        "strike_price": target_open,
        "strike_source": strike_source,   # "chainlink_ws" (matches Polymarket) or "binance_open" (fallback)
        "open_reason": decision.get("reason", "entry"),  # "ev_enter" (EV signal) or "flip_entry" (flip)
        "end_ts": end_ts,
        # Window this trade belongs to (aligned 15m boundary) — lets a flip-closed
        # trade be scored against the market's true open/close after the window ends.
        "window_start_ms": int(end_ts * 1000) - settings.CANDLE_WINDOW_MINUTES * 60_000,
        "mode": state["trading_mode"]
    }

    if state["trading_mode"] == "paper":
        state["paper_balance"] -= amount_to_risk
        state["active_trades"].append(trade)
        state["last_trade_side"] = side
        save_state()

        open_why = "FLIP" if decision.get("reason") == "flip_entry" else "EV signal"
        log_message(f"Executed PAPER trade [{open_why}]: {side} @ {price} for {market.get('slug')} (Amount: ${amount_to_risk:.2f})")
        return "entered"
    else:
        # LIVE: place a real Fill-Or-Kill market BUY on the Polymarket CLOB.
        token_id = token_ids.get("up") if side == "UP" else token_ids.get("down")
        if not token_id:
            log_message(f"LIVE trade aborted: missing token_id for side {side}")
            return "missing_token_id"

        result = await asyncio.to_thread(clob_trader.place_market_buy, token_id, amount_to_risk, price)
        if result.get("ok"):
            resp = result.get("response") or {}
            order_id = None
            if isinstance(resp, dict):
                order_id = resp.get("orderID") or resp.get("orderId") or resp.get("id")
            trade["order_id"] = order_id
            trade["order_response"] = resp
            state["active_trades"].append(trade)
            state["last_trade_side"] = side
            save_state()
            open_why = "FLIP" if decision.get("reason") == "flip_entry" else "EV signal"
            log_message(f"Executed LIVE trade [{open_why}]: {side} ${amount_to_risk:.2f} on {market.get('slug')} (order {order_id})")
            return "entered"
        else:
            log_message(f"LIVE trade FAILED ({side}): {result.get('error')}")
            return "live_order_failed"

async def maybe_flip_position(fair_up: Optional[float], poly_snapshot: Dict[str, Any], time_left_min: Optional[float],
                              strike_open: Optional[float], strike_source: str = "chainlink_ws"):
    """Close the open position early and flip to the side favoured by FAIR PROBABILITY.

    EV is NOT considered here — the flip is driven purely by the model's fair
    probability. Opt-in (FLIP_ENABLED). Guards: the favoured side's fair prob must
    clear FLIP_MIN_CONVICTION, at least FLIP_MIN_MINUTES_LEFT must remain, we only flip
    within the same market, and we must have this window's Chainlink open. This
    function both CLOSES the old side and OPENS the new one (the normal execute_trade
    path is EV-gated and would refuse to re-enter).

    Every skip is logged (once per reason-change) so it's always visible WHY a flip
    that "should" have happened did not.
    """
    def _skip(reason: str):
        if state.get("last_flip_skip") != reason:
            state["last_flip_skip"] = reason
            log_message(f"FLIP skipped: {reason}")

    if not settings.FLIP_ENABLED:
        return
    if not state["active_trades"] or fair_up is None:
        return

    trade = state["active_trades"][0]

    # Side favoured purely by the fair-probability model (no EV).
    new_side = "UP" if fair_up >= 0.5 else "DOWN"
    new_prob = fair_up if new_side == "UP" else (1.0 - fair_up)

    # Only interesting once the model favours the OPPOSITE side of what we hold.
    if trade["side"] == new_side:
        state["last_flip_skip"] = None  # back on the held side — reset
        return

    # ONE FLIP PER MARKET: if this position is itself the result of a flip, hold it
    # to settlement — never flip again. (Analysis showed the flip *trigger* is usually
    # right, but repeated flipping/churn bleeds the edge on spread + late re-entries.)
    if trade.get("open_reason") == "flip_entry":
        _skip("already flipped once this market — holding to settlement")
        return

    if new_prob < settings.FLIP_MIN_CONVICTION:
        _skip(f"{new_side} conviction {new_prob:.2f} < {settings.FLIP_MIN_CONVICTION:.2f}")
        return
    if strike_open is None:
        _skip("no Chainlink open captured for this window")
        return
    if time_left_min is not None and time_left_min < settings.FLIP_MIN_MINUTES_LEFT:
        _skip(f"only {time_left_min:.1f}m left < {settings.FLIP_MIN_MINUTES_LEFT:.0f}m required")
        return

    market = poly_snapshot["market"]
    prices = poly_snapshot["prices"]
    token_ids = poly_snapshot.get("token_ids", {})
    orderbook = poly_snapshot.get("orderbook", {})

    if str(trade.get("market_id")) != str(market.get("id")):
        _skip("position is in a prior market (window rolled)")
        return

    held_key = "up" if trade["side"] == "UP" else "down"
    ob = orderbook.get(held_key) or {}
    exit_price = ob.get("bestBid") or prices.get(held_key)
    if not exit_price or exit_price <= 0:
        _skip(f"no exit price for {trade['side']}")
        return

    state["last_flip_skip"] = None  # clearing — we're about to flip

    if state["trading_mode"] == "live":
        token_id = token_ids.get(held_key)
        result = await asyncio.to_thread(clob_trader.place_market_sell, token_id, trade["shares"], exit_price)
        if not result.get("ok"):
            log_message(f"FLIP sell FAILED ({trade['side']}): {result.get('error')}")
            return
        # live balance is refreshed from chain elsewhere
    else:
        state["paper_balance"] += trade["shares"] * exit_price  # proceeds from selling out

    trade["status"] = "CLOSED"
    trade["exit_time"] = datetime.now().isoformat()
    trade["exit_reason"] = "flip"
    trade["settlement_price_at_expiry"] = exit_price
    trade["profit_loss"] = (trade["shares"] * exit_price) - trade["amount"]
    # The realized P/L above is the early-exit sell. Record the market OPEN now; the
    # CLOSE + whether this side actually won gets backfilled once the window ends
    # (see the backfill pass in update_loop). This is why a flip can book a loss even
    # though the market later resolves in this side's favour.
    trade["open_price"] = trade.get("strike_price")
    trade["exit_mark"] = exit_price  # price we sold out at (not the market close)
    state["trade_history"].append(trade)
    state["active_trades"] = [t for t in state["active_trades"] if t is not trade]
    state["last_trade_side"] = None
    save_state()
    log_message(f"FLIP: closed {trade['side']} @ {exit_price:.2f} (P/L ${trade['profit_loss']:.2f}); opening {new_side} (fair prob {new_prob:.2f})")

    # Open the new side immediately — driven by FAIR PROBABILITY, not EV. A synthetic
    # ENTER decision reuses execute_trade's sizing / liquidity / live-order logic.
    flip_decision = {"action": "ENTER", "side": new_side, "phase": "FLIP", "strength": "FLIP",
                     "prob": new_prob, "reason": "flip_entry"}
    await execute_trade(flip_decision, prices, market, strike_open, token_ids, orderbook, strike_source)
    return new_side  # signal to the loop that a flip happened this tick


async def close_open_position(poly_snapshot: Dict[str, Any], reason: str):
    """Sell the open position at the current bid and book the realized P/L. Used by
    take-profit / stop-loss and by the auto-withdrawal (to go flat before extracting
    funds). Returns {"side","exit_price","pl"} on success, else None."""
    if not state["active_trades"] or not poly_snapshot.get("ok"):
        return None
    trade = state["active_trades"][0]
    market = poly_snapshot["market"]
    if str(trade.get("market_id")) != str(market.get("id")):
        return None  # position is in a prior market — let it settle on its own

    prices = poly_snapshot["prices"]
    orderbook = poly_snapshot.get("orderbook", {})
    token_ids = poly_snapshot.get("token_ids", {})
    held_key = "up" if trade["side"] == "UP" else "down"
    ob = orderbook.get(held_key) or {}
    exit_price = ob.get("bestBid") or prices.get(held_key)
    if not exit_price or exit_price <= 0:
        return None

    if state["trading_mode"] == "live":
        token_id = token_ids.get(held_key)
        result = await asyncio.to_thread(clob_trader.place_market_sell, token_id, trade["shares"], exit_price)
        if not result.get("ok"):
            log_message(f"{reason} sell FAILED ({trade['side']}): {result.get('error')}")
            return None
    else:
        state["paper_balance"] += trade["shares"] * exit_price

    pl = (trade["shares"] * exit_price) - trade["amount"]
    side = trade["side"]
    trade["status"] = "CLOSED"
    trade["exit_time"] = datetime.now().isoformat()
    trade["exit_reason"] = reason
    trade["settlement_price_at_expiry"] = exit_price
    trade["profit_loss"] = pl
    trade["open_price"] = trade.get("strike_price")
    trade["exit_mark"] = exit_price
    state["trade_history"].append(trade)
    state["active_trades"] = [t for t in state["active_trades"] if t is not trade]
    state["last_trade_side"] = None
    save_state()
    return {"side": side, "exit_price": exit_price, "pl": pl}


async def maybe_tp_sl(poly_snapshot: Dict[str, Any]):
    """Take-profit / stop-loss early exit. Closes the open position when its unrealized
    P/L (marked at the current bid) reaches +TAKE_PROFIT_PCT or -STOP_LOSS_PCT of the
    stake. After it fires, the market is locked so we don't re-enter until the next
    15m window. Returns "take_profit"/"stop_loss" if it closed, else None."""
    if not settings.TP_SL_ENABLED or not state["active_trades"]:
        return None
    if not poly_snapshot.get("ok"):
        return None

    trade = state["active_trades"][0]
    market = poly_snapshot["market"]
    if str(trade.get("market_id")) != str(market.get("id")):
        return None  # position is in a prior market — let it settle on its own

    prices = poly_snapshot["prices"]
    orderbook = poly_snapshot.get("orderbook", {})
    held_key = "up" if trade["side"] == "UP" else "down"
    ob = orderbook.get(held_key) or {}
    exit_price = ob.get("bestBid") or prices.get(held_key)   # what we'd get selling now
    if not exit_price or exit_price <= 0:
        return None

    amount = trade["amount"]
    unreal = (trade["shares"] * exit_price) - amount
    pl_pct = (unreal / amount) * 100.0 if amount else 0.0

    hit = None
    if settings.TAKE_PROFIT_PCT > 0 and pl_pct >= settings.TAKE_PROFIT_PCT:
        hit = "take_profit"
    elif settings.STOP_LOSS_PCT > 0 and pl_pct <= -settings.STOP_LOSS_PCT:
        hit = "stop_loss"
    if not hit:
        return None

    res = await close_open_position(poly_snapshot, hit)
    if not res:
        return None
    # Lock this market so no re-entry until the next 15m window.
    state["tp_sl_locked_market"] = str(market.get("id"))
    save_state()
    label = "TAKE PROFIT" if hit == "take_profit" else "STOP LOSS"
    log_message(f"{label}: closed {res['side']} @ {res['exit_price']:.2f} "
                f"(P/L ${res['pl']:.2f}, {pl_pct:+.1f}%); locked market until next window")
    return hit


async def update_trades(current_prices: Dict[str, Any]):
    remaining_active = []
    trades_changed = False
    now_ts = time.time()

    # Freshest price to settle against — the CLOSE price. Polymarket settles on
    # Chainlink, and the strike (open) is now the Chainlink WS value too, so prefer
    # Chainlink here so open and close come from the SAME feed (no cross-feed offset
    # can flip a near-the-money result). Binance spot is only a last-resort fallback.
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

        # Freeze the CLOSE price the instant the window ends. Polymarket settles on the
        # Chainlink value AT the close time — not whenever we happen to resolve (which
        # can lag 15s+ behind). Capturing it once here stops post-expiry price drift
        # from flipping a near-the-money win/loss.
        if expired and trade.get("close_price") is None:
            frozen_close = cur_price or trade.get("last_price")
            if frozen_close:
                trade["close_price"] = frozen_close

        # Always poll the market (throttled ~15s) so we can read the AUTHORITATIVE
        # Polymarket resolution even after the local clock says the window expired.
        market = None
        if trade.get("last_api_check", 0) < now_ts - 15:
            try:
                market = await data.fetch_market_by_slug(trade["market_slug"])
            except Exception:
                market = None
            trade["last_api_check"] = now_ts
            if market is not None:
                trade["_market_closed"] = bool(market.get("closed"))
        market_closed = trade.get("_market_closed", False)

        # Still live: window running and market still open → keep waiting.
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
        # 1) Authoritative: a settled Polymarket outcome trades at ~$1.
        for i, p in enumerate(outcome_prices):
            try:
                if float(p) > 0.9:
                    winning_index = i
                    resolution = "polymarket_settled"
                    break
            except Exception:
                pass

        # 2) Fallback once the window/market is over: frozen CLOSE vs STRIKE (open).
        # Both are Chainlink (Polymarket WS) values, so this mirrors how Polymarket
        # resolves: did the close finish above or below the open?
        strike = trade.get("strike_price")  # the marked OPEN
        settlement_price = (trade.get("close_price") or trade.get("settlement_price_at_expiry")
                            or trade.get("last_price") or cur_price)  # the frozen CLOSE
        if winning_index == -1 and (expired or market_closed):
            if strike and settlement_price:
                trade["settlement_price_at_expiry"] = settlement_price
                winning_index = up_index if settlement_price > strike else down_index
                resolution = "close_vs_open"

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
            state["trade_history"].append(trade)
            trades_changed = True
            log_message(f"VOID: Trade for {trade['market_slug']} unresolved past grace; stake refunded (paper).")
            continue

        # ---- Settle WIN / LOSS ----
        won = ((trade["side"] == "UP" and winning_index == up_index) or
               (trade["side"] == "DOWN" and winning_index == down_index))

        # Open/close context — record it and show it in the log so the direction
        # (and which side that made win) is always visible.
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
            result = "WIN"
        else:
            trade["profit_loss"] = -trade["amount"]
            result = "LOSS"

        log_message(f"{result} [{resolution or 'unknown'}] {trade['side']}: {dir_txt} -> P/L ${trade['profit_loss']:.2f} ({trade['market_slug']})")

        trade["status"] = "CLOSED"
        trade["exit_reason"] = "settled"
        trade["exit_time"] = datetime.now().isoformat()
        trade["settlement_price_at_expiry"] = trade.get("settlement_price_at_expiry") or settlement_price
        trade["winning_outcome"] = outcomes[winning_index] if 0 <= winning_index < len(outcomes) else None
        state["trade_history"].append(trade)
        trades_changed = True

    state["active_trades"] = remaining_active
    if trades_changed:
        save_state()

async def maybe_auto_withdraw(equity: Optional[float], poly_snapshot: Dict[str, Any]):
    """Auto-withdrawal (capital extractor) state machine — LIVE mode only.

        ARMED --(EQUITY >= trigger)--> WAITING_FLAT --(close any open trade, go flat)-->
        WITHDRAWING --(submitted)--> WITHDRAW_SUBMITTED --> ARMED

    The trigger uses **equity** (cash + the value of any open position), so a running
    trade still counts toward the threshold. If a trade is open when the trigger fires,
    it is CLOSED IMMEDIATELY (sold at the bid) so the balance settles into cash.

    After the withdrawal:
      - auto_resume ON  -> trading resumes at the NEXT 15m market (this one is locked).
      - auto_resume OFF -> the bot is STOPPED entirely.
    """
    if state["trading_mode"] != "live" or not settings.AUTO_WITHDRAW_ENABLED:
        if state["withdraw_state"] != "ARMED":   # disabled → never keep entries paused
            state["withdraw_state"] = "ARMED"
        return

    st = state["withdraw_state"]
    cash = state["paper_balance"]  # live pUSD balance is mirrored here

    if st == "ARMED":
        # Trigger on EQUITY, not just cash — so an open trade counts toward it.
        if equity is not None and equity >= settings.WITHDRAW_TRIGGER_BALANCE:
            state["withdraw_state"] = "WAITING_FLAT"
            state["withdraw_flat_since"] = None
            log_message(f"Auto-withdraw: equity ${equity:.2f} >= ${settings.WITHDRAW_TRIGGER_BALANCE:.2f} "
                        f"→ pausing entries and closing any open trade")

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
        log_message("Auto-withdraw: account is flat → withdrawing")

    elif st == "WITHDRAWING":
        # Destination: the user-set address, or fall back to your own wallet (the EOA
        # derived from the key/seed) when left blank.
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
            log_message(f"Auto-withdraw: submitted ${amount:.2f} → {recipient} (tx {tx})")
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
        state["last_balance_refresh"] = 0  # fresh balance read so we don't re-trigger on a stale value
        if not settings.WITHDRAW_AUTO_RESUME:
            state["running"] = False
            log_message("Auto-withdraw complete; auto-resume OFF → bot STOPPED.")
        else:
            # Resume, but not in the market we just exited — wait for the next 15m one.
            mkt_id = str(poly_snapshot["market"].get("id")) if poly_snapshot.get("ok") else None
            if mkt_id:
                state["withdraw_locked_market"] = mkt_id
            log_message("Auto-withdraw complete; trading resumes at the next 15m market.")
        state["withdraw_state"] = "ARMED"


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

async def update_loop():
    csv_header = [
        "timestamp", "entry_minute", "time_left_min", "signal",
        "model_up", "model_down", "mkt_up", "mkt_down", "edge_up", "edge_down",
        # Chosen-side inputs on EVERY tick (even no-trade) so filters can be mined later:
        "chosen_side", "chosen_prob", "chosen_price", "chosen_ev",
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

            mc_steps = max(1, __import__('math').ceil(timing["remainingMinutes"] / 5))

            # ── Authoritative window start = the market's eventStartTime ──────────
            # NOT the local aligned clock. eventStartTime is the exact second the
            # contract's "Price to Beat" (strike) is fixed, so a feed value captured
            # at that instant matches Polymarket's settlement. Fall back to
            # endDate - window, then the local aligned boundary. (Polymarket exposes
            # no numeric strike field — it's the Chainlink price at eventStartTime.)
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

            current_price = None
            price_source = None

            # Prefer Polymarket's OWN Chainlink WS feed — it's the exact price stream
            # Polymarket settles on, so marking open/close from it matches the market
            # most faithfully. Fall back to the direct Chainlink RPC WS, then REST.
            if poly_ws.get("price"):
                current_price = poly_ws["price"]
                price_source = "Polymarket WS"
            elif cl_ws.get("price"):
                current_price = cl_ws["price"]
                price_source = "Chainlink RPC WS"
            elif chainlink_data.get("price"):
                current_price = chainlink_data["price"]
                price_source = "Chainlink RPC REST"

            # ── Mark this window's OPEN (strike) from the feed at eventStartTime ──
            # We snapshot the Chainlink feed value at the market's real start second,
            # then compare the Chainlink price at expiry against it (see update_trades).
            # Open and close come from the SAME feed, keyed to the market's own clock —
            # so the near-the-money "mistakes" from local-clock skew disappear.
            start_ms = event_start_ms
            opens = state["market_opens"]
            prev_ws = state.get("last_window_start")
            # When the window rolls over, freeze the PRIOR window's CLOSE = the last
            # Chainlink price we saw in it, so every trade there has a true close.
            if prev_ws is not None and prev_ws != start_ms and prev_ws in opens:
                if opens[prev_ws].get("close") is None and state.get("last_seen_price"):
                    opens[prev_ws]["close"] = state["last_seen_price"]
            if current_price:
                state["last_seen_price"] = current_price
            # "genuine" == we were already running in the immediately-preceding window,
            # so the first price we see in this one really is its open.
            observed_prev = prev_ws is not None and abs((start_ms - window_ms) - prev_ws) < 2000
            if start_ms not in opens:
                opens[start_ms] = {"chainlink": None, "close": None, "genuine": observed_prev}
                for k in list(opens.keys()):           # prune old windows
                    if k < start_ms - 4 * window_ms:
                        del opens[k]
            win = opens[start_ms]
            if (win["chainlink"] is None and win["genuine"] and current_price
                    and (time.time() * 1000 - start_ms) < 20_000):
                win["chainlink"] = current_price        # strike, captured at eventStartTime
                log_message(f"Window open marked @ eventStartTime (Chainlink): {current_price:.2f} @ {price_source}")
            state["last_window_start"] = start_ms

            # Strike (open) for a trade entered now: ONLY the Chainlink open captured
            # at eventStartTime. If we didn't capture it (bot wasn't running at the
            # boundary), strike_open stays None and NO trade opens this window.
            chainlink_open = win["chainlink"]
            strike_open = chainlink_open          # None until/unless captured
            strike_source = "chainlink_ws"

            # Model's fair-prob strike = the SAME authoritative open (Chainlink at
            # eventStartTime). NO Binance/spot fallback — if this window's real open
            # wasn't captured, target_open stays None: the fair prob is neutral and no
            # trade opens, rather than scoring against a wrong reference.
            target_open = strike_open

            # Fast closed-form fair probability (replaces 1000-sim Monte Carlo —
            # backtest-verified equivalent, ~1000x cheaper, which a latency play needs).
            drift_5m, sigma_5m = indicators.realized_drift_vol(klines_5m, lookback=300)
            fair_up = indicators.fair_prob_up((current_price or spot_price) or 0, target_open or 0, mc_steps, sigma_5m, drift_per_step=drift_5m or 0.0)
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

            # EV / execution uses the ASK (what a BUY actually costs).
            market_up = poly_snapshot["prices"]["up"] if poly_snapshot["ok"] else None
            market_down = poly_snapshot["prices"]["down"] if poly_snapshot["ok"] else None
            # Displayed "Polymarket Odds" = the real-time book MIDPOINT (matches the site).
            poly_odds = poly_snapshot.get("odds", {}) if poly_snapshot.get("ok") else {}
            odds_up = poly_odds.get("up")
            odds_down = poly_odds.get("down")

            # ── LATENCY EDGE ─────────────────────────────────────────────────────
            # Our fast Binance-derived fair prob vs the market's (possibly stale)
            # implied prob. The implied prob shown to the user is the MIDPOINT odds
            # (what Polymarket displays), not the ask we transact at.
            market_implied_up = None
            if odds_up is not None and odds_down is not None and (odds_up + odds_down) > 0:
                market_implied_up = odds_up / (odds_up + odds_down)
            elif market_up is not None and market_down is not None and (market_up + market_down) > 0:
                market_implied_up = market_up / (market_up + market_down)
            edge = {
                "marketUp": market_implied_up,
                "marketDown": (1 - market_implied_up) if market_implied_up is not None else None,
                "edgeUp": (fair_up - market_implied_up) if market_implied_up is not None else None,
                "edgeDown": ((1 - fair_up) - (1 - market_implied_up)) if market_implied_up is not None else None,
            }
            prob_view = {"adjustedUp": fair_up, "adjustedDown": 1 - fair_up}

            decision = engines.decide_ev({
                "mcProbUp": fair_up,
                "priceUp": market_up,
                "priceDown": market_down,
                "minProb": settings.MIN_PROB_EV,
                "evThreshold": settings.EV_THRESHOLD,
            })

            current_prices_dict = {"spot": spot_price, "chainlink": current_price}

            exec_result = None
            flip_side = None
            tp_sl_hit = None

            # Clear the market locks (take-profit/stop-loss and post-withdrawal) once the
            # window has rolled to a new market, so the next 15m window can trade again.
            cur_market_id = str(poly_snapshot["market"].get("id")) if poly_snapshot.get("ok") else None
            if state.get("tp_sl_locked_market") and cur_market_id and state["tp_sl_locked_market"] != cur_market_id:
                state["tp_sl_locked_market"] = None
            if state.get("withdraw_locked_market") and cur_market_id and state["withdraw_locked_market"] != cur_market_id:
                state["withdraw_locked_market"] = None

            # Take-profit / stop-loss runs first (even mid-withdrawal we still want to
            # honour an exit), and closing locks the market against re-entry this window.
            if poly_snapshot["ok"] and state["running"]:
                tp_sl_hit = await maybe_tp_sl(poly_snapshot)

            # Entries/flips run only when STARTED, no withdrawal pending, this market is
            # not locked (by TP/SL or by a just-completed withdrawal), and TP/SL didn't
            # just fire this tick.
            tp_sl_locked = state.get("tp_sl_locked_market") is not None and state["tp_sl_locked_market"] == cur_market_id
            withdraw_locked = state.get("withdraw_locked_market") is not None and state["withdraw_locked_market"] == cur_market_id
            entries_allowed = (state["running"] and state["withdraw_state"] == "ARMED"
                               and not tp_sl_locked and not withdraw_locked and not tp_sl_hit)
            if poly_snapshot["ok"] and entries_allowed:
                flip_side = await maybe_flip_position(fair_up, poly_snapshot, time_left_min, strike_open, strike_source)
                exec_result = await execute_trade(decision, poly_snapshot["prices"], poly_snapshot["market"], strike_open, poly_snapshot.get("token_ids", {}), poly_snapshot.get("orderbook", {}), strike_source)
            elif not state["running"]:
                exec_result = "stopped"
            elif tp_sl_hit:
                exec_result = tp_sl_hit
            elif tp_sl_locked:
                exec_result = "tp_sl_locked"
            elif withdraw_locked:
                exec_result = "withdraw_locked"

            await update_trades(current_prices_dict)

            # ── Mark-to-market: value each OPEN position at the current bid of its
            # held side (what you'd get selling right now) and roll it into equity so
            # the headline number doesn't just drop by the stake on entry.
            open_value = 0.0
            snap_ob = poly_snapshot.get("orderbook", {}) if poly_snapshot.get("ok") else {}
            snap_market_id = str(poly_snapshot["market"].get("id")) if poly_snapshot.get("ok") else None
            for t in state["active_trades"]:
                held_key = "up" if t["side"] == "UP" else "down"
                # Only trust the live book if this trade is in the market we just snapshotted.
                if snap_market_id is not None and str(t.get("market_id")) == snap_market_id:
                    bid = (snap_ob.get(held_key) or {}).get("bestBid")
                    if bid and bid > 0:
                        t["cur_bid"] = bid
                mark = t.get("cur_bid") or t.get("entry_price")  # fall back to entry (neutral) until we see a bid
                t["unrealized_pl"] = (t["shares"] * mark) - t["amount"]
                open_value += t["shares"] * mark
            equity = state["paper_balance"] + open_value

            # ── Backfill the market OUTCOME onto flip-closed trades ──────────────
            # A flip books its P/L on the early-exit sell, so a side can show a loss
            # even though the market later resolves in its favour. Once that window's
            # CLOSE is known, score each flipped-out side against the true open→close
            # so the history shows whether the market actually went its way.
            backfilled = False
            for h in state["trade_history"]:
                if h.get("exit_reason") != "flip" or h.get("market_won") is not None:
                    continue
                ws = h.get("window_start_ms")
                winfo = opens.get(ws) if ws is not None else None
                close_px = winfo.get("close") if winfo else None
                open_px = h.get("open_price") or h.get("strike_price")
                if close_px is not None and open_px:
                    h["close_price"] = close_px
                    h["market_won"] = (("UP" if close_px > open_px else "DOWN") == h["side"])
                    backfilled = True
            if backfilled:
                save_state()

            # In live mode, reflect the real on-chain pUSD balance in the dashboard
            # (refreshed ~10s so the header balance tracks near-realtime).
            if state["trading_mode"] == "live":
                now_ts = time.time()
                if now_ts - state.get("last_balance_refresh", 0) > 10:
                    real_bal = await asyncio.to_thread(clob_trader.get_usdc_balance)
                    if real_bal is not None:
                        state["paper_balance"] = real_bal
                    state["last_balance_refresh"] = now_ts
                    equity = state["paper_balance"] + open_value  # keep equity in step with the fresh balance
                # Auto-withdrawal (capital extractor) — triggers on EQUITY (cash + open
                # position value), force-closes any open trade, withdraws, then resumes
                # at the next market (or stops the bot if auto-resume is off).
                await maybe_auto_withdraw(equity, poly_snapshot)

            # A flip opens a position independently of the EV signal — surface it so
            # a flip tick never reads as a bare "NO TRADE".
            if flip_side:
                signal_label = f"FLIP {flip_side}"
                exec_result = f"flipped_to_{flip_side}"
            else:
                signal_label = f"BUY {decision['side']}" if decision["action"] == "ENTER" else "NO TRADE"
            utils.append_csv_row("./logs/signals.csv", csv_header, [
                datetime.now().isoformat(), timing["elapsedMinutes"], time_left_min,
                signal_label, fair_up, 1 - fair_up, market_up, market_down,
                edge["edgeUp"], edge["edgeDown"],
                # chosen-side prob/price/ev — present on ENTER and NO_TRADE alike
                decision.get("side"), decision.get("prob"), decision.get("price"), decision.get("ev"),
                f"{decision['side']}:{decision['phase']}:{decision['strength']}" if decision["action"] == "ENTER" else "NO_TRADE",
                decision.get("reason", ""), exec_result or ""
            ])

            state["latest_data"] = {
                "timestamp": datetime.now().isoformat(),
                "timing": timing,
                "market": poly_snapshot.get("market") if poly_snapshot["ok"] else None,
                "trading_state": {
                    "mode": state["trading_mode"],
                    "balance": state["paper_balance"],     # cash only
                    "open_value": open_value,              # mark-to-market value of open positions
                    "equity": equity,                      # cash + open position value
                    "active_trades": state["active_trades"],
                    "history_count": len(state["trade_history"]),
                    "risk": {"type": settings.RISK_TYPE, "value": settings.RISK_VALUE},
                    "symbol": settings.SYMBOL,
                    "running": state["running"],
                    "withdraw": {
                        "enabled": settings.AUTO_WITHDRAW_ENABLED,
                        "state": state["withdraw_state"],
                        "trigger_balance": settings.WITHDRAW_TRIGGER_BALANCE,
                        "amount": settings.WITHDRAW_AMOUNT,
                        "last": state["last_withdrawal"],
                    },
                },
                "prices": {
                    "spot": spot_price,
                    "chainlink": current_price,
                    "chainlink_source": price_source,
                    # Displayed "Polymarket Odds" = real-time book MIDPOINT (matches the site).
                    "poly_up": odds_up if odds_up is not None else market_up,
                    "poly_down": odds_down if odds_down is not None else market_down,
                    # The executable ask (what a BUY costs) — used for EV/execution.
                    "poly_up_ask": market_up,
                    "poly_down_ask": market_down,
                    "odds_source": "clob_ws_mid",
                    "window_open": strike_open,          # this window's marked OPEN (strike @ eventStartTime)
                    "window_open_source": strike_source  # "chainlink_ws" (Polymarket Chainlink at eventStartTime)
                },
                "indicators": {
                    "fair": fair_data
                },
                "analysis": {
                    "probability": prob_view, "edge": edge, "decision": decision
                }
            }
            state["last_update_ts"] = time.time()

        except Exception as e:
            print(f"Error in update loop: {e}")

        await asyncio.sleep(settings.POLL_INTERVAL_MS / 1000)


def render_template(template_name: str, request: Request, context: Optional[Dict[str, Any]] = None):
    ctx = {"request": request}
    if context:
        ctx.update(context)
    try:
        return templates.TemplateResponse(request=request, name=template_name, context=ctx)
    except TypeError:
        return templates.TemplateResponse(template_name, ctx)


@app.get("/", response_class=HTMLResponse)
async def get_dashboard(request: Request):
    return render_template("index.html", request)

@app.get("/settings", response_class=HTMLResponse)
async def get_settings_page(request: Request):
    return render_template("settings.html", request)

@app.get("/api/latest")
async def get_latest():
    return state["latest_data"]

@app.get("/api/logs")
async def get_logs():
    return state["logs"]

@app.get("/api/available-series")
async def get_available_series():
    return await data.fetch_available_15m_series()

@app.get("/api/settings")
async def get_settings():
    pk = settings.PRIVATE_KEY
    masked_pk = pk[:6] + "..." + pk[-4:] if pk and len(pk) > 10 else pk

    return {
        "mode": settings.MODE,
        "paper_balance_usd": settings.PAPER_BALANCE_USD,
        "private_key": masked_pk,
        "relayer": {
            "api_key": "set" if settings.RELAYER_API_KEY else ""
        },
        "capital_extractor": {
            "enabled": settings.AUTO_WITHDRAW_ENABLED,
            "trigger_balance": settings.WITHDRAW_TRIGGER_BALANCE,
            "withdraw_amount": settings.WITHDRAW_AMOUNT,
            "withdraw_address": settings.WITHDRAW_ADDRESS,
            "auto_resume_after_withdrawal": settings.WITHDRAW_AUTO_RESUME,
            "resume_after": settings.WITHDRAW_RESUME_AFTER
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
        "tp_sl": {
            "enabled": settings.TP_SL_ENABLED,
            "take_profit_pct": settings.TAKE_PROFIT_PCT,
            "stop_loss_pct": settings.STOP_LOSS_PCT
        },
        "telegram": {
            "enabled": settings.TELEGRAM_ENABLED,
            "bot_token": "set" if settings.TELEGRAM_BOT_TOKEN else ""
        },
        "chainlink": {
            "alchemy_api_key": "set" if settings.ALCHEMY_API_KEY else ""
        }
    }

@app.post("/api/settings")
async def post_settings(new_settings: Dict[str, Any]):
    global binance_stream, polymarket_ws_stream, chainlink_ws_stream, binance_kline_1m, binance_kline_5m
    old_symbol = settings.SYMBOL

    new_pk = new_settings.get("private_key")
    if new_pk and "..." in new_pk:
        # masked value returned by GET — keep the stored key unchanged
        new_settings["private_key"] = settings.PRIVATE_KEY
    elif new_pk is not None:
        from bot.config import normalize_private_key
        settings.PRIVATE_KEY = normalize_private_key(new_pk)
        new_settings["private_key"] = settings.PRIVATE_KEY  # persist the derived hex key, never the seed

    # "set" is the masked sentinel returned by GET for stored secrets — if the form
    # sends it back unchanged, don't overwrite the real key with the sentinel.
    if isinstance(new_settings.get("relayer"), dict) and new_settings["relayer"].get("api_key") == "set":
        new_settings["relayer"].pop("api_key", None)
    if isinstance(new_settings.get("chainlink"), dict) and new_settings["chainlink"].get("alchemy_api_key") == "set":
        new_settings["chainlink"].pop("alchemy_api_key", None)
    if isinstance(new_settings.get("telegram"), dict) and new_settings["telegram"].get("bot_token") == "set":
        new_settings["telegram"].pop("bot_token", None)

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

    if "tp_sl" in new_settings:
        ts = new_settings["tp_sl"]
        if "enabled" in ts: settings.TP_SL_ENABLED = bool(ts["enabled"])
        settings.TAKE_PROFIT_PCT = float(ts.get("take_profit_pct", settings.TAKE_PROFIT_PCT))
        settings.STOP_LOSS_PCT = float(ts.get("stop_loss_pct", settings.STOP_LOSS_PCT))

    if "telegram" in new_settings:
        tg = new_settings["telegram"]
        if "enabled" in tg: settings.TELEGRAM_ENABLED = bool(tg["enabled"])
        if "bot_token" in tg: settings.TELEGRAM_BOT_TOKEN = tg["bot_token"]

    if "polymarket" in new_settings:
        p = new_settings["polymarket"]
        settings.POLYMARKET_SERIES_ID = p.get("series_id", settings.POLYMARKET_SERIES_ID)
        settings.POLYMARKET_UP_LABEL = p.get("up_label", settings.POLYMARKET_UP_LABEL)
        settings.POLYMARKET_DOWN_LABEL = p.get("down_label", settings.POLYMARKET_DOWN_LABEL)

    if "relayer" in new_settings and isinstance(new_settings["relayer"], dict):
        if "api_key" in new_settings["relayer"]:
            settings.RELAYER_API_KEY = new_settings["relayer"]["api_key"]

    if "chainlink" in new_settings and isinstance(new_settings["chainlink"], dict):
        if "alchemy_api_key" in new_settings["chainlink"]:
            settings.ALCHEMY_API_KEY = new_settings["chainlink"]["alchemy_api_key"]

    if "capital_extractor" in new_settings:
        ce = new_settings["capital_extractor"]
        if "enabled" in ce: settings.AUTO_WITHDRAW_ENABLED = bool(ce["enabled"])
        if "trigger_balance" in ce: settings.WITHDRAW_TRIGGER_BALANCE = float(ce["trigger_balance"])
        if "withdraw_amount" in ce: settings.WITHDRAW_AMOUNT = float(ce["withdraw_amount"])
        if "withdraw_address" in ce: settings.WITHDRAW_ADDRESS = ce["withdraw_address"]
        if "auto_resume_after_withdrawal" in ce: settings.WITHDRAW_AUTO_RESUME = bool(ce["auto_resume_after_withdrawal"])
        if "resume_after" in ce: settings.WITHDRAW_RESUME_AFTER = ce["resume_after"]

    # Credentials may have changed — drop the cached CLOB client so the next live
    # order re-initialises with the new key / relayer / alchemy settings.
    clob_trader.reset()

    state["trading_mode"] = settings.MODE
    # Only reset the displayed balance in paper mode; live mode reads the on-chain
    # pUSD balance and we don't want to clobber it with the paper default on save.
    if settings.MODE == "paper":
        state["paper_balance"] = settings.PAPER_BALANCE_USD

    if settings.SYMBOL != old_symbol:
        binance_stream.close()
        binance_stream = ws_data.BinanceTradeStream(symbol=settings.SYMBOL)
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
    return {"ok": True, "running": True}

@app.post("/api/stop")
async def stop_trading():
    """Stop all trading. New entries and flips halt immediately; any open position
    keeps settling to expiry so it can't get stuck."""
    state["running"] = False
    _reflect_running_now()
    log_message("Trading STOPPED by user")
    return {"ok": True, "running": False}

@app.post("/api/test-connection")
async def test_connection():
    """Validate the saved key/seed + relayer: derive the EOA and candidate wallets and
    report which one holds pUSD (the trading wallet) and the chosen signature type."""
    result = await asyncio.to_thread(clob_trader.test_connection)
    if result.get("ok"):
        log_message(f"Connection test OK: EOA {result.get('eoa')} → trading wallet {result.get('funder')}"
                    + ("" if result.get("relayer_key_set") else "  (relayer key MISSING)"))
    else:
        log_message(f"Connection test failed: {result.get('error')}")
    return result

@app.get("/api/telegram-subscribers")
async def get_telegram_subscribers():
    """The list of chats that have started the bot (auto-collected) — shown on the
    Settings page so you can see who receives alerts and remove anyone."""
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
                        "Polymarket bot. Send /stop to unsubscribe.")
    log_message(f"Telegram test alert broadcast to {len(state['telegram_subscribers'])} subscriber(s)")
    return {"ok": True, "count": len(state["telegram_subscribers"])}

@app.get("/health")
async def health():
    return {"status": "ok", "last_update": state["last_update_ts"], "mode": state["trading_mode"], "running": state["running"]}

@app.get("/history")
async def get_history():
    return state["trade_history"]

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8010))
    uvicorn.run(app, host="0.0.0.0", port=port)
