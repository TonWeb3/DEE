# Polymarket BTC 15m Assistant (Python FastAPI)

A real-time trading assistant for Polymarket **"Bitcoin Up or Down" 15-minute** markets, ported to Python and FastAPI.

It runs a **latency-arbitrage** strategy: a fast closed-form fair probability from
Binance spot vs Polymarket's (possibly stale) implied price, traded on the gap, with
position size set by a simple percent-of-balance or fixed-dollar risk. See
[`strategy.md`](strategy.md) for the full rationale.

## Features

- Real-time Web Dashboard (FastAPI + Jinja2 + Alpine.js)
- Fast fair-probability model (closed-form GBM) + EV entry engine
- Trade Execution: Paper Trading simulation vs Live Mode toggle
- Data Sources: Binance, Polymarket (Gamma/CLOB), Chainlink (WebSocket + RPC)
- Proxy Support: Global HTTP/HTTPS/SOCKS proxy configuration

## Requirements

- Python **3.11+**
- pip (comes with Python)

## Local Run

### 1) Install dependencies

```bash
pip install -r requirements.txt
```

### 2) Configure `config.json`

Set your trading mode, risk preferences, and optional private key in `config.json`.
Position size is set by `trading.risk_type` (`"percent"` = `risk_value`% of balance,
or `"fixed"` = `risk_value` dollars) and `trading.risk_value`. The entry engine is
tuned in the `ev` block:

```jsonc
"ev": {
  "ev_threshold": 0.04,          // enter only when fair prob − share price ≥ this (the edge gate)
  "min_prob": 0.55,              // never bet near-coinflips even if EV looks positive
  "min_book_liquidity_usd": 20.0 // skip if the ask side can't absorb the stake
}
```

All of these are also editable live on the **Settings** page.

### 3) Run

```bash
uvicorn main:app --host 0.0.0.0 --port 8000
```

Access the dashboard at `http://localhost:8000`.

## Docker

```bash
docker build -t polymarket-assistant .
docker run -p 8000:8000 polymarket-assistant
```

## Deployment on Render

If you are seeing errors related to Node.js or `npm run start`, it is because Render is auto-detecting the old environment. **You must manually set the runtime to Python.**

### Recommended: Use `render.yaml`
The repository includes a `render.yaml`. When creating a new blueprint on Render, it will automatically set the correct environment.

### Manual Setup
1. Create a **Web Service** on Render.
2. Under **Runtime**, explicitly select **Python 3**.
3. Set the following commands:
   - **Build Command**: `pip install -r requirements.txt`
   - **Start Command**: `uvicorn main:app --host 0.0.0.0 --port 8000`
4. Add any necessary environment variables (optional).

## Live Trading

Switching **Mode** to `live` (config or the Settings page) makes the bot place real
**Fill-Or-Kill market BUY** orders on the Polymarket CLOB via `py-clob-client`.

Before enabling live mode you must, once and outside this app:

1. Set a `private_key` (Settings → Credentials, or `config.json`).
2. Choose the **Signature Type**: `0` = EOA (your own wallet), `1` = Email/Magic
   proxy, `2` = Browser proxy. For `1`/`2` you must also set the **Funder** (proxy
   wallet address). EOA leaves Funder blank.
3. Fund that wallet with **USDC on Polygon** (plus a little POL for gas).
4. **Approve the exchange contracts.** For an EOA, click **Setup Allowances
   (EOA only)** on the Settings page (or `POST /api/setup-allowances`) — it sends
   the one-time USDC/CTF approvals to the Polymarket exchange, neg-risk exchange,
   and neg-risk adapter, skipping any that are already set. Proxy wallets already
   have allowances managed by Polymarket.

Orders are placed as **slippage-capped marketable Fill-Or-Kill** orders: the limit
price is the current market quote plus a small buffer (`CLOB_MAX_SLIPPAGE`, default
2¢), so if the book moves away the order is killed rather than filled at a bad
price. In live mode the dashboard balance reflects the real on-chain USDC balance
(refreshed periodically). Order failures are reported in the Console Log.

## Safety

This is not financial advice. Use at your own risk; live mode trades real funds.

**The edge is unproven.** The model has no predictive edge over "is spot already above
the open" — that signal is fully priced by the market. The only remaining edge is
latency (beating the book's repricing). Run in **paper mode** and confirm EV-positive
trades actually exist against the real book *before* risking capital.
