from typing import Dict, Any

# ─────────────────────────────────────────────────────────────────────────────
#  Latency-arb entry engine.
#
#  Backtest verdict: the model has NO predictive edge over the trivial "is spot
#  already above the 15m open?" baseline — that signal is fully priced by the
#  market. The only edge left is LATENCY: act on a Binance spot move before
#  Polymarket's thin book reprices.
#
#  The decision is purely a fast fair probability (from Binance spot) vs the
#  market's implied price. Enter when the gap (expected value) is large enough
#  that the book looks stale. There are no other filters.
# ─────────────────────────────────────────────────────────────────────────────


def _no_ev(reason: str, side=None, prob=None, price=None, ev=None) -> Dict[str, Any]:
    # Carry the chosen side's prob/price/ev even on a no-trade so every tick can be
    # logged and mined for filters later.
    return {"action": "NO_TRADE", "side": side, "phase": "EV", "strength": "EV",
            "reason": reason, "prob": prob, "price": price, "ev": ev}


def decide_ev(inputs: Dict[str, Any]) -> Dict[str, Any]:
    """EV gate: fair probability (Binance) vs market ask price (Polymarket).

    EV_side = p_side - ask_price_side. A positive EV beyond `evThreshold` means the
    book is underpricing the side our fast feed already favours — the latency edge.
    Position sizing (percent/fixed of balance) is handled by the caller.
    """
    p_up = inputs.get("mcProbUp")
    price_up = inputs.get("priceUp")     # ask (buy) price for the UP share, 0..1
    price_down = inputs.get("priceDown") # ask (buy) price for the DOWN share, 0..1

    if p_up is None:
        return _no_ev("missing_model_data")
    if price_up is None or price_down is None:
        return _no_ev("missing_prices")

    p_down = 1.0 - p_up
    ev_up = p_up - price_up
    ev_down = p_down - price_down

    side = "UP" if ev_up >= ev_down else "DOWN"
    p = p_up if side == "UP" else p_down
    price = price_up if side == "UP" else price_down
    ev = ev_up if side == "UP" else ev_down

    min_prob = inputs.get("minProb", 0.55)
    ev_threshold = inputs.get("evThreshold", 0.04)

    # ── GATES ──
    if p < min_prob:
        return _no_ev(f"prob_{p:.2f}_below_{min_prob:.2f}", side, p, price, ev)
    if ev < ev_threshold:
        return _no_ev(f"ev_{ev:.3f}_below_{ev_threshold:.3f}", side, p, price, ev)

    strength = "HIGH_CONVICTION" if p >= 0.70 else "STRONG"
    return {
        "action": "ENTER", "side": side, "phase": "EV", "strength": strength,
        "prob": p, "price": price, "ev": ev, "reason": "ev_enter"
    }
