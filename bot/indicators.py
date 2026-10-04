import numpy as np
import math
from typing import List, Optional, Dict


def realized_drift_vol(candles: List[Dict], lookback: int = 300):
    """(mean, std) of per-candle log returns — the per-step drift & sigma for the
    fair-prob model. Returns (None, None) if there isn't enough data."""
    closes = [c["close"] for c in candles[-lookback:] if c.get("close")]
    if len(closes) < 20:
        return None, None
    arr = np.asarray(closes, dtype=float)
    rets = np.diff(np.log(arr))
    rets = rets[np.isfinite(rets)]
    if len(rets) < 10:
        return None, None
    return float(np.mean(rets)), float(np.std(rets))


def fair_prob_up(current_price: float, strike: float, steps: int,
                 sigma_per_step: Optional[float], drift_per_step: float = 0.0) -> float:
    """Closed-form GBM probability that price closes ABOVE `strike` after `steps`
    5-minute intervals — the core direction/edge model. The model is just
    persistence: "is spot above the open, given the volatility still to come?"
    Returns 0..1.
    """
    if not current_price or not strike or current_price <= 0 or strike <= 0:
        return 0.5
    n = max(1, int(steps))
    if sigma_per_step is None or sigma_per_step <= 0:
        return 1.0 if current_price > strike else 0.0
    mu = (drift_per_step - 0.5 * sigma_per_step ** 2) * n
    sd = sigma_per_step * math.sqrt(n)
    # P(S * exp(X) > K) for X ~ N(mu, sd^2)  ->  1 - Phi(z)  ->  0.5 * erfc(z/sqrt2)
    z = (math.log(strike / current_price) - mu) / sd
    prob = 0.5 * math.erfc(z / math.sqrt(2))
    return float(min(1.0, max(0.0, prob)))
