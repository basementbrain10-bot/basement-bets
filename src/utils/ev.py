"""Central EV + odds conversion utilities.

Goal: one canonical implementation used everywhere (models, risk manager, UI exports).

Definitions:
- American odds: -110, +120, etc.
- Decimal odds: total return per 1 staked (includes stake), e.g. -110 -> 1.9091
- b (net payout multiplier): decimal_odds - 1
- EV per unit risked: p*b - (1-p)

Guardrails:
- Clamp win prob to [0,1]
- Sanitize impossible american odds (e.g. -9) by returning None or defaulting upstream
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional


def clamp_prob(p: Any, default: float = 0.5) -> float:
    try:
        x = float(p)
    except Exception:
        return float(default)
    if x < 0.0:
        return 0.0
    if x > 1.0:
        return 1.0
    return x


def sanitize_american_odds(price: Any, default: Optional[int] = -110) -> Optional[int]:
    """Return a plausible American odds int.

    If default is None, returns None when odds are invalid.
    """
    try:
        if price is None or price == '':
            return default
        p = int(price)
    except Exception:
        return default

    # Typical odds: <= -100 or >= +100.
    if -100 < p < 100:
        return default
    return p


def american_to_decimal(price: Any) -> Optional[float]:
    p = sanitize_american_odds(price, default=None)
    if p is None:
        return None
    if p > 0:
        return 1.0 + (p / 100.0)
    return 1.0 + (100.0 / abs(p))


def american_to_b(price: Any) -> Optional[float]:
    """Net payout multiplier b (profit per 1 risked if win)."""
    dec = american_to_decimal(price)
    if dec is None:
        return None
    return dec - 1.0


def implied_prob_american(price: Any) -> Optional[float]:
    p = sanitize_american_odds(price, default=None)
    if p is None:
        return None
    if p > 0:
        return 100.0 / (p + 100.0)
    return abs(p) / (abs(p) + 100.0)


def ev_per_unit(win_prob: Any, price: Any, *, clamp: bool = True) -> Optional[float]:
    p = clamp_prob(win_prob) if clamp else win_prob
    b = american_to_b(price)
    if b is None:
        return None
    return (p * b) - (1.0 - p)


def kelly_fraction(win_prob: Any, price: Any, *, kelly_mult: float = 0.25) -> Optional[float]:
    p = clamp_prob(win_prob)
    b = american_to_b(price)
    if b is None or b <= 0:
        return None
    q = 1.0 - p
    f = (b * p - q) / b
    f = max(0.0, float(f))
    return f * float(kelly_mult)


@dataclass
class EvBreakdown:
    win_prob: float
    american_odds: int
    decimal_odds: float
    b: float
    implied_prob: float
    ev: float
    kelly_q: float


def ev_breakdown(win_prob: Any, price: Any, *, kelly_mult: float = 0.25) -> Optional[EvBreakdown]:
    p = clamp_prob(win_prob)
    a = sanitize_american_odds(price, default=None)
    if a is None:
        return None
    dec = american_to_decimal(a)
    b = american_to_b(a)
    ip = implied_prob_american(a)
    ev = ev_per_unit(p, a)
    k = kelly_fraction(p, a, kelly_mult=kelly_mult)
    if dec is None or b is None or ip is None or ev is None or k is None:
        return None
    return EvBreakdown(
        win_prob=p,
        american_odds=a,
        decimal_odds=dec,
        b=b,
        implied_prob=ip,
        ev=ev,
        kelly_q=k,
    )
