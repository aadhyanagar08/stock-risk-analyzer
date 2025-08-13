"""Tickers

Non-empty strings; length 1–10

Allowed chars: A–Z, digits, ., -

Uppercase them for consistency

Weights

Must include: vol, max_dd, sharpe, expense_ratio, yield, r2_align

Each weight ≥ 0

Sum ≈ 1.0 (tolerance ±0.01); if not, rescale proportionally

Timeframe

Allowed presets: 1y, 3y, 5y (you can add more)

Start < End if using explicit dates

Ensure enough points for metrics (e.g., ≥ 120 daily returns)

Selection limits

2 ≤ number of tickers ≤ 25 for Compare (keeps UI responsive)

Journal entries

date ISO format; tickers non-empty; profile_name valid

action ∈ {BUY, REJECT, WATCH}

Profiles

YAML must match schema keys; reject unknown/missing keys

r2_align_target ∈ {high, low, none}

Data readiness (later combined with factors)

If a ticker’s aligned history < threshold after intersection, mark insufficient_history

"""
from typing import Dict, Iterable, List
ALLOWED_METRIC =["vol","max_dd", "sharpe", "expense_ratio", "yield", "r2_align"]

def validate_tickers(tickers: Iterable[str]) -> List[str]:
    out=[]
    for t in tickers:
        if not isinstance (t, str):
            continue
        t2= t.strip().upper()
        if 1<= len(t2)<=10 and all (c.isalum() or c in ".-" for c in t2):
            out.append(t2)
    if len(out)< 1:
        raise ValueError("At least one valid ticker is required")
    return out
def normalise_weights(weights: Dict[str, float]) -> Dict[str,float]:
    #keeping only known metrics coerce negatives to 0
    w= {k: max(0.0, float(v)) for k, v in weights.items() if k in ALLOWED_METRIC}
    total = sum(w.values())
    if total == 0:
        raise ValueError("At least one weight must be non-zero")
    return {k: v / total for k, v in w.items()}
def validate_timeframe(timeframe: str) -> str:
    allowed = {"1y", "3y", "5y"}
    tf2 = timeframe.strip().lower()
    if tf2 not in allowed:
        raise ValueError(f"Invalid timeframe: {timeframe}. Allowed: {allowed}")
    return tf2
def validate_frequency(frequency: str) -> str:
    allowed = {"D", "W", "M"}
    if frequency not in allowed:
        raise ValueError(f"Invalid frequency: {frequency}. Allowed: {allowed}")
    return frequency
