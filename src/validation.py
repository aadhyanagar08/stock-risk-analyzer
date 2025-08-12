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