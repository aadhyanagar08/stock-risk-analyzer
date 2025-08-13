# src/compare.py

import os
from typing import Dict, List, Optional

from .validation import validate_tickers, validate_timeframe, validate_freq, normalize_weights
from .profiles import load_profile, merge_overrides
from .factors import compute_factors
from .scoring import score_and_rank

def compare_and_rank(
    tickers: List[str],
    benchmark: str = "SPY",
    profile_name: str = "default",
    weight_overrides: Optional[Dict[str, float]] = None,
    timeframe: str = "3y",
    freq: str = "D",
) :
    # Validation
    tickers = validate_tickers(tickers)
    benchmark = validate_tickers([benchmark])[0]
    timeframe = validate_timeframe(timeframe)
    freq = validate_freq(freq)

    # Profile and weights
    prof = load_profile(profile_name)
    prof = merge_overrides(prof, weight_overrides)
    weights = normalize_weights(prof.get("weights", {}))
    r2_target = prof.get("r2_align_target", "high")

    # Config
    rf = float(os.getenv("RISK_FREE_RATE", "0.02"))
    ttl_days = int(os.getenv("CACHE_TTL_DAYS", "3"))

    # Factors
    fdf = compute_factors(tickers, benchmark, timeframe, freq=freq, risk_free_rate_annual=rf, ttl_days=ttl_days)

    # Scoring
    sdf = score_and_rank(fdf, weights=weights, r2_align_target=r2_target)
    return sdf
