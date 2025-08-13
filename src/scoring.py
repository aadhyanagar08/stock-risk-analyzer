# src/scoring.py

from __future__ import annotations
from typing import Dict, List, Optional
import numpy as np
import pandas as pd

HIGHER_BETTER = {"sharpe", "yield", "r2_align"}
LOWER_BETTER = {"vol", "expense_ratio"}  # drawdown handled specially

def _minmax(series: pd.Series) -> pd.Series:
    s = series.astype(float)
    if s.dropna().nunique() <= 1:
        return pd.Series([0.5] * len(s), index=s.index)
    mn, mx = s.min(), s.max()
    return (s - mn) / (mx - mn)

def _invert(s: pd.Series) -> pd.Series:
    return 1.0 - s

def score_and_rank(
    factors: pd.DataFrame,
    weights: Dict[str, float],
    r2_align_target: str = "high",
) -> pd.DataFrame:
    df = factors.copy()

    # Build r2_align depending on target
    if "r2" in df.columns:
        if r2_align_target == "low":
            df["r2_align"] = 1.0 - df["r2"]
        elif r2_align_target == "high":
            df["r2_align"] = df["r2"]
        else:
            df["r2_align"] = np.nan

    # Prepare a working set of metric columns that exist in df and in weights
    metric_cols: List[str] = []
    for k in weights.keys():
        if k == "max_dd":
            if "max_dd" in df.columns:
                # use drawdown magnitude (lower is better)
                df["dd_mag"] = df["max_dd"].abs()
                metric_cols.append("dd_mag")
        elif k in df.columns:
            metric_cols.append(k)

    # Normalize each metric to 0-1 where "higher is better"
    norm_cols = []
    for m in metric_cols:
        col = m
        norm = _minmax(df[col])
        if m in LOWER_BETTER or m == "dd_mag":
            norm = _invert(norm)
        df[f"{col}_norm"] = norm
        norm_cols.append(f"{col}_norm")

    # Compute contributions only for metrics with non-null norms
    # Reweight per-row if some metrics are NaN
    contrib_cols = []
    for idx, row in df.iterrows():
        # Build row weights map aligned to norm columns
        pairs = []
        for m in metric_cols:
            w_key = "max_dd" if m == "dd_mag" else m  # weights use 'max_dd'
            w = float(weights.get(w_key, 0.0))
            v = row.get(f"{m}_norm", np.nan)
            if pd.notna(v) and w > 0:
                pairs.append((m, w, v))
        if not pairs:
            for m in metric_cols:
                df.at[idx, f"contrib_{m}"] = np.nan
            df.at[idx, "score"] = np.nan
            continue
        # Renormalize weights to sum 1 for available metrics
        wsum = sum(w for _, w, _ in pairs)
        pairs = [(m, w / wsum, v) for (m, w, v) in pairs]
        # Contributions and score
        sc = 0.0
        for m, w, v in pairs:
            c = w * v
            df.at[idx, f"contrib_{m}"] = c
            sc += c
        df.at[idx, "score"] = sc
        contrib_cols.extend([f"contrib_{m}" for m, _, _ in pairs])

    # Rank (desc), with tie-breakers: Sharpe desc → dd_mag asc → expense_ratio asc
    df["rank"] = df["score"].rank(ascending=False, method="min")
    # stable sort using tie-breakers
    sort_cols = [("score", False)]
    if "sharpe" in df.columns:
        sort_cols.append(("sharpe", False))
    if "dd_mag" in df.columns:
        sort_cols.append(("dd_mag", True))
    if "expense_ratio" in df.columns:
        sort_cols.append(("expense_ratio", True))
    by = [c for c, _ in sort_cols]
    ascending = [asc for _, asc in sort_cols]
    df = df.sort_values(by=by, ascending=ascending, na_position="last").reset_index(drop=True)
    # Re-number rank cleanly
    df["rank"] = range(1, len(df) + 1)

    return df
