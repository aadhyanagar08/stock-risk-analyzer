
# src/factors.py

from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List
import json
import os

import numpy as np
import pandas as pd

from .fetch import get_prices

PER_YEAR = {"D": 252, "W": 52, "M": 12}

@dataclass
class SeriesBundle:
    symbol: str
    df: pd.DataFrame  # columns: date, adj_close



def _load_prices_csv(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    df["date"] = pd.to_datetime(df["date"])
    return df[["date", "adj_close"]].dropna()

def _bundle(symbol: str, bench: str, timeframe: str, freq: str, ttl_days: int) -> SeriesBundle:
    item = get_prices(symbol, bench, timeframe, freq, ttl_days=ttl_days)
    df = _load_prices_csv(item.path_prices)
    force = os.getenv("FORCE_REFRESH") == "1"
    item = get_prices(symbol, bench, timeframe, freq, ttl_days=ttl_days, force=force)
    return SeriesBundle(symbol=symbol.upper(), df=df)


def _align_on_intersection(bundles: List[SeriesBundle]) -> List[SeriesBundle]:
    # Find common dates intersection
    sets = [set(b.df["date"]) for b in bundles]
    common = sorted(list(set.intersection(*sets)))
    if len(common) < 60:
        # Give back as-is; compare.py can decide to exclude
        return [SeriesBundle(symbol=b.symbol, df=b.df[b.df["date"].isin(common)].reset_index(drop=True)) for b in bundles]
    return [SeriesBundle(symbol=b.symbol, df=b.df[b.df["date"].isin(common)].reset_index(drop=True)) for b in bundles]

def _simple_returns(arr: np.ndarray) -> np.ndarray:
    # r_t = P_t / P_{t-1} - 1
    return arr[1:] / arr[:-1] - 1.0

def _max_drawdown(prices: np.ndarray) -> float:
    # prices assumed aligned, np.array
    curve = prices / prices[0]
    peaks = np.maximum.accumulate(curve)
    dd = curve / peaks - 1.0
    return float(dd.min())  # negative

def compute_factors(
    tickers: List[str],
    benchmark: str,
    timeframe: str,
    freq: str = "D",
    risk_free_rate_annual: float = 0.02,
    ttl_days: int = 3,
) -> pd.DataFrame:
    """
    Returns a DataFrame with rows per ticker and columns:
    symbol, currency, as_of, n_periods, vol, max_dd, sharpe, beta, r2
    (yield/expense_ratio can be merged later)
    """
    # Load bundles (benchmark + tickers) then align
    b_bundle = _bundle(benchmark, benchmark, timeframe, freq, ttl_days)
    t_bundles = [_bundle(t, benchmark, timeframe, freq, ttl_days) for t in tickers]
    bundles = _align_on_intersection([b_bundle] + t_bundles)
    # After alignment, benchmark is bundles[0]
    bench_df = bundles[0].df
    bench_prices = bench_df["adj_close"].to_numpy(dtype=float)
    bench_ret = _simple_returns(bench_prices)

    N = PER_YEAR[freq]
    rf_per = risk_free_rate_annual / N

    rows = []
    as_of = str(bench_df["date"].iloc[-1].date())
    n_periods = len(bench_ret)

    for b in bundles[1:]:
        px = b.df["adj_close"].to_numpy(dtype=float)
        ret = _simple_returns(px)

        # Guard: lengths match
        if len(ret) != len(bench_ret) or len(ret) < 60:
            rows.append({
                "symbol": b.symbol,
                "as_of": as_of,
                "n_periods": int(len(ret)),
                "vol": np.nan,
                "max_dd": np.nan,
                "sharpe": np.nan,
                "beta": np.nan,
                "r2": np.nan,
                "warnings": "insufficient_history",
            })
            continue

        vol = float(np.std(ret, ddof=1) * np.sqrt(N))
        max_dd = _max_drawdown(px)
        ex = ret - rf_per
        ex_mean = float(np.mean(ex))
        ex_std = float(np.std(ret, ddof=1))  # use raw stdev of returns for Sharpe denom
        sharpe = float((ex_mean / ex_std) * np.sqrt(N)) if ex_std > 0 else np.nan

        # Beta & R2
        # If bench variance ~0, beta undefined
        bench_var = float(np.var(bench_ret, ddof=1))
        cov = float(np.cov(ret, bench_ret, ddof=1)[0][1])
        if bench_var > 0:
            beta = cov / bench_var
            corr = float(np.corrcoef(ret, bench_ret)[0][1])
            r2 = corr ** 2
        else:
            beta, r2 = np.nan, np.nan

        rows.append({
            "symbol": b.symbol,
            "as_of": as_of,
            "n_periods": n_periods,
            "vol": vol,
            "max_dd": max_dd,
            "sharpe": sharpe,
            "beta": float(beta),
            "r2": float(r2),
            "warnings": "",
        })

    df = pd.DataFrame(rows)
    return df
