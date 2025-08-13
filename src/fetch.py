"""
Add these bullets as top-of-file comments:

Responsibilities

Build cache keys and paths ({TICKER}__{BENCH}__{TIMEFRAME}__{FREQ})

Read from cache if fresh (TTL check)

Fetch adjusted close via yfinance if stale/missing

Write prices CSV + meta JSON

Update manifest/index.json

Return a lightweight object: { "symbol": ..., "prices_path": ..., "meta": {...} }

Behavior

Always use Adjusted Close

Store currency and as_of in meta

If API returns empty series → raise a clear error

Respect MAX_TICKERS_PER_COMPARE

Edge cases

Short history (flag for later exclusion in Factors)

Non-trading days (OK—time alignment happens in Factors)

Temporary API errors → surface a user-friendly message

(You’re not coding this yet—just documenting the contract.)
"""
# src/fetch.py

from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
import json
from typing import Dict, Tuple

import yfinance as yf

CACHE_DIR = Path("data/cache")
PRICES_DIR = CACHE_DIR / "prices"
MANIFEST_DIR = CACHE_DIR / "manifest"
MANIFEST_PATH = MANIFEST_DIR / "index.json"

FREQ_TO_YF = {"D": ("1d",), "W": ("1wk",), "M": ("1mo",)}
TF_ALLOWED = {"1y", "3y", "5y"}

@dataclass
class CacheItem:
    key: str
    path_prices: Path
    path_meta: Path
    last_updated_utc: str
    ttl_days: int
    expires_utc: str
    source: str = "yfinance"
    currency: str = "USD"

def _ensure_dirs():
    PRICES_DIR.mkdir(parents=True, exist_ok=True)
    MANIFEST_DIR.mkdir(parents=True, exist_ok=True)
    if not MANIFEST_PATH.exists():
        MANIFEST_PATH.write_text(json.dumps({"version": 1, "items": []}, indent=2))

def _load_manifest() -> Dict:
    _ensure_dirs()
    try:
        return json.loads(MANIFEST_PATH.read_text() or "{}")
    except Exception:
        return {"version": 1, "items": []}

def _save_manifest(m: Dict) -> None:
    MANIFEST_PATH.write_text(json.dumps(m, indent=2))

def cache_key(ticker: str, bench: str, timeframe: str, freq: str) -> str:
    return f"{ticker.upper()}__{bench.upper()}__{timeframe.lower()}__{freq}"

def _paths_for(key: str) -> Tuple[Path, Path]:
    p = PRICES_DIR / f"{key}.csv"
    meta = PRICES_DIR / f"{key}__meta.json"
    return p, meta

def _manifest_get(manifest: Dict, key: str) -> Dict | None:
    for item in manifest.get("items", []):
        if item.get("key") == key:
            return item
    return None

def _manifest_upsert(manifest: Dict, entry: Dict) -> None:
    items = manifest.setdefault("items", [])
    for i, it in enumerate(items):
        if it.get("key") == entry["key"]:
            items[i] = entry
            break
    else:
        items.append(entry)

def get_prices(ticker: str, bench: str, timeframe: str, freq: str = "D", ttl_days: int = 3, force: bool = False):
    """
    Returns a CacheItem; creates/refreshes cache as needed.
    """
    assert timeframe in TF_ALLOWED, f"timeframe must be one of {TF_ALLOWED}"
    assert freq in FREQ_TO_YF, "freq must be D/W/M"
    _ensure_dirs()

    key = cache_key(ticker, bench, timeframe, freq)
    prices_path, meta_path = _paths_for(key)

    manifest = _load_manifest()
    entry = _manifest_get(manifest, key)
    now = datetime.now(timezone.utc)

    # Check freshness via manifest
    if not force and entry:
        expires = datetime.fromisoformat(entry["expires_utc"].replace("Z", "+00:00"))
        if now <= expires and prices_path.exists() and meta_path.exists():
            # Return cached
            return CacheItem(
                key=key,
                path_prices=prices_path,
                path_meta=meta_path,
                last_updated_utc=entry["last_updated_utc"],
                ttl_days=entry["ttl_days"],
                expires_utc=entry["expires_utc"],
                source=entry.get("source", "yfinance"),
                currency=entry.get("currency", "USD"),
            )

    # Fetch fresh with yfinance
    interval = FREQ_TO_YF[freq][0]
    period = timeframe  # yfinance supports "1y", "3y", "5y"
    df = yf.download(ticker, period=period, interval=interval, auto_adjust=True, progress=False)
    if df is None or df.empty:
        raise RuntimeError(f"No price data for {ticker} {period} {interval}")
    # auto_adjust=True => adjusted prices in "Close"
    pr = df[["Close"]].rename(columns={"Close": "adj_close"}).reset_index()
    pr["Date"] = pr["Date"].dt.strftime("%Y-%m-%d")
    pr = pr.rename(columns={"Date": "date"})
    pr.to_csv(prices_path, index=False)

    # Meta
    try:
        info = yf.Ticker(ticker).fast_info
        currency = str(info.get("currency")) if info and info.get("currency") else "USD"
    except Exception:
        currency = "USD"
    as_of = pr["date"].iloc[-1]
    last_updated = now.isoformat().replace("+00:00", "Z")
    expires = (now + timedelta(days=ttl_days)).isoformat().replace("+00:00", "Z")

    meta = {
        "symbol": ticker.upper(),
        "benchmark": bench.upper(),
        "timeframe": timeframe,
        "frequency": freq,
        "source": "yfinance",
        "currency": currency,
        "as_of": as_of,
        "rows": int(pr.shape[0]),
        "last_updated_utc": last_updated,
    }
    meta_path.write_text(json.dumps(meta, indent=2))

    # Manifest update
    entry = {
        "key": key,
        "path_prices": str(prices_path),
        "path_meta": str(meta_path),
        "last_updated_utc": last_updated,
        "ttl_days": ttl_days,
        "expires_utc": expires,
        "source": "yfinance",
        "currency": currency,
    }
    _manifest_upsert(manifest, entry)
    _save_manifest(manifest)

    return CacheItem(
        key=key,
        path_prices=prices_path,
        path_meta=meta_path,
        last_updated_utc=last_updated,
        ttl_days=ttl_days,
        expires_utc=expires,
        source="yfinance",
        currency=currency,
    )
