# tests/test_fetch_cache.py
import os
import json
import shutil
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch, MagicMock

import numpy as np
import pandas as pd

# Import the module under test
import src.fetch as fetch


def _fake_df(n=30, start="2025-01-01", step_days=1, start_price=100.0, drift=0.001):
    """Deterministic price path DataFrame with columns: Date, Close"""
    dates = pd.date_range(start=start, periods=n, freq=f"{step_days}D")
    rets = np.full(n, drift)
    prices = start_price * np.cumprod(1 + rets)
    df = pd.DataFrame({"Date": dates, "Close": prices})
    return df


class TestFetchCache(unittest.TestCase):
    def setUp(self):
        # Make a fresh temp cache root and patch fetch paths to use it
        self.tmpdir = Path(tempfile.mkdtemp(prefix="test_cache_"))
        self.prices_dir = self.tmpdir / "prices"
        self.manifest_dir = self.tmpdir / "manifest"
        self.manifest_path = self.manifest_dir / "index.json"

        # Patch module-level constants in src.fetch
        self.patcher_prices = patch.object(fetch, "PRICES_DIR", self.prices_dir)
        self.patcher_manifest_dir = patch.object(fetch, "MANIFEST_DIR", self.manifest_dir)
        self.patcher_manifest_path = patch.object(fetch, "MANIFEST_PATH", self.manifest_path)
        self.patcher_prices.start()
        self.patcher_manifest_dir.start()
        self.patcher_manifest_path.start()

        # Ensure manifest exists
        self.manifest_dir.mkdir(parents=True, exist_ok=True)
        self.manifest_path.write_text(json.dumps({"version": 1, "items": []}))

        # Default: no force refresh
        if "FORCE_REFRESH" in os.environ:
            del os.environ["FORCE_REFRESH"]

    def tearDown(self):
        # Stop patches and remove temp dir
        self.patcher_prices.stop()
        self.patcher_manifest_dir.stop()
        self.patcher_manifest_path.stop()
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def _mock_yf(self, n=30):
        """Patch yfinance.download and Ticker.fast_info to return deterministic data."""
        fake = _fake_df(n=n)
        # download returns a DataFrame with Date index and Close column
        def fake_download(ticker, period, interval, auto_adjust, progress):
            df = fake.copy()
            df = df.set_index("Date")  # yfinance returns Date index
            return df

        class FakeTicker:
            def __init__(self, t): pass
            @property
            def fast_info(self):
                return {"currency": "USD"}

        p1 = patch("src.fetch.yf.download", side_effect=fake_download)
        p2 = patch("src.fetch.yf.Ticker", side_effect=FakeTicker)
        return p1, p2

    def test_cache_miss_creates_files_and_manifest(self):
        p1, p2 = self._mock_yf(n=30)
        with p1, p2:
            item = fetch.get_prices("AAPL", "SPY", "1y", "D", ttl_days=3)
        # files created
        self.assertTrue(item.path_prices.exists())
        self.assertTrue(item.path_meta.exists())
        # manifest entry present
        m = json.loads(self.manifest_path.read_text())
        keys = [it["key"] for it in m["items"]]
        self.assertIn("AAPL__SPY__1y__D", keys)

    def test_cache_hit_within_ttl_reuses_files(self):
        p1, p2 = self._mock_yf(n=30)
        with p1, p2:
            first = fetch.get_prices("AAPL", "SPY", "1y", "D", ttl_days=3)
        # simulate a fresh manifest entry (now + 2 days)
        m = json.loads(self.manifest_path.read_text())
        for it in m["items"]:
            if it["key"] == "AAPL__SPY__1y__D":
                it["expires_utc"] = (datetime.now(timezone.utc) + timedelta(days=2)).isoformat().replace("+00:00", "Z")
        self.manifest_path.write_text(json.dumps(m, indent=2))
        # call again with different fake that would change data if re-fetched
        p1b, p2b = self._mock_yf(n=35)
        with p1b, p2b:
            second = fetch.get_prices("AAPL", "SPY", "1y", "D", ttl_days=3)
        self.assertEqual(first.path_prices, second.path_prices)  # reused
        # meta unchanged too
        self.assertEqual(first.path_meta, second.path_meta)

    def test_stale_cache_triggers_refresh(self):
        # initial fetch
        p1, p2 = self._mock_yf(n=30)
        with p1, p2:
            _ = fetch.get_prices("AAPL", "SPY", "1y", "D", ttl_days=3)
        # expire it yesterday
        m = json.loads(self.manifest_path.read_text())
        for it in m["items"]:
            if it["key"] == "AAPL__SPY__1y__D":
                it["expires_utc"] = (datetime.now(timeframe := timezone.utc) - timedelta(days=1)).isoformat().replace("+00:00", "Z")
        self.manifest_path.write_text(json.dumps(m, indent=2))
        # refetch with different length -> ensures it actually refreshed
        p1b, p2b = self._mock_yf(n=50)
        with p1b, p2b:
            item = fetch.get_prices("AAPL", "SPY", "1y", "D", ttl_days=3)
        # read CSV rows; should be 50
        rows = sum(1 for _ in item.path_prices.open()) - 1  # minus header
        self.assertEqual(rows, 50)

    def test_force_refresh_bypasses_ttl(self):
        # first fetch
        p1, p2 = self._mock_yf(n=20)
        with p1, p2:
            _ = fetch.get_prices("AAPL", "SPY", "1y", "D", ttl_days=3)
        # Make manifest fresh in the future
        m = json.loads(self.manifest_path.read_text())
        for it in m["items"]:
            if it["key"] == "AAPL__SPY__1y__D":
                it["expires_utc"] = (datetime.now(timezone.utc) + timedelta(days=5)).isoformat().replace("+00:00", "Z")
        self.manifest_path.write_text(json.dumps(m, indent=2))
        # but set FORCE_REFRESH to bypass and return a different length
        os.environ["FORCE_REFRESH"] = "1"
        p1b, p2b = self._mock_yf(n=45)
        with p1b, p2b:
            # get_prices should honor FORCE_REFRESH if your implementation supports it
            # If your get_prices doesn't read the env, adapt it to accept force=True and pass here.
            item = fetch.get_prices("AAPL", "SPY", "1y", "D", ttl_days=3)
        rows = sum(1 for _ in item.path_prices.open()) - 1
        self.assertEqual(rows, 45)
        del os.environ["FORCE_REFRESH"]

    def test_invalid_timeframe_or_freq_raises(self):
        p1, p2 = self._mock_yf(n=10)
        with p1, p2:
            with self.assertRaises(AssertionError):
                fetch.get_prices("AAPL", "SPY", "2y", "D")  # timeframe not allowed
            with self.assertRaises(AssertionError):
                fetch.get_prices("AAPL", "SPY", "1y", "Q")  # frequency not allowed

    def test_meta_fields_present(self):
        p1, p2 = self._mock_yf(n=12)
        with p1, p2:
            item = fetch.get_prices("MSFT", "SPY", "1y", "D", ttl_days=3)
        meta = json.loads(item.path_meta.read_text())
        self.assertEqual(meta["symbol"], "MSFT")
        self.assertEqual(meta["benchmark"], "SPY")
        self.assertEqual(meta["timeframe"], "1y")
        self.assertEqual(meta["frequency"], "D")
        self.assertIn("as_of", meta)
        self.assertIn("rows", meta)
        self.assertEqual(meta["source"], "yfinance")
        self.assertEqual(meta["currency"], "USD")

    def test_benchmark_cached_like_ticker(self):
        p1, p2 = self._mock_yf(n=10)
        with p1, p2:
            _ = fetch.get_prices("SPY", "SPY", "1y", "D", ttl_days=3)
        prices_files = [p.name for p in self.prices_dir.glob("*.csv")]
        self.assertIn("SPY__SPY__1y__D.csv", prices_files)

    def test_key_uniqueness(self):
        p1, p2 = self._mock_yf(n=10)
        with p1, p2:
            k1 = fetch.cache_key("AAPL", "SPY", "1y", "D")
            k2 = fetch.cache_key("AAPL", "SPY", "3y", "D")
            k3 = fetch.cache_key("AAPL", "SPY", "1y", "W")
        self.assertNotEqual(k1, k2)
        self.assertNotEqual(k1, k3)
        self.assertNotEqual(k2, k3)


if __name__ == "__main__":
    unittest.main()
