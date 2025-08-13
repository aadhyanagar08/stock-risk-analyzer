# tests/test_compare.py

import os
import pytest
from src.compare import compare_and_rank

@pytest.mark.integration
def test_compare_smoke():
    # Skip in offline/CI environments by setting OFFLINE=1
    if os.getenv("OFFLINE") == "1":
        pytest.skip("Offline mode")
    df = compare_and_rank(["AAPL", "MSFT"], benchmark="SPY", profile_name="default", timeframe="1y", freq="D")
    # basic shape
    assert "symbol" in df.columns
    assert "score" in df.columns
    assert "rank" in df.columns
    assert len(df) >= 2
