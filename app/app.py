# app/app.py

import os
import streamlit as st
import pandas as pd

from src.compare import compare_and_rank
from src.profiles import load_profile

st.set_page_config(page_title="Stock Compare & Rank", layout="wide")

st.title("📊 Stock Compare & Rank — Investor Coach")

col1, col2, col3 = st.columns([2,1,1])
with col1:
    tickers_text = st.text_input("Tickers (comma-separated)", value="AAPL, MSFT, VTI")
with col2:
    benchmark = st.text_input("Benchmark", value="SPY")
with col3:
    timeframe = st.selectbox("Timeframe", ["1y", "3y", "5y"], index=1)

profile_name = st.selectbox("Profile", ["default", "low_vol", "income"], index=0)

st.caption(f"Risk-free rate (RFF): {os.getenv('RISK_FREE_RATE', '0.02')} • Cache TTL days: {os.getenv('CACHE_TTL_DAYS', '3')}")

if st.button("Compare & Rank"):
    try:
        tickers = [t.strip() for t in tickers_text.split(",") if t.strip()]
        df = compare_and_rank(tickers, benchmark=benchmark, profile_name=profile_name, timeframe=timeframe, freq="D")
        st.success("Done.")
        # Reorder columns for nicer view
        cols = [c for c in ["rank", "symbol", "score", "sharpe", "vol", "max_dd", "beta", "r2"] if c in df.columns]
        cols += [c for c in df.columns if c not in cols]
        st.dataframe(df[cols], use_container_width=True)
        st.download_button("Download CSV", df.to_csv(index=False), file_name="compare_rank.csv", mime="text/csv")
    except Exception as e:
        st.error(str(e))
