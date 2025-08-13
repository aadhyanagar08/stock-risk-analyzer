import os
import pandas as pd
import yfinance as yf
from alpha_vantage.timeseries import TimeSeries

# Get Alpha Vantage key
AV_API_KEY = os.getenv("ALPHAVANTAGE_API_KEY")
ts = TimeSeries(key=AV_API_KEY, output_format='pandas')

def fetch_yahoo(ticker, start, end):
    df = yf.download(ticker, start=start, end=end, auto_adjust=True)
    
    # If auto_adjust=True, the returned column is usually 'Close'
    if 'Adj Close' in df.columns:
        df = df['Adj Close'].rename('yahoo')
    elif 'Close' in df.columns:
        df = df['Close'].rename('yahoo')
    else:
        # If the returned columns are multi-index (e.g., multiple tickers)
        df = df.xs('Close', axis=1, level=1).rename('yahoo')
    
    return df


def fetch_av(ticker):
    try:
        df, meta = ts.get_daily(symbol=ticker, outputsize='full')
        df = df['5. adjusted close'].rename('av')
        return df
    except ValueError as e:
        print(f"Error fetching data from Alpha Vantage for {ticker}: {e}")
        return pd.Series(dtype='float64')

def save_data(ticker, start, end, folder='data'):
    df_yahoo = fetch_yahoo(ticker, start, end)
    df_av = fetch_av(ticker)
    df = pd.concat([df_yahoo, df_av], axis=1).dropna()
    df.to_csv(f"{folder}/{ticker}_raw.csv")
    print(f"Saved raw data for {ticker} ({len(df)} rows)")

if __name__ == "__main__":
    os.makedirs('data', exist_ok=True)
    save_data("SPY", "2020-01-01", "2025-07-30")
