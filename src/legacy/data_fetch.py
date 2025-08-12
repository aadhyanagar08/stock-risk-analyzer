import yfinance as yf
from alpha_vantage.timeseries import TimeSeries
import pandas as pd
import os
from dotenv import load_dotenv

# Load API key
load_dotenv()
API_KEY = os.getenv("ALPHAVANTAGE_API_KEY")

def fetch_yahoo_data(ticker, start_date, end_date):
    print(f"\n--- Fetching Yahoo Finance data for {ticker} ---")
    # fetching data for the recent 100 days
    end_date = datetime.today().date()
    start_date = end_date - timedelta(days=130)
    df = yf.download(ticker, start=start_date, end=end_date)

    if df.empty:
        print("No data returned from Yahoo Finance!")
        return None

    # Save to CSV and print columns
    os.makedirs("data", exist_ok=True)
    path = f"data/{ticker}_yahoo.csv"
    df.to_csv(path)
    print(f"Yahoo data saved to {path}")
    print("Yahoo data columns:", df.columns)
    print(df.head(5))  # print first 5 rows
    return df

def fetch_alpha_data(ticker):
    print(f"\n--- Fetching Alpha Vantage data for {ticker} ---")
    ts = TimeSeries(key=API_KEY, output_format='pandas')

    data, meta = ts.get_daily(symbol=ticker, outputsize='full')
    if data.empty:
        print("No data returned from Alpha Vantage!")
        return None

    # Save to CSV and print columns
    path = f"data/{ticker}_alpha.csv"
    data.to_csv(path)
    print(f"Alpha Vantage data saved to {path}")
    print("Alpha Vantage data columns:", data.columns)
    print(data.head(5))  # print first 5 rows
    return data
def compare_data(yahoo_df, alpha_df):
    print("\n--- Comparing Yahoo and Alpha Vantage data ---")

    # 1. Flatten Yahoo's multi-index columns
    yahoo_df.columns = [col[0] for col in yahoo_df.columns]  # e.g., ('Close', 'AAPL') -> 'Close'

    # 2. Extract relevant columns
    yahoo_close = yahoo_df[['Close']].copy()
    yahoo_close.rename(columns={'Close': 'Yahoo_Close'}, inplace=True)

    alpha_close = alpha_df[['4. close']].copy()
    alpha_close.rename(columns={'4. close': 'Alpha_Close'}, inplace=True)

    # 3. Align by date
    combined = yahoo_close.join(alpha_close, how='inner')

    if combined.empty:
        print("No overlapping dates between Yahoo and Alpha data!")
        return None

    # 4. Calculate percentage difference
    combined['pct_diff'] = ((combined['Yahoo_Close'] - combined['Alpha_Close']) / combined['Alpha_Close']) * 100

    discrepancies = combined[combined['pct_diff'].abs() > 2]  # flag >2%

    if discrepancies.empty:
        print("No major discrepancies found!")
    else:
        print(f"Found {len(discrepancies)} discrepancies (>2% difference):")
        print(discrepancies.head())

    # Save discrepancy report
    os.makedirs("data", exist_ok=True)
    discrepancies.to_csv("data/discrepancy_report.csv")
    print("\nDiscrepancy report saved to data/discrepancy_report.csv")

    return discrepancies


if __name__ == "__main__":
    ticker = "AAPL"
    yahoo_data = fetch_yahoo_data(ticker, "2020-01-01", "2021-01-01")
    alpha_data = fetch_alpha_data(ticker)

    if yahoo_data is not None and alpha_data is not None:
        discrepancies = compare_data(yahoo_data, alpha_data)
