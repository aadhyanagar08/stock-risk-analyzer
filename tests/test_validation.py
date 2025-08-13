# scripts/validate_tester.py
from src.validation import validate_tickers, validate_timeframe, validate_freq, normalize_weights

def main():
    print("Tickers:", validate_tickers(["aapl"," msft ", "VTI", "bad ticker"]))
    print("Timeframe:", validate_timeframe("3y"))
    print("Freq:", validate_freq("D"))
    weights = {"vol":0.2, "max_dd":0.2, "sharpe":0.3, "expense_ratio":0.15, "yield":0.1, "r2_align":0.05}
    print("Weights (normalized):", normalize_weights(weights))

if __name__ == "__main__":
    main()
