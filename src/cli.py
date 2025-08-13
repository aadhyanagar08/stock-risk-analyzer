# src/cli.py
# Terminal interface for compare/rank and (optionally) saving a decision.

import os, json, argparse
from datetime import date
from tabulate import tabulate

from .compare import compare_and_rank
from .profiles import load_profile, merge_overrides
from .validation import normalize_weights
from .storage import append_decision  # must exist

def parse_args():
    p = argparse.ArgumentParser(
        prog="investor-coach",
        description="Compare & rank tickers by your profile (terminal)."
    )
    sub = p.add_subparsers(dest="cmd", required=True)

    cmp = sub.add_parser("compare", help="Compare and rank tickers")
    cmp.add_argument("--tickers", required=True,
                     help="Comma-separated tickers, e.g. AAPL,MSFT,VTI")
    cmp.add_argument("--benchmark", default="SPY", help="Benchmark ticker (default: SPY)")
    cmp.add_argument("--profile", default="default", choices=["default","low_vol","income","custom"],
                     help="Weight preset")
    cmp.add_argument("--weights-json", default=None,
                     help='Optional JSON overrides, e.g. \'{"sharpe":0.35}\'')
    cmp.add_argument("--timeframe", default="3y", choices=["1y","3y","5y"])
    cmp.add_argument("--freq", default="D", choices=["D","W","M"])
    cmp.add_argument("--export", default=None, help="Optional path to save ranked CSV")
    cmp.add_argument("--refresh", action="store_true", help="Force refresh prices (ignore cache TTL)")

    j = sub.add_parser("journal", help="Append a decision to your local journal")
    j.add_argument("--category", required=True, help='Name of your watchlist/category')
    j.add_argument("--tickers", required=True, help="Comma-separated list used in the compare")
    j.add_argument("--profile", required=True, help="Profile name you used (e.g., default)")
    j.add_argument("--weights-json", required=True, help="Weights snapshot JSON used for ranking")
    j.add_argument("--top-pick", required=True, help="Winning ticker")
    j.add_argument("--action", required=True, choices=["BUY","REJECT","WATCH"])
    j.add_argument("--note", default="", help="Short note")
    j.add_argument("--snapshot-path", default="", help="Optional path to the comparison CSV you saved")

    return p.parse_args()

def main():
    args = parse_args()

    if args.cmd == "compare":
        # Optional: bypass cache freshness
        if args.refresh:
            os.environ["FORCE_REFRESH"] = "1"

        tickers = [t.strip() for t in args.tickers.split(",") if t.strip()]
        weight_overrides = json.loads(args.weights_json) if args.weights_json else None

        # Load profile + apply overrides so we can display the final weights
        prof = load_profile(args.profile)
        prof = merge_overrides(prof, weight_overrides)
        weights = normalize_weights(prof.get("weights", {}))

        df = compare_and_rank(
            tickers=tickers,
            benchmark=args.benchmark,
            profile_name=args.profile,
            weight_overrides=weight_overrides,
            timeframe=args.timeframe,
            freq=args.freq,
        )

        # Reorder nice-to-see columns if present
        preferred = ["rank","symbol","score","sharpe","vol","max_dd","beta","r2"]
        cols = [c for c in preferred if c in df.columns] + [c for c in df.columns if c not in preferred]
        df = df[cols]

        print("\nWeights used:", json.dumps(weights, indent=2))
        print("\nResult:")
        print(tabulate(df.head(50), headers="keys", tablefmt="github", showindex=False))

        if args.export:
            df.to_csv(args.export, index=False)
            print(f"\nSaved CSV → {args.export}")

    elif args.cmd == "journal":
        row = {
            "date": str(date.today()),
            "category": args.category,
            "tickers": ";".join([t.strip().upper() for t in args.tickers.split(",") if t.strip()]),
            "profile_name": args.profile,
            "weights_json": args.weights_json,
            "top_pick": args.top_pick.strip().upper(),
            "action": args.action,
            "note": args.note,
            "snapshot_path": args.snapshot_path,
        }
        append_decision(row)
        print("Appended decision to data/decisions/decisions.csv")

if __name__ == "__main__":
    main()
