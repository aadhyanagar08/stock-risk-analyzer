"""
load_categories() / save_categories(data)

create_category(name), rename_category(old,new), delete_category(name)

add_tickers(category, tickers[]), remove_tickers(category, tickers[])

append_decision(row_dict) → appends a validated line to decisions.csv

Basic validation & deduping inside these helpers

"""
# src/storage.py

from pathlib import Path
import csv

DECISIONS_CSV = Path("data/decisions/decisions.csv")

def append_decision(row: dict) -> None:
    """
    Append a decision row to decisions.csv.
    Expects keys: date, category, tickers, profile_name, weights_json, top_pick, action, note, snapshot_path
    """
    DECISIONS_CSV.parent.mkdir(parents=True, exist_ok=True)
    file_exists = DECISIONS_CSV.exists()

    with DECISIONS_CSV.open("a", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "date","category","tickers","profile_name","weights_json",
                "top_pick","action","note","snapshot_path"
            ]
        )
        if not file_exists:
            writer.writeheader()
        writer.writerow(row)
