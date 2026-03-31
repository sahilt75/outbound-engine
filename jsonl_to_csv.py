#!/usr/bin/env python3

import json
import csv
import sys
import os
from typing import List, Dict, Any

def flatten(obj, parent_key: str = "", sep: str = "."):
    """
    Safely flattens nested dicts and skips None values.
    """
    if obj is None:
        return {}  # ← NEW: ignore None completely

    if not isinstance(obj, dict):
        # if it's a simple value (string, int, bool), return directly
        return {parent_key: obj} if parent_key else {}

    items = []
    for k, v in obj.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k

        if isinstance(v, dict):
            items.extend(flatten(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))

    return dict(items)

def read_jsonl(path: str):
    rows = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                data = json.loads(line)
                if data is None:   # skip "null"
                    continue
                rows.append(data)
            except json.JSONDecodeError:
                print(f"[skip] Bad JSON line in {path}")
    return rows

def write_csv(rows: List[Dict[str, Any]], out_path: str):
    """Writes rows to CSV after flattening + column auto-detection."""
    if not rows:
        print("[!] No data found, CSV not written.")
        return

    # Flatten all rows to determine full column set
    flat_rows = [flatten(r) for r in rows]

    # Collect all keys from all rows
    headers = set()
    for r in flat_rows:
        headers.update(r.keys())
    headers = sorted(headers)  # consistent ordering

    print(f"[+] Writing CSV with {len(headers)} columns → {out_path}")

    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        for r in flat_rows:
            writer.writerow({k: r.get(k, "") for k in headers})

    print(f"[+] CSV saved: {out_path}")


def main():
    in_path = "scrapers/data/raw/behance_2025-12-08.jsonl"
    out_path = "scrapers/data/raw/behance_2025-12-08.csv"

    if not os.path.isfile(in_path):
        print(f"[!] Input JSONL not found: {in_path}")
        sys.exit(1)

    rows = read_jsonl(in_path)
    print(f"[+] Loaded {len(rows)} rows from JSONL")

    write_csv(rows, out_path)


if __name__ == "__main__":
    main()
