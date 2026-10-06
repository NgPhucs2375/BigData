"""Stream the official Amazon Reviews 2023 5-core temporal benchmark splits."""
import argparse
import csv
from datetime import datetime, timedelta, timezone
import gzip
import hashlib
import io
import json
from pathlib import Path
import sqlite3
import tempfile
from urllib.request import Request, urlopen

from normalize_catalog import write_json

DATASET = "amazon_reviews_2023"
BASE_URL = "https://mcauleylab.ucsd.edu/public_datasets/data/amazon_2023/benchmark/5core/last_out_w_his/"
SPLITS = ("train", "valid", "test")
CATEGORY = "Clothing_Shoes_and_Jewelry"


def event(row, category, split):
    user = (row.get("user_id") or "").strip()
    item = (row.get("parent_asin") or "").strip()
    if not user or not item:
        raise ValueError("missing_user_or_parent_asin")
    try:
        rating = float(row["rating"])
        timestamp = int(row["timestamp"])
    except (KeyError, TypeError, ValueError):
        raise ValueError("invalid_rating_or_timestamp")
    if not 1 <= rating <= 5 or timestamp <= 0:
        raise ValueError("invalid_rating_or_timestamp")
    date = datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(milliseconds=timestamp)
    if not 1996 <= date.year <= 2023:
        raise ValueError("timestamp_outside_dataset_timespan")
    return {
        "interaction_id": "amazon2023_i_" + hashlib.sha256(
            f"{split}|{user}|{item}|{timestamp}|{rating}".encode()).hexdigest(),
        "user_id": "amazon2023_u_" + hashlib.sha256(user.encode()).hexdigest(),
        "user_source": "amazon_reviewer_id_hash",
        "product_id": "amazon2023_p_" + item,
        "event_type": "review", "rating": rating, "quantity": None, "weight": None,
        "timestamp": date.isoformat(timespec="milliseconds").replace("+00:00", "Z"),
        "timestamp_source": "official_5core_timestamp_ms",
        "source": DATASET, "source_kind": "real", "source_category": category,
        "benchmark_split": split, "source_document_id": f"{user}:{item}:{timestamp}:{rating}",
    }


def open_csv(path=None, url=None):
    if url:
        response = urlopen(Request(url, headers={"User-Agent": "MUA-BigData-Research/1.0"}), timeout=180)
        return gzip.open(response, "rt", encoding="utf-8", newline="")
    return gzip.open(path, "rt", encoding="utf-8", newline="") if path.suffix == ".gz" else path.open(encoding="utf-8", newline="")


def import_split(stream, output_path, category, split, limit):
    output_path.parent.mkdir(parents=True, exist_ok=True)
    counts = {"input_records": 0, "output_records": 0, "duplicates": 0, "rejected_records": 0}
    seen = set()
    with output_path.open("w", encoding="utf-8") as output:
        reader = csv.DictReader(stream)
        for row in reader:
            if limit and counts["input_records"] >= limit:
                break
            counts["input_records"] += 1
            try:
                item = event(row, category, split)
            except ValueError:
                counts["rejected_records"] += 1
                continue
            if item["interaction_id"] in seen:
                counts["duplicates"] += 1
                continue
            seen.add(item["interaction_id"])
            output.write(json.dumps(item, ensure_ascii=False) + "\n")
            counts["output_records"] += 1
    return counts


def import_file(path, output_path, category, split, limit):
    with gzip.open(path, "rt", encoding="utf-8", newline="") as stream:
        return import_split(stream, output_path, category, split, limit)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--category", default=CATEGORY)
    parser.add_argument("--input-dir", type=Path, help="Directory containing <category>.<split>.csv.gz files")
    parser.add_argument("--download", action="store_true")
    parser.add_argument("--split", choices=SPLITS, nargs="+", default=list(SPLITS))
    parser.add_argument("--limit", type=int, default=0, help="Per-split record limit; 0 means full split")
    parser.add_argument("--output-dir", type=Path)
    args = parser.parse_args()
    if not args.download and not args.input_dir:
        parser.error("provide --download or --input-dir")
    if args.limit < 0:
        parser.error("--limit must be >= 0")
    output_dir = args.output_dir or Path(__file__).resolve().parent / "data/public" / args.category / "5core_last_out"
    output_dir.mkdir(parents=True, exist_ok=True)
    if args.download and args.limit == 0:
        parser.error("full download requires --input-dir; download the .csv.gz files with a resumable downloader first")
    reports = {}
    for split in args.split:
        filename = f"{args.category}.{split}.csv.gz"
        url = BASE_URL + filename if args.download else None
        path = args.input_dir / filename if args.input_dir else None
        if path:
            reports[split] = import_file(path, output_dir / f"{split}.jsonl", args.category, split, args.limit)
        else:
            with open_csv(None, url) as stream:
                reports[split] = import_split(stream, output_dir / f"{split}.jsonl", args.category, split, args.limit)
    write_json(output_dir / "benchmark_quality_report.json", {
        "dataset": DATASET, "category": args.category, "benchmark": "5core/last_out_w_his",
        "source_url": BASE_URL, "reports": reports,
        "notes": ["Official source split: per-user first N-2 interactions train, N-1 valid, N test.",
                  "A limit is an ordered prefix of each official split and is only a smoke test.",
                  "The official 5-core preprocessing applies to the full source; capped output may not retain 5-core guarantees.",
                  "Metadata is not included; join later by namespaced parent_asin."],
    })
    print(json.dumps(reports, ensure_ascii=False, indent=2))
    print(f"Output: {output_dir}")


if __name__ == "__main__":
    main()
