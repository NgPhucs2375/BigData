"""Stream public fashion reviews into an isolated, provenance-aware evaluation dataset."""
import argparse
from collections import Counter
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import gzip
import hashlib
import io
import json
import math
from pathlib import Path
import sqlite3
import tempfile
from urllib.request import Request, urlopen

from normalize_catalog import write_json

SOURCE = "amazon_reviews_2023"
CATEGORIES = ("Amazon_Fashion", "Clothing_Shoes_and_Jewelry")
HOST = "https://mcauleylab.ucsd.edu/public_datasets/data/amazon_2023/raw/review_categories/"


def review_event(row, category):
    if not isinstance(row, dict):
        raise ValueError("invalid_record")
    user, parent = row.get("user_id"), row.get("parent_asin")
    if not isinstance(user, str) or not user.strip():
        raise ValueError("missing_user_id")
    if not isinstance(parent, str) or not parent.strip():
        raise ValueError("missing_parent_asin")
    raw_rating = row.get("rating")
    if isinstance(raw_rating, bool):
        raise ValueError("invalid_rating")
    try:
        rating = float(raw_rating)
    except (TypeError, ValueError):
        raise ValueError("invalid_rating")
    if not math.isfinite(rating) or not 1 <= rating <= 5:
        raise ValueError("invalid_rating")
    timestamp = row.get("timestamp", row.get("sort_timestamp"))
    if isinstance(timestamp, bool) or not isinstance(timestamp, int) or timestamp <= 0:
        raise ValueError("invalid_timestamp_ms")
    if "timestamp" in row and "sort_timestamp" in row and row["timestamp"] != row["sort_timestamp"]:
        raise ValueError("conflicting_timestamps")
    try:
        # Source timestamps are milliseconds, not seconds. Preserve exact millisecond precision.
        date = datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(milliseconds=timestamp)
    except (OverflowError, ValueError):
        raise ValueError("invalid_timestamp_ms")
    if not 1996 <= date.year <= 2023:
        raise ValueError("timestamp_outside_dataset_timespan")
    digest = hashlib.sha256(json.dumps(row, sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode()).hexdigest()
    uid = "amazon2023_u_" + hashlib.sha256(user.encode()).hexdigest()
    return {
        "interaction_id": "amazon2023_i_" + digest,
        "user_id": uid, "user_source": "amazon_reviewer_id_hash",
        "product_id": "amazon2023_p_" + parent,
        "event_type": "review", "rating": rating, "quantity": None, "weight": None,
        "timestamp": date.isoformat(timespec="milliseconds").replace("+00:00", "Z"),
        "timestamp_source": "review_timestamp_ms",
        "source": SOURCE, "source_kind": "real", "source_category": category,
        "source_document_id": digest,
        "verified_purchase": row.get("verified_purchase") if isinstance(row.get("verified_purchase"), bool) else None,
    }


@contextmanager
def open_source(path=None, url=None):
    if url:
        with urlopen(Request(url, headers={"User-Agent": "MUA-BigData-Research/1.0"}), timeout=180) as response:
            with gzip.GzipFile(fileobj=response) as compressed:
                with io.TextIOWrapper(compressed, encoding="utf-8") as stream:
                    yield stream
    elif path.suffix == ".gz":
        with gzip.open(path, "rt", encoding="utf-8") as stream:
            yield stream
    else:
        with path.open(encoding="utf-8-sig") as stream:
            yield stream


def import_stream(stream, directory, category, limit, source_location, sample_mode):
    """SQLite disk indexes prevent dedup/user/item statistics from growing in RAM."""
    directory.mkdir(parents=True, exist_ok=True)
    counts = Counter(input_records=0, output_reviews=0, rejected_records=0, duplicates=0)
    first, last = None, None
    # Temporary index is discarded after import. JSONL outputs are written to a staging directory.
    with tempfile.TemporaryDirectory(prefix="amazon-index-", dir=directory) as temporary:
        staging = Path(temporary)
        db = sqlite3.connect(staging / "index.sqlite")
        try:
            db.execute("CREATE TABLE events (id TEXT PRIMARY KEY, user_id TEXT, product_id TEXT)")
            with (staging / "explicit_ratings.jsonl").open("w", encoding="utf-8") as output, \
                 (staging / "reviews_raw.jsonl").open("w", encoding="utf-8") as raw_out, \
                 (staging / "import_issues.jsonl").open("w", encoding="utf-8") as issues:
                iterator = iter(stream)
                while not limit or counts["input_records"] < limit:
                    line = next(iterator, None)
                    if line is None:
                        break
                    if not line.strip():
                        continue
                    counts["input_records"] += 1
                    raw_out.write(line.rstrip("\r\n") + "\n")
                    try:
                        row = json.loads(line)
                        event = review_event(row, category)
                    except (json.JSONDecodeError, ValueError) as error:
                        reason = "invalid_json" if isinstance(error, json.JSONDecodeError) else str(error)
                        counts["rejected_records"] += 1
                        counts["issue:" + reason] += 1
                        issues.write(json.dumps({"source_line": counts["input_records"], "reason": reason}) + "\n")
                        continue
                    try:
                        db.execute("INSERT INTO events VALUES (?, ?, ?)",
                                   (event["interaction_id"], event["user_id"], event["product_id"]))
                    except sqlite3.IntegrityError:
                        counts["duplicates"] += 1
                        continue
                    output.write(json.dumps(event, ensure_ascii=False) + "\n")
                    counts["output_reviews"] += 1
                    timestamp = event["timestamp"]
                    first = min(first, timestamp) if first else timestamp
                    last = max(last, timestamp) if last else timestamp
                    if counts["input_records"] % 10000 == 0:
                        db.commit()
            db.commit()
            users = db.execute("SELECT count(DISTINCT user_id) FROM events").fetchone()[0]
            products = db.execute("SELECT count(DISTINCT product_id) FROM events").fetchone()[0]
            pairs = db.execute("SELECT count(*) FROM (SELECT user_id, product_id FROM events GROUP BY user_id, product_id)").fetchone()[0]
            user_activity = db.execute("SELECT count(*), sum(n=1), sum(n>=5) FROM (SELECT user_id, count(DISTINCT product_id) n FROM events GROUP BY user_id)").fetchone()
            with (staging / "observed_product_ids.jsonl").open("w", encoding="utf-8") as output:
                for (pid,) in db.execute("SELECT DISTINCT product_id FROM events ORDER BY product_id"):
                    output.write(json.dumps({"product_id": pid, "source_item_id": pid.removeprefix("amazon2023_p_"),
                                             "source": SOURCE, "metadata_loaded": False}) + "\n")
            report = {
                "dataset": SOURCE, "category": category, "source_kind": "real",
                "source_location": source_location, "imported_at": datetime.now(timezone.utc).isoformat(),
                "sampling": "ordered_prefix" if sample_mode else "full_input_file",
                "requested_limit": limit, "counts": dict(counts), "users": users, "products": products,
                "unique_user_product_pairs": pairs, "users_with_one_product": user_activity[1] or 0,
                "users_with_at_least_five_products": user_activity[2] or 0,
                "user_item_sparsity": 1 - pairs / (users * products) if users and products else None,
                "timestamp_min": first, "timestamp_max": last,
                "metadata_loaded": False,
                "notes": ["Ordered prefix is an ingestion smoke test, not a representative evaluation sample.",
                          "IDs remain independent of MUA; no mapping to MUA products or customers is assumed.",
                          "verified_purchase is review metadata, not a separate purchase event.",
                          "Exact duplicate records are removed; repeated user-item interactions are retained for temporal splitting.",
                          "Observed product IDs are not a clean catalog; metadata must be loaded separately."],
            }
            write_json(staging / "dataset_quality_report.json", report)
            write_json(staging / "source_manifest.json", {
                "dataset": "Amazon Reviews 2023", "provider": "McAuley Lab", "category": category,
                "documentation": "https://amazon-reviews-2023.github.io/",
                "reviews_url": HOST + category + ".jsonl.gz",
                "metadata_url": HOST.replace("review_categories/", "meta_categories/") + "meta_" + category + ".jsonl.gz",
                "citation": "Hou et al. (2024), Bridging Language and Items for Retrieval and Recommendation, arXiv:2403.03952",
            })
            for file in staging.glob("*.json*"):
                file.replace(directory / file.name)
            return report
        finally:
            db.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--input", type=Path, help="Downloaded source JSONL or JSONL.gz")
    source.add_argument("--download", action="store_true", help="Stream from official URL; limit prevents a full download")
    parser.add_argument("--category", choices=CATEGORIES, default="Amazon_Fashion")
    parser.add_argument("--limit", type=int, default=1000, help="Maximum input records, 0 for full source")
    parser.add_argument("--output-dir", type=Path)
    args = parser.parse_args()
    if args.limit < 0:
        parser.error("--limit must be >= 0")
    directory = args.output_dir or Path(__file__).resolve().parent / "data" / "public" / args.category
    url = HOST + args.category + ".jsonl.gz" if args.download else None
    with open_source(args.input, url) as stream:
        report = import_stream(stream, directory, args.category, args.limit,
                               url or str(args.input.resolve()), bool(args.limit))
    print(json.dumps(report, ensure_ascii=False, indent=2))
    print(f"Output: {directory}")


if __name__ == "__main__":
    main()
