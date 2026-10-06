"""Compare imported smoke-test snapshots, not model accuracy or full dataset quality."""
import argparse
from collections import Counter
import json
from pathlib import Path

from catalog_dictionary import normalize_value, token
from import_amazon_metadata import GENDERS
from import_amazon_reviews import CATEGORIES
from normalize_catalog import read_jsonl, write_json


def sample_summary(directory):
    review_report = json.loads((directory / "dataset_quality_report.json").read_text(encoding="utf-8"))
    meta_report = json.loads((directory / "metadata/metadata_quality_report.json").read_text(encoding="utf-8"))
    pairs = {(row["user_id"], row["product_id"]) for row in read_jsonl(directory / "explicit_ratings.jsonl")}
    product_users = Counter(pid for _, pid in pairs)
    # Diagnostic only: full-snapshot k-core is not a training split policy.
    core = pairs.copy()
    while core:
        users = Counter(uid for uid, _ in core)
        items = Counter(pid for _, pid in core)
        remaining = {(uid, pid) for uid, pid in core if users[uid] >= 5 and items[pid] >= 5}
        if len(remaining) == len(core):
            break
        core = remaining
    gender_conflicts = 0
    for row in read_jsonl(directory / "metadata/metadata_raw.jsonl"):
        details = row.get("details") or {}
        if isinstance(details, str):
            try:
                details = json.loads(details)
            except json.JSONDecodeError:
                continue
        if not isinstance(details, dict):
            continue
        department = details.get("Department", details.get("Gender"))
        declared = GENDERS.get(token(department), normalize_value("gender", department)) if department else None
        labels = row.get("categories") or []
        source_genders = {GENDERS.get(token(label), normalize_value("gender", label)) for label in labels if isinstance(label, str)} - {None}
        if declared and source_genders and source_genders != {declared}:
            gender_conflicts += 1
    known = meta_report["products_with_known_field"]
    return {
        "category": review_report["category"], "reviews": review_report["counts"]["output_reviews"],
        "users": review_report["users"], "products": review_report["products"],
        "users_with_at_least_five_products": review_report["users_with_at_least_five_products"],
        "products_reviewed_by_multiple_users": sum(count >= 2 for count in product_users.values()),
        "products_reviewed_by_at_least_five_users": sum(count >= 5 for count in product_users.values()),
        "diagnostic_5core_pairs": len(core),
        "metadata_scan_records": meta_report["counts"]["scanned_records"],
        "metadata_products_matched": meta_report["counts"]["matched_products"],
        "reviews_with_metadata": meta_report["counts"]["matched_reviews"],
        "products_with_category_l2": known.get("category_l2", 0),
        "products_with_material": known.get("material", 0),
        "products_with_style": known.get("style", 0),
        "products_with_sizes": known.get("sizes", 0),
        "source_department_category_gender_conflicts": gender_conflicts,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-dir", type=Path, default=Path(__file__).resolve().parent / "data/public")
    args = parser.parse_args()
    summaries = [sample_summary(args.base_dir / category) for category in CATEGORIES]
    report = {
        "comparison_scope": "ordered-prefix ingestion smoke tests",
        "datasets": summaries,
        "provisional_candidate": "Clothing_Shoes_and_Jewelry",
        "conclusions": [
            "The imported Clothing prefix has more users with longer histories and more mapped source categories.",
            "Different prefix ordering and partial metadata scans prevent extrapolating these rates to the full datasets.",
            "Item overlap and 5-core diagnostics must be checked before claiming these samples can evaluate collaborative filtering.",
            "Source gender conflicts need review; current normalization gives Department precedence over categories.",
            "Content attributes remain incomplete; do not synthesize style, season or size as observed ground truth.",
        ],
        "next_steps": [
            "Import a larger bounded Clothing snapshot and inspect user/item histories before any ALS training.",
            "Scan more metadata or download a reusable local metadata file to improve join coverage.",
            "Resolve source taxonomy gaps and contradictory attributes with versioned, auditable rules.",
            "Split chronologically, choose parameters with validation and report cold-start coverage on independent test data.",
        ],
    }
    write_json(args.base_dir / "dataset_comparison.json", report)
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
