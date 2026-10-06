"""Join observed Amazon parent ASINs to source metadata and normalize known attributes."""
import argparse
from collections import Counter
import json
from pathlib import Path
import re
import sqlite3
import tempfile

from catalog_dictionary import CATEGORY_ALIASES, VOCABULARY, category_paths, normalize_value, token
from import_amazon_reviews import CATEGORIES, HOST, SOURCE, open_source
from normalize_catalog import Normalizer, numeric, read_jsonl, values, write_json

METADATA_HOST = HOST.replace("review_categories/", "meta_categories/")
CATEGORY_LABELS = {
    "t-shirts": "t-shirt", "tees": "t-shirt", "t-shirts-tanks": "tops",
    "shirts": "shirt", "polos": "polo", "hoodies": "hoodie",
    "sweatshirts": "sweatshirt", "sweaters": "sweater", "tank-tops": "tank-top",
    "cardigans": "cardigan", "pants": "bottoms", "dress-pants": "trousers",
    "cargo-pants": "cargo-pants", "jeans": "jeans", "shorts": "shorts",
    "jackets": "jacket", "coats": "coat", "blazers": "blazer", "vests": "vest",
    "hats": "hat", "caps": "cap", "beanies": "beanie", "bags": "bag",
    "backpacks": "backpack", "totes": "tote-bag", "belts": "belt",
    "wallets": "wallet", "scarves": "scarf", "socks": "socks", "gloves": "gloves",
    "necklaces": "necklace", "bracelets": "bracelet", "rings": "ring",
    "shoes": "footwear", "fashion-sneakers": "sneakers", "sneakers": "sneakers",
    "boots": "boots", "sandals": "sandals", "loafers": "loafers",
}
GENDERS = {"mens": "male", "men-s": "male", "womens": "female", "women-s": "female",
           "boys": "kids", "girls": "kids", "baby-boys": "kids", "baby-girls": "kids",
           "unisex-adult": "unisex", "unisex-child": "kids"}
SIZE_LABELS = {"extra-small": "XS", "small": "S", "medium": "M", "large": "L", "extra-large": "XL", "xx-large": "XXL"}


def feature_materials(features):
    """Accept complete composition statements, not generic mentions in marketing text."""
    found, evidence = [], []
    for feature in features if isinstance(features, list) else []:
        if not isinstance(feature, str):
            continue
        remainder = token(feature)
        codes = []
        for code in sorted(VOCABULARY["material"], key=len, reverse=True):
            pattern = r"(?<![a-z0-9])" + re.escape(code) + r"(?![a-z0-9])"
            if re.search(pattern, remainder):
                codes.append(code)
                remainder = re.sub(pattern, "", remainder)
        remainder = re.sub(r"\d+|blend|recycled|fabric|material", "", remainder).strip("-")
        if codes and not remainder:
            found.extend(code for code in codes if code not in found)
            evidence.append(feature)
    return found, evidence


def metadata_product(row, category):
    if not isinstance(row, dict) or not isinstance(row.get("parent_asin"), str) or not row["parent_asin"].strip():
        raise ValueError("missing_parent_asin")
    pid = "amazon2023_p_" + row["parent_asin"]
    issues, rules = [], []
    details = row.get("details") or {}
    if isinstance(details, str):
        try:
            details = json.loads(details)
        except json.JSONDecodeError:
            details = {}
            issues.append({"product_id": pid, "field": "details", "reason": "invalid_details_json"})
    if not isinstance(details, dict):
        details = {}
        issues.append({"product_id": pid, "field": "details", "reason": "invalid_details_type"})
    keys = {token(key): value for key, value in details.items()}
    categories = row.get("categories") or []
    if not isinstance(categories, list):
        categories = []
        issues.append({"product_id": pid, "field": "categories", "reason": "invalid_categories_type"})
    paths = category_paths()
    candidates = []
    for label in categories:
        if not isinstance(label, str):
            continue
        code = CATEGORY_LABELS.get(token(label), CATEGORY_ALIASES.get(token(label), token(label)))
        if code in paths:
            candidates.append(paths[code])
    path = max(candidates, key=lambda p: sum(v is not None for v in p), default=(None, None, None))
    if any(p[0] != path[0] or (p[1] and path[1] and p[1] != path[1]) or
           (p[2] and path[2] and p[2] != path[2]) for p in candidates):
        path = (None, None, None)
        issues.append({"product_id": pid, "field": "categories", "reason": "conflicting_source_categories"})
    if path[0]:
        rules.append({"field": "category", "rule": "exact_source_category_label", "source": categories})
    else:
        issues.append({"product_id": pid, "field": "categories", "reason": "unmapped_or_missing_source_categories", "value": categories})
    gender_raw = keys.get("gender", keys.get("department"))
    gender = None
    if gender_raw is not None:
        gender = GENDERS.get(token(gender_raw), normalize_value("gender", gender_raw))
        if gender is None:
            issues.append({"product_id": pid, "field": "gender", "reason": "unknown_department", "value": gender_raw})
    else:
        possible = {GENDERS.get(token(label), normalize_value("gender", label))
                    for label in categories if isinstance(label, str)} - {None}
        if len(possible) == 1:
            gender = possible.pop()
    if gender:
        rules.append({"field": "gender", "rule": "source_department_or_category", "source": gender_raw or categories})
    sizes = [SIZE_LABELS.get(token(value), value) for value in values(keys.get("size"))]
    material = keys.get("material", keys.get("fabric-type"))
    if material is None:
        material, evidence = feature_materials(row.get("features", []))
        if material:
            rules.append({"field": "material", "rule": "source_feature_composition_statement", "source": evidence})
    raw = {
        "_id": pid, "source_item_id": row["parent_asin"], "name": row.get("title") or "",
        "category_l1": path[0], "category_l2": path[1], "category_l3": path[2],
        "is_active": None, "colors": values(keys.get("color", keys.get("colour"))), "sizes": sizes,
        "attributes": {"gender": gender, "material": material,
                       "fit": keys.get("fit", keys.get("fit-type")), "style": keys.get("style"),
                       "season": keys.get("season", keys.get("seasons")), "brand": keys.get("brand")},
    }
    clean, normalized_issues, _ = Normalizer().normalize(raw)
    # Amazon price is USD. Missing VND is intentional, not an invalid source price.
    issues.extend(issue for issue in normalized_issues if issue["field"] != "price_vnd")
    price = numeric(row.get("price"))
    if price is None:
        issues.append({"product_id": pid, "field": "price_amount", "reason": "missing_or_invalid_source_price"})
    clean.update({"price_vnd": None, "price_amount": price, "price_currency": "USD",
                  "source": SOURCE, "source_kind": "real", "source_category": category,
                  "metadata_loaded": True, "source_store": row.get("store"),
                  "source_categories": categories, "attribute_provenance": rules,
                  "size_system": None})
    if not clean["name"]:
        issues.append({"product_id": pid, "field": "name", "reason": "missing_title"})
    # Never interpret US shoe size 10 as EU size; explicit size system is needed.
    if path[0] == "footwear" and sizes:
        clean["sizes"] = []
        issues.append({"product_id": pid, "field": "sizes", "reason": "unknown_footwear_size_system", "value": sizes})
    return clean, issues


def import_metadata(stream, directory, observed_path, ratings_path, category, scan_limit, source_location):
    directory.mkdir(parents=True, exist_ok=True)
    counts = Counter(scanned_records=0, matched_products=0, duplicate_metadata=0,
                     invalid_records=0, matched_reviews=0, unmatched_reviews=0)
    fields = Counter()
    exhausted = False
    with tempfile.TemporaryDirectory(prefix="metadata-index-", dir=directory) as temporary:
        staging = Path(temporary)
        db = sqlite3.connect(staging / "index.sqlite")
        try:
            db.execute("CREATE TABLE targets (id TEXT PRIMARY KEY, matched INTEGER NOT NULL DEFAULT 0)")
            for product in read_jsonl(observed_path):
                db.execute("INSERT OR IGNORE INTO targets (id) VALUES (?)", (product["product_id"],))
            total = db.execute("SELECT count(*) FROM targets").fetchone()[0]
            db.commit()
            with (staging / "products_clean.jsonl").open("w", encoding="utf-8") as output, \
                 (staging / "metadata_raw.jsonl").open("w", encoding="utf-8") as raw_out, \
                 (staging / "metadata_issues.jsonl").open("w", encoding="utf-8") as issue_out:
                iterator = iter(stream)
                while total and counts["matched_products"] < total and (not scan_limit or counts["scanned_records"] < scan_limit):
                    line = next(iterator, None)
                    if line is None:
                        exhausted = True
                        break
                    if not line.strip():
                        continue
                    counts["scanned_records"] += 1
                    try:
                        row = json.loads(line)
                        if not isinstance(row, dict) or not isinstance(row.get("parent_asin"), str):
                            raise ValueError("missing_parent_asin")
                    except (json.JSONDecodeError, ValueError):
                        counts["invalid_records"] += 1
                        issue_out.write(json.dumps({"source_line": counts["scanned_records"], "reason": "invalid_metadata_record"}) + "\n")
                        continue
                    pid = "amazon2023_p_" + row["parent_asin"]
                    target = db.execute("SELECT matched FROM targets WHERE id=?", (pid,)).fetchone()
                    if target is None:
                        continue
                    if target[0]:
                        counts["duplicate_metadata"] += 1
                        continue
                    clean, issues = metadata_product(row, category)
                    output.write(json.dumps(clean, ensure_ascii=False) + "\n")
                    raw_out.write(line.rstrip("\r\n") + "\n")
                    for issue in issues:
                        issue_out.write(json.dumps(issue, ensure_ascii=False) + "\n")
                        counts["issue:" + issue["field"] + ":" + issue["reason"]] += 1
                    for field in ["category_l1", "category_l2", "category_l3", "gender", "style", "material", "fit", "colors", "sizes", "season", "brand", "price_amount"]:
                        if clean.get(field) is not None and clean.get(field) != []:
                            fields[field] += 1
                    counts["matched_products"] += 1
                    db.execute("UPDATE targets SET matched=1 WHERE id=?", (pid,))
                db.commit()
            with (staging / "ratings_with_metadata.jsonl").open("w", encoding="utf-8") as output:
                for event in read_jsonl(ratings_path):
                    target = db.execute("SELECT matched FROM targets WHERE id=?", (event["product_id"],)).fetchone()
                    if target and target[0]:
                        output.write(json.dumps(event, ensure_ascii=False) + "\n")
                        counts["matched_reviews"] += 1
                    else:
                        counts["unmatched_reviews"] += 1
            with (staging / "unmatched_product_ids.jsonl").open("w", encoding="utf-8") as output:
                for (pid,) in db.execute("SELECT id FROM targets WHERE matched=0 ORDER BY id"):
                    output.write(json.dumps({"product_id": pid, "reason": "not_found_in_scanned_metadata"}) + "\n")
            report = {
                "dataset": SOURCE, "category": category, "source_location": source_location,
                "requested_scan_limit": scan_limit, "source_exhausted": exhausted,
                "target_products": total, "counts": dict(counts),
                "product_join_coverage": counts["matched_products"] / total if total else None,
                "products_with_known_field": dict(fields),
                "notes": ["A metadata match does not guarantee attributes or taxonomy are complete.",
                          "Unmatched IDs may exist beyond the scan limit; they are not proven missing from the full dataset.",
                          "Prices remain USD; availability and footwear size system are not assumed.",
                          "All original ratings stay intact; metadata subset is for content-aware experiments."],
            }
            write_json(staging / "metadata_quality_report.json", report)
            write_json(staging / "metadata_category_dictionary.json", {"category_labels": CATEGORY_LABELS, "department_gender_labels": GENDERS, "size_labels": SIZE_LABELS})
            for file in staging.glob("*.json*"):
                file.replace(directory / file.name)
            return report
        finally:
            db.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--input", type=Path)
    source.add_argument("--download", action="store_true")
    parser.add_argument("--category", choices=CATEGORIES, default="Amazon_Fashion")
    parser.add_argument("--scan-limit", type=int, default=10000, help="Maximum metadata records to scan; 0 for full source")
    parser.add_argument("--dataset-dir", type=Path)
    args = parser.parse_args()
    if args.scan_limit < 0:
        parser.error("--scan-limit must be >= 0")
    dataset = args.dataset_dir or Path(__file__).resolve().parent / "data/public" / args.category
    url = METADATA_HOST + "meta_" + args.category + ".jsonl.gz" if args.download else None
    with open_source(args.input, url) as stream:
        report = import_metadata(stream, dataset / "metadata", dataset / "observed_product_ids.jsonl",
                                 dataset / "explicit_ratings.jsonl", args.category, args.scan_limit,
                                 url or str(args.input.resolve()))
    print(json.dumps(report, ensure_ascii=False, indent=2))
    print(f"Output: {dataset / 'metadata'}")


if __name__ == "__main__":
    main()
