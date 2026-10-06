"""Raw Mongo JSONL -> clean analytics catalog. Never writes back to MongoDB."""
import argparse
from collections import Counter
from decimal import Decimal, InvalidOperation
import hashlib
import json
from pathlib import Path
import re

from catalog_dictionary import (
    ALIASES, CATEGORY_ALIASES, COLLECTIONS, SEED_CATEGORIES, TAXONOMY,
    VERSION, VOCABULARY, category_paths, normalize_value, token,
)


def values(value):
    if value is None:
        return []
    return value if isinstance(value, list) else [value]


def mongo_id(value):
    return str(value.get("$oid", "")) if isinstance(value, dict) else str(value or "")


def numeric(value):
    if isinstance(value, dict):
        value = value.get("$numberDecimal", value.get("$numberInt"))
    try:
        result = Decimal(str(value))
        if result.is_finite() and result >= 0:
            return int(result) if result == result.to_integral() else float(result)
    except (InvalidOperation, ValueError, TypeError):
        pass
    return None


class Normalizer:
    def __init__(self, categories=(), overrides=None, default_brand=None, seed_profiles=None):
        self.categories = {mongo_id(c.get("_id", c.get("id"))): c for c in categories}
        self.overrides = overrides or {}
        self.default_brand = default_brand
        self.paths = category_paths()
        self.seed_profiles = seed_profiles or {}

    def normalize(self, product):
        issues = []
        decisions = []
        pid = mongo_id(product.get("_id", product.get("product_id")))
        attrs = dict(product.get("attributes") or {})
        slug = re.sub(r"-\d+$", "", product.get("slug", ""))
        profile = self.seed_profiles.get(slug, {})
        for field in ["category", "gender", "style", "season", "brand"]:
            if field in profile and attrs.get(field) is None and product.get(field) is None:
                source_tags = [normalize_value(field, tag) for tag in product.get("tags", [])] if field in {"gender", "style", "season"} else []
                source_tags = [value for value in source_tags if value]
                if field == "gender" and source_tags:
                    continue
                result = profile[field]
                if field in {"style", "season"}:
                    result = list(dict.fromkeys(source_tags + values(result)))
                attrs[field] = result
                decisions.append({"field": field, "rule": "editorial_demo_profile", "profile": slug, "source": result})
        attrs.update(self.overrides.get(pid, {}))
        decisions.extend({"field": field, "rule": "reviewed_override", "source": value}
                         for field, value in self.overrides.get(pid, {}).items())

        def issue(field, value, reason="unknown_value"):
            issues.append({"product_id": pid, "field": field, "value": value, "reason": reason})

        def mapped(field, source):
            out = []
            for raw in values(source):
                alias = next((value for key, value in profile.get("color_aliases", {}).items() if token(key) == token(raw)), None) if field == "color" else None
                result = normalize_value(field, alias if alias is not None else raw, path[0] == "footwear")
                if alias is not None:
                    decisions.append({"field": "color", "rule": "editorial_demo_profile", "profile": slug, "source": raw, "canonical_value": result})
                if result is None:
                    issue(field, raw)
                elif result not in out:
                    out.append(result)
            return out

        candidates = []
        for raw in [attrs.get("category"), product.get("category_l3"), product.get("category_l2"), product.get("category_l1")]:
            if raw:
                key = CATEGORY_ALIASES.get(token(raw), token(raw))
                if key in self.paths:
                    candidates.append(self.paths[key])
                else:
                    issue("category", raw)
        for cid in product.get("categoryIds", []):
            category = self.categories.get(mongo_id(cid))
            if category is None:
                issue("categoryIds", cid, "unresolved_reference")
                continue
            key = CATEGORY_ALIASES.get(token(category.get("slug", "")), token(category.get("slug", "")))
            if key in self.paths:
                candidates.append(self.paths[key])
            else:
                issue("category", category.get("slug"))
        path = max(candidates, key=lambda p: sum(v is not None for v in p), default=(None, None, None))
        if any(p[0] != path[0] or (p[1] and path[1] and p[1] != path[1]) or (p[2] and path[2] and p[2] != path[2]) for p in candidates):
            issue("category", candidates, "conflicting_categories")
            path = (None, None, None)
        elif path[1] is None:
            slug = re.sub(r"-\d+$", "", product.get("slug", ""))
            known = SEED_CATEGORIES.get(slug)
            if known and (path[0] is None or path[0] == known[0]):
                path = known
                decisions.append({"field": "category", "rule": "reviewed_seed_slug", "source": slug})
        if path[1] is None:
            issue("category_l2", None, "missing_category")

        tags = product.get("tags") or []
        gender_source = attrs.get("gender", product.get("gender"))
        if gender_source is None:
            gender_source = [t for t in tags if normalize_value("gender", t)]
        gender = mapped("gender", gender_source)
        if len(gender) != 1:
            issue("gender", gender_source, "missing_or_ambiguous")
        style_source = attrs.get("style", product.get("style"))
        styles = mapped("style", style_source) if style_source is not None else mapped("style", [t for t in tags if normalize_value("style", t)])
        season_source = attrs.get("season", product.get("season"))
        seasons = mapped("season", season_source) if season_source is not None else mapped("season", [t for t in tags if normalize_value("season", t)])
        for field, result in [("style", styles), ("season", seasons)]:
            if not result:
                issue(field, None, "missing_value")

        materials = []
        weight = numeric(attrs.get("weight_gsm", product.get("weight_gsm")))
        for raw in values(attrs.get("material", product.get("material"))):
            exact = normalize_value("material", raw)
            text = token(raw)
            gsm = re.search(r"(\d+(?:\.\d+)?)\s*gsm\b", str(raw), re.I)
            if gsm and weight is None:
                weight = numeric(gsm.group(1))
            found = [exact] if exact else []
            if not exact:
                # Longest matches first: faux-leather must not become leather.
                remainder = text
                for code in sorted(VOCABULARY["material"], key=len, reverse=True):
                    pattern = r"(?<![a-z0-9])" + re.escape(code) + r"(?![a-z0-9])"
                    if re.search(pattern, remainder):
                        found.append(code)
                        remainder = re.sub(pattern, "", remainder)
                remainder = re.sub(r"\d+(?:-\d+)?(?:gsm|oz)?|knit|oxford|ripstop|blend|lot", "", remainder).strip("-")
                if remainder:
                    issue("material", raw, "unmapped_material_component")
            if not found:
                issue("material", raw)
            for code in found:
                if code not in materials:
                    materials.append(code)
        if not materials:
            issue("material", None, "missing_value")
        fit_raw = attrs.get("fit", product.get("fit"))
        fit = normalize_value("fit", fit_raw) if fit_raw is not None else None
        if fit is None and path[0] not in {"accessories", "footwear"}:
            issue("fit", fit_raw, "missing_or_unknown")
        elif fit_raw is not None and fit is None and token(fit_raw) not in {"one-size", "os"}:
            issue("fit", fit_raw)

        variants = []
        colors, sizes, prices = [], [], []
        for variant in product.get("variants", []):
            options = variant.get("options") or {}
            color = mapped("color", options.get("color"))
            size = mapped("size", options.get("size"))
            if not color:
                issue("variant.color", variant.get("sku"), "missing_or_unknown")
            if not size:
                issue("variant.size", variant.get("sku"), "missing_or_unknown")
            price = numeric(variant.get("price"))
            if price is None:
                issue("price_vnd", variant.get("price"), "invalid_variant_price")
            else:
                prices.append(price)
            variants.append({"sku": variant.get("sku"), "color": color[0] if color else None,
                             "size": size[0] if size else None, "price_vnd": price, "stock": variant.get("stock")})
            colors.extend(c for c in color if c not in colors)
            sizes.extend(s for s in size if s not in sizes)
        if not variants:
            colors = mapped("color", product.get("colors", attrs.get("colors")))
            sizes = mapped("size", product.get("sizes", attrs.get("sizes")))
        for field, result in [("colors", colors), ("sizes", sizes)]:
            if not result:
                issue(field, None, "missing_value")
        price = min(prices) if prices else numeric(product.get("price_vnd", product.get("priceFrom")))
        if price is None:
            issue("price_vnd", None, "missing_or_invalid")
        brand_raw = attrs.get("brand", product.get("brand"))
        if brand_raw is None and self.default_brand:
            brand_raw = self.default_brand
            decisions.append({"field": "brand", "rule": "configured_source_brand", "source": brand_raw})
        brand = normalize_value("brand", brand_raw) if brand_raw is not None else None
        if brand is None:
            issue("brand", None, "missing_value")
        if not pid:
            issue("product_id", None, "missing_id")
        source_id = product.get("source_item_id") or attrs.get("source_item_id") or attrs.get("itemId")
        if not source_id and pid:
            source_id = "mua-" + hashlib.sha256(pid.encode()).hexdigest()[:24]
        clean = {
            "product_id": pid, "source_item_id": source_id, "name": product.get("name", ""),
            "category_l1": path[0], "category_l2": path[1], "category_l3": path[2],
            "gender": gender[0] if len(gender) == 1 else None, "style": styles,
            "material": materials, "fit": fit, "colors": colors, "sizes": sizes,
            "season": seasons, "weight_gsm": weight, "price_vnd": price,
            "brand": brand, "is_active": product.get("is_active", product.get("status") == "Active"),
            "variants": variants,
        }
        return clean, issues, decisions


def read_jsonl(path):
    with path.open(encoding="utf-8-sig") as stream:
        for line in stream:
            if line.strip():
                yield json.loads(line)


def write_json(path, value):
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    base = Path(__file__).resolve().parent / "data"
    parser.add_argument("--input-dir", type=Path, default=base)
    parser.add_argument("--output-dir", type=Path, default=base / "silver")
    parser.add_argument("--overrides", type=Path, help="JSON object keyed by product_id with reviewed attributes/category")
    parser.add_argument("--seed-profiles", type=Path, help="Optional editorial demo profiles keyed by seed slug; not verified real-world attributes")
    parser.add_argument("--default-brand", help="Explicit source brand, e.g. mua; no implicit default")
    parser.add_argument("--parquet", action="store_true", help="Also write Parquet; requires pyarrow")
    args = parser.parse_args()
    if args.parquet:
        import pyarrow as pa
        import pyarrow.parquet as pq
    category_file = args.input_dir / "categories.jsonl"
    categories = list(read_jsonl(category_file)) if category_file.exists() else []
    overrides = json.loads(args.overrides.read_text(encoding="utf-8-sig")) if args.overrides else None
    profiles_document = json.loads(args.seed_profiles.read_text(encoding="utf-8-sig")) if args.seed_profiles else None
    normalizer = Normalizer(categories, overrides, args.default_brand,
                            profiles_document["profiles"] if profiles_document else None)
    args.output_dir.mkdir(parents=True, exist_ok=True)
    seen, source_ids = set(), set()
    counts, distribution = Counter(), Counter()
    parquet_rows = []
    with (args.output_dir / "products_clean.jsonl").open("w", encoding="utf-8") as clean_out, \
         (args.output_dir / "catalog_issues.jsonl").open("w", encoding="utf-8") as issue_out, \
         (args.output_dir / "product_id_mapping.jsonl").open("w", encoding="utf-8") as mapping_out:
        for product in read_jsonl(args.input_dir / "products.jsonl"):
            counts["input_products"] += 1
            clean, issues, decisions = normalizer.normalize(product)
            if clean["product_id"] in seen or clean["source_item_id"] in source_ids:
                issues.append({"product_id": clean["product_id"], "field": "identity", "reason": "duplicate_id"})
                counts["rejected_products"] += 1
            elif not clean["product_id"]:
                counts["rejected_products"] += 1
            else:
                seen.add(clean["product_id"])
                source_ids.add(clean["source_item_id"])
                clean_out.write(json.dumps(clean, ensure_ascii=False) + "\n")
                mapping_out.write(json.dumps({"product_id": clean["product_id"], "source_item_id": clean["source_item_id"], "normalization_rules": decisions}, ensure_ascii=False) + "\n")
                counts["output_products"] += 1
                distribution["/".join(v for v in [clean["category_l1"], clean["category_l2"], clean["category_l3"]] if v) or "unknown"] += 1
                if args.parquet:
                    parquet_rows.append(clean)
            if issues:
                counts["products_with_issues"] += 1
            for issue in issues:
                issue_out.write(json.dumps(issue, ensure_ascii=False) + "\n")
                counts["issue:" + issue["field"] + ":" + issue["reason"]] += 1
    if args.parquet and parquet_rows:
        variant_type = pa.struct([("sku", pa.string()), ("color", pa.string()), ("size", pa.string()), ("price_vnd", pa.float64()), ("stock", pa.int64())])
        schema = pa.schema([(k, pa.string()) for k in ["product_id", "source_item_id", "name", "category_l1", "category_l2", "category_l3", "gender", "fit", "brand"]] +
                           [(k, pa.list_(pa.string())) for k in ["style", "material", "colors", "sizes", "season"]] +
                           [("weight_gsm", pa.float64()), ("price_vnd", pa.float64()), ("is_active", pa.bool_()), ("variants", pa.list_(variant_type))])
        pq.write_table(pa.Table.from_pylist(parquet_rows, schema=schema), args.output_dir / "products_clean.parquet", compression="snappy")
    write_json(args.output_dir / "catalog_quality_report.json", {"dictionary_version": VERSION, "categories_source_available": category_file.exists(), "seed_profiles_provenance": profiles_document.get("provenance") if profiles_document else None, "counts": dict(counts), "category_distribution": dict(distribution)})
    write_json(args.output_dir / "product_attribute_dictionary.json", {"version": VERSION, "taxonomy": TAXONOMY, "vocabulary": VOCABULARY, "aliases": ALIASES, "category_aliases": CATEGORY_ALIASES, "seed_category_rules": SEED_CATEGORIES})
    with (args.output_dir / "categories_clean.jsonl").open("w", encoding="utf-8") as stream:
        for code, path in category_paths().items():
            stream.write(json.dumps({"code": code, "parent_code": path[1] if path[2] else path[0] if path[1] else None, "path": "/".join(v for v in path if v)}, ensure_ascii=False) + "\n")
    write_json(args.output_dir / "collections_dictionary.json", COLLECTIONS)
    print(json.dumps(dict(counts), ensure_ascii=False, indent=2))
    print(f"Output: {args.output_dir}")


if __name__ == "__main__":
    main()
