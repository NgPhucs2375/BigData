"""Normalize orders/reviews against the clean catalog, with auditable identity resolution."""
import argparse
from collections import Counter, defaultdict
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import json
from pathlib import Path
import re
import unicodedata

from normalize_catalog import mongo_id, read_jsonl, write_json

PURCHASE_WEIGHT = 5.0
FULFILLMENTS = {"Unfulfilled", "Processing", "Shipped", "Delivered"}


def email_user_id(email):
    if not isinstance(email, str):
        return None
    normalized = unicodedata.normalize("NFKC", email).strip().casefold()
    if not re.fullmatch(r"[^\s@]+@[^\s@]+\.[^\s@]+", normalized):
        return None
    return "u_" + hashlib.sha256(("email:" + normalized).encode("utf-8")).hexdigest()


def utc_timestamp(value):
    if isinstance(value, dict):
        value = value.get("$date")
    if not isinstance(value, str):
        return None
    try:
        date = datetime.fromisoformat(value.replace("Z", "+00:00"))
        # Mongo exported naive datetimes are UTC, not local Windows time.
        if date.tzinfo is None:
            date = date.replace(tzinfo=timezone.utc)
        return date.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    except ValueError:
        return None


def positive_integer(value):
    if isinstance(value, bool):
        return None
    try:
        number = Decimal(str(value))
        if number.is_finite() and number > 0 and number == number.to_integral():
            return int(number)
    except (InvalidOperation, ValueError, TypeError):
        pass
    return None


def event_id(source, document_id, product_id):
    return "i_" + hashlib.sha256(json.dumps([source, document_id, product_id], separators=(",", ":")).encode()).hexdigest()


def parsed_timestamp(value):
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


class InteractionNormalizer:
    def __init__(self, catalog, orders, source_kind="seed"):
        self.products = {row["product_id"] for row in catalog}
        self.orders = list(orders)
        self.source_kind = source_kind
        self.events, self.issues = [], []
        self.users = defaultdict(set)
        self.counts = Counter(input_orders=len(self.orders), input_reviews=0, output_interactions=0,
                              rejected_orders=0, rejected_reviews=0, invalid_order_lines=0,
                              duplicate_events=0)
        self.by_number = defaultdict(list)
        self.order_ids = Counter(mongo_id(o.get("_id")) for o in self.orders)
        for order in self.orders:
            if order.get("number"):
                self.by_number[order["number"]].append(order)
        self.seen_events = set()

    def issue(self, source, document_id, reason, product_id=None):
        self.issues.append({"source": source, "source_document_id": document_id,
                            "product_id": product_id, "reason": reason})
        self.counts["issue:" + reason] += 1

    def order_problem(self, order):
        oid = mongo_id(order.get("_id"))
        if not oid:
            return "missing_order_id"
        if self.order_ids[oid] != 1:
            return "ambiguous_order_id"
        if order.get("number") and len(self.by_number[order["number"]]) != 1:
            return "ambiguous_order_number"
        if order.get("fulfillmentStatus") not in FULFILLMENTS:
            return "cancelled_or_unknown_fulfillment"
        paid = order.get("paymentStatus") == "Paid"
        delivered_cod = (order.get("paymentStatus") == "Pending" and
                         order.get("paymentMethod") == "Cod" and order.get("fulfillmentStatus") == "Delivered")
        if not (paid or delivered_cod):
            return "not_completed_purchase"
        if not email_user_id((order.get("contact") or {}).get("email")):
            return "unresolved_order_user"
        if not utc_timestamp(order.get("createdAt")):
            return "invalid_order_timestamp"
        return None

    def emit(self, source, document_id, product_id, user_id, timestamp, timestamp_source,
             quantity=None, rating=None):
        iid = event_id(source, document_id, product_id)
        if iid in self.seen_events:
            self.counts["duplicate_events"] += 1
            self.issue(source, document_id, "duplicate_event", product_id)
            return
        self.seen_events.add(iid)
        self.events.append({
            "interaction_id": iid, "user_id": user_id, "user_source": "email_hash",
            "product_id": product_id, "event_type": "purchase" if source == "orders" else "review",
            "rating": rating, "quantity": quantity,
            "weight": PURCHASE_WEIGHT if source == "orders" else None,
            "timestamp": timestamp, "timestamp_source": timestamp_source,
            "source": source, "source_document_id": document_id, "source_kind": self.source_kind,
        })
        self.counts["output_interactions"] += 1

    def purchases(self):
        for order in self.orders:
            oid = mongo_id(order.get("_id"))
            problem = self.order_problem(order)
            if problem:
                self.counts["rejected_orders"] += 1
                self.issue("orders", oid, problem)
                continue
            uid = email_user_id(order["contact"]["email"])
            quantities = Counter()
            lines = order.get("lines") or []
            if not lines:
                self.counts["rejected_orders"] += 1
                self.issue("orders", oid, "empty_order")
                continue
            for line in lines:
                pid = mongo_id(line.get("productId"))
                quantity = positive_integer(line.get("quantity"))
                if pid not in self.products or quantity is None:
                    self.counts["invalid_order_lines"] += 1
                    self.issue("orders", oid, "unknown_product" if pid not in self.products else "invalid_quantity", pid)
                    continue
                # Variants of the same product in one order form one product-level event.
                quantities[pid] += quantity
            if not quantities:
                self.counts["rejected_orders"] += 1
            for pid, quantity in sorted(quantities.items()):
                self.emit("orders", oid, pid, uid, utc_timestamp(order["createdAt"]),
                          "order_created_at_proxy", quantity=quantity)
            if quantities:
                self.users[uid].add(oid)

    def reviews(self, reviews):
        reviews = list(reviews)
        ids = Counter(mongo_id(r.get("_id")) for r in reviews)
        self.counts["input_reviews"] += len(reviews)
        for review in reviews:
            rid = mongo_id(review.get("_id"))
            pid = mongo_id(review.get("productId"))
            problem = None
            rating = positive_integer(review.get("rating"))
            timestamp = utc_timestamp(review.get("createdAt"))
            matches = self.by_number.get(review.get("orderNumber"), [])
            if not rid or ids[rid] != 1:
                problem = "missing_or_ambiguous_review_id"
            elif review.get("status") != "Published":
                problem = "unpublished_review"
            elif pid not in self.products:
                problem = "unknown_product"
            elif rating is None or rating > 5:
                problem = "invalid_rating"
            elif timestamp is None:
                problem = "invalid_review_timestamp"
            elif not review.get("orderNumber") or not matches:
                problem = "unresolved_review_user"
            elif len(matches) != 1:
                problem = "ambiguous_review_order"
            else:
                order = matches[0]
                problem = self.order_problem(order)
                if not problem:
                    matching_lines = [line for line in order.get("lines", []) if mongo_id(line.get("productId")) == pid and positive_integer(line.get("quantity"))]
                    if not matching_lines:
                        problem = "review_product_not_in_order"
                    elif parsed_timestamp(timestamp) < parsed_timestamp(utc_timestamp(order["createdAt"])):
                        problem = "review_before_order"
            if problem:
                self.counts["rejected_reviews"] += 1
                self.issue("reviews", rid, problem, pid)
                continue
            uid = email_user_id(order["contact"]["email"])
            self.users[uid].add(mongo_id(order["_id"]))
            self.emit("reviews", rid, pid, uid, timestamp, "review_created_at", rating=rating)

    def report(self):
        user_counts = Counter(e["user_id"] for e in self.events)
        pairs = {(e["user_id"], e["product_id"]) for e in self.events}
        product_count = len(self.products)
        possible = len(user_counts) * product_count
        timestamps = [e["timestamp"] for e in self.events]
        return {
            "schema_version": 1, "source_kind": self.source_kind,
            "counts": dict(self.counts), "events_by_type": dict(Counter(e["event_type"] for e in self.events)),
            "users": len(user_counts), "catalog_products": product_count,
            "products_with_interactions": len({e["product_id"] for e in self.events}),
            "unique_user_product_pairs": len(pairs),
            "users_with_one_interaction": sum(n == 1 for n in user_counts.values()),
            "user_item_sparsity": 1 - len(pairs) / possible if possible else None,
            "sparsity_denominator": "resolved users x all clean catalog products",
            "timestamp_min": min(timestamps, key=parsed_timestamp) if timestamps else None,
            "timestamp_max": max(timestamps, key=parsed_timestamp) if timestamps else None,
            "purchase_weight": PURCHASE_WEIGHT,
            "available_sources": ["orders", "reviews"],
            "unavailable_event_sources": ["view", "cart", "wishlist"],
            "notes": ["Email hash is a pseudonymous identity, not verified account ownership.",
                      "Order createdAt is a proxy for purchase time, not confirmed payment time.",
                      "Seed source is demo data, not observed real customer behavior.",
                      "Purchase weight is an implicit signal; review rating stays explicit and separate."],
        }


def write_jsonl(path, rows):
    with path.open("w", encoding="utf-8") as stream:
        for row in rows:
            stream.write(json.dumps(row, ensure_ascii=False) + "\n")


def main():
    base = Path(__file__).resolve().parent / "data"
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-dir", type=Path, default=base)
    parser.add_argument("--catalog", type=Path, default=base / "silver/products_clean.jsonl")
    parser.add_argument("--output-dir", type=Path, default=base / "silver")
    parser.add_argument("--source-kind", choices=["seed", "real", "simulated"], default="seed")
    parser.add_argument("--parquet", action="store_true")
    args = parser.parse_args()
    if args.parquet:
        import pyarrow as pa
        import pyarrow.parquet as pq
    pipeline = InteractionNormalizer(read_jsonl(args.catalog), read_jsonl(args.input_dir / "orders.jsonl"), args.source_kind)
    pipeline.purchases()
    pipeline.reviews(read_jsonl(args.input_dir / "reviews.jsonl"))
    events = sorted(pipeline.events, key=lambda e: (parsed_timestamp(e["timestamp"]), e["interaction_id"]))
    args.output_dir.mkdir(parents=True, exist_ok=True)
    write_jsonl(args.output_dir / "interactions_clean.jsonl", events)
    write_jsonl(args.output_dir / "implicit_interactions.jsonl", [e for e in events if e["event_type"] == "purchase"])
    write_jsonl(args.output_dir / "explicit_ratings.jsonl", [e for e in events if e["event_type"] == "review"])
    write_jsonl(args.output_dir / "interaction_issues.jsonl", pipeline.issues)
    write_jsonl(args.output_dir / "user_id_mapping.jsonl", [
        {"user_id": uid, "user_source": "email_hash", "source_order_ids": sorted(ids)}
        for uid, ids in sorted(pipeline.users.items())])
    report = pipeline.report()
    write_json(args.output_dir / "interaction_quality_report.json", report)
    if args.parquet:
        schema = pa.schema([(key, pa.string()) for key in ["interaction_id", "user_id", "user_source", "product_id", "event_type", "timestamp", "timestamp_source", "source", "source_document_id", "source_kind"]] +
                           [("rating", pa.int64()), ("quantity", pa.int64()), ("weight", pa.float64())])
        pq.write_table(pa.Table.from_pylist(events, schema=schema), args.output_dir / "interactions_clean.parquet", compression="snappy")
    print(json.dumps(report, ensure_ascii=False, indent=2))
    print(f"Output: {args.output_dir}")


if __name__ == "__main__":
    main()
