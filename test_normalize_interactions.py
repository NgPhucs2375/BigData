import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from normalize_interactions import InteractionNormalizer, email_user_id, utc_timestamp


class InteractionTests(unittest.TestCase):
    def order(self, **changes):
        result = {"_id": "o1", "number": "N1", "contact": {"email": " Alice@Example.com "},
                  "paymentStatus": "Paid", "paymentMethod": "Cod", "fulfillmentStatus": "Delivered",
                  "createdAt": "2026-09-01 12:00:00", "lines": [{"productId": "p1", "quantity": 2}]}
        result.update(changes)
        return result

    def review(self, **changes):
        result = {"_id": "r1", "orderNumber": "N1", "productId": "p1", "rating": 4,
                  "status": "Published", "createdAt": "2026-09-02T12:00:00Z", "authorName": "Alice"}
        result.update(changes)
        return result

    def pipeline(self, orders=None):
        return InteractionNormalizer([{"product_id": "p1"}, {"product_id": "p2"}],
                                     orders if orders is not None else [self.order()])

    def test_email_normalization_and_utc(self):
        self.assertEqual(email_user_id(" Alice@Example.com "), email_user_id("alice@example.com"))
        self.assertIsNone(email_user_id("Alice"))
        self.assertIsNone(email_user_id(None))
        self.assertEqual(utc_timestamp("2026-09-01T19:00:00+07:00"), "2026-09-01T12:00:00Z")
        self.assertEqual(utc_timestamp("2026-09-01 12:00:00"), "2026-09-01T12:00:00Z")
        self.assertIsNone(utc_timestamp("not a date"))

    def test_variants_aggregate_but_separate_orders_stay_separate(self):
        order = self.order(lines=[{"productId": "p1", "quantity": 2}, {"productId": "p1", "quantity": 3}])
        other = self.order(_id="o2", number="N2")
        pipeline = self.pipeline([order, other])
        pipeline.purchases()
        self.assertEqual(len(pipeline.events), 2)
        self.assertEqual(pipeline.events[0]["quantity"], 5)
        self.assertEqual(pipeline.events[0]["weight"], 5.0)
        self.assertIsNone(pipeline.events[0]["rating"])
        self.assertNotEqual(pipeline.events[0]["interaction_id"], pipeline.events[1]["interaction_id"])

    def test_cancelled_refunded_failed_pending_orders_are_not_positive(self):
        for change in [{"fulfillmentStatus": "Cancelled"}, {"paymentStatus": "Refunded"},
                       {"paymentStatus": "Failed"}, {"paymentStatus": "Pending", "fulfillmentStatus": "Processing"}]:
            with self.subTest(change=change):
                pipeline = self.pipeline([self.order(**change)])
                pipeline.purchases()
                self.assertEqual(pipeline.events, [])
        pipeline = self.pipeline([self.order(paymentStatus="Pending")])
        pipeline.purchases()
        self.assertEqual(len(pipeline.events), 1)  # Delivered COD.

    def test_review_requires_order_product_user_and_chronology(self):
        bad = [(self.review(orderNumber=None), "unresolved_review_user"),
               (self.review(productId="p2"), "review_product_not_in_order"),
               (self.review(rating=6), "invalid_rating"),
               (self.review(createdAt="2026-08-01T12:00:00Z"), "review_before_order"),
               (self.review(status="Pending"), "unpublished_review")]
        for review, reason in bad:
            with self.subTest(reason=reason):
                pipeline = self.pipeline()
                pipeline.reviews([review])
                self.assertEqual(pipeline.events, [])
                self.assertEqual(pipeline.issues[0]["reason"], reason)
        pipeline = self.pipeline()
        pipeline.purchases()
        pipeline.reviews([self.review()])
        self.assertEqual(pipeline.events[0]["user_id"], pipeline.events[1]["user_id"])
        self.assertEqual(pipeline.events[1]["rating"], 4)
        self.assertIsNone(pipeline.events[1]["weight"])
        self.assertNotIn("Alice", json.dumps(pipeline.events))

    def test_duplicate_orders_and_reviews_do_not_duplicate_positive_signals(self):
        pipeline = self.pipeline([self.order(), self.order()])
        pipeline.purchases()
        pipeline.reviews([self.review()])
        self.assertEqual(pipeline.events, [])
        pipeline = self.pipeline()
        pipeline.reviews([self.review(), self.review()])
        self.assertEqual(pipeline.events, [])

    def test_invalid_lines_missing_email_and_invalid_timestamp(self):
        for change in [{"contact": {"email": "invalid"}}, {"createdAt": "invalid"},
                       {"lines": [{"productId": "unknown", "quantity": 1}]},
                       {"lines": [{"productId": "p1", "quantity": True}]},
                       {"lines": [{"productId": "p1", "quantity": 1.5}]}]:
            pipeline = self.pipeline([self.order(**change)])
            pipeline.purchases()
            self.assertEqual(pipeline.events, [])

    def test_empty_report_and_pair_sparsity(self):
        self.assertIsNone(self.pipeline([]).report()["user_item_sparsity"])
        pipeline = self.pipeline()
        pipeline.purchases()
        pipeline.reviews([self.review()])
        report = pipeline.report()
        self.assertEqual(report["users"], 1)
        self.assertEqual(report["unique_user_product_pairs"], 1)
        self.assertEqual(report["user_item_sparsity"], 0.5)

    def test_cli_outputs_separate_signals_and_are_repeatable(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            for filename, rows in [("orders.jsonl", [self.order()]), ("reviews.jsonl", [self.review()]),
                                   ("catalog.jsonl", [{"product_id": "p1"}])]:
                (base / filename).write_text("".join(json.dumps(r) + "\n" for r in rows), encoding="utf-8")
            command = [sys.executable, str(Path(__file__).with_name("normalize_interactions.py")),
                       "--input-dir", str(base), "--catalog", str(base / "catalog.jsonl"),
                       "--output-dir", str(base / "out")]
            subprocess.run(command, check=True, capture_output=True)
            events = (base / "out/interactions_clean.jsonl").read_bytes()
            self.assertNotIn(b"Alice", events)
            self.assertNotIn(b"Example.com", events)
            for file in ["implicit_interactions.jsonl", "explicit_ratings.jsonl"]:
                self.assertEqual(len((base / "out" / file).read_text().splitlines()), 1)
            subprocess.run(command, check=True, capture_output=True)
            self.assertEqual((base / "out/interactions_clean.jsonl").read_bytes(), events)


if __name__ == "__main__":
    unittest.main()
