import io
import json
from pathlib import Path
import tempfile
import unittest

from import_amazon_reviews import import_stream, review_event


class PublicReviewTests(unittest.TestCase):
    def row(self, **changes):
        row = {"user_id": "reviewerA", "parent_asin": "PARENT", "asin": "VARIANT",
               "rating": 4.0, "timestamp": 1588687728923, "verified_purchase": True}
        row.update(changes)
        return row

    def test_real_review_preserves_time_and_separates_id_namespace(self):
        event = review_event(self.row(), "Amazon_Fashion")
        self.assertEqual(event["timestamp"], "2020-05-05T14:08:48.923Z")
        self.assertEqual(event["product_id"], "amazon2023_p_PARENT")
        self.assertTrue(event["user_id"].startswith("amazon2023_u_"))
        self.assertEqual(event["rating"], 4.0)
        self.assertIsNone(event["weight"])
        self.assertEqual(event["source_kind"], "real")
        self.assertEqual(event["event_type"], "review")

    def test_rejects_invalid_fields_and_does_not_fallback_to_variant_asin(self):
        for changes in [{"parent_asin": None}, {"user_id": ""}, {"rating": float("nan")},
                        {"rating": 6}, {"timestamp": 1588687728}, {"rating": True},
                        {"timestamp": -1}, {"timestamp": 1588687728923.0}]:
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                review_event(self.row(**changes), "Amazon_Fashion")

    def test_legacy_timestamp_field_and_duplicates(self):
        row = self.row()
        row["sort_timestamp"] = row.pop("timestamp")
        self.assertEqual(review_event(row, "Amazon_Fashion")["timestamp"], "2020-05-05T14:08:48.923Z")
        with tempfile.TemporaryDirectory() as directory:
            records = [self.row(), self.row(), self.row(rating=5, timestamp=1588687729923)]
            stream = io.StringIO("".join(json.dumps(r) + "\n" for r in records) + "invalid\n")
            report = import_stream(stream, Path(directory), "Amazon_Fashion", 0, "test_fixture", False)
            self.assertEqual(report["counts"]["output_reviews"], 2)
            self.assertEqual(report["counts"]["duplicates"], 1)
            self.assertEqual(report["counts"]["rejected_records"], 1)
            self.assertEqual(report["unique_user_product_pairs"], 1)
            self.assertFalse(report["metadata_loaded"])

    def test_limit_does_not_consume_or_publish_more_records(self):
        with tempfile.TemporaryDirectory() as directory:
            stream = io.StringIO(json.dumps(self.row()) + "\n" + json.dumps(self.row(user_id="B")) + "\n")
            report = import_stream(stream, Path(directory), "Amazon_Fashion", 1, "test_fixture", True)
            self.assertEqual(report["counts"]["input_records"], 1)
            self.assertEqual(report["sampling"], "ordered_prefix")
            self.assertTrue(stream.readline())


if __name__ == "__main__":
    unittest.main()
