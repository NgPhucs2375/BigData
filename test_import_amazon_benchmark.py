import csv
import io
import json
from pathlib import Path
import tempfile
import unittest

from import_amazon_benchmark import event, import_split


class BenchmarkTests(unittest.TestCase):
    def row(self, **changes):
        row = {"user_id": "u", "parent_asin": "p", "rating": "4.0", "timestamp": "1588687728923", "history": "x y"}
        row.update(changes)
        return row

    def test_event_keeps_split_and_namespaces_ids(self):
        item = event(self.row(), "Clothing_Shoes_and_Jewelry", "valid")
        self.assertEqual(item["benchmark_split"], "valid")
        self.assertEqual(item["product_id"], "amazon2023_p_p")
        self.assertEqual(item["timestamp"], "2020-05-05T14:08:48.923Z")
        self.assertIsNone(item["weight"])

    def test_rejects_invalid_event(self):
        for changes in [{"user_id": ""}, {"parent_asin": ""}, {"rating": "x"}, {"rating": "6"}, {"timestamp": "1"}]:
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                event(self.row(**changes), "Clothing_Shoes_and_Jewelry", "train")

    def test_stream_import_limits_and_deduplicates(self):
        rows = [self.row(), self.row(), self.row(timestamp="1588687729923")]
        content = io.StringIO()
        writer = csv.DictWriter(content, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
        content.seek(0)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "events.jsonl"
            report = import_split(content, path, "Clothing_Shoes_and_Jewelry", "train", 0)
            self.assertEqual(report, {"input_records": 3, "output_records": 2, "duplicates": 1, "rejected_records": 0})
            self.assertEqual(len(path.read_text(encoding="utf-8").splitlines()), 2)


if __name__ == "__main__":
    unittest.main()
