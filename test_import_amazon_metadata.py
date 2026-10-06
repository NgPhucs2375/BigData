import io
import json
from pathlib import Path
import tempfile
import unittest

from import_amazon_metadata import feature_materials, import_metadata, metadata_product


class MetadataTests(unittest.TestCase):
    def row(self, **changes):
        row = {"parent_asin": "A", "title": "A cotton tee", "price": "19.99", "store": "Retail store",
               "categories": ["Clothing, Shoes & Jewelry", "Men", "Clothing", "T-Shirts"],
               "details": {"Material": "Cotton 240gsm", "Color": "Black", "Size": "Large", "Fit Type": "Regular", "Brand": "Example Brand"}}
        row.update(changes)
        return row

    def test_source_attributes_currency_and_brand_are_not_fabricated(self):
        clean, issues = metadata_product(self.row(), "Amazon_Fashion")
        self.assertEqual(clean["category_l1"], "tops")
        self.assertEqual(clean["category_l2"], "t-shirt")
        self.assertEqual(clean["gender"], "male")
        self.assertEqual(clean["material"], ["cotton"])
        self.assertEqual(clean["weight_gsm"], 240)
        self.assertEqual(clean["sizes"], ["L"])
        self.assertEqual(clean["price_amount"], 19.99)
        self.assertEqual(clean["price_currency"], "USD")
        self.assertIsNone(clean["price_vnd"])
        self.assertIsNone(clean["is_active"])
        self.assertEqual(clean["brand"], "example-brand")
        clean, _ = metadata_product(self.row(details={}), "Amazon_Fashion")
        self.assertIsNone(clean["brand"])
        self.assertEqual(clean["source_store"], "Retail store")

    def test_empty_categories_and_json_details_stay_auditable(self):
        clean, issues = metadata_product(self.row(categories=[], details=json.dumps({"Department": "womens", "Material": "Linen"})), "Amazon_Fashion")
        self.assertIsNone(clean["category_l1"])
        self.assertEqual(clean["gender"], "female")
        self.assertEqual(clean["material"], ["linen"])
        self.assertTrue(any(i["reason"] == "unmapped_or_missing_source_categories" for i in issues))
        clean, _ = metadata_product(self.row(details="bad json"), "Amazon_Fashion")
        self.assertEqual(clean["material"], [])

    def test_conflicting_categories_and_unknown_shoe_size_system(self):
        clean, issues = metadata_product(self.row(categories=["T-Shirts", "Jeans"]), "Amazon_Fashion")
        self.assertIsNone(clean["category_l1"])
        self.assertTrue(any(i["reason"] == "conflicting_source_categories" for i in issues))
        clean, issues = metadata_product(self.row(categories=["Shoes", "Fashion Sneakers"], details={"Size": "41"}), "Amazon_Fashion")
        self.assertEqual(clean["sizes"], [])
        self.assertTrue(any(i["reason"] == "unknown_footwear_size_system" for i in issues))

    def test_explicit_composition_features_are_used_but_marketing_mentions_are_not(self):
        clean, _ = metadata_product(self.row(details={}, features=["85% Recycled Polyester, 15% Spandex", "Machine Wash"]), "Amazon_Fashion")
        self.assertEqual(set(clean["material"]), {"polyester", "spandex"})
        self.assertTrue(any(p["rule"] == "source_feature_composition_statement" for p in clean["attribute_provenance"]))
        self.assertEqual(feature_materials(["A comfortable cotton look for daily wear"])[0], [])
        self.assertEqual(set(feature_materials(["Cotton Blend,Polyester,Cotton"])[0]), {"cotton", "polyester"})

    def test_join_filters_metadata_without_altering_original_ratings(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            observed = base / "observed.jsonl"
            observed.write_text(' {"product_id":"amazon2023_p_A"}\n{"product_id":"amazon2023_p_B"}\n', encoding="utf-8")
            ratings = base / "ratings.jsonl"
            ratings.write_text('{"product_id":"amazon2023_p_A","rating":4}\n{"product_id":"amazon2023_p_B","rating":5}\n', encoding="utf-8")
            original = ratings.read_bytes()
            stream = io.StringIO(json.dumps(self.row(parent_asin="C")) + "\n" + json.dumps(self.row()) + "\n")
            report = import_metadata(stream, base / "out", observed, ratings, "Amazon_Fashion", 2, "fixture")
            self.assertEqual(report["counts"]["matched_products"], 1)
            self.assertEqual(report["product_join_coverage"], 0.5)
            self.assertEqual(report["counts"]["matched_reviews"], 1)
            self.assertEqual(report["counts"]["unmatched_reviews"], 1)
            self.assertEqual(original, ratings.read_bytes())
            self.assertIn("amazon2023_p_B", (base / "out/unmatched_product_ids.jsonl").read_text())


if __name__ == "__main__":
    unittest.main()
