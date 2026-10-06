import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from catalog_dictionary import category_paths, normalize_value
from normalize_catalog import Normalizer


class CatalogNormalizationTests(unittest.TestCase):
    def product(self, **changes):
        product = {
            "_id": "product-1", "name": "Test Tee", "status": "Active",
            "category_l1": "tops", "category_l2": "t-shirt",
            "attributes": {"gender": "Nam/Nữ", "material": "Cotton 240gsm", "fit": "Oversize",
                           "style": ["streetwear", "minimal"], "season": "summer", "brand": "MUA Official"},
            "variants": [{"sku": "A", "options": {"color": "Jet Black", "size": "XL"}, "price": {"$numberDecimal": "390000"}, "stock": 5}],
        }
        product.update(changes)
        return product

    def test_canonical_product_preserves_identity_and_variant_relationships(self):
        product = self.product()
        product["variants"].append({"sku": "B", "options": {"color": "Off White", "size": "M"}, "price": "450000", "stock": 2})
        clean, issues, _ = Normalizer().normalize(product)
        self.assertEqual(issues, [])
        self.assertEqual(clean["product_id"], "product-1")
        self.assertEqual(clean["gender"], "unisex")
        self.assertEqual(clean["material"], ["cotton"])
        self.assertEqual(clean["weight_gsm"], 240)
        self.assertEqual(clean["fit"], "oversized")
        self.assertEqual(clean["colors"], ["black", "cream"])
        self.assertEqual(clean["sizes"], ["XL", "M"])
        self.assertEqual(clean["price_vnd"], 390000)
        self.assertEqual(clean["brand"], "mua")
        self.assertEqual(clean["variants"][1]["color"], "cream")
        self.assertNotIn("fashion", clean)
        self.assertEqual(Normalizer().normalize(product)[0]["source_item_id"], clean["source_item_id"])

    def test_unknown_values_are_reported_not_guessed(self):
        product = self.product()
        product["attributes"]["gender"] = "unknown"
        product["attributes"]["season"] = None
        product["variants"][0]["options"]["color"] = "midnight-shadow-black"
        clean, issues, _ = Normalizer().normalize(product)
        self.assertIsNone(clean["gender"])
        self.assertEqual(clean["season"], [])
        self.assertEqual(clean["colors"], [])
        self.assertTrue(any(i["field"] == "color" for i in issues))

    def test_material_longest_match_and_blend(self):
        product = self.product()
        product["attributes"]["material"] = "Faux leather + cotton 240gsm"
        clean, _, _ = Normalizer().normalize(product)
        self.assertEqual(set(clean["material"]), {"faux-leather", "cotton"})
        self.assertNotIn("leather", clean["material"])

    def test_category_reference_and_conflict(self):
        category = {"_id": "c1", "slug": "tops-thun"}
        product = self.product(categoryIds=["c1"])
        self.assertEqual(Normalizer([category]).normalize(product)[0]["category_l2"], "t-shirt")
        product["category_l1"] = "bottoms"
        clean, issues, _ = Normalizer([category]).normalize(product)
        self.assertIsNone(clean["category_l1"])
        self.assertTrue(any(i["reason"] == "conflicting_categories" for i in issues))

    def test_footwear_and_accessory_sizes(self):
        self.assertEqual(normalize_value("size", "41", footwear=True), "41")
        self.assertIsNone(normalize_value("size", "41"))
        self.assertEqual(normalize_value("size", "OS"), "ONE_SIZE")
        self.assertEqual(normalize_value("size", "xxl"), "XXL")
        self.assertEqual(normalize_value("color", "Đen"), "black")
        self.assertEqual(normalize_value("gender", "Trẻ em"), "kids")

    def test_taxonomy_covers_all_levels_and_excludes_collections(self):
        paths = category_paths()
        self.assertEqual(paths["running-sneakers"], ("footwear", "sneakers", "running-sneakers"))
        self.assertEqual(paths["hoodie"], ("tops", "hoodie", None))
        self.assertNotIn("collections", paths)

    def test_missing_fields_remain_missing_and_invalid_price_rejected(self):
        clean, issues, _ = Normalizer().normalize({"_id": "p2", "priceFrom": "NaN"})
        self.assertIsNone(clean["gender"])
        self.assertIsNone(clean["brand"])
        self.assertIsNone(clean["price_vnd"])
        self.assertTrue(issues)

    def test_cli_deduplicates_ids_and_writes_auditable_outputs(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            product = self.product()
            (base / "products.jsonl").write_text((json.dumps(product) + "\n") * 2, encoding="utf-8")
            subprocess.run([sys.executable, str(Path(__file__).with_name("normalize_catalog.py")),
                            "--input-dir", str(base), "--output-dir", str(base / "silver")],
                           check=True, capture_output=True, text=True)
            report = json.loads((base / "silver/catalog_quality_report.json").read_text(encoding="utf-8"))
            self.assertEqual(report["counts"]["input_products"], 2)
            self.assertEqual(report["counts"]["output_products"], 1)
            self.assertEqual(report["counts"]["rejected_products"], 1)
            self.assertEqual(len((base / "silver/products_clean.jsonl").read_text(encoding="utf-8").splitlines()), 1)
            taxonomy = [json.loads(line) for line in (base / "silver/categories_clean.jsonl").read_text(encoding="utf-8").splitlines()]
            leaf = next(row for row in taxonomy if row["code"] == "basic-t-shirt")
            self.assertEqual(leaf["path"], "tops/t-shirt/basic-t-shirt")

    def test_editorial_profiles_fill_missing_fields_and_log_provenance(self):
        profiles = {"wool-beanie": {"gender": "unisex", "style": ["casual"], "season": ["winter"], "category": "beanie", "color_aliases": {"Rust": "brown"}}}
        product = {"_id": "p", "slug": "wool-beanie-123", "status": "Active",
                   "attributes": {"material": "Merino wool", "fit": "One size"},
                   "variants": [{"sku": "WB", "options": {"size": "OS", "color": "Rust"}, "price": 280000}]}
        clean, issues, decisions = Normalizer(default_brand="mua", seed_profiles=profiles).normalize(product)
        self.assertEqual(issues, [])
        self.assertEqual(clean["gender"], "unisex")
        self.assertEqual(clean["colors"], ["brown"])
        self.assertEqual(clean["material"], ["wool"])
        self.assertIsNone(clean["weight_gsm"])
        self.assertTrue(any(d["rule"] == "editorial_demo_profile" for d in decisions))
        product["attributes"]["gender"] = "female"
        self.assertEqual(Normalizer(seed_profiles=profiles).normalize(product)[0]["gender"], "female")
        # Profiles do not silently apply to products outside the known seed collection.
        product["slug"] = "another-beanie"
        self.assertEqual(Normalizer(seed_profiles=profiles).normalize(product)[0]["style"], [])


if __name__ == "__main__":
    unittest.main()
