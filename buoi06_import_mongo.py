# Buổi 6a (bước 2) — Nạp vào MongoDB ec_store:
#   1) 300 sản phẩm mô phỏng vào collection `products` (schema khớp app MÙA: camelCase, Decimal128, Id 24-hex)
#   2) Bảng gợi ý Top-10 vào collection mới `suggestions`
# Chạy (máy host, MongoDB của MÙA đang chạy):  python buoi06_import_mongo.py
# Chạy lại an toàn (idempotent): sản phẩm chỉ import 1 lần, suggestions được ghi đè.
import datetime
import json
import os
import uuid

import pymongo
from bson.decimal128 import Decimal128

BASE = os.path.dirname(os.path.abspath(__file__))
NOW = datetime.datetime.utcnow()


def slugify(s: str) -> str:
    return s.lower().replace(" ", "-").replace("&", "and")


def import_products(db) -> dict:
    """Import 300 sản phẩm mô phỏng; trả về map item_id ('i00001') -> _id của product."""
    item_to_id = {}
    existing = db.products.find(
        {"attributes.itemId": {"$exists": True}}, {"_id": 1, "name": 1, "attributes.itemId": 1}
    )
    for p in existing:
        item_to_id[p["attributes"]["itemId"]] = p["_id"]
    if item_to_id:
        # Sản phẩm đã import trước đó -> chỉ làm mới ảnh về tông grayscale (idempotent)
        for item_id, pid in item_to_id.items():
            db.products.update_one(
                {"_id": pid},
                {"$set": {"media": [{
                    "kind": "image",
                    "url": f"https://picsum.photos/seed/{item_id}/600/800?grayscale",
                    "alt": "",
                    "position": 0,
                }]}},
            )
        print(f"products: đã cập nhật ảnh grayscale cho {len(item_to_id)} sản phẩm")
        return item_to_id

    docs = []
    with open(os.path.join(BASE, "data", "items.jsonl"), encoding="utf-8") as f:
        for line in f:
            it = json.loads(line)
            pid = uuid.uuid4().hex  # 24-hex string, khớp DocumentBase.Id của app
            item_to_id[it["item_id"]] = pid
            docs.append({
                "_id": pid,
                "name": it["name"],
                "slug": slugify(it["name"]),
                "description": f"Sản phẩm mẫu của pipeline gợi ý — phong cách {it['style']}.",
                "status": "Active",
                "categoryIds": [],
                "tags": [it["style"]],
                "attributes": {"style": it["style"], "itemId": it["item_id"]},
                "variants": [{
                    "sku": f"{it['item_id'].upper()}-STD",
                    "name": "Standard",
                    "price": Decimal128(str(it["price"])),
                    "options": {},
                    "stock": 500,
                    "imageUrl": None,
                }],
                "media": [{
                    "kind": "image",
                    "url": f"https://picsum.photos/seed/{it['item_id']}/600/800?grayscale",
                    "alt": it["name"],
                    "position": 0,
                }],
                "priceFrom": Decimal128(str(it["price"])),
                "priceTo": Decimal128(str(it["price"])),
                "rating": {"average": Decimal128("0"), "count": 0},
                "publishedAt": NOW,
                "createdAt": NOW,
                "updatedAt": NOW,
                "schemaVersion": 1,
            })
    db.products.insert_many(docs)
    print(f"products: đã import {len(docs)} sản phẩm mô phỏng")
    return item_to_id


def import_suggestions(db, item_to_id: dict) -> None:
    """Gom các dòng gợi ý (mỗi dòng 1 gợi ý) thành 1 document mỗi user."""
    by_user: dict = {}
    skipped = 0
    with open(os.path.join(BASE, "data", "suggestions.json"), encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            row = json.loads(line)
            pid = item_to_id.get(row["item_id"])
            if pid is None:
                skipped += 1
                continue
            by_user.setdefault(row["userId"], []).append({
                "productId": pid,
                "slug": slugify(row["name"]),
                "name": row["name"],
                "style": row["style"],
                "imageUrl": f"https://picsum.photos/seed/{row['item_id']}/600/800?grayscale",
                "price": Decimal128(str(row["price"])),
                "score": float(row["score"]),
                "rank": int(row["rank"]),
            })

    db.suggestions.delete_many({})
    docs = [{
        "userId": user,
        "items": sorted(items, key=lambda x: x["rank"]),
        "updatedAt": NOW,
        "schemaVersion": 1,
    } for user, items in by_user.items()]
    db.suggestions.insert_many(docs)
    db.suggestions.create_index([("userId", 1)], unique=True)
    note = f" (bỏ qua {skipped} dòng thiếu mapping)" if skipped else ""
    print(f"suggestions: đã import Top-10 cho {len(docs)} users{note}")


def main():
    client = pymongo.MongoClient("mongodb://localhost:27017")
    db = client["ec_store"]
    item_to_id = import_products(db)
    import_suggestions(db, item_to_id)
    client.close()


if __name__ == "__main__":
    main()
