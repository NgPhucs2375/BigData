"""Seed dữ liệu storefront để trình diễn recommendation rõ ràng hơn.

Script idempotent:
- Mở rộng wishlist của tài khoản user@mua.vn theo nhiều style.
- Gán rating/count có chủ đích cho 300 sản phẩm ALS để catalog có thứ hạng
  và điểm số trực quan.

Không xóa orders, carts, reviews hoặc suggestions hiện có.
"""
import datetime
import base64
import hashlib
import random
import secrets
import sys

import pymongo
from bson.decimal128 import Decimal128


CUSTOMER_EMAIL = "user@mua.vn"
SEED = 20261022
NOW = datetime.datetime.utcnow()
DEMO_ACCOUNTS = [
    ("minimal@mua.vn", "Minimal@123", "Minh Tối Giản", ["minimal", "classic", "streetwear", "oversize"]),
    ("streetwear@mua.vn", "Street@123", "Sơn Streetwear", ["streetwear", "oversize", "minimal", "classic"]),
    ("classic@mua.vn", "Classic@123", "Lan Cổ Điển", ["classic", "minimal", "oversize", "streetwear"]),
    ("oversize@mua.vn", "Oversize@123", "Nam Oversize", ["oversize", "streetwear", "classic", "minimal"]),
]

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass


def hash_password(password: str) -> str:
    salt = secrets.token_bytes(16)
    digest = hashlib.pbkdf2_hmac("sha256", password.encode(), salt, 100_000, 32)
    return "pbkdf2:100000:{}:{}".format(
        base64.b64encode(salt).decode(),
        base64.b64encode(digest).decode(),
    )


def main() -> None:
    client = pymongo.MongoClient("mongodb://localhost:27017")
    db = client["ec_store"]
    customer = db.customers.find_one({"email": CUSTOMER_EMAIL})
    if customer is None:
        raise RuntimeError(f"Không tìm thấy tài khoản seed: {CUSTOMER_EMAIL}")

    als_products = list(
        db.products.find(
            {"attributes.itemId": {"$exists": True}, "status": "Active"},
            {"_id": 1, "slug": 1, "attributes": 1, "tags": 1},
        )
    )
    if len(als_products) < 10:
        raise RuntimeError(
            f"Catalog ALS chỉ có {len(als_products)} sản phẩm, cần ít nhất 10"
        )

    # Chọn 12 sản phẩm thuộc nhiều style để profile của user thật đủ tín hiệu.
    preferred_styles = ["minimal", "streetwear", "classic", "oversize"]
    selected_by_style = {}
    for style in preferred_styles:
        selected_by_style[style] = [
            p for p in als_products
            if p.get("attributes", {}).get("style") == style
        ]
    selected = [
        selected_by_style[style][index]
        for index in range(3)
        for style in preferred_styles
        if index < len(selected_by_style[style])
    ]
    wishlist = list(dict.fromkeys(
        [p["slug"] for p in selected] + list(customer.get("wishlist", []))
    ))[:12]
    db.customers.update_one(
        {"_id": customer["_id"]},
        {"$set": {"wishlist": wishlist, "updatedAt": NOW}},
    )

    style_products = {
        style: [
            p["slug"]
            for p in als_products
            if p.get("attributes", {}).get("style") == style
        ]
        for style in ["minimal", "streetwear", "classic", "oversize"]
    }
    for email, password, full_name, style_mix in DEMO_ACCOUNTS:
        # Profile có gu chính, gu phụ và nhóm khám phá, không phải một style tuyệt đối.
        quotas = [6, 3, 2, 1]
        wishlist = [
            style_products[style][index]
            for style, quota in zip(style_mix, quotas)
            for index in range(quota)
            if index < len(style_products[style])
        ]
        password_hash = hash_password(password)
        db.customers.update_one(
            {"email": email},
            {
                "$set": {
                    "fullName": full_name,
                    "passwordHash": password_hash,
                    "role": "Customer",
                    "isLocked": False,
                    "wishlist": wishlist,
                    "updatedAt": NOW,
                    "metadata": {
                        "seeded": True,
                        "demoStyle": style_mix[0],
                        "demoStyleMix": style_mix,
                    },
                },
                "$setOnInsert": {
                    "_id": secrets.token_hex(12),
                    "createdAt": NOW,
                    "schemaVersion": 1,
                    "phone": "",
                    "addresses": [],
                },
            },
            upsert=True,
        )

    # Điểm rating khác nhau theo style và chất lượng ổn định theo item_id.
    # Dữ liệu này chỉ phục vụ demo, không đại diện review thật của khách hàng.
    rng = random.Random(SEED)
    updated = 0
    for product in als_products:
        item_id = product["attributes"].get("itemId", "")
        style = product["attributes"].get("style", "basic")
        base = {
            "minimal": 4.35,
            "classic": 4.25,
            "streetwear": 4.15,
            "oversize": 4.05,
        }.get(style, 4.1)
        jitter = rng.uniform(-0.28, 0.28)
        average = round(max(3.5, min(4.9, base + jitter)), 2)
        count = 18 + (sum(ord(char) for char in item_id) % 83)
        db.products.update_one(
            {"_id": product["_id"]},
            {"$set": {
                "rating": {
                    "average": Decimal128(str(average)),
                    "count": count,
                },
                "updatedAt": NOW,
            }},
        )
        updated += 1

    print(f"wishlist {CUSTOMER_EMAIL}: {len(wishlist)} sản phẩm")
    print(f"demo storefront accounts: {len(DEMO_ACCOUNTS)}")
    print(f"catalog ALS: cập nhật rating cho {updated} sản phẩm")
    client.close()


if __name__ == "__main__":
    main()
