# Buổi 3 — Sinh bộ dữ liệu đánh giá quy mô lớn có "quy luật ẩn"
#
# Ý tưởng: mỗi user có một "gu" (style ưa thích), mỗi sản phẩm thuộc một style + có "chất lượng" riêng.
# Rating = điểm nền + độ hợp gu + chất lượng SP + tính khó tính của user + nhiễu.
# → Các quy luật này được "giấu" vào dữ liệu; về Buổi 4-5 ALS sẽ học LẠI được chúng.
# User ở đây có ID ổn định (u00001...) — giải quyết luôn bài toán MÙA không có userId.
#
# Chạy:  python buoi03_generate_ratings.py
# (mặc định: 10000 users, 500 items, ~250 rating/user tối đa ≈ 1.9M ratings)
import argparse
import json
import os
import random
import sys
import time

STYLES = ["minimal", "oversize", "streetwear", "classic"]

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--users", type=int, default=10000)
    ap.add_argument("--items", type=int, default=500)
    ap.add_argument("--per-user", type=int, default=250)
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()
    rng = random.Random(args.seed)

    data_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
    os.makedirs(data_dir, exist_ok=True)

    # ---- Catalog: mỗi sản phẩm một style + chất lượng nền ----
    items = []
    for i in range(args.items):
        style = rng.choice(STYLES)
        items.append({
            "item_id": f"i{i:05d}",
            "name": f"{style.title()} Piece {i + 1}",
            "style": style,
            "quality": round(rng.uniform(-0.4, 0.6), 3),
            "price": rng.randrange(2, 15) * 50000,
        })
    with open(os.path.join(data_dir, "items.jsonl"), "w", encoding="utf-8") as f:
        for it in items:
            f.write(json.dumps(it, ensure_ascii=False) + "\n")
    print(f"items.jsonl: {len(items)} sản phẩm")

    # ---- Ratings: 80% lượt rating rơi vào sản phẩm đúng gu user ----
    start = time.time()
    total = 0
    fav_items = {s: [it for it in items if it["style"] == s] for s in STYLES}
    with open(os.path.join(data_dir, "ratings.jsonl"), "w", encoding="utf-8") as out:
        for u in range(args.users):
            user_id = f"u{u:05d}"
            fav = rng.choice(STYLES)
            generosity = rng.uniform(-0.5, 0.5)  # user hào phóng / khó tính
            n = max(10, rng.randrange(args.per_user // 2, args.per_user + 1))
            for _ in range(n):
                it = rng.choice(fav_items[fav]) if rng.random() < 0.8 else rng.choice(items)
                match = 1.0 if it["style"] == fav else 0.0
                score = 3.0 + match * 1.2 + it["quality"] + generosity + rng.uniform(-0.8, 0.8)
                rating = max(1, min(5, round(score)))
                out.write(json.dumps({
                    "user_id": user_id,
                    "item_id": it["item_id"],
                    "rating": rating,
                    "ts": int(time.time()) - rng.randrange(0, 86400 * 90),
                }, ensure_ascii=False) + "\n")
                total += 1
    eval_users = {
        "minimal": "u00001",
        "oversize": "u02500",
        "classic": "u05000",
        "streetwear": "u07500",
        "mixed_holdout": "u09999",
    }
    with open(os.path.join(data_dir, "evaluation_users.json"), "w", encoding="utf-8") as out:
        json.dump({
            "seed": args.seed,
            "users": args.users,
            "items": args.items,
            "ratings": total,
            "split": {"train": 0.7, "validation": 0.1, "test": 0.2},
            "demo_users": eval_users,
            "note": "Spark randomSplit seed=42 is used by the ALS and ranking jobs.",
        }, out, ensure_ascii=False, indent=2)
    print(f"ratings.jsonl: {total:,} ratings cho {args.users} users x {args.items} items "
          f"trong {time.time() - start:.1f}s")


if __name__ == "__main__":
    main()
