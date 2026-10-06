# Buổi 2 — Export dữ liệu MÙA từ MongoDB ra JSON Lines (mỗi dòng = 1 document)
# Chạy:  pip install pymongo   (nếu chưa có)
#        python buoi02_export_mongo.py
import argparse
import json
import os

import pymongo

OUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
COLLECTIONS = ["products", "categories", "collections", "orders", "reviews"]


def main():
    parser = argparse.ArgumentParser(description="Export MongoDB collections as raw JSONL")
    parser.add_argument("--collections", nargs="+", choices=COLLECTIONS, default=COLLECTIONS)
    args = parser.parse_args()
    os.makedirs(OUT_DIR, exist_ok=True)
    client = pymongo.MongoClient("mongodb://localhost:27017")
    db = client["ec_store"]

    for name in args.collections:
        docs = db[name].find({})
        count = 0
        path = os.path.join(OUT_DIR, f"{name}.jsonl")
        with open(path, "w", encoding="utf-8") as f:
            for doc in docs:
                # default=str: ObjectId/datetime/Decimal128 -> chuỗi
                f.write(json.dumps(doc, default=str, ensure_ascii=False) + "\n")
                count += 1
        print(f"{name}: {count} documents -> {path}")

    client.close()


if __name__ == "__main__":
    main()
