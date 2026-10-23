# Áp model live-train vào cửa hàng MÙA — đóng vòng lặp: Train -> Serve -> UI thấy.
#
# Quy trình (1 lệnh, an toàn, idempotent):
#   1. BƯỚC 1 (đã chạy bởi dashboard): buoi08_live_train.py ghi
#      hdfs:///mua/parquet/suggestions_top10_live (Top-10 từ model VỪA train).
#   2. Tool này: export parquet -> JSON trên HDFS (tái sử dụng buoi06_export với env).
#   3. getmerge về /data_host/suggestions_live.json (mount -> BigData/code/data).
#   4. Nạp vào MongoDB ec_store.suggestions (CHỈ suggestions, không đụng products),
#      mỗi document 1 user = Top-10 đã xếp theo rank.
#   5. Trả về JSON so sánh trước/sau cho các user demo (u00001, u02500, ...).
#
# Chạy (máy host, cần MongoDB + docker):  python tools/apply_suggestions.py
# JSON kết quả in ra stdout (đối tượng JSON cuối cùng).
import datetime
import json
import os
import subprocess
import sys

import pymongo

# Windows console mặc định cp1252 -> JSON có tiếng Việt sẽ ném UnicodeEncodeError.
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

BASE = os.path.dirname(os.path.abspath(__file__))
CODE_DIR = os.path.dirname(BASE)          # BigData/code
LIVE_SRC = "hdfs://namenode:8020/mua/parquet/suggestions_top10_live"
LIVE_OUT = "hdfs://namenode:8020/mua/export/suggestions_live"
HOST_FILE = os.path.join(CODE_DIR, "data", "suggestions_live.json")
DEMO_USERS = ["u00001", "u02500", "u00002", "u00003"]


def log(msg: str) -> None:
    print(msg, file=sys.stderr, flush=True)


def run(cmd: list, timeout: int = 300) -> None:
    log("$ " + " ".join(cmd))
    proc = subprocess.run(
        cmd, capture_output=True, text=True,
        encoding="utf-8", errors="replace", timeout=timeout,
    )
    if proc.stdout:
        log(proc.stdout[-2000:])
    if proc.returncode != 0:
        raise RuntimeError(
            f"lệnh thất bại (exit {proc.returncode}): {cmd[0]}... {proc.stderr[-500:]}"
        )


def snapshot(db) -> dict:
    """Top-10 hiện tại của các user demo từ MongoDB (trước khi ghi đè)."""
    out = {}
    for doc in db.suggestions.find({"userId": {"$in": DEMO_USERS}}):
        out[doc["userId"]] = [
            {"rank": i["rank"], "name": i["name"], "style": i["style"], "score": float(i["score"])}
            for i in sorted(doc.get("items", []), key=lambda x: x["rank"])
        ]
    return out


def item_id_map(db) -> dict:
    """Map attributes.itemId ('i00001') -> Mongo product _id (24-hex)."""
    return {
        p["attributes"]["itemId"]: p["_id"]
        for p in db.products.find({"attributes.itemId": {"$exists": True}}, {"_id": 1, "attributes.itemId": 1})
    }


def load_suggestions_file(path: str) -> dict:
    """Đọc JSONL (mỗi dòng 1 gợi ý) -> {userId: [item...]} đã sort theo rank."""
    by_user: dict = {}
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            row = json.loads(line)
            by_user.setdefault(row["userId"], []).append(row)
    for rows in by_user.values():
        rows.sort(key=lambda r: int(r["rank"]))
    return by_user


def main() -> int:
    now = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
    client = pymongo.MongoClient("mongodb://localhost:27017")
    db = client["ec_store"]

    before = snapshot(db)
    log(f"đã chụp Top-10 trước khi áp cho {len(before)} user demo")

    if not os.path.exists(os.path.join(CODE_DIR, "jobs", "buoi06_export_suggestions.py")):
        raise RuntimeError("không tìm thấy jobs/buoi06_export_suggestions.py")

    # Bước export: tái sử dụng job buoi06 với env trỏ sang nguồn _live.
    # BẮT BUỘC 512m: worker chỉ 768MB, executor mặc định 1024MB sẽ kẹt app ở WAITING.
    run([
        "docker", "exec", "-e", f"MUA_SRC={LIVE_SRC}", "-e", f"MUA_OUT={LIVE_OUT}",
        "spark-master", "/spark/bin/spark-submit",
        "--master", "spark://spark-master:7077",
        "--executor-memory", "512m", "--driver-memory", "512m",
        "/jobs/buoi06_export_suggestions.py",
    ], timeout=360)

    # getmerge JSON về mount /data_host = BigData/code/data.
    # Bản Hadoop này không hỗ trợ getmerge -f -> xoá file đích trước rồi merge.
    run(["docker", "exec", "namenode", "sh", "-c",
         "rm -f /data_host/suggestions_live.json"], timeout=30)
    run([
        "docker", "exec", "namenode", "hdfs", "dfs", "-getmerge",
        LIVE_OUT, "/data_host/suggestions_live.json",
    ], timeout=120)

    by_user = load_suggestions_file(HOST_FILE)
    mapping = item_id_map(db)
    skipped = 0
    docs = []
    for user, rows in by_user.items():
        items = []
        for r in rows:
            pid = mapping.get(r["item_id"])
            if pid is None:
                skipped += 1
                continue
            items.append({
                "productId": pid,
                "slug": str(r["name"]).lower().replace(" ", "-").replace("&", "and"),
                "name": r["name"],
                "style": r["style"],
                "imageUrl": f"https://picsum.photos/seed/{r['item_id']}/600/800?grayscale",
                "price": r["price"],
                "score": float(r["score"]),
                "rank": int(r["rank"]),
            })
        if items:
            docs.append({"userId": user, "items": items, "updatedAt": now, "schemaVersion": 1})

    if not docs:
        raise RuntimeError("suggestions_live.json rỗng hoặc thiếu mapping sản phẩm")

    db.suggestions.delete_many({})
    db.suggestions.insert_many(docs)
    db.suggestions.create_index([("userId", 1)], unique=True)
    log(f"đã ghi Top-10 cho {len(docs)} users (bỏ qua {skipped} dòng thiếu mapping)")

    after = snapshot(db)
    sample = []
    changed_demo = 0
    for user in DEMO_USERS:
        b = before.get(user, [])
        a = after.get(user, [])
        b_ids = [x["name"] for x in b]
        a_ids = [x["name"] for x in a]
        changed = sum(1 for x, y in zip(b_ids, a_ids) if x != y) + abs(len(b_ids) - len(a_ids))
        if changed:
            changed_demo += 1
        sample.append({
            "user": user,
            "changed": changed,
            "before": b[:5],
            "after": a[:5],
        })

    result = {
        "ok": True,
        "applied_at": now.strftime("%Y-%m-%d %H:%M:%S UTC"),
        "users_updated": len(docs),
        "skipped_rows": skipped,
        "demo_users_changed": changed_demo,
        "sample": sample,
        "store_url": "/recommendations",
    }
    print(json.dumps(result, ensure_ascii=False))
    client.close()
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:  # mọi lỗi đều phải về tay UI được
        print(json.dumps({"ok": False, "error": str(exc)}, ensure_ascii=False))
        sys.exit(1)
