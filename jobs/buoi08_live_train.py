# Buổi 8 — LIVE TRAIN: huấn luyện ALS trước mắt hội đồng và in số liệu ra mắt họ xem
#
# Khác với buoi05 (chạy nền khi chuẩn bị), job này chạy TRONG LÚC THUYẾT TRÌNH:
#   1. Nạp Parquet 748K ratings từ HDFS, chia 70/10/20 seed=42 (cùng protocol buoi05/07).
#   2. Baseline item-mean -> RMSE trên test.
#   3. Train ALS (rank nhận argv[1], mặc định 5) MỘT lần -> RMSE/MAE trên test.
#   4. Precision@10/Recall@10 (ALS vs top-popular) — cùng định nghĩa với buoi07.
#   5. Case study 2 user giấu gu (u00001 oversize / u02500 streetwear).
#   6. In phân bố task trên từng executor — bằng chứng chạy PHÂN TÁN trên 2 worker.
#   7. Ghi Top-10 cho mọi user ra /mua/parquet/suggestions_top10_live (KHÔNG đè bản
#      benchmark /mua/parquet/suggestions_top10) — bước "Áp model" của dashboard sẽ
#      đọc path này nạp vào MongoDB để cửa hàng MÙA hiển thị ngay.
#
# Mọi dòng tiến trình/metric in dạng "LIVE|{json}" để tools/stream_dashboard_server.py
# parse đưa lên dashboard http://localhost:8090 (UI: trang /bigdata — Live train lab).
#
# Rank nhận qua argv[1] (mặc định 5): dashboard server truyền 5/10/20 — rank khác
# thì Top-10 sau khi áp model đổi thấy được (rank=5 trùng benchmark).
#
# Chạy (từ thư mục code) — BẮT BUỘC 512m, worker chỉ có 768MB:
#   docker exec spark-master /spark/bin/spark-submit --master spark://spark-master:7077 \
#     --executor-memory 512m --driver-memory 512m /jobs/buoi08_live_train.py [rank]
import json
import sys
import time
import urllib.request

from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS
from pyspark.sql import SparkSession, functions as F

HDFS = "hdfs://namenode:8020"
K = 10
GOOD_RATING = 4.0
SAMPLE_USERS = [1, 2500]
T0 = time.time()

# rank nhận từ argv (dashboard server truyền): 5 = khớp bảng benchmark;
# 10/20 = model khác hẳn -> Top-10 sau khi "Áp model" đổi thấy được.
try:
    RANK = int(sys.argv[1])
except (IndexError, ValueError):
    RANK = 5


def emit(**payload):
    """In một dòng JSON gắn cờ LIVE — server bắt chính dòng này."""
    print("LIVE|" + json.dumps(payload, ensure_ascii=False), flush=True)


def stage(name, detail=""):
    emit(kind="stage", stage=name, detail=detail, elapsed=round(time.time() - T0, 1))
    print(f"=== [{time.time() - T0:6.1f}s] STAGE {name} {detail}", flush=True)


def executor_task_counts(app_id):
    """Số task từng executor.

    Spark 3.3 chưa có StatusTracker.getExecutorInfos (API mới từ 3.5) nên đọc
    Spark REST API trên driver UI (port 4040) — cùng nguồn với trang Executors.
    """
    import socket

    urls = [
        f"http://127.0.0.1:4040/api/v1/applications/{app_id}/executors",
        f"http://{socket.gethostname()}:4040/api/v1/applications/{app_id}/executors",
    ]
    last_error = None
    for url in urls:
        try:
            with urllib.request.urlopen(url, timeout=20) as response:
                execs = json.load(response)
            return [
                {"id": str(e.get("id", "?")), "tasks": int(e.get("totalTasks", 0) or 0),
                 "driver": str(e.get("id", "")) == "driver"}
                for e in execs
            ]
        except Exception as exc:
            last_error = exc
    print(f"executor stats không lấy được qua REST: {last_error}", flush=True)
    return []


try:
    # utf-8: container không set LANG -> Python dùng ASCII, dòng tiếng Việt thành "????".
    sys.stdout.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
except Exception:
    pass

stage("load", "đọc Parquet 748K ratings từ HDFS")
spark = (
    SparkSession.builder.appName("buoi08-live-train")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)
sc = spark.sparkContext
sc.setLogLevel("WARN")

ratings = (
    spark.read.parquet(f"{HDFS}/mua/parquet/ratings")
    .withColumn("user_int", F.substring("user_id", 2, 10).cast("int"))
    .withColumn("item_int", F.substring("item_id", 2, 10).cast("int"))
)
items = (
    spark.read.json(f"{HDFS}/mua/items.jsonl")
    .withColumn("item_int", F.substring("item_id", 2, 10).cast("int"))
)

stage("split", "randomSplit 70/10/20 seed=42")
train, valid, test = ratings.randomSplit([0.7, 0.1, 0.2], seed=42)
train.cache()
valid.cache()
test.cache()
n_train, n_valid, n_test = train.count(), valid.count(), test.count()
emit(kind="metric", name="rows", train=n_train, valid=n_valid, test=n_test,
     total=n_train + n_valid + n_test)

stage("baseline", "điểm TB từng sản phẩm — mốc ALS phải đánh bại")
means = train.groupBy("item_int").agg(F.avg("rating").alias("item_mean"))
global_mean = train.agg(F.avg("rating")).first()[0]
baseline_preds = test.join(means, "item_int", "left").na.fill({"item_mean": global_mean})
baseline_rmse = RegressionEvaluator(
    metricName="rmse", labelCol="rating", predictionCol="item_mean"
).evaluate(baseline_preds)
emit(kind="metric", name="baseline_rmse", value=round(baseline_rmse, 4))

stage("fit", f"ALS rank={RANK} · maxIter=10 · regParam=0.1 — chia việc xuống 2 worker")
fit_t0 = time.time()
als = ALS(
    userCol="user_int", itemCol="item_int", ratingCol="rating",
    rank=RANK, maxIter=10, regParam=0.1, seed=42, coldStartStrategy="drop",
)
model = als.fit(train)
fit_seconds = round(time.time() - fit_t0, 1)
emit(kind="metric", name="fit_seconds", value=fit_seconds)

stage("evaluate", "RMSE/MAE trên test — tập chỉ mở 1 lần cuối")
valid_rmse = RegressionEvaluator(
    metricName="rmse", labelCol="rating", predictionCol="prediction"
).evaluate(model.transform(valid))
test_preds = model.transform(test)
test_rmse = RegressionEvaluator(
    metricName="rmse", labelCol="rating", predictionCol="prediction"
).evaluate(test_preds)
test_mae = RegressionEvaluator(
    metricName="mae", labelCol="rating", predictionCol="prediction"
).evaluate(test_preds)
emit(kind="metric", name="als_valid_rmse", value=round(valid_rmse, 4))
emit(kind="metric", name="als_rmse", value=round(test_rmse, 4))
emit(kind="metric", name="als_mae", value=round(test_mae, 4))

stage("ranking", f"Precision@{K}/Recall@{K} — relevant = rating >= {GOOD_RATING:.0f} trên test")
relevant = (
    test.filter(F.col("rating") >= GOOD_RATING)
    .select("user_int", "item_int")
    .distinct()
)
user_rel = relevant.groupBy("user_int").agg(F.count("*").alias("n_rel"))


def ranking_metrics(rec_pairs, eval_users):
    hits = (
        rec_pairs.join(relevant, ["user_int", "item_int"], "inner")
        .groupBy("user_int").agg(F.count("*").alias("hits"))
    )
    joined = eval_users.join(hits, "user_int", "left").na.fill({"hits": 0})
    row = joined.agg(
        F.avg(F.col("hits") / F.lit(K)).alias("precision"),
        F.avg(F.col("hits") / F.col("n_rel")).alias("recall"),
        F.count("*").alias("users"),
    ).first()
    return round(row["precision"], 4), round(row["recall"], 4), row["users"]


popular = (
    train.groupBy("item_int").agg(F.count("*").alias("n"))
    .orderBy(F.desc("n")).limit(K)
    .select("item_int")
)
baseline_pairs = user_rel.select("user_int").crossJoin(popular)
b_p, b_r, n_users = ranking_metrics(baseline_pairs, user_rel)
emit(kind="metric", name="baseline_p10", value=b_p)
emit(kind="metric", name="baseline_r10", value=b_r)

als_recs = model.recommendForAllUsers(K)
# Cache: recommendForAllUsers là lazy — nếu không cache thì evaluate + case_study +
# serving mỗi bước tính lại từ đầu (rank cao thì vượt watchdog 90s).
als_recs.cache()
als_pairs = (
    als_recs.select("user_int", F.explode("recommendations").alias("r"))
    .select("user_int", F.col("r.item_int").alias("item_int"))
    .distinct()
)
a_p, a_r, _ = ranking_metrics(als_pairs, user_rel)
emit(kind="metric", name="als_p10", value=a_p)
emit(kind="metric", name="als_r10", value=a_r)
emit(kind="metric", name="eval_users", value=n_users)

stage("case_study", "mở gu ẩn của u00001 / u02500 — model không hề biết chữ 'oversize'")
for uid in SAMPLE_USERS:
    top_styles = (
        als_pairs.filter(F.col("user_int") == uid)
        .join(items, "item_int")
        .groupBy("style").count()
        .orderBy(F.desc("count"))
        .limit(3)
    ).collect()
    truth = (
        train.filter((F.col("user_int") == uid) & (F.col("rating") >= 4))
        .join(items, "item_int")
        .groupBy("style").count()
        .orderBy(F.desc("count"))
        .limit(3)
    ).collect()
    emit(kind="case_study", user=f"u{uid:05d}",
         truth={r["style"]: r["count"] for r in truth},
         recs={r["style"]: r["count"] for r in top_styles})

stage("executors", "phân bố task trên các executor — chứng minh phân tán")
executors = executor_task_counts(sc.applicationId)
emit(kind="executors", executors=executors)

# Serving: model vừa train phải SỐNG SÓT ra cửa hàng — ghi Top-10 cho mọi user
# xuống HDFS (path mới _live, không đè bản benchmark). Bước "Áp model" sẽ đọc
# path này để nạp vào MongoDB — vòng lặp train -> serve -> UI thấy ngay.
stage("serving", f"recommendForAllUsers({K}) → hdfs:///mua/parquet/suggestions_top10_live")
serving_rows = als_recs.count()
als_recs.write.mode("overwrite").parquet(f"{HDFS}/mua/parquet/suggestions_top10_live")
emit(kind="metric", name="serving_users", value=serving_rows)

total_seconds = round(time.time() - T0, 1)
emit(kind="done",
     metrics={
         "baseline_rmse": round(baseline_rmse, 4),
         "als_rmse": round(test_rmse, 4),
         "als_mae": round(test_mae, 4),
         "als_valid_rmse": round(valid_rmse, 4),
         "baseline_p10": b_p,
         "als_p10": a_p,
         "baseline_r10": b_r,
         "als_r10": a_r,
         "fit_seconds": fit_seconds,
         "total_seconds": total_seconds,
         "rows": {"train": n_train, "valid": n_valid, "test": n_test},
         "workers": 2,
         "rank": RANK,
         "serving_users": serving_rows,
     })
print(f"=== LIVE TRAIN DONE trong {total_seconds}s", flush=True)
spark.stop()
