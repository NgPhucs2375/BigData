# Buổi 7a — Đánh giá ranking: Precision@K / Recall@K (ALS vs baseline top-popular)
#
# Ý nghĩa: RMSE đo "đoán ĐIỂM có khớp không", còn Precision@K / Recall@K đo
# "DANH SÁCH gợi ý có trúng sở thích thật không" — đúng tiêu chí của đề tài 15.
#   Relevant  = item user thật sự thích (rating >= 4 trong tập test)
#   Hits@K    = số item trong Top-K gợi ý trùng với relevant
#   P@K       = trung bình hits/K ;  R@K = trung bình hits/số relevant của user đó
#
# Chạy (từ thư mục code):
#   docker cp jobs/buoi07_metrics_ranking.py spark-master:/jobs/
#   docker exec spark-master /spark/bin/spark-submit --master spark://spark-master:7077 /jobs/buoi07_metrics_ranking.py 2>nul
from pyspark.ml.recommendation import ALSModel
from pyspark.sql import SparkSession, functions as F

HDFS = "hdfs://namenode:8020"
K = 10
GOOD_RATING = 4.0  # ngưỡng "user thật sự thích"

spark = (
    SparkSession.builder.appName("buoi07-ranking-metrics")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)

ratings = (
    spark.read.parquet(f"{HDFS}/mua/parquet/ratings")
    .withColumn("user_int", F.substring("user_id", 2, 10).cast("int"))
    .withColumn("item_int", F.substring("item_id", 2, 10).cast("int"))
)

# Cùng seed 42, cùng tỉ lệ 70/10/20 với Buổi 5 -> test set giống hệt, số liệu nhất quán
train, _valid, test = ratings.randomSplit([0.7, 0.1, 0.2], seed=42)

relevant = (
    test.filter(F.col("rating") >= GOOD_RATING)
    .select("user_int", "item_int")
    .distinct()
)

# Người dùng được đánh giá: có ít nhất 1 item relevant trong test
user_rel = relevant.groupBy("user_int").agg(F.count("*").alias("n_rel"))


def ranking_metrics(rec_pairs, eval_users, label):
    """rec_pairs: DataFrame (user_int, item_int) = danh sách Top-K đã gợi ý."""
    hits = (
        rec_pairs.join(relevant, ["user_int", "item_int"], "inner")
        .groupBy("user_int").agg(F.count("*").alias("hits"))
    )
    joined = eval_users.join(hits, "user_int", "left").na.fill({"hits": 0})
    row = joined.agg(
        F.round(F.avg(F.col("hits") / F.lit(K)), 4).alias("precision"),
        F.round(F.avg(F.col("hits") / F.col("n_rel")), 4).alias("recall"),
        F.count("*").alias("users"),
    ).first()
    print(f"{label}: Precision@{K} = {row['precision']} | Recall@{K} = {row['recall']} "
          f"(| users đánh giá = {row['users']})")
    return row


# ---- Danh sách user cần đánh giá (có relevant trong test) ----
eval_users = user_rel  # baseline dùng chung; ALS sẽ left-join thêm

# ---- 1) Baseline "gợi ý phổ biến": K item được rate NHIỀU nhất trên train ----
popular = (
    train.groupBy("item_int").agg(F.count("*").alias("n"))
    .orderBy(F.desc("n")).limit(K)
    .select("item_int")
)
baseline_pairs = user_rel.select("user_int").crossJoin(popular)
ranking_metrics(baseline_pairs, eval_users, f"BASELINE top-popular")

# ---- 2) ALS: model tốt nhất Buổi 5 (rank=5) đã lưu trên HDFS ----
model = ALSModel.load(f"{HDFS}/models/als_mua")
als_pairs = (
    model.recommendForAllUsers(K)
    .select("user_int", F.explode("recommendations").alias("r"))
    .select("user_int", F.col("r.item_int").alias("item_int"))
    .distinct()
)
ranking_metrics(als_pairs, eval_users, "ALS (rank=5)      ")

spark.stop()
