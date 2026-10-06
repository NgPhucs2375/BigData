# Buổi 4 — Huấn luyện ALS (lọc cộng tác) trên MovieLens-100K
# MovieLens-100K: 100,000 ratings | 943 users | 1682 phim | bộ dữ liệu "chuẩn ngành" để học ALS
#
# Chạy (từ thư mục code):
#   docker cp jobs/buoi04_train_als.py spark-master:/jobs/
#   docker exec spark-master /spark/bin/spark-submit --master spark://spark-master:7077 /jobs/buoi04_train_als.py
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS
from pyspark.sql import SparkSession, functions as F

HDFS = "hdfs://namenode:8020"

spark = SparkSession.builder.appName("buoi04-als-movielens").getOrCreate()

# ---- 1. Đọc ratings: u.data = user_id \t item_id \t rating \t timestamp ----
ratings = (
    spark.read.csv(f"{HDFS}/movielens/u.data", sep="\t")
    .toDF("user_id", "item_id", "rating", "ts")
    .select("user_id", "item_id", "rating")
    .withColumn("user_id", F.col("user_id").cast("int"))
    .withColumn("item_id", F.col("item_id").cast("int"))
    .withColumn("rating", F.col("rating").cast("double"))
)

# ---- 2. Tên phim: u.item cột 0 = id, cột 1 = title (mã hóa latin1) ----
titles = (
    spark.read.csv(f"{HDFS}/movielens/u.item", sep="|", encoding="latin1")
    .select(F.col("_c0").cast("int").alias("item_id"), F.col("_c1").alias("title"))
)

# ---- 3. Chia train/test 80/20 ----
train, test = ratings.randomSplit([0.8, 0.2], seed=42)
print(f"train: {train.count()} ratings | test: {test.count()} ratings")

# ---- 4. Huấn luyện ALS ----
# rank = số chiều vector "gu" ẩn của user/item (k)
# regParam = hệ số chống overfit (giống lambda trong hồi quy có regularize)
als = ALS(
    userCol="user_id", itemCol="item_id", ratingCol="rating",
    rank=10, maxIter=10, regParam=0.1, seed=42,
    coldStartStrategy="drop",  # bỏ dự đoán cho user/item chưa từng thấy trong train
)
model = als.fit(train)

# ---- 5. Đánh giá trên tập test ----
preds = model.transform(test)
rmse = RegressionEvaluator(
    metricName="rmse", labelCol="rating", predictionCol="prediction"
).evaluate(preds)
print(f"\nRMSE (test) = {rmse:.4f}")
print("(RMSE 0.9 = dự đoán lệch trung bình ~0.9 điểm trên thang 5 điểm)")

# ---- 6. Gợi ý Top-3 cho vài user, kèm tên phim ----
recs = model.recommendForAllUsers(3)
sample = (
    recs.filter(F.col("user_id").isin([1, 196, 880]))
    .select("user_id", F.explode("recommendations").alias("r"))
    .select("user_id", F.col("r.item_id").alias("item_id"),
            F.round(F.col("r.rating"), 2).alias("pred_rating"))
    .join(titles, "item_id")
    .orderBy("user_id", F.desc("pred_rating"))
)
print("\n===== ALS gợi ý cho user 1, 196, 880: =====")
sample.show(truncate=False)

# ---- 7. Đối chiếu: user 196 thật sự thích gì? ----
print("===== User 196 đã chấm 5 sao cho: =====")
(ratings.filter((F.col("user_id") == 196) & (F.col("rating") == 5))
 .join(titles, "item_id")
 .select("title", "rating")
 .show(truncate=False))

spark.stop()
