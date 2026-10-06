# Buổi 5 — Huấn luyện ALS trên dữ liệu thời trang MÙA (748K ratings) + đánh giá
#
# Có 2 điều đặc biệt ở buổi này:
# 1. So sánh ALS với BASELINE "điểm trung bình của sản phẩm" — model phải hơn baseline mới đáng tiền.
# 2. Data mô phỏng có "đáp án" giấu sẵn (gu của từng user) → kiểm chứng được ALS có học đúng gu không.
#
# Chạy (từ thư mục code):
#   docker cp jobs/buoi05_train_als_mua.py spark-master:/jobs/
#   docker exec spark-master /spark/bin/spark-submit --master spark://spark-master:7077 /jobs/buoi05_train_als_mua.py
# Lưu ý: 3 lần fit ALS, tổng chạy vài phút — mở http://localhost:8080 xem tiến độ cho vui.
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS
from pyspark.sql import SparkSession, functions as F

HDFS = "hdfs://namenode:8020"

spark = (
    SparkSession.builder.appName("buoi05-als-mua")
    .config("spark.sql.shuffle.partitions", "8")  # dữ liệu nhỏ: 200 partition mặc định là lãng phí
    .getOrCreate()
)

# ---- 1. Nạp dữ liệu; ALS yêu cầu user/item là SỐ nên cast "u00001" -> 1, "i00042" -> 42 ----
ratings = (
    spark.read.parquet(f"{HDFS}/mua/parquet/ratings")
    .withColumn("user_int", F.substring("user_id", 2, 10).cast("int"))
    .withColumn("item_int", F.substring("item_id", 2, 10).cast("int"))
)
items = (
    spark.read.json(f"{HDFS}/mua/items.jsonl")
    .withColumn("item_int", F.substring("item_id", 2, 10).cast("int"))
)
print(f"ratings: {ratings.count():,} | users: {ratings.select('user_int').distinct().count()} | "
      f"items: {ratings.select('item_int').distinct().count()}")

# ---- 2. Chia train/test 80/20 ----
train, test = ratings.randomSplit([0.8, 0.2], seed=42)
train.cache()
test.cache()
print(f"train: {train.count():,} | test: {test.count():,}")

# ---- 3. Baseline: "gợi ý phổ biến" — đoán điểm = trung bình của từng sản phẩm trên train ----
means = train.groupBy("item_int").agg(F.avg("rating").alias("item_mean"))
global_mean = train.agg(F.avg("rating")).first()[0]
baseline = test.join(means, "item_int", "left").na.fill({"item_mean": global_mean})
rmse_baseline = RegressionEvaluator(
    metricName="rmse", labelCol="rating", predictionCol="item_mean"
).evaluate(baseline)
print(f"\nRMSE baseline (item mean) = {rmse_baseline:.4f}  <- ALS phải nhỏ hơn con số này")

# ---- 4. Huấn luyện ALS với 3 bộ tham số, chọn cái tốt nhất ----
def fit_eval(rank, reg):
    als = ALS(
        userCol="user_int", itemCol="item_int", ratingCol="rating",
        rank=rank, maxIter=10, regParam=reg, seed=42,
        coldStartStrategy="drop",
    )
    model = als.fit(train)
    preds = model.transform(test)
    rmse = RegressionEvaluator(metricName="rmse", labelCol="rating",
                               predictionCol="prediction").evaluate(preds)
    mae = RegressionEvaluator(metricName="mae", labelCol="rating",
                              predictionCol="prediction").evaluate(preds)
    print(f"ALS  rank={rank:3d} regParam={reg} -> RMSE={rmse:.4f} | MAE={mae:.4f}")
    return model, rmse

best_model, best_rmse = None, float("inf")
for rank, reg in [(5, 0.1), (10, 0.1), (20, 0.1)]:
    model, rmse = fit_eval(rank, reg)
    if rmse < best_rmse:
        best_model, best_rmse = model, rmse
print(f"\nConfig tốt nhất có RMSE = {best_rmse:.4f}")

# ---- 5. Kiểm chứng "học đúng gu": gợi ý Top-5 cho 2 user + đối chiếu style user đã thích ----
SAMPLE_USERS = [1, 2500]
recs = best_model.recommendForAllUsers(5)
(
    recs.filter(F.col("user_int").isin(SAMPLE_USERS))
    .select("user_int", F.explode("recommendations").alias("r"))
    .select(F.col("user_int").alias("user"), F.col("r.item_int").alias("item_int"),
            F.round(F.col("r.rating"), 2).alias("pred"))
    .join(items, "item_int")
    .select("user", "name", "style", "pred")
    .orderBy("user", F.desc("pred"))
).show(truncate=False)

for uid in SAMPLE_USERS:
    print(f"--- User u{uid:05d} đã chấm 4-5 sao cho style nào? ---")
    (train.filter((F.col("user_int") == uid) & (F.col("rating") >= 4))
     .join(items, "item_int")
     .groupBy("style").count()
     .orderBy(F.desc("count"))).show()

# ---- 6. Lưu model + bảng gợi ý Top-10 (Buổi 6 sẽ đưa vào app MÙA) ----
best_model.write().overwrite().save(f"{HDFS}/models/als_mua")
(best_model.recommendForAllUsers(10)
 .write.mode("overwrite").parquet(f"{HDFS}/mua/parquet/suggestions_top10"))
print("Đã lưu model -> hdfs:///models/als_mua và bảng gợi ý -> hdfs:///mua/parquet/suggestions_top10")

spark.stop()
