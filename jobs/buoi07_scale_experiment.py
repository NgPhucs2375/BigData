# Buổi 7b — Thí nghiệm scale: tăng kích thước dữ liệu, đo thời gian huấn luyện ALS
#
# Mục đích (mục "Thực nghiệm" của báo cáo): chứng minh pipeline tận dụng được xử lý
# phân tán — thời gian huấn luyện tăng chậm hơn tuyến tính khi dữ liệu tăng.
#
# Chạy (từ thư mục code):
#   docker cp jobs/buoi07_scale_experiment.py spark-master:/jobs/
#   docker exec spark-master /spark/bin/spark-submit --master spark://spark-master:7077 /jobs/buoi07_scale_experiment.py 2>nul
import time

from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS
from pyspark.sql import SparkSession, functions as F

HDFS = "hdfs://namenode:8020"

spark = (
    SparkSession.builder.appName("buoi07-scale")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)

ratings = (
    spark.read.parquet(f"{HDFS}/mua/parquet/ratings")
    .withColumn("user_int", F.substring("user_id", 2, 10).cast("int"))
    .withColumn("item_int", F.substring("item_id", 2, 10).cast("int"))
)
total = ratings.count()
print(f"Tổng số ratings sẵn có: {total:,}")

evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating", predictionCol="prediction")

print("\nsize      | train    | fit(s)  | RMSE")
print("----------+----------+---------+--------")
for size in [100_000, 300_000, total]:
    sample = ratings if size >= total else ratings.orderBy(F.rand(seed=7)).limit(size)
    tr, te = sample.randomSplit([0.8, 0.2], seed=42)
    tr = tr.cache()
    te = te.cache()
    n_train = tr.count()

    als = ALS(
        userCol="user_int", itemCol="item_int", ratingCol="rating",
        rank=5, maxIter=10, regParam=0.1, seed=42, coldStartStrategy="drop",
    )
    t0 = time.time()
    model = als.fit(tr)
    fit_seconds = time.time() - t0
    rmse = evaluator.evaluate(model.transform(te))
    print(f"{size:>9,} | {n_train:>8,} | {fit_seconds:7.1f} | {rmse:.4f}")

spark.stop()
