# Buổi 5b — Tính riêng RMSE/MAE của BASELINE (điểm trung bình theo sản phẩm)
# Chạy (từ thư mục code):
#   docker cp jobs/buoi05b_baseline.py spark-master:/jobs/
#   docker exec spark-master /spark/bin/spark-submit --master spark://spark-master:7077 /jobs/buoi05b_baseline.py 2>nul
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql import SparkSession, functions as F

HDFS = "hdfs://namenode:8020"

spark = (
    SparkSession.builder.appName("buoi05b-baseline")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)

ratings = (
    spark.read.parquet(f"{HDFS}/mua/parquet/ratings")
    .withColumn("item_int", F.substring("item_id", 2, 10).cast("int"))
)

train, test = ratings.randomSplit([0.8, 0.2], seed=42)

# Baseline "gợi ý phổ biến": đoán điểm của 1 sản phẩm = trung bình rating nó nhận được trên train.
# Sản phẩm lạ chưa có trong train -> dùng trung bình toàn cục.
means = train.groupBy("item_int").agg(F.avg("rating").alias("item_mean"))
global_mean = train.agg(F.avg("rating")).first()[0]
pred = test.join(means, "item_int", "left").na.fill({"item_mean": global_mean})

rmse = RegressionEvaluator(metricName="rmse", labelCol="rating",
                           predictionCol="item_mean").evaluate(pred)
mae = RegressionEvaluator(metricName="mae", labelCol="rating",
                          predictionCol="item_mean").evaluate(pred)
print(f"BASELINE (item mean) -> RMSE={rmse:.4f} | MAE={mae:.4f}")

spark.stop()
