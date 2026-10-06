# Buổi 3 — EDA bộ ratings + chuyển JSON -> Parquet trên HDFS
# Chạy (từ thư mục code):
#   docker cp jobs/buoi03_ratings_eda.py spark-master:/jobs/
#   docker exec spark-master /spark/bin/spark-submit --master spark://spark-master:7077 /jobs/buoi03_ratings_eda.py
from pyspark.sql import SparkSession, functions as F

HDFS = "hdfs://namenode:8020"

spark = SparkSession.builder.appName("buoi03-ratings").getOrCreate()

ratings = spark.read.json(f"{HDFS}/mua/ratings.jsonl")
items = spark.read.json(f"{HDFS}/mua/items.jsonl")

print("Tổng số ratings:", ratings.count())
print("Số user khác nhau:", ratings.select("user_id").distinct().count())
print("Số item khác nhau:", ratings.select("item_id").distinct().count())

print("\nPhân phối điểm rating (1..5):")
ratings.groupBy("rating").count().orderBy("rating").show()

print("\nĐiểm trung bình theo style — JOIN ratings x items:")
(ratings.join(items, "item_id")
        .groupBy("style")
        .agg(F.round(F.avg("rating"), 2).alias("avg_rating"),
             F.count("*").alias("n_ratings"))
        .orderBy(F.desc("avg_rating"))).show()

# ---- Chuyển sang Parquet (format cột, nén) rồi đọc lại ----
ratings.write.mode("overwrite").parquet(f"{HDFS}/mua/parquet/ratings")
pq = spark.read.parquet(f"{HDFS}/mua/parquet/ratings")
print("Parquet — đọc lại, count =", pq.count())
pq.printSchema()

spark.stop()
