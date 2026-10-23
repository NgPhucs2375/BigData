# Buổi 6a (bước 1) — Xuất bảng gợi ý Top-10 từ HDFS ra JSON để nạp vào MongoDB
# Chạy (từ thư mục code):
#   docker cp jobs/buoi06_export_suggestions.py spark-master:/jobs/
#   docker exec spark-master /spark/bin/spark-submit --master spark://spark-master:7077 /jobs/buoi06_export_suggestions.py 2>nul
#   docker exec namenode hdfs dfs -getmerge /mua/export/suggestions /data_host/suggestions.json
# -> file kết quả nằm tại BigData/code/data/suggestions.json (nhờ mount /data_host)
#
# Biến môi trường (mặc định = bản benchmark):
#   MUA_SRC — parquet nguồn   (vd live train: /mua/parquet/suggestions_top10_live)
#   MUA_OUT — thư mục JSON ra (vd live train: /mua/export/suggestions_live)
import os

from pyspark.sql import SparkSession, functions as F

HDFS = "hdfs://namenode:8020"
SRC = os.environ.get("MUA_SRC", f"{HDFS}/mua/parquet/suggestions_top10")
OUT = os.environ.get("MUA_OUT", f"{HDFS}/mua/export/suggestions")

spark = (
    SparkSession.builder.appName("buoi06-export")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)

items = (
    spark.read.json(f"{HDFS}/mua/items.jsonl")
    .withColumn("item_int", F.substring("item_id", 2, 10).cast("int"))
)

# suggestions_top10: mỗi dòng = 1 user + mảng 10 (item_int, rating) đã xếp theo điểm giảm dần
# posexplode lấy cả vị trí trong mảng -> rank 1..10
sug = (
    spark.read.parquet(SRC)
    .select(
        F.format_string("u%05d", "user_int").alias("userId"),
        F.posexplode("recommendations").alias("pos", "rec"),
    )
    .select(
        "userId",
        "pos",
        F.col("rec.item_int").alias("item_int"),
        F.round(F.col("rec.rating"), 2).alias("score"),
    )
)

out = (
    sug.join(items, "item_int")
    .select("userId", (F.col("pos") + 1).alias("rank"),
            "item_id", "name", "style", "price", "score")
)

out.coalesce(1).orderBy("userId", "rank").write.mode("overwrite").json(OUT)
print(f"Đã ghi gợi ý từ {SRC} ra {OUT} (jsonl, mỗi dòng 1 gợi ý)")

spark.stop()
