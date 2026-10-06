# Buổi 2 — Đọc dữ liệu MÙA từ HDFS bằng Spark DataFrame
# Chạy (từ thư mục code):
#   docker cp jobs/buoi02_read_hdfs.py spark-master:/jobs/
#   docker exec spark-master spark-submit --master spark://spark-master:7077 /jobs/buoi02_read_hdfs.py
from pyspark.sql import SparkSession

HDFS = "hdfs://namenode:8020"

spark = SparkSession.builder.appName("buoi02-doc-data-mua").getOrCreate()

for name in ["products", "orders", "reviews"]:
    df = spark.read.json(f"{HDFS}/mua/{name}.jsonl")
    print(f"\n===== {name}: {df.count()} records =====")
    df.printSchema()

    if name == "products":
        df.select("name", "priceFrom", "rating.average", "rating.count").show(10, truncate=False)
    elif name == "orders":
        df.select("number", "contact.email", "totals.total").show(5, truncate=False)
    else:
        df.select("authorName", "productId", "rating").show(9, truncate=False)

spark.stop()
