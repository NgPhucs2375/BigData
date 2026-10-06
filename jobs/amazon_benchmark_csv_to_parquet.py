"""Convert official Amazon 5-core CSV.gz files to typed Parquet on HDFS."""
import os
from pyspark.sql import SparkSession, functions as F
from pyspark import StorageLevel

HDFS = "hdfs://namenode:8020"
SOURCE = f"{HDFS}/mua/public/Clothing_Shoes_and_Jewelry/5core_source"
OUT = os.environ.get("MUA_OUT", f"{HDFS}/mua/public/Clothing_Shoes_and_Jewelry/5core_parquet")
LIMIT = int(os.environ.get("MUA_LIMIT", "0"))

spark = (SparkSession.builder.appName("mua-amazon-5core-csv-parquet")
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.sql.shuffle.partitions", "200")
         .getOrCreate())

try:
    frames = {}
    for split in ["train", "valid", "test"]:
        source = f"{SOURCE}/Clothing_Shoes_and_Jewelry.{split}.csv.gz"
        frames[split] = (spark.read.option("header", True).option("inferSchema", True).csv(source)
                        .select("user_id", "parent_asin", "rating", "timestamp")
                        .withColumn("user_id", F.concat(F.lit("amazon2023_u_"), F.sha2("user_id", 256)))
                        .withColumn("product_id", F.concat(F.lit("amazon2023_p_"), F.col("parent_asin")))
                        .withColumn("rating", F.col("rating").cast("double"))
                        .withColumn("event_time", F.to_timestamp(F.from_unixtime(F.col("timestamp") / 1000)))
                        .withColumn("benchmark_split", F.lit(split))
                        .select("user_id", "product_id", "rating", "timestamp", "event_time", "benchmark_split"))
        if LIMIT:
            frames[split] = frames[split].limit(LIMIT)

    all_rows = (frames["train"].unionByName(frames["valid"]).unionByName(frames["test"])
                .persist(StorageLevel.DISK_ONLY))
    # Build mappings with distributed zipWithIndex instead of StringIndexer or a
    # global window, both of which can concentrate all labels in one heap.
    user_mapping = (all_rows.select("user_id").distinct().rdd
                    .map(lambda row: row[0]).zipWithIndex()
                    .map(lambda pair: (pair[0], int(pair[1])))
                    .toDF(["user_id", "user_int"])
                    .persist(StorageLevel.DISK_ONLY))
    item_mapping = (all_rows.select("product_id").distinct().rdd
                    .map(lambda row: row[0]).zipWithIndex()
                    .map(lambda pair: (pair[0], int(pair[1])))
                    .toDF(["product_id", "item_int"])
                    .persist(StorageLevel.DISK_ONLY))

    for split, frame in frames.items():
        (frame.join(user_mapping, "user_id").join(item_mapping, "product_id")
         .write.mode("overwrite").partitionBy("benchmark_split").parquet(f"{OUT}/{split}"))
    user_mapping.write.mode("overwrite").parquet(f"{OUT}/user_mapping")
    item_mapping.write.mode("overwrite").parquet(f"{OUT}/item_mapping")
    all_rows.unpersist()
finally:
    spark.stop()
