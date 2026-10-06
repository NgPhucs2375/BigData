"""Convert the official Amazon 5-core JSONL splits to typed Parquet on HDFS."""
from pyspark.ml.feature import StringIndexer
from pyspark.sql import SparkSession, functions as F, types as T

HDFS = "hdfs://namenode:8020"
ROOT = f"{HDFS}/mua/public/Clothing_Shoes_and_Jewelry/5core_last_out_full"
OUT = f"{HDFS}/mua/public/Clothing_Shoes_and_Jewelry/5core_parquet"

schema = T.StructType([
    T.StructField("interaction_id", T.StringType(), False),
    T.StructField("user_id", T.StringType(), False),
    T.StructField("user_source", T.StringType(), True),
    T.StructField("product_id", T.StringType(), False),
    T.StructField("event_type", T.StringType(), True),
    T.StructField("rating", T.DoubleType(), False),
    T.StructField("quantity", T.LongType(), True),
    T.StructField("weight", T.DoubleType(), True),
    T.StructField("timestamp", T.StringType(), False),
    T.StructField("timestamp_source", T.StringType(), True),
    T.StructField("source", T.StringType(), True),
    T.StructField("source_kind", T.StringType(), True),
    T.StructField("source_category", T.StringType(), True),
    T.StructField("benchmark_split", T.StringType(), False),
    T.StructField("source_document_id", T.StringType(), True),
])

spark = (SparkSession.builder.appName("mua-amazon-5core-parquet")
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.sql.shuffle.partitions", "200")
         .getOrCreate())

try:
    splits = {
        name: spark.read.schema(schema).json(f"{ROOT}/{name}.jsonl")
        for name in ["train", "valid", "test"]
    }
    all_rows = splits["train"].unionByName(splits["valid"]).unionByName(splits["test"]).cache()

    # Fit mappings once across all splits so a user/item keeps one integer ID.
    user_indexer = StringIndexer(inputCol="user_id", outputCol="user_int", handleInvalid="error").fit(all_rows)
    item_indexer = StringIndexer(inputCol="product_id", outputCol="item_int", handleInvalid="error").fit(all_rows)
    for name, frame in splits.items():
        indexed = (item_indexer.transform(user_indexer.transform(frame))
                   .withColumn("user_int", F.col("user_int").cast("int"))
                   .withColumn("item_int", F.col("item_int").cast("int"))
                   .withColumn("event_time", F.to_timestamp("timestamp")))
        indexed.write.mode("overwrite").partitionBy("benchmark_split").parquet(f"{OUT}/{name}")

    users = user_indexer.labels
    items = item_indexer.labels
    spark.createDataFrame(enumerate(users), ["user_int", "user_id"]).write.mode("overwrite").parquet(f"{OUT}/user_mapping")
    spark.createDataFrame(enumerate(items), ["item_int", "product_id"]).write.mode("overwrite").parquet(f"{OUT}/item_mapping")
    all_rows.unpersist()
finally:
    spark.stop()
