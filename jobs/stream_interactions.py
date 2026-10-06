"""Low-resource Spark Structured Streaming ingestion for interaction JSONL files."""
import os
import json

from pyspark.sql import SparkSession, functions as F, types as T

HDFS = "hdfs://namenode:8020"
SOURCE = os.environ.get("MUA_STREAM_SOURCE", f"{HDFS}/mua/stream/incoming/interactions")
OUTPUT = os.environ.get("MUA_STREAM_OUTPUT", f"{HDFS}/mua/stream/curated/interactions")
CHECKPOINT = os.environ.get("MUA_STREAM_CHECKPOINT", f"{HDFS}/mua/stream/checkpoints/interactions")
PROGRESS_FILE = os.environ.get("MUA_STREAM_PROGRESS", "/tmp/mua-stream-progress.json")

schema = T.StructType([
    T.StructField("interaction_id", T.StringType(), False),
    T.StructField("user_id", T.StringType(), False),
    T.StructField("user_source", T.StringType(), True),
    T.StructField("product_id", T.StringType(), False),
    T.StructField("event_type", T.StringType(), False),
    T.StructField("rating", T.DoubleType(), True),
    T.StructField("quantity", T.LongType(), True),
    T.StructField("weight", T.DoubleType(), True),
    T.StructField("timestamp", T.StringType(), False),
    T.StructField("timestamp_source", T.StringType(), True),
    T.StructField("source", T.StringType(), True),
    T.StructField("source_document_id", T.StringType(), True),
    T.StructField("source_kind", T.StringType(), True),
])

spark = (SparkSession.builder.appName("mua-interactions-stream")
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.sql.shuffle.partitions", "2")
         .getOrCreate())

try:
    events = (spark.readStream.schema(schema).option("maxFilesPerTrigger", 1).json(SOURCE)
              .withColumn("event_time", F.to_timestamp("timestamp"))
              .filter(F.col("event_time").isNotNull())
              .withWatermark("event_time", "10 minutes")
              .dropDuplicates(["interaction_id"])
              .withColumn("event_date", F.to_date("event_time")))

    query = (events.writeStream.format("parquet").outputMode("append")
             .option("path", OUTPUT).option("checkpointLocation", CHECKPOINT)
             .partitionBy("event_date", "event_type")
             .trigger(processingTime="10 seconds").start())

    while query.isActive:
        query.awaitTermination(2)
        progress = query.lastProgress
        if progress:
            with open(PROGRESS_FILE, "w", encoding="utf-8") as output:
                json.dump(progress, output)
finally:
    spark.stop()
