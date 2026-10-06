"""Convert normalized interaction JSONL to typed Parquet on HDFS."""
from pyspark.sql import SparkSession, functions as F, types as T

HDFS = "hdfs://namenode:8020"
schema = T.StructType([
    T.StructField(key, T.StringType(), True)
    for key in ["interaction_id", "user_id", "user_source", "product_id", "event_type",
                "timestamp", "timestamp_source", "source", "source_document_id", "source_kind"]
] + [T.StructField("rating", T.LongType(), True),
     T.StructField("quantity", T.LongType(), True),
     T.StructField("weight", T.DoubleType(), True)])

spark = (SparkSession.builder.appName("mua-interactions-parquet")
         .config("spark.sql.session.timeZone", "UTC").getOrCreate())
try:
    events = (spark.read.schema(schema).option("mode", "FAILFAST")
              .json(f"{HDFS}/mua/silver/interactions_clean.jsonl")
              .withColumn("timestamp", F.to_timestamp("timestamp")))
    events.write.mode("overwrite").parquet(f"{HDFS}/mua/silver/interactions")
    events.filter(F.col("event_type") == "purchase").write.mode("overwrite").parquet(f"{HDFS}/mua/silver/implicit_interactions")
    events.filter(F.col("event_type") == "review").write.mode("overwrite").parquet(f"{HDFS}/mua/silver/explicit_ratings")
finally:
    spark.stop()
