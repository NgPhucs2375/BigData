"""Train/evaluate ALS on the official temporal 5-core benchmark."""
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS
from pyspark.sql import SparkSession, functions as F

HDFS = "hdfs://namenode:8020"
ROOT = f"{HDFS}/mua/public/Clothing_Shoes_and_Jewelry/5core_parquet"
MODEL_OUT = f"{HDFS}/mua/public/Clothing_Shoes_and_Jewelry/als_model"
K = 10

spark = (SparkSession.builder.appName("mua-amazon-5core-als")
         .config("spark.sql.shuffle.partitions", "200")
         .getOrCreate())

try:
    train = spark.read.parquet(f"{ROOT}/train").select("user_int", "item_int", "rating").cache()
    valid = spark.read.parquet(f"{ROOT}/valid").select("user_int", "item_int", "rating").cache()
    test = spark.read.parquet(f"{ROOT}/test").select("user_int", "item_int", "rating").cache()
    print(f"train={train.count():,} valid={valid.count():,} test={test.count():,}")

    evaluator_rmse = RegressionEvaluator(labelCol="rating", predictionCol="prediction", metricName="rmse")
    evaluator_mae = RegressionEvaluator(labelCol="rating", predictionCol="prediction", metricName="mae")
    candidates = [(5, 0.05), (10, 0.05), (20, 0.1)]
    best, best_valid_rmse = None, float("inf")
    for rank, reg in candidates:
        als = ALS(userCol="user_int", itemCol="item_int", ratingCol="rating",
                  rank=rank, maxIter=10, regParam=reg, seed=42,
                  coldStartStrategy="drop")
        model = als.fit(train)
        predictions = model.transform(valid)
        valid_rmse = evaluator_rmse.evaluate(predictions)
        valid_mae = evaluator_mae.evaluate(predictions)
        print(f"VALID rank={rank} regParam={reg} RMSE={valid_rmse:.4f} MAE={valid_mae:.4f}")
        if valid_rmse < best_valid_rmse:
            best, best_valid_rmse = model, valid_rmse

    final_predictions = best.transform(test)
    print(f"TEST RMSE={evaluator_rmse.evaluate(final_predictions):.4f} "
          f"MAE={evaluator_mae.evaluate(final_predictions):.4f}")
    best.write().overwrite().save(MODEL_OUT)

    relevant = test.filter(F.col("rating") >= 4).select("user_int", "item_int").distinct()
    user_relevant = relevant.groupBy("user_int").agg(F.count("*").alias("n_relevant"))
    recs = (best.recommendForAllUsers(K)
            .select("user_int", F.explode("recommendations").alias("rec"))
            .select("user_int", F.col("rec.item_int").alias("item_int")))
    hits = recs.join(relevant, ["user_int", "item_int"]).groupBy("user_int").agg(F.count("*").alias("hits"))
    ranking = user_relevant.join(hits, "user_int", "left").na.fill({"hits": 0})
    result = ranking.agg(
        F.avg(F.col("hits") / F.lit(K)).alias("precision_at_10"),
        F.avg(F.col("hits") / F.col("n_relevant")).alias("recall_at_10"),
        F.count("*").alias("evaluated_users"),
    ).first()
    print(f"TEST Precision@10={result['precision_at_10']:.4f} "
          f"Recall@10={result['recall_at_10']:.4f} users={result['evaluated_users']}")
finally:
    spark.stop()
