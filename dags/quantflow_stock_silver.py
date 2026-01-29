from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, window, avg, max as spark_max, min as spark_min
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("QuantFlowSilver").getOrCreate()

# SILVER: JSON raw → Daily OHLCV aggregates
df = spark.read.json("s3://kafka-stock-market-market-cap-bucket/raw/*.json")

silver_df = df \
    .withColumn("dt", to_timestamp(col("timestamp"))) \
    .withColumn("date", col("dt").cast("date")) \
    .groupBy("symbol", "date") \
    .agg(
        avg("close").alias("daily_close"),
        spark_max("high").alias("daily_high"),
        spark_min("low").alias("daily_low"),
        avg("volume").alias("daily_volume")
    ) \
    .orderBy("symbol", "date")

silver_df.write.mode("overwrite") \
    .partitionBy("symbol") \
    .parquet("s3://kafka-stock-market-market-cap-bucket/data/silver/MarketCap/")

print("✅ SILVER layer complete")
spark.stop()
