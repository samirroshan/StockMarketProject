"""
quantflow_stock_spark.py

BRONZE layer Spark job for QuantFlow Stock Market Pipeline.
Reads raw CSV stock data from S3 and writes cleaned, schema-enforced
Parquet files to the BRONZE zone.

Medallion Architecture:
- BRONZE: Raw, append-only, lightly structured data
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    DoubleType
)
from pyspark.sql.functions import col, current_timestamp, to_timestamp


# ----------------------------
# Configuration
# ----------------------------
RAW_S3_PATH = "s3://quantflow-stock-market-1769723824/raw/*.csv"
BRONZE_S3_PATH = "s3://quantflow-stock-market-1769723824/bronze/stocks/"


# ----------------------------
# Spark Session
# ----------------------------
spark = (
    SparkSession.builder
    .appName("QuantFlow-Bronze-Stock-Ingestion")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ----------------------------
# Schema Definition (STRICT)
# ----------------------------
stock_schema = StructType([
    StructField("symbol", StringType(), True),
    StructField("timestamp", StringType(), True),  # parsed later
    StructField("open", DoubleType(), True),
    StructField("high", DoubleType(), True),
    StructField("low", DoubleType(), True),
    StructField("close", DoubleType(), True),
])


# ----------------------------
# Read Raw CSV Data
# ----------------------------
raw_df = (
    spark.read
    .option("header", "true")
    .option("mode", "PERMISSIVE")
    .schema(stock_schema)
    .csv(RAW_S3_PATH)
)


# ----------------------------
# Light Bronze Transformations
# ----------------------------
bronze_df = (
    raw_df
    .withColumn(
        "event_timestamp",
        to_timestamp(col("timestamp"))
    )
    .drop("timestamp")
    .withColumn("ingestion_timestamp", current_timestamp())
)


# ----------------------------
# Write to BRONZE Zone (Parquet)
# ----------------------------
(
    bronze_df
    .write
    .mode("append")
    .format("parquet")
    .partitionBy("symbol")
    .save(BRONZE_S3_PATH)
)


# ----------------------------
# Completion
# ----------------------------
print("✅ Bronze layer ingestion completed successfully.")

spark.stop()
