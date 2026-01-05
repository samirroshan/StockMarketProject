from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import sqlite3 
import os

# 1. Initialize Spark Session with Kafka support
spark = SparkSession.builder \
    .appName("StockMarketMovingAverage") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. Define the schema to match your Kafka JSON data
schema = StructType([
    StructField("symbol", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("timestamp", DoubleType(), True)
])

# 3. Read from Kafka
raw_df = (spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", "stock_market").option("startingOffsets", "earliest").load())

# 4. Parse JSON and convert the Unix number to a real Timestamp
df = raw_df.selectExpr("CAST(value AS STRING)").select(F.from_json(F.col("value"), schema).alias("data")).select("data.*")

df_clean = df.withColumn("timestamp", F.from_unixtime(F.col("timestamp")).cast("timestamp"))
df_watermarked = df_clean.withWatermark("timestamp", "10 minutes")


# 5. Moving Average
moving_avg_df = (df_watermarked.groupBy(F.window("timestamp", "5 minutes", "1 minute"), "symbol").agg(F.avg("price").alias("moving_avg_price")))
# 6. Write to a SINGLE CSV
def write_to_single_csv(batch_df, batch_id):
    if batch_df.isEmpty():
        return
    file_name = '/Users/samirroshan/Workspaces/reventure/StockMarketProject/data/live_stock_averages.csv'
    
    os.makedirs(os.path.dirname(file_name), exist_ok=True)


    rows = batch_df.select(
        F.col("window.start").cast("string").alias("start"),
        F.col("window.end").cast("string").alias("end"),
        "symbol",
        "moving_avg_price"
    ).collect()
    file_exists = os.path.isfile(file_name)
    with open(file_name, 'a') as f:
        if  not file_exists:
             f.write("start,end,symbol,moving_avg_price\n")
        for row in rows:
             f.write(f"{row['start']},{row['end']},{row['symbol']},{row['moving_avg_price']}\n")
    print(f"Successfully processed batch {batch_id} and updated {file_name}")

query = (moving_avg_df.writeStream.outputMode("update").foreachBatch(write_to_single_csv).start())


print("🚀 Spark Processor is running and calculating moving averages...")
query.awaitTermination()
