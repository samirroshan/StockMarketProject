from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# ---------- CONFIG ----------
KAFKA_BROKER = "172.31.10.227:9092"
TOPIC = "stockMarketCap"
S3_BUCKET = "kafka-stock-market-market-cap-bucket"
S3_PATH = f"s3a://{S3_BUCKET}/data/bronze/MarketCap/"
CHECKPOINT_PATH = f"s3a://{S3_BUCKET}/checkpoints/events/"
# ----------------------------

# Create Spark Session
spark = SparkSession.builder \
    .appName("KafkaToS3") \
    .getOrCreate()

# Read from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", TOPIC) \
    .option("startingOffsets", "earliest") \
    .load()

# Kafka values are bytes; convert to string
df_string = df.selectExpr("CAST(value AS STRING) as value")

# Optional: parse JSON if your messages are JSON
# from pyspark.sql.functions import from_json, schema_of_json
# schema = schema_of_json(df_string.select("value").first()[0])
# df_parsed = df_string.withColumn("jsonData", from_json(col("value"), schema))

# Write stream to S3 in JSON format
query = df_string.writeStream \
    .format("json") \
    .option("path", S3_PATH) \
    .option("checkpointLocation", CHECKPOINT_PATH) \
    .outputMode("append") \
    .start()

query.awaitTermination()
