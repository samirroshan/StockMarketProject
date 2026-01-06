from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
import datetime

def verify_split_logic():
    spark = SparkSession.builder \
        .appName("Verification") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("ERROR")

    # Create dummy data with a window-like structure or just timestamps
    data = [
        ("AAPL", datetime.datetime(2023, 10, 27, 10, 0, 0), datetime.datetime(2023, 10, 27, 10, 5, 0)),
        ("GOOG", datetime.datetime(2023, 10, 27, 10, 5, 0), datetime.datetime(2023, 10, 27, 10, 10, 0))
    ]
    
    schema = StructType([
        StructField("symbol", StringType(), True),
        StructField("start", TimestampType(), True),
        StructField("end", TimestampType(), True)
    ])
    
    df = spark.createDataFrame(data, schema)
    
    print("Original DataFrame:")
    df.show(truncate=False)
    
    # Simulate the transformation logic to be used in spark_processor.py
    # Logic: 
    # start -> start_date, start_time
    # end -> end_date, end_time
    
    transformed_df = df.select(
        "symbol",
        F.date_format(F.col("start"), "yyyy-MM-dd").alias("start_date"),
        F.date_format(F.col("start"), "HH:mm:ss").alias("start_time"),
        F.date_format(F.col("end"), "yyyy-MM-dd").alias("end_date"),
        F.date_format(F.col("end"), "HH:mm:ss").alias("end_time")
    )
    
    print("Transformed DataFrame:")
    transformed_df.show(truncate=False)
    
    # Check output
    rows = transformed_df.collect()
    for row in rows:
        print(f"Row: {row}")

if __name__ == "__main__":
    verify_split_logic()
