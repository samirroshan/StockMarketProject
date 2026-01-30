# Real-Time Stock Market Processing Pipeline

A distributed data pipeline built with **Python**, **Apache Kafka**, and **PySpark**.

## 🏗️ Architecture
- **Producer**: Python script fetching live prices from Finnhub API.
- **Broker**: Apache Kafka managing the data streams.
- **Stream Processor**: PySpark performing a 5-minute sliding window moving average.
- **Storage**: Processed data saved as CSV in `/data`.
- **Dashboard**: Live Matplotlib visualizer.

## 🚀 How to Run
1. Start Zookeeper & Kafka.
2. python3 -m venv venv
3. source venv/bin/activate
4. python3 -m pip install -r requirements.txt
5. Run `python producer/stream_producer.py`.
6. Run `python spark_scripts/spark_processor.py`.
7. Run `python spark_scripts/visualizer.py`.



## EC2 Commands
# Spark 

python3 -m venv venv
source venv/bin/activate
python3 -m pip install -r requirements.txt
spark-submit   --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2,org.apache.hadoop:hadoop-aws:3.3.2  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem  --conf spark.hadoop.fs.s3a.access.key=ACCESS_KEY --conf spark.hadoop.fs.s3a.secret.key=SECRET_ACCESS_KEY  spark_scripts/spark_to_kafka.py

# Kafka 
bin/kafka-storage.sh format \
  --config config/kraft/server.properties \
  --cluster-id $(bin/kafka-storage.sh random-uuid)
bin/kafka-server-start.sh config/kraft/server.properties

python3 -m venv venv
source venv/bin/activate
python3 -m pip install -r requirements.txt
python producer/marketCapKafka.py