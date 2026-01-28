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
