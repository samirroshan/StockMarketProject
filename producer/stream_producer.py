import time
import json
import finnhub
import os
from dotenv import load_dotenv
from kafka import KafkaProducer

# 1. Load API Key
load_dotenv('/Users/samirroshan/Workspaces/reventure/StockMarketProject/.env')
API_KEY = os.getenv('FINNHUB_API_KEY')
finnhub_client = finnhub.Client(api_key=API_KEY)

# 2. Setup Kafka Producer
# Connecting to your local Kafka broker
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topic_name = 'stock_market'

print(f"🚀 Starting Producer... sending data to topic: {topic_name}")

try:
    while True:
        # 3. Fetch real-time quote from Finnhub
        # 'c' = current price, 't' = unix timestamp
        quote = finnhub_client.quote('AAPL')
        
        # 4. Create the message for Spark
        # We match the schema keys: symbol, price, timestamp
        data = {
            "symbol": "AAPL",
            "price": quote['c'],
            "timestamp": quote['t']
        }
        
        # 5. Send to Kafka
        producer.send(topic_name, value=data)
        print(f"✅ Sent to Kafka: {data}")
        
        # Wait 2 seconds between updates
        time.sleep(2)
        
except Exception as e:
    print(f"❌ Error: {e}")
finally:
    producer.flush()
    producer.close()
