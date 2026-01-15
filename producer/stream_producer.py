import time
import json
import finnhub
import os
import sys
from dotenv import load_dotenv

# Add the project root to sys.path to ensure we can import from kafka.config
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from kafka.config import KAFKA_CONF, TOPIC_NAME
from confluent_kafka import Producer


# ... imports remain ...

def run_producer(producer, topic, finnhub_client, ticker='AAPL', interval=2, max_iterations=None):
    """
    Main producer loop.
    :param producer: Kafka producer instance (or mock)
    :param topic: Kafka topic name
    :param finnhub_client: Finnhub client instance (or mock)
    :param ticker: Stock ticker to fetch
    :param interval: Time to sleep between fetches
    :param max_iterations: If set, stop after N messages (useful for testing)
    """
    print(f"🚀 Starting Producer... sending data to topic: {topic}")
    
    count = 0
    try:
        while True:
            # Check exit condition for testing
            if max_iterations is not None and count >= max_iterations:
                print(f"🛑 Reached max_iterations ({max_iterations}). Stopping.")
                break

            # 3. Fetch real-time quote from Finnhub
            try:
                quote = finnhub_client.quote(ticker)
                
                # 4. Create the message
                data = {
                    "symbol": ticker,
                    "price": quote['c'],
                    "timestamp": quote['t']
                }
                
                # 5. Send to Kafka
                # confluent-kafka uses produce() which is asynchronous.
                producer.produce(topic, value=json.dumps(data).encode('utf-8'), callback=delivery_report)
                producer.poll(0)
                
                count += 1
                
            except Exception as api_err:
                 print(f"⚠️ API Error: {api_err}")

            # Wait between updates
            time.sleep(interval)
            
    except KeyboardInterrupt:
        print("Stopping producer...")
    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        print("Flushing messages...")
        producer.flush()

if __name__ == "__main__":
    # 1. Load API Key and AWS Creds
    load_dotenv('/Users/samirroshan/Workspaces/reventure/StockMarketProject/.env')
    
    # Load .env_global for AWS credentials if needed/not handled globally
    global_env_path = '/Users/samirroshan/Workspaces/reventure/StockMarketProject/.env_global'
    if os.path.exists(global_env_path):
        with open(global_env_path, 'r') as f:
            for line in f:
                if '=' in line:
                    key, value = line.strip().split('=', 1)
                    key = key.strip()
                    value = value.strip()
                    if key == 'Access_key':
                        os.environ['AWS_ACCESS_KEY_ID'] = value
                    elif key == 'Secret_access_key':
                        os.environ['AWS_SECRET_ACCESS_KEY'] = value

    API_KEY = os.getenv('FINNHUB_API_KEY')
    if not API_KEY:
        print("❌ Error: FINNHUB_API_KEY not found in .env")
        sys.exit(1)

    real_finnhub_client = finnhub.Client(api_key=API_KEY)

    # 2. Setup Kafka Producer
    real_producer = Producer(KAFKA_CONF)
    
    run_producer(real_producer, TOPIC_NAME, real_finnhub_client)

