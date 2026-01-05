import json 
import time
from kafka import KafkaProducer

try:
    producer = KafkaProducer(
        bootstrap_servers="[::1]:9092",
        api_version=(2, 5, 0),
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    test_data = {
        "symbol": "APPL",
        "price": 150.25,
        "timestamp" : time.time()
    }
    print(f"Sending: {test_data}")
    producer.send('stock_market', value=test_data)

    producer.flush()
    print("Message delivered to 'stock_market'!")

except Exception as e:
    print(f"Failed to send : {e}")
