from confluent_kafka import Producer
import requests
import json
import time

# Kafka setup
conf = {"bootstrap.servers": "localhost:9092"}  # or your EC2 IP
producer = Producer(conf)

# Delivery callback
def delivery_report(err, msg):
    if err:
        print(f"Delivery failed: {err}")
    else:
        print(f"Delivered to {msg.topic()} [{msg.partition()}]")

# Example API endpoint
API_URL = "https://api.api-ninjas.com/v1/marketcap?ticker=NVDA"

while True:
    response = requests.get(API_URL)
    print("Status code:", response.status_code)
    print("Headers:", response.headers)
    print("Body:", response.text[:500])
    
    if response.status_code == 200:
        data = response.json()  # assuming API returns JSON
        # Optional: send each item if data is a list
        if isinstance(data, list):
            for item in data:
                producer.produce("events", json.dumps(item), on_delivery=delivery_report)
        else:
            producer.produce("events", json.dumps(data), on_delivery=delivery_report)
            
        producer.poll(0)
    else:
        print(f"API request failed with status {response.status_code}")
    
    time.sleep(5)  # wait 5 seconds before next request
