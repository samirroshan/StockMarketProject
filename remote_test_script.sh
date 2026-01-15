#!/bin/bash
set -e

# Update and Install
if command -v apt-get &> /dev/null; then
    # Ubuntu/Debian
    apt-get update
    apt-get install -y python3-pip git python3-venv
else
    # Amazon Linux/RHEL
    dnf install -y python3-pip git
fi

# Create venv to avoid externally-managed-environment errors on newer OS
python3 -m venv venv
. venv/bin/activate
pip3 install boto3 aws-msk-iam-sasl-signer-python confluent-kafka finnhub-python python-dotenv

# Setup Directory
mkdir -p /home/ec2-user/stock_project/kafka
mkdir -p /home/ec2-user/stock_project/producer
cd /home/ec2-user/stock_project

# Write Config
cat << 'EOF' > kafka/config.py
import os
import socket
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider

def get_msk_auth_token(oauth_config):
    return MSKAuthTokenProvider.generate_auth_token('us-east-1')

KAFKA_CONF = {
    'bootstrap.servers': 'boot-rixdshpq.c2.kafka-serverless.us-east-1.amazonaws.com:9098',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'OAUTHBEARER',
    'oauth_cb': get_msk_auth_token,
    'client.id': socket.gethostname(),
}
TOPIC_NAME = 'stock_market'
EOF

# Write Producer
cat << 'EOF' > producer/stream_producer.py
import time
import json
import finnhub
import os
import sys
from dotenv import load_dotenv

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from kafka.config import KAFKA_CONF, TOPIC_NAME
from confluent_kafka import Producer

# We use IAM Role, so no need for manual creds
API_KEY = os.getenv('FINNHUB_API_KEY')
finnhub_client = finnhub.Client(api_key=API_KEY)

def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Message delivery failed: {err}")
    else:
        print(f"✅ Sent to Kafka: {msg.value().decode('utf-8')}")

producer = Producer(KAFKA_CONF)
print(f"🚀 Starting Producer (Remote Test)... topic: {TOPIC_NAME}")

# Send 5 messages then exit
for i in range(5):
    try:
        quote = finnhub_client.quote('AAPL')
        data = {"symbol": "AAPL", "price": quote['c'], "timestamp": quote['t']}
        producer.produce(TOPIC_NAME, value=json.dumps(data).encode('utf-8'), callback=delivery_report)
        producer.poll(0)
    except Exception as e:
        print(f"⚠️ Error: {e}")
    time.sleep(1)

producer.flush()
print("Test Complete")
EOF

# Run
export FINNHUB_API_KEY='d5c0h8hr01qsbmghopggd5c0h8hr01qsbmghoph0'
python3 producer/stream_producer.py
