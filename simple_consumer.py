
import sys
import os
import socket
import json
from confluent_kafka import Consumer
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider

def get_msk_auth_token(oauth_config):
    return MSKAuthTokenProvider.generate_auth_token('us-east-1')

conf = {
    'bootstrap.servers': 'boot-rixdshpq.c2.kafka-serverless.us-east-1.amazonaws.com:9098',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'OAUTHBEARER',
    'oauth_cb': get_msk_auth_token,
    'group.id': 'visual-demo-group',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(conf)
consumer.subscribe(['stock_market'])

print("👀 Watching for stocks... (Ctrl+C to stop)")
try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue

        # Visual Output
        try:
            val = json.loads(msg.value().decode('utf-8'))
            print(f"📈 RECEIVED: {val['symbol']} | ${val['price']} | TS: {val['timestamp']}")
        except:
             print(f"Received: {msg.value().decode('utf-8')}")

except KeyboardInterrupt:
    pass
finally:
    consumer.close()
