
import sys
import os
import socket
from confluent_kafka.admin import AdminClient, NewTopic
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider

# Import config (ensure we are in project root or path is set)
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
# Mock the config import if needed, or just redundant define for standalone script simplicity
def get_msk_auth_token(oauth_config):
    return MSKAuthTokenProvider.generate_auth_token('us-east-1')

# We use the same config, but AdminClient payload is slightly different in some libs, 
# for confluent-kafka it is the same dict.
conf = {
    'bootstrap.servers': 'boot-rixdshpq.c2.kafka-serverless.us-east-1.amazonaws.com:9098',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'OAUTHBEARER',
    'oauth_cb': get_msk_auth_token,
    'client.id': socket.gethostname(),
}

admin_client = AdminClient(conf)

topic_name = "stock_market"
new_topic = NewTopic(topic_name, num_partitions=2, replication_factor=2)

print(f"Creating topic: {topic_name}...")
fs = admin_client.create_topics([new_topic])

# Wait for operation to finish.
for topic, f in fs.items():
    try:
        f.result()  # The result itself is None
        print(f"✅ Topic '{topic}' created successfully!")
    except Exception as e:
        print(f"⚠️ Failed to create topic '{topic}': {e}")
