
import os
import socket
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider

# Load environment variables for AWS credentials if not already set
# We assume the producer script will handle loading .env files, or we check here.

def get_msk_auth_token(oauth_config):
    """
    Callback function to generate an authentication token for MSK.
    """
    # Create the token provider.
    # We can pass credentials explicitly if they are available in env vars
    # named AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY.
    # If using .env_global, we might need to rely on the caller setting them up 
    # or pass them here.
    
    # Ideally, boto3 session would pick up standard AWS env vars.
    # If the user uses non-standard names (Access_key, Secret_access_key), we need to map them.
    
    return MSKAuthTokenProvider.generate_auth_token(
        'us-east-1',
        # aws_debug_creds=True  # Uncomment for debugging
    )

KAFKA_CONF = {
    'bootstrap.servers': 'boot-rixdshpq.c2.kafka-serverless.us-east-1.amazonaws.com:9098',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'OAUTHBEARER',
    # For confluent-kafka python, we use oauth_cb
    'oauth_cb': get_msk_auth_token,
    'client.id': socket.gethostname(),
}

TOPIC_NAME = 'stock_market'
