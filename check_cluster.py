
import os
import boto3
import json

# Load creds manually
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

client = boto3.client('kafka', region_name='us-east-1')

arn = 'arn:aws:kafka:us-east-1:873041805629:cluster/StockMarketStream/c82ad939-e4a5-49de-98a4-aa0b14450b54-s2'

try:
    response = client.describe_cluster_v2(ClusterArn=arn)
    print("Cluster Info:")
    print(json.dumps(response, indent=2, default=str))
except Exception as e:
    print(f"Error describing v2: {e}")
    try:
        response = client.describe_cluster(ClusterArn=arn)
        print("Cluster Info (v1):")
        print(json.dumps(response, indent=2, default=str))
    except Exception as e2:
        print(f"Error describing v1: {e2}")

