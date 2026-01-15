
import os
import sys
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider

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

print(f"AWS_ACCESS_KEY_ID set: {'AWS_ACCESS_KEY_ID' in os.environ}")
print(f"AWS_SECRET_ACCESS_KEY set: {'AWS_SECRET_ACCESS_KEY' in os.environ}")

print("Generating token...")
try:
    token, expiry = MSKAuthTokenProvider.generate_auth_token('us-east-1')
    print("Token generated successfully!") 
    # print(f"Token: {token}") # Don't print token in logs if possible, but for debug we know it's short lived
except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()
