from datacanary.utils.logging import setup_logging
from datacanary.connectors.s3_connector import S3Connector

# Set up logging
setup_logging()

# Create an S3 connector
connector = S3Connector()

# Try listing buckets to generate some log messages
try:
    s3_client = connector.s3
    response = s3_client.list_buckets()
    buckets = [bucket['Name'] for bucket in response['Buckets']]
    print(f"Found {len(buckets)} buckets")
except Exception as e:
    print(f"Error: {e}")