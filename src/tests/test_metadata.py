from datacanary.connectors.s3_connector import S3Connector
from datacanary.utils.logging import setup_logging

# Set up logging
setup_logging()

# Create connector
connector = S3Connector()

# Test the method directly
bucket_name = "datacanary-test-bucket"
file_key = "test_data.parquet"

try:
    metadata = connector.get_object_metadata(bucket_name, file_key)
    print(f"Metadata retrieved successfully: {metadata}")
except Exception as e:
    print(f"Error: {type(e).__name__}: {e}")
    
    # Debug information
    print("\nDebug info:")
    print(f"Available methods: {[m for m in dir(connector) if not m.startswith('_')]}")