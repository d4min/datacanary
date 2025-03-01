from datacanary.utils.logging import setup_logging
from datacanary.connectors.s3_connector import S3Connector

setup_logging()

# Define your bucket and key here
BUCKET_NAME = "datacanary-test-bucket"  # Replace with your actual bucket name
FILE_KEY = "test_data.parquet"  # Replace this if you have a Parquet file to test

def test_list_buckets():
    """Test that we can connect to AWS and list buckets."""
    connector = S3Connector()
    s3_client = connector.s3
    
    try:
        response = s3_client.list_buckets()
        buckets = [bucket['Name'] for bucket in response['Buckets']]
        print(f"Successfully connected to AWS. Found {len(buckets)} buckets:")
        for bucket in buckets:
            print(f"  - {bucket}")
        return True
    except Exception as e:
        print(f"Error listing buckets: {e}")
        return False

def test_list_parquet_files():
    """Test that we can list Parquet files in a bucket."""
    connector = S3Connector()
    
    try:
        files = connector.list_parquet_files(BUCKET_NAME)
        print(f"Found {len(files)} Parquet files in {BUCKET_NAME}:")
        for file in files:
            print(f"  - {file}")
        return True
    except Exception as e:
        print(f"Error listing Parquet files: {e}")
        return False

def test_read_parquet():
    """Test that we can read a Parquet file."""
    # Skip this test if no file key is specified
    if not FILE_KEY or FILE_KEY == "path/to/your/file.parquet":
        print("No valid file key provided, skipping test_read_parquet")
        return False
    
    connector = S3Connector()
    
    try:
        df = connector.read_parquet(BUCKET_NAME, FILE_KEY)
        print(f"Successfully read Parquet file with {len(df)} rows and {len(df.columns)} columns")
        print("\nColumns:")
        for col in df.columns:
            print(f"  - {col}: {df[col].dtype}")
        print("\nFirst 5 rows:")
        print(df.head(5))
        return True
    except Exception as e:
        print(f"Error reading Parquet file: {e}")
        return False

if __name__ == "__main__":
    print("\n===== Testing S3 Connector =====\n")
    
    # Test 1: List buckets
    print("\n----- Test 1: List Buckets -----")
    test_list_buckets()
    
    # Test 2: List Parquet files in a bucket
    print("\n----- Test 2: List Parquet Files -----")
    test_list_parquet_files()
    
    # Test 3: Read a Parquet file
    print("\n----- Test 3: Read Parquet File -----")
    test_read_parquet()