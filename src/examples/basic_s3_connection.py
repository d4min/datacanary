import argparse

from datacanary.connectors.s3_connector import S3Connector
from datacanary.utils.logging import setup_logging

def main():
    """Run the example script."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="DataCanary S3 connector example")
    parser.add_argument("--bucket", required=True, help="S3 bucket name")
    parser.add_argument("--prefix", default="", help="S3 prefix (folder path)")
    parser.add_argument("--key", help="Specific Parquet file key to read")
    parser.add_argument("--profile", help="AWS profile name")
    parser.add_argument("--region", default="us-east-1", help="AWS region")
    args = parser.parse_args()
    
    # Set up logging
    setup_logging()

    # Create S3 connector
    connector = S3Connector(
        aws_profile=args.profile,
        aws_region=args.region
    )
    
    # List available Parquet files
    print(f"\nListing Parquet files in s3://{args.bucket}/{args.prefix}...")
    files = connector.list_parquet_files(args.bucket, args.prefix)
    if not files:
        print("No Parquet files found.")
    else:
        print(f"Found {len(files)} Parquet files:")
        for file in files:
            try:
                metadata = connector.get_object_metadata(args.bucket, file)
                size_mb = metadata['size_bytes'] / (1024 * 1024)
                print(f"  - {file} ({size_mb:.2f} MB)")
            except Exception as e:
                print(f"  - {file} (Error getting metadata: {e})")
    
    # Read a specific Parquet file if requested
    if args.key:
        print(f"\nReading Parquet file s3://{args.bucket}/{args.key}...")
        try:
            df = connector.read_parquet(args.bucket, args.key)
            print(f"Successfully read file with {len(df)} rows and {len(df.columns)} columns")
            print("\nColumns:")
            for col_name in df.columns:
                col = df[col_name]
                example = col.iloc[0] if not col.empty else None
                print(f"  - {col_name}: {col.dtype} (Example: {example})")
            
            print("\nFirst 5 rows:")
            print(df.head(5))
        except Exception as e:
            print(f"Error reading file: {e}")

if __name__ == "__main__":
    main()