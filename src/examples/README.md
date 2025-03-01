# DataCanary Examples

This directory contains example scripts showing how to use the DataCanary tool.

## Basic S3 Connection

The `basic_s3_connection.py` script demonstrates how to:
- Connect to AWS S3
- List Parquet files in a bucket
- Get metadata for S3 objects
- Read a Parquet file into a pandas DataFrame

### Usage

```bash
# List all Parquet files in a bucket
python basic_s3_connection.py --bucket your-bucket-name

# List Parquet files in a specific prefix (folder)
python basic_s3_connection.py --bucket your-bucket-name --prefix data/

# Read a specific Parquet file
python basic_s3_connection.py --bucket your-bucket-name --key path/to/file.parquet

# Use a specific AWS profile
python basic_s3_connection.py --bucket your-bucket-name --profile your-profile-name