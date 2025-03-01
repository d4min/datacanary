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

```

## Data Analysis Example

The `analyse_data.py` script demonstrates how to use DataCanary's statistical analyzer to extract insights from your data:

- Connect to AWS S3
- Read a Parquet file
- Perform statistical analysis on all columns
- Display a summary of the analysis
- Save the detailed analysis as JSON

### Usage

```bash
# Basic usage - analyse data and display results
python analyse_data.py --bucket your-bucket-name --key path/to/file.parquet

# Save the analysis results to a JSON file
python analyse_data.py --bucket your-bucket-name --key path/to/file.parquet --output analysis.json

# Use a specific AWS profile and region
python analyse_data.py --bucket your-bucket-name --key path/to/file.parquet --profile your-profile --region us-west-2
```


