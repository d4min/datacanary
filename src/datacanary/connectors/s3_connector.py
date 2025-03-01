import boto3
import pandas as pd 

class S3Connector:
    """
    Connects to AWS S3 to retrieve data files 
    """

    def __init__(self, aws_profile=None):
        """
        Initialise the S3 Connector.

        Args:
            aws_profile: Optional AWS profile name to use for credentials 
        """
        self.session = boto3.Session(profile_name = aws_profile)
        self.s3 = self.session.client('s3')

    def read_parquet(self, bucket, key):
        """
        Read a Parquet file from S3 into a pandas DataFrame.

        Args:
            bucket: S3 bucket name
            key: S3 object key (path to the file)

        Returns:
            DataFrame containing the Parquet file data 

        Raises:
            ValueError: If the bucket or key is invalid
            Exception: If there's an error reading the file 
        """
        if not bucket or not key:
            raise ValueError('Bucket and Key must be provided')

        s3_uri = f"s3://{bucket}/{key}"

        try:
            return pd.read_parquet(s3_uri)
        except Exception as e:
            print(f"Error reading Parquet file from {s3_uri}: {e}")
            raise

    def list_parquet_files(self, bucket, prefix=""):
        """
        List all parquet files in a bucket with the given prefix. 

        Args:
            bucket: S3 bucket name
            prefix: Optional prefix to filter objects

        Returns:
            List of S3 keys for Parquet files. 
        """
        result = []
        paginator = self.s3.get_paginator('list_objects_v2')

        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            if 'Contents' not in page:
                continue

            for obj in page['Contents']:
                key = obj['Key']
                if key.endswith('.parquet'):
                    result.append(key)

        return result
    
    def get_object_metadeta(self, bucket, key):
        """
        Get metadata for an S3 object.

        Args:
            bucket: S3 bucket name
            key: S3 object key

        Returns:
            Dictionary of object metadata
        """
        try:
            response = self.s3.head_object(Bucket=bucket, Key=key)
            return {
                'size_bytes': response.get('ContentLength', 0),
                'last_modified': response.get('LastModified'),
                'content_type': response.get('ContentType'),
                'metadata': response.get('Metadata', {})
            }
        except Exception as e:
            print(f"Error getting metadata for s3://{bucket}/{key}: {e}")
            raise