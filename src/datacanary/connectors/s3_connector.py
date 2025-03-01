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
        self.session = boto3.session(profile_name = aws_profile)
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