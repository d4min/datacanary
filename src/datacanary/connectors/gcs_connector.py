"""
Connector for Google Cloud Storage data sources.
"""
import logging
import os
from io import BytesIO
import pandas as pd
from google.cloud import storage
from google.auth.exceptions import DefaultCredentialsError

logger = logging.getLogger(__name__)

class GCSConnector:
    """
    Connects to Google Cloud Storage to retrieve data files. 
    """

    def __init__(self, credentials_path = None, project_id = None):
        """
        Initialise the Google Cloud Connector.

        Args:
            credentials_path: Optional path to service account JSON key file
            project_id: Optional Google Cloud project id
        """
        logger.info(f"Initializing GCSConnector with credentials_path: {credentials_path}, project_id: {project_id}")

        # Set environment variable for credentials if provided
        if credentials_path:
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path
        
        try:
            # Initialize the storage client
            self.client = storage.Client(project=project_id)
            if project_id is None:
                project_id = self.client.project
            logger.info(f"Successfully connected to GCS with project: {project_id}")
        except DefaultCredentialsError as e:
            logger.error(f"Error initializing GCS client: {e}")
            logger.error("Make sure you have provided valid credentials or are running in an environment with default credentials")
            raise
        except Exception as e:
            logger.error(f"Error initializing GCS client: {e}")
            raise

    def read_parquet(self, bucket_name, blob_name):
        """
        Read a Parquet file from Google Cloud Storage into a Pandas DataFrame.

        Args:
            bucket_name: GCS bucket_name
            blob_name: GCS blob_name

        Returns:
            DataFrame containing the Parquet file data
        """
        if not bucket_name or not blob_name:
            logger.error("Invalid bucket or blob name provided")
            raise ValueError("Bucket name and blob name must be provided")
            
        gcs_path = f"gs://{bucket_name}/{blob_name}"
        logger.info(f"Reading Parquet file from {gcs_path}")

        try:
            # Get a reference to the bucket and blob
            bucket = self.client.bucket(bucket_name)
            blob = bucket.blob(blob_name)
            
            # Download the blob content to memory
            content = blob.download_as_bytes()
            
            # Read the Parquet file from the downloaded content
            return pd.read_parquet(BytesIO(content))
        except Exception as e:
            logger.error(f"Error reading Parquet file from {gcs_path}: {e}")
            raise

    def list_parquet_files(self, bucket_name, prefix=""):
        """
        List all Parquet files in a GCS bucket with the given prefix.

        Args:
            bucket_name: GCS bucket_name
            prefix: Optional prefix to filter objects
        
        Returns:
            List of blob names for Parquet files
        """
        logger.info(f"Listing Parquet files in gs://{bucket_name}/{prefix}")

        try:
            # Get a reference to the bucket
            bucket = self.client.bucket(bucket_name)
            
            # List all blobs with the given prefix
            blobs = bucket.list_blobs(prefix=prefix)
            
            # Filter for Parquet files
            parquet_files = [blob.name for blob in blobs if blob.name.endswith('.parquet')]
            
            logger.info(f"Found {len(parquet_files)} Parquet files")
            return parquet_files
        except Exception as e:
            logger.error(f"Error listing Parquet files: {e}")
            raise

    def get_object_metadata(self, bucket_name, blob_name):
        """
        Get metadata for a GCS blob.

        Args:
            bucket_name: GCS bucket_name
            blob_name: GCS blob_name

        Returns:
            Dictionary of object metadata
        """
        logger.info(f"Getting metadata for gs://{bucket_name}/{blob_name}")

        try:
            # Get a reference to the bucket and blob
            bucket = self.client.bucket(bucket_name)
            blob = bucket.blob(blob_name)
            
            # Get blob metadata
            blob.reload()  # Ensure we have the latest metadata
            
            return {
                'size_bytes': blob.size,
                'last_modified': blob.updated,
                'content_type': blob.content_type,
                'metadata': blob.metadata or {}
            }
        except Exception as e:
            logger.error(f"Error getting metadata for gs://{bucket_name}/{blob_name}: {e}")
            raise