"""
Connector for Azure Blob Storage data sources.
"""
import logging
import os
from io import BytesIO
import pandas as pd
from azure.storage.blob import BlobServiceClient, BlobClient
from azure.core.exceptions import ResourceNotFoundError, AzureError

logger = logging.getLogger(__name__)

class AzureConnector():
    """
    Connects to Azure Blob Storage to retrieve data files.
    """

    def __init__(self, connection_string=None, account_url=None, account_key=None, credential=None):
        """
        Initialise the Azure Blob Storage connector. 

        Args:
            connection_string: Optional Azure connection string
            account_url: Optional Azure Storage account URL 
            account_key: Optional Azure Storage account key
            credential: Optional credential object for authentication
        """
        # Try different authentication methods
        if connection_string:
            logger.info("Initializing AzureConnector with connection string")
            self.service_client = BlobServiceClient.from_connection_string(connection_string)
        elif account_url:
            if account_key:
                logger.info("Initializing AzureConnector with account URL and key")
                self.service_client = BlobServiceClient(
                    account_url=account_url, 
                    credential=account_key
                )
            elif credential:
                logger.info("Initializing AzureConnector with account URL and credential")
                self.service_client = BlobServiceClient(
                    account_url=account_url, 
                    credential=credential
                )
            else:
                raise ValueError("When using account_url, either account_key or credential must be provided")
        else:
            # Try using environment variables
            logger.info("Initializing AzureConnector with environment variables")
            try:
                default_connection_string = os.environ.get('AZURE_STORAGE_CONNECTION_STRING')
                if default_connection_string:
                    self.service_client = BlobServiceClient.from_connection_string(default_connection_string)
                else:
                    account_name = os.environ.get('AZURE_STORAGE_ACCOUNT')
                    account_key = os.environ.get('AZURE_STORAGE_KEY')
                    if account_name and account_key:
                        account_url = f"https://{account_name}.blob.core.windows.net"
                        self.service_client = BlobServiceClient(
                            account_url=account_url, 
                            credential=account_key
                        )
                    else:
                        raise ValueError("No valid authentication method provided and environment variables not set")
            except Exception as e:
                logger.error(f"Error initializing with environment variables: {e}")
                raise

    def read_parquet(self, container_name, blob_name):
        """
        Read a Parquet file from Azure Blob Storage into a Pandas DataFrame.

        Args:
            container_name: Azure Storage container name (equivelant to S3 bucket)
            blob_name: Blob name (path to the file)

        Returns:
            DataFrame containing the Parquet file
        """
        if not container_name or not blob_name:
            logger.error("Invalid container or blob name provided")
            raise ValueError("Container name and blob name must be provided")
        
        storage_path = f"azure://{container_name}/{blob_name}"
        logger.info(f"Reading Parquet file from {storage_path}")

        try:
            # Get a client to the blob
            container_client = self.service_client.get_container_client(container_name)
            blob_client = container_client.get_blob_client(blob_name)
            
            # Download the blob
            download_stream = blob_client.download_blob()
            blob_data = download_stream.readall()
            
            # Read the Parquet file from the downloaded data
            return pd.read_parquet(BytesIO(blob_data))
        except ResourceNotFoundError as e:
            logger.error(f"Blob not found: {storage_path}, Error: {e}")
            raise
        except AzureError as e:
            logger.error(f"Azure service error accessing {storage_path}: {e}")
            raise
        except Exception as e:
            logger.error(f"Error reading Parquet file from {storage_path}: {e}")
            raise

    def list_parquet_files(self, container_name, prefix=""):
        """
        List all Parquet files in a container within a given prefix

        Args:
            container_name: Azure Storage container name
            prefix: Optional prefix to filter objects

        Returns:  
            List of blob names for Parquet files 
        """
        logger.info(f"Listing Parquet files in azure://{container_name}/{prefix}")

        try:
            # Get a client to the container
            container_client = self.service_client.get_container_client(container_name)
            
            # List all blobs with the given prefix
            blobs = container_client.list_blobs(name_starts_with=prefix)
            
            # Filter for Parquet files
            parquet_files = [blob.name for blob in blobs if blob.name.endswith('.parquet')]
            
            logger.info(f"Found {len(parquet_files)} Parquet files")
            return parquet_files
        except ResourceNotFoundError as e:
            logger.error(f"Container not found: {container_name}, Error: {e}")
            raise
        except AzureError as e:
            logger.error(f"Azure service error listing blobs: {e}")
            raise
        except Exception as e:
            logger.error(f"Error listing Parquet files: {e}")
            raise

    def get_object_metadata(self, container_name, blob_name):
        """
        Get metadata for an Azure blob.

        Args:
            container_name: Azure Storage container name
            blob_name: Blob name (path to the file)

        Returns:
            Dictionary of object metadata
        """
        logger.info(f"Getting metadata for azure://{container_name}/{blob_name}")

        try:
            # Get a client to the blob
            container_client = self.service_client.get_container_client(container_name)
            blob_client = container_client.get_blob_client(blob_name)
            
            # Get blob properties
            properties = blob_client.get_blob_properties()
            
            return {
                'size_bytes': properties.size,
                'last_modified': properties.last_modified,
                'content_type': properties.content_settings.content_type,
                'metadata': properties.metadata
            }
        except ResourceNotFoundError as e:
            logger.error(f"Blob not found: {container_name}/{blob_name}, Error: {e}")
            raise
        except AzureError as e:
            logger.error(f"Azure service error accessing metadata: {e}")
            raise
        except Exception as e:
            logger.error(f"Error getting metadata for {container_name}/{blob_name}: {e}")
            raise

