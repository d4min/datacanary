"""
Tests for the Azure connector.
"""
import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
import io
from azure.storage.blob import BlobServiceClient, ContainerClient, BlobClient
from azure.core.exceptions import ResourceNotFoundError

from datacanary.connectors.azure_connector import AzureConnector

class TestAzureConnector(unittest.TestCase):
    """Test cases for the AzureConnector class."""
    
    def setUp(self):
        """Set up test fixtures."""
        # Mock the Azure BlobServiceClient
        self.mock_service_patcher = patch('datacanary.connectors.azure_connector.BlobServiceClient')
        self.mock_service_client = self.mock_service_patcher.start()
        
        # Create a connector with a mocked service client
        self.connector = AzureConnector(connection_string="mock_connection_string")
    
    def tearDown(self):
        """Tear down test fixtures."""
        self.mock_service_patcher.stop()
    
    def test_initialization_with_connection_string(self):
        """Test that the connector initializes properly with a connection string."""
        BlobServiceClient.from_connection_string.assert_called_once_with("mock_connection_string")
    
    @patch('datacanary.connectors.azure_connector.BlobServiceClient')
    def test_initialization_with_account_url_and_key(self, mock_client):
        """Test initialization with account URL and key."""
        connector = AzureConnector(account_url="https://test.blob.core.windows.net", account_key="test_key")
        mock_client.assert_called_once_with(account_url="https://test.blob.core.windows.net", credential="test_key")
    
    @patch('datacanary.connectors.azure_connector.os')
    @patch('datacanary.connectors.azure_connector.BlobServiceClient')
    def test_initialization_with_environment_variables(self, mock_client, mock_os):
        """Test initialization with environment variables."""
        # Set up environment variable mocks
        mock_os.environ.get.side_effect = lambda key: {
            'AZURE_STORAGE_CONNECTION_STRING': 'env_connection_string'
        }.get(key)
        
        connector = AzureConnector()
        BlobServiceClient.from_connection_string.assert_called_with("env_connection_string")
    
    def test_read_parquet(self):
        """Test reading a Parquet file."""
        # Mock the container client and blob client
        mock_container_client = MagicMock()
        mock_blob_client = MagicMock()
        
        # Set up the chain of mocks
        self.connector.service_client.get_container_client.return_value = mock_container_client
        mock_container_client.get_blob_client.return_value = mock_blob_client
        
        # Mock the download_blob result
        mock_download_stream = MagicMock()
        mock_blob_client.download_blob.return_value = mock_download_stream
        mock_download_stream.readall.return_value = b'mock_parquet_data'
        
        # Mock pandas read_parquet
        mock_df = pd.DataFrame({'col1': [1, 2], 'col2': ['a', 'b']})
        with patch('pandas.read_parquet', return_value=mock_df) as mock_read_parquet:
            # Call the method
            result = self.connector.read_parquet('test-container', 'test-blob.parquet')
            
            # Verify the mocks were called correctly
            self.connector.service_client.get_container_client.assert_called_once_with('test-container')
            mock_container_client.get_blob_client.assert_called_once_with('test-blob.parquet')
            mock_blob_client.download_blob.assert_called_once()
            mock_download_stream.readall.assert_called_once()
            
            # Verify the result
            self.assertIs(result, mock_df)
    
    def test_read_parquet_invalid_inputs(self):
        """Test that read_parquet validates its inputs."""
        # Test with empty container
        with self.assertRaises(ValueError):
            self.connector.read_parquet('', 'test-blob.parquet')
        
        # Test with empty blob name
        with self.assertRaises(ValueError):
            self.connector.read_parquet('test-container', '')
    
    def test_read_parquet_blob_not_found(self):
        """Test that read_parquet handles 'not found' errors properly."""
        # Set up mocks to raise ResourceNotFoundError
        mock_container_client = MagicMock()
        mock_blob_client = MagicMock()
        
        self.connector.service_client.get_container_client.return_value = mock_container_client
        mock_container_client.get_blob_client.return_value = mock_blob_client
        mock_blob_client.download_blob.side_effect = ResourceNotFoundError("Blob not found")
        
        # Call the method and check that it re-raises the exception
        with self.assertRaises(ResourceNotFoundError):
            self.connector.read_parquet('test-container', 'nonexistent.parquet')
    
    def test_list_parquet_files(self):
        """Test listing Parquet files in a container."""
        # Create mock blobs
        mock_blob1 = MagicMock()
        mock_blob1.name = 'file1.parquet'
        mock_blob2 = MagicMock()
        mock_blob2.name = 'file2.txt'
        mock_blob3 = MagicMock()
        mock_blob3.name = 'folder/file3.parquet'
        
        # Mock the container client
        mock_container_client = MagicMock()
        self.connector.service_client.get_container_client.return_value = mock_container_client
        
        # Set up the list_blobs return value
        mock_container_client.list_blobs.return_value = [mock_blob1, mock_blob2, mock_blob3]
        
        # Call the method
        result = self.connector.list_parquet_files('test-container', 'prefix')
        
        # Verify the method was called correctly
        self.connector.service_client.get_container_client.assert_called_once_with('test-container')
        mock_container_client.list_blobs.assert_called_once_with(name_starts_with='prefix')
        
        # Verify the result contains only Parquet files
        expected_result = ['file1.parquet', 'folder/file3.parquet']
        self.assertEqual(result, expected_result)
    
    def test_get_object_metadata(self):
        """Test getting metadata for a blob."""
        # Mock the container client and blob client
        mock_container_client = MagicMock()
        mock_blob_client = MagicMock()
        
        self.connector.service_client.get_container_client.return_value = mock_container_client
        mock_container_client.get_blob_client.return_value = mock_blob_client
        
        # Mock the properties object
        mock_properties = MagicMock()
        mock_properties.size = 1024
        mock_properties.last_modified = 'timestamp'
        mock_properties.content_settings.content_type = 'application/octet-stream'
        mock_properties.metadata = {'custom-key': 'custom-value'}
        
        mock_blob_client.get_blob_properties.return_value = mock_properties
        
        # Call the method
        result = self.connector.get_object_metadata('test-container', 'test-blob')
        
        # Verify the calls
        self.connector.service_client.get_container_client.assert_called_once_with('test-container')
        mock_container_client.get_blob_client.assert_called_once_with('test-blob')
        mock_blob_client.get_blob_properties.assert_called_once()
        
        # Verify the result
        expected_result = {
            'size_bytes': 1024,
            'last_modified': 'timestamp',
            'content_type': 'application/octet-stream',
            'metadata': {'custom-key': 'custom-value'}
        }
        self.assertEqual(result, expected_result)

if __name__ == '__main__':
    unittest.main()