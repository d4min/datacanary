"""
Tests for the GCS connector.
"""
import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
import io
from google.cloud import storage
from google.auth.exceptions import DefaultCredentialsError

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))
from datacanary.connectors.gcs_connector import GCSConnector

class TestGCSConnector(unittest.TestCase):
    """Test cases for the GCSConnector class."""
    
    def setUp(self):
        """Set up test fixtures."""
        # Mock the Google Cloud Storage client
        self.mock_storage_patcher = patch('datacanary.connectors.gcs_connector.storage.Client')
        self.mock_storage_client = self.mock_storage_patcher.start()
        
        # Create a connector with a mocked storage client
        self.mock_client_instance = MagicMock()
        self.mock_storage_client.return_value = self.mock_client_instance
        self.mock_client_instance.project = "mock-project"
        
        self.connector = GCSConnector()
    
    def tearDown(self):
        """Tear down test fixtures."""
        self.mock_storage_patcher.stop()
    
    def test_initialization_with_credentials(self):
        """Test that the connector initializes properly with credentials."""
        # Reset the mock before this test to clear previous calls
        self.mock_storage_client.reset_mock()
        with patch('datacanary.connectors.gcs_connector.os') as mock_os:
            connector = GCSConnector(credentials_path="/path/to/credentials.json")
            mock_os.environ.__setitem__.assert_called_with(
                "GOOGLE_APPLICATION_CREDENTIALS", "/path/to/credentials.json"
            )
            self.mock_storage_client.assert_called_once()
    
    def test_initialization_with_project_id(self):
        """Test initialization with project ID."""
        # Reset the mock before this test to clear previous calls
        self.mock_storage_client.reset_mock()
        connector = GCSConnector(project_id="test-project")
        self.mock_storage_client.assert_called_once_with(project="test-project")
    
    def test_initialization_error(self):
        """Test error handling during initialization."""
        self.mock_storage_client.side_effect = DefaultCredentialsError("Credentials error")
        
        with self.assertRaises(DefaultCredentialsError):
            connector = GCSConnector()
    
    def test_read_parquet(self):
        """Test reading a Parquet file."""
        # Mock the bucket and blob
        mock_bucket = MagicMock()
        mock_blob = MagicMock()
        
        self.mock_client_instance.bucket.return_value = mock_bucket
        mock_bucket.blob.return_value = mock_blob
        
        # Mock the download_as_bytes result
        mock_blob.download_as_bytes.return_value = b'mock_parquet_data'
        
        # Mock pandas read_parquet
        mock_df = pd.DataFrame({'col1': [1, 2], 'col2': ['a', 'b']})
        with patch('pandas.read_parquet', return_value=mock_df) as mock_read_parquet:
            # Call the method
            result = self.connector.read_parquet('test-bucket', 'test-blob.parquet')
            
            # Verify the mocks were called correctly
            self.mock_client_instance.bucket.assert_called_once_with('test-bucket')
            mock_bucket.blob.assert_called_once_with('test-blob.parquet')
            mock_blob.download_as_bytes.assert_called_once()
            
            # Verify the result
            self.assertIs(result, mock_df)
    
    def test_read_parquet_invalid_inputs(self):
        """Test that read_parquet validates its inputs."""
        # Test with empty bucket
        with self.assertRaises(ValueError):
            self.connector.read_parquet('', 'test-blob.parquet')
        
        # Test with empty blob name
        with self.assertRaises(ValueError):
            self.connector.read_parquet('test-bucket', '')
    
    def test_read_parquet_error(self):
        """Test that read_parquet handles errors properly."""
        # Mock the bucket and blob
        mock_bucket = MagicMock()
        mock_blob = MagicMock()
        
        self.mock_client_instance.bucket.return_value = mock_bucket
        mock_bucket.blob.return_value = mock_blob
        
        # Mock an error during download
        mock_blob.download_as_bytes.side_effect = Exception("Download error")
        
        # Call the method and check that it re-raises the exception
        with self.assertRaises(Exception) as context:
            self.connector.read_parquet('test-bucket', 'test-blob.parquet')
        
        # Verify the exception message
        self.assertEqual(str(context.exception), "Download error")
    
    def test_list_parquet_files(self):
        """Test listing Parquet files in a bucket."""
        # Create mock blobs
        mock_blob1 = MagicMock()
        mock_blob1.name = 'file1.parquet'
        mock_blob2 = MagicMock()
        mock_blob2.name = 'file2.txt'
        mock_blob3 = MagicMock()
        mock_blob3.name = 'folder/file3.parquet'
        
        # Mock the bucket and list_blobs
        mock_bucket = MagicMock()
        self.mock_client_instance.bucket.return_value = mock_bucket
        mock_bucket.list_blobs.return_value = [mock_blob1, mock_blob2, mock_blob3]
        
        # Call the method
        result = self.connector.list_parquet_files('test-bucket', 'prefix')
        
        # Verify the method was called correctly
        self.mock_client_instance.bucket.assert_called_once_with('test-bucket')
        mock_bucket.list_blobs.assert_called_once_with(prefix='prefix')
        
        # Verify the result contains only Parquet files
        expected_result = ['file1.parquet', 'folder/file3.parquet']
        self.assertEqual(result, expected_result)
    
    def test_get_object_metadata(self):
        """Test getting metadata for a blob."""
        # Mock the bucket and blob
        mock_bucket = MagicMock()
        mock_blob = MagicMock()
        
        self.mock_client_instance.bucket.return_value = mock_bucket
        mock_bucket.blob.return_value = mock_blob
        
        # Set properties on the mock blob
        mock_blob.size = 1024
        mock_blob.updated = 'timestamp'
        mock_blob.content_type = 'application/octet-stream'
        mock_blob.metadata = {'custom-key': 'custom-value'}
        
        # Call the method
        result = self.connector.get_object_metadata('test-bucket', 'test-blob')
        
        # Verify the calls
        self.mock_client_instance.bucket.assert_called_once_with('test-bucket')
        mock_bucket.blob.assert_called_once_with('test-blob')
        mock_blob.reload.assert_called_once()
        
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