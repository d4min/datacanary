import os
import io
from typing import Optional, List, Dict, Any, BinaryIO
from google.cloud import storage
from google.oauth2 import service_account

class GCSConnector:
    """
    A connector class for Google Cloud Storage operations.
    """
    
    def __init__(
        self,
        service_account_info: Optional[Dict[str, Any]] = None,
        service_account_file: Optional[str] = None,
        project_id: Optional[str] = None
    ):
        """
        Initialize a Google Cloud Storage connector.
        
        Args:
            service_account_info: Dictionary containing service account info
            service_account_file: Path to service account JSON file
            project_id: Google Cloud project ID
        """
        self.credentials = None
        self.project_id = project_id
        
        if service_account_info:
            self.credentials = service_account.Credentials.from_service_account_info(service_account_info)
        elif service_account_file:
            self.credentials = service_account.Credentials.from_service_account_file(service_account_file)
        
        if self.credentials and project_id:
            self.client = storage.Client(credentials=self.credentials, project=project_id)
        else:
            # Use default credentials from environment
            self.client = storage.Client()
    
    def list_buckets(self) -> List[str]:
        """
        List all buckets in the project.
        
        Returns:
            List of bucket names
        """
        return [bucket.name for bucket in self.client.list_buckets()]
    
    def bucket_exists(self, bucket_name: str) -> bool:
        """
        Check if a bucket exists.
        
        Args:
            bucket_name: Name of the bucket
            
        Returns:
            True if bucket exists, False otherwise
        """
        bucket = self.client.bucket(bucket_name)
        return bucket.exists()
    
    def create_bucket(self, bucket_name: str, location: str = "us-central1") -> bool:
        """
        Create a new bucket.
        
        Args:
            bucket_name: Name of the new bucket
            location: Location for the bucket
            
        Returns:
            True if creation was successful, False otherwise
        """
        try:
            bucket = self.client.bucket(bucket_name)
            bucket.create(location=location)
            return True
        except Exception as e:
            print(f"Error creating bucket: {str(e)}")
            return False
    
    def delete_bucket(self, bucket_name: str, force: bool = False) -> bool:
        """
        Delete a bucket.
        
        Args:
            bucket_name: Name of the bucket to delete
            force: If True, delete all objects in the bucket before deleting
            
        Returns:
            True if deletion was successful, False otherwise
        """
        try:
            bucket = self.client.bucket(bucket_name)
            
            if force:
                blobs = bucket.list_blobs()
                for blob in blobs:
                    blob.delete()
            
            bucket.delete()
            return True
        except Exception as e:
            print(f"Error deleting bucket: {str(e)}")
            return False
    
    def list_objects(self, bucket_name: str, prefix: str = "") -> List[str]:
        """
        List objects in a bucket.
        
        Args:
            bucket_name: Name of the bucket
            prefix: Optional prefix to filter objects
            
        Returns:
            List of object keys/names
        """
        bucket = self.client.bucket(bucket_name)
        blobs = bucket.list_blobs(prefix=prefix)
        return [blob.name for blob in blobs]
    
    def object_exists(self, bucket_name: str, object_name: str) -> bool:
        """
        Check if an object exists in a bucket.
        
        Args:
            bucket_name: Name of the bucket
            object_name: Name of the object
            
        Returns:
            True if object exists, False otherwise
        """
        bucket = self.client.bucket(bucket_name)
        blob = bucket.blob(object_name)
        return blob.exists()
    
    def upload_file(
        self, 
        bucket_name: str, 
        object_name: str, 
        file_path: str, 
        content_type: Optional[str] = None
    ) -> bool:
        """
        Upload a file to a bucket.
        
        Args:
            bucket_name: Name of the bucket
            object_name: Name to give the object in GCS
            file_path: Path to the file to upload
            content_type: Optional content type for the file
            
        Returns:
            True if upload was successful, False otherwise
        """
        try:
            bucket = self.client.bucket(bucket_name)
            blob = bucket.blob(object_name)
            
            with open(file_path, "rb") as file:
                if content_type:
                    blob.upload_from_file(file, content_type=content_type)
                else:
                    blob.upload_from_file(file)
            
            return True
        except Exception as e:
            print(f"Error uploading file: {str(e)}")
            return False
    
    def upload_fileobj(
        self, 
        bucket_name: str, 
        object_name: str, 
        fileobj: BinaryIO, 
        content_type: Optional[str] = None
    ) -> bool:
        """
        Upload a file-like object to a bucket.
        
        Args:
            bucket_name: Name of the bucket
            object_name: Name to give the object in GCS
            fileobj: File-like object to upload
            content_type: Optional content type for the file
            
        Returns:
            True if upload was successful, False otherwise
        """
        try:
            bucket = self.client.bucket(bucket_name)
            blob = bucket.blob(object_name)
            
            if content_type:
                blob.upload_from_file(fileobj, content_type=content_type)
            else:
                blob.upload_from_file(fileobj)
            
            return True
        except Exception as e:
            print(f"Error uploading file object: {str(e)}")
            return False
    
    def download_file(self, bucket_name: str, object_name: str, file_path: str) -> bool:
        """
        Download an object to a file.
        
        Args:
            bucket_name: Name of the bucket
            object_name: Name of the object to download
            file_path: Path where to save the file
            
        Returns:
            True if download was successful, False otherwise
        """
        try:
            bucket = self.client.bucket(bucket_name)
            blob = bucket.blob(object_name)
            
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            blob.download_to_filename(file_path)
            
            return True
        except Exception as e:
            print(f"Error downloading file: {str(e)}")
            return False
    
    def download_fileobj(self, bucket_name: str, object_name: str) -> Optional[bytes]:
        """
        Download an object as bytes.
        
        Args:
            bucket_name: Name of the bucket
            object_name: Name of the object to download
            
        Returns:
            Object content as bytes or None if failed
        """
        try:
            bucket = self.client.bucket(bucket_name)
            blob = bucket.blob(object_name)
            
            file_stream = io.BytesIO()
            blob.download_to_file(file_stream)
            file_stream.seek(0)
            
            return file_stream.read()
        except Exception as e:
            print(f"Error downloading file to memory: {str(e)}")
            return None
    
    def delete_object(self, bucket_name: str, object_name: str) -> bool:
        """
        Delete an object from a bucket.
        
        Args:
            bucket_name: Name of the bucket
            object_name: Name of the object to delete
            
        Returns:
            True if deletion was successful, False otherwise
        """
        try:
            bucket = self.client.bucket(bucket_name)
            blob = bucket.blob(object_name)
            blob.delete()
            
            return True
        except Exception as e:
            print(f"Error deleting object: {str(e)}")
            return False
    
    def delete_objects(self, bucket_name: str, object_names: List[str]) -> Dict[str, bool]:
        """
        Delete multiple objects from a bucket.
        
        Args:
            bucket_name: Name of the bucket
            object_names: List of object names to delete
            
        Returns:
            Dictionary mapping object names to success/failure status
        """
        results = {}
        bucket = self.client.bucket(bucket_name)
        
        for object_name in object_names:
            try:
                blob = bucket.blob(object_name)
                blob.delete()
                results[object_name] = True
            except Exception:
                results[object_name] = False
        
        return results
    
    def copy_object(
        self, 
        source_bucket: str, 
        source_object: str, 
        dest_bucket: str, 
        dest_object: str
    ) -> bool:
        """
        Copy an object from one location to another.
        
        Args:
            source_bucket: Name of the source bucket
            source_object: Name of the source object
            dest_bucket: Name of the destination bucket
            dest_object: Name to give the object in the destination bucket
            
        Returns:
            True if copy was successful, False otherwise
        """
        try:
            source_bucket = self.client.bucket(source_bucket)
            source_blob = source_bucket.blob(source_object)
            
            destination_bucket = self.client.bucket(dest_bucket)
            
            source_bucket.copy_blob(
                source_blob, destination_bucket, new_name=dest_object
            )
            
            return True
        except Exception as e:
            print(f"Error copying object: {str(e)}")
            return False
    
    def generate_signed_url(
        self,
        bucket_name: str,
        object_name: str,
        expiration: int = 3600,
        method: str = "GET"
    ) -> Optional[str]:
        """
        Generate a signed URL for an object.
        
        Args:
            bucket_name: Name of the bucket
            object_name: Name of the object
            expiration: Time in seconds until the URL expires
            method: HTTP method allowed (GET, PUT, etc.)
            
        Returns:
            Signed URL or None if generation failed
        """
        try:
            bucket = self.client.bucket(bucket_name)
            blob = bucket.blob(object_name)
            
            url = blob.generate_signed_url(
                version="v4",
                expiration=expiration,
                method=method
            )
            
            return url
        except Exception as e:
            print(f"Error generating signed URL: {str(e)}")
            return None