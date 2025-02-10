"""
bier s3 function library (bier.py)
Author: Michael Dykes (michael.dykes@gov.bc.ca)
Created: February 10, 2025

Description:
        Contains common classes and functions used by the Business Innovation and Emergency Response section at GeoBC
        for working with S3 Object Storage.

Dependencies:
    - minio
    - tenacity
"""

import logging
import os
from typing import Optional
from minio import Minio
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type

# Set script logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
_log = logging.getLogger(os.path.basename(os.path.splitext(__file__)[0]))

# S3 Connection Class
class S3:
    """
    Manages connection to an S3-compatible storage.

    Attributes:
        client (Minio): The Minio S3 client.
        bucket (object): The specified S3 bucket, or first available bucket.
    """

    def __init__(self, endpoint: str, access_key: str, secret_key: str, bucket_name: Optional[str] = None) -> None:
        """
        Initializes an S3 connection.

        Args:
            endpoint (str): The S3 endpoint URL.If not provided,
                                      it is read from `S3_ENDPOINT`.
            access_key (str): The S3 access key.If not provided,
                                      it is read from `S3_ACCESSKEY`.
            secret_key (str): The S3 secret key.If not provided,
                                      it is read from `S3_SECRETKEY`.
            bucket_name (str, optional): The S3 bucket name. Defaults to None.
        """
        self.endpoint: str = endpoint or os.getenv("S3_ENDPOINT")
        self.access_key: str = access_key or os.getenv("S3_ACCESSKEY")
        self.secret_key: str = secret_key or os.getenv("S3_SECRETKEY")
        self.bucket_name: str = bucket_name
        self._connect_to_s3()

    @retry(
        stop=stop_after_attempt(3),  # Retry up to 3 times
        wait=wait_fixed(2),  # Wait 2 seconds between retries
        retry=retry_if_exception_type(Exception),  # Retry on any exception
    )
    def _connect_to_s3(self) -> None:
        """Handles connection to the S3 storage."""
        _log.info("Connecting to S3...")
        try:
            self.client = Minio(
                self.endpoint,
                access_key=self.access_key,
                secret_key=self.secret_key,
                secure=True,
            )
            buckets: list[object] = self.client.list_buckets()
            if self.bucket_name:
                # If a specific bucket name is provided, use it
                self.bucket: object = next(
                    (bucket for bucket in buckets if bucket.name == self.bucket_name), None
                )
                if self.bucket:
                    _log.info(f"Connected to S3. Using specified bucket: {self.bucket.name}")
                else:
                    _log.warning(f"Bucket {self.bucket_name} not found.")
                    self.bucket = None
            else:
                # If no bucket name is provided, use the first available bucket
                if buckets:
                    self.bucket: object = buckets[0]
                    _log.info(f"Connected to S3. Using first available bucket: {self.bucket.name}")
                else:
                    _log.warning("No buckets available in S3.")
                    self.bucket: object = None
        except Exception as e:
            _log.warning(f"Failed to connect to S3: {e}")
            raise

    @retry(
        stop=stop_after_attempt(3),  # Retry up to 3 times
        wait=wait_fixed(2),  # Wait 2 seconds between retries
        retry=retry_if_exception_type(Exception),  # Retry on any exception
    )
    def upload_object(
        self, s3_path: str, file_path: str, content_type: Optional[str], public: Optional[bool] = None, part_size: int =15728640
    ) -> None:
        """
        Uploads an object to an S3 bucket.

        Args:
            s3_path (str): The S3 object path.
            file_path (str): The local file path.
            content_type (str, optional): The MIME type of the file. Defaults to None.
            public (bool, optional): Whether to make the object public. Defaults to False.
            part_size (int, optional): The multipart upload part size. Defaults to 15MB.

        Raises:
            Exception: If upload fails.
        """
        try:
            metadata: Optional[dict[str,str]] = {"x-amz-acl": "public-read"} if public else None
            self.client.fput_object(
                self.bucket.name,
                s3_path,
                file_path,
                content_type=content_type,
                metadata=metadata,
                part_size=part_size,
            )
            _log.info(f"File {file_path} uploaded successfully to {s3_path}")
        except Exception as e:
            _log.warning(f"Failed to upload {file_path} to {s3_path}: {e}")
            raise

    @retry(
        stop=stop_after_attempt(3),  # Retry up to 3 times
        wait=wait_fixed(2),  # Wait 2 seconds between retries
        retry=retry_if_exception_type(Exception),  # Retry on any exception
    )
    def download_object(self, s3_path: str, file_path: str) -> None:
        """
        Downloads an object from the S3 bucket to the local file system.

        Args:
            s3_path (str): The S3 object path.
            file_path (str): The local file path where the object should be saved.

        Raises:
            Exception: If download fails.
        """
        try:
            self.client.fget_object(self.bucket.name, s3_path, file_path)
            _log.info(f"File downloaded successfully from {s3_path} to {file_path}")
        except Exception as e:
            _log.warning(f"Failed to download {s3_path} to {file_path}: {e}")
            raise
