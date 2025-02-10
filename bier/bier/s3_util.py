"""
bier function library (bier.py)
Author: Michael Dykes (michael.dykes@gov.bc.ca)
Created: May 29, 2023

Description:
        Contains common classes and functions used by the Business Innovation and Emergency Response section at GeoBC
        for GIS data processing and interaction with ArcGIS Online (AGO) and other GIS tools.

Dependencies:
    - arcgis
    - minio
    - tenacity
"""

import logging
import os
from minio import Minio

# Set script logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
_log = logging.getLogger(os.path.basename(os.path.splitext(__file__)[0]))

# S3 Connection Class
class S3Connection:
    """
    Manages connection to an S3-compatible storage.

    Attributes:
        client (Minio): The Minio S3 client.
        bucket: The first available S3 bucket.
    """

    def __init__(self, endpoint, access_key, secret_key):
        """
        Initializes an S3 connection.

        Args:
            endpoint (str): The S3 endpoint URL.
            access_key (str): The S3 access key.
            secret_key (str): The S3 secret key.
        """
        _log.info("Connecting to S3...")
        try:
            self.client = Minio(
                endpoint, access_key=access_key, secret_key=secret_key, secure=True
            )
            self.bucket = self.client.list_buckets()[0]
            _log.info("Connected to S3 successfully")
        except Exception as e:
            _log.warning(f"Failed to connect to S3: {e}")

    def upload_object(
        self, s3_path, file_path, content_type=None, public=False, part_size=15728640
    ):
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
            metadata = {"x-amz-acl": "public-read"} if public else None
            self.client.fput_object(
                self.bucket.name,
                s3_path,
                file_path,
                content_type=content_type,
                metadata=metadata,
                part_size=part_size,
            )
            _log.info("File uploaded successfully")
        except Exception as e:
            _log.warning(f"Failed to upload file: {e}")
