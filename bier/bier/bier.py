'''
bier.py

Written by: Michael Dykes (michael.dykes@gov.bc.ca)
Created: May 29 2023

Purpose: Common classes + functions used by the Business Innovation and Emergency Response section at GeoBC
'''

import logging
import os
import sys
import json
import requests
from arcgis.gis import GIS
from minio import Minio

# Configure logging
_log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Generic Python Utilities
def set_logging_level(logging_level=logging.INFO):
    logging.basicConfig(level=logging_level)
    _log.info(f"Script console logging level set to - {logging.getLevelName(logging_level)}")

def find_config_file(script_file_path):
    config_file_path = os.path.join(os.path.dirname(script_file_path), 'config.json')
    try:
        with open(config_file_path) as json_conf:
            config_dict = json.load(json_conf)
        _log.info("Config file found")
        return config_dict
    except Exception as e:
        _log.warning(f"No config file found: {e}")
        return None

# ArcGIS Online Connection Class
class AGOConnection:
    def __init__(self, url=None, username=None, password=None):
        _log.info("Creating Connection to ArcGIS Online...")
        self.url = url or os.getenv('AGO_PORTAL_URL')
        self.username = username or os.getenv('AGO_USER')
        self.password = password or os.getenv('AGO_PASS')
        self.connection = None

        if not self.url or not self.username or not self.password:
            _log.warning("Missing AGO credentials")
            return
        
        try:
            self.connection = GIS(self.url, self.username, self.password, expiration=9999)
            _log.info("Connected to ArcGIS Online successfully")
        except Exception as e:
            _log.warning(f"Failed to connect to ArcGIS Online: {e}")

    def reconnect(self):
        try:
            self.connection = GIS(self.url, self.username, self.password, expiration=9999, verify_cert=False)
            _log.info("Reconnected to ArcGIS Online successfully")
        except Exception as e:
            _log.warning(f"Reconnection to ArcGIS Online failed: {e}")

    def disconnect(self):
        try:
            if self.connection:
                self.connection._con.logout()
                _log.info("Disconnected from ArcGIS Online")
        except Exception as e:
            _log.warning(f"Failed to disconnect: {e}")

# ArcGIS Online Item Class
class AGOItem:
    def __init__(self, ago_connection, itemid):
        self.ago_connection = ago_connection
        self.itemid = itemid
        self.item = ago_connection.connection.content.get(itemid)

    def delete_and_truncate(self, layer_num=0, attempts=5):
        attempt = 0
        while attempt < attempts:
            try:
                feature_layer = self.item.layers[layer_num]
                feature_count = feature_layer.query(return_count_only=True)
                feature_layer.delete_features(where="objectid >= 0")
                _log.info(f"Deleted {feature_count} features from ItemID: {self.itemid}")
                feature_layer.manager.truncate()
                _log.info("Data truncated successfully")
                return
            except Exception as e:
                _log.warning(f"Attempt {attempt + 1} failed: {e}")
                attempt += 1
                self.ago_connection.reconnect()
        raise RuntimeError(f"All {attempts} attempts failed. AGO update unsuccessful.")

    def append_data(self, new_features_list, layer_num=0, attempts=5):
        attempt = 0
        while attempt < attempts:
            try:
                result = self.item.layers[layer_num].edit_features(adds=new_features_list)
                _log.info(f"Appended {len(new_features_list)} features to ItemID: {self.itemid}")
                return result
            except Exception as e:
                _log.warning(f"Attempt {attempt + 1} failed: {e}")
                attempt += 1
                self.ago_connection.reconnect()
        raise RuntimeError(f"All {attempts} attempts failed. AGO update unsuccessful.")

    def add_field(self, new_field, layer_num=0):
        try:
            self.item.layers[layer_num].manager.add_to_definition({'fields': new_field})
            _log.info("Field added successfully")
        except Exception as e:
            _log.warning(f"Failed to add field: {e}")

# API Connection
def connect_to_api_json(url, encode=False):
    try:
        response = requests.get(url)
        if encode:
            response.encoding = response.apparent_encoding
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        _log.warning(f"API connection failed: {e}")
        return None

# S3 Connection Class
class S3Connection:
    def __init__(self, endpoint, access_key, secret_key):
        _log.info("Connecting to S3...")
        try:
            self.client = Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=True)
            self.bucket = self.client.list_buckets()[0]
            _log.info("Connected to S3 successfully")
        except Exception as e:
            _log.warning(f"Failed to connect to S3: {e}")

    def access_object(self, s3_path, filename, download=False, download_path=None):
        try:
            if download:
                if download_path:
                    self.client.fget_object(self.bucket.name, f"{s3_path}/{filename}", f"{download_path}/{filename}")
                    return f"{download_path}/{filename}"
                else:
                    _log.warning("Download path not provided")
                    return None
            return self.client.get_object(self.bucket.name, f"{s3_path}/{filename}")
        except Exception as e:
            _log.warning(f"Failed to access S3 object: {e}")
            return None

    def upload_object(self, s3_path, file_path, content_type=None, public=False, part_size=15728640):
        try:
            metadata = {"x-amz-acl": "public-read"} if public else None
            self.client.fput_object(self.bucket.name, s3_path, file_path, content_type=content_type, metadata=metadata, part_size=part_size)
            _log.info("File uploaded successfully")
        except Exception as e:
            _log.warning(f"Failed to upload file: {e}")
