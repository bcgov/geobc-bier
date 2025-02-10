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
import requests

# Set script logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
_log = logging.getLogger(os.path.basename(os.path.splitext(__file__)[0]))

# API Connection
def connect_to_api_json(url, encode=False):
    """
    Connects to an API and retrieves JSON data.

    Args:
        url (str): The API endpoint URL.
        encode (bool, optional): Whether to set response encoding. Defaults to False.

    Returns:
        dict or None: The JSON response data if successful, otherwise None.
    """
    try:
        response = requests.get(url, timeout=10)
        if encode:
            response.encoding = response.apparent_encoding
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        _log.warning(f"API connection failed: {e}")
        return None