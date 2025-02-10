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
from arcgis.gis import GIS

# Set script logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
_log = logging.getLogger(os.path.basename(os.path.splitext(__file__)[0]))

# ArcGIS Online Connection Class
class AGOConnection:
    """
    Handles authentication and connection to ArcGIS Online.

    Attributes:
        url (str): The ArcGIS Online portal URL.
        username (str): The ArcGIS Online username.
        password (str): The ArcGIS Online password.
        connection (GIS or None): The active GIS connection object.
    """

    def __init__(self, url=None, username=None, password=None):
        """
        Initializes an ArcGIS Online connection.

        Args:
            url (str): The ArcGIS Online portal URL. If not provided,
                                 it is read from the environment variable `AGO_PORTAL_URL`.
            username (str): The ArcGIS Online username. If not provided,
                                      it is read from `AGO_USER`.
            password (str): The ArcGIS Online password. If not provided,
                                      it is read from `AGO_PASS`.
        """
        _log.info("Creating connection to ArcGIS Online...")
        self.url = url or os.getenv("AGO_PORTAL_URL")
        self.username = username or os.getenv("AGO_USER")
        self.password = password or os.getenv("AGO_PASS")
        self.connection = None

        if not all([self.url, self.username, self.password]):
            _log.warning("Missing AGO credentials")
            return

        self._connect()

    def _connect(self):
        """Attempts to establish a connection to ArcGIS Online."""
        try:
            self.connection = GIS(
                self.url, self.username, self.password, expiration=9999
            )
            _log.info("Connected to ArcGIS Online successfully")
        except Exception as e:
            _log.warning(f"Failed to connect to ArcGIS Online: {e}")

    def reconnect(self):
        """Re-establishes the ArcGIS Online connection."""
        try:
            self.connection = GIS(
                self.url, self.username, self.password, expiration=9999
            )
            _log.info("Reconnected to ArcGIS Online successfully")
        except Exception as e:
            _log.warning(f"Reconnection to ArcGIS Online failed: {e}")

    def disconnect(self):
        """Logs out and disconnects from ArcGIS Online."""
        if self.connection:
            try:
                self.connection._con.logout()
                _log.info("Disconnected from ArcGIS Online")
            except Exception as e:
                _log.warning(f"Failed to disconnect: {e}")


# ArcGIS Online Item Class
class AGOItem:
    """
    Represents an item in ArcGIS Online.

    Attributes:
        ago_connection (object): The ArcGIS Online connection instance.
        itemid (str): The unique identifier of the ArcGIS Online item.
        item (object): The ArcGIS Online item object.
    """

    def __init__(self, ago_connection, itemid):
        """
        Initializes an ArcGIS Online item.

        Args:
            ago_connection (object): An established ArcGIS Online connection.
            itemid (str): The ID of the ArcGIS Online item.
        """
        self.ago_connection = ago_connection
        self.itemid = itemid
        self.item = ago_connection.connection.content.get(itemid)

    def delete_and_truncate(self, layer_num=0, attempts=5):
        """
        Deletes all features from a layer and truncates the data.

        Args:
            layer_num (int, optional): The layer index. Defaults to 0.
            attempts (int, optional): Number of attempts before failure. Defaults to 5.

        Raises:
            RuntimeError: If all attempts fail.
        """
        for attempt in range(attempts):
            try:
                feature_layer = self.item.layers[layer_num]
                feature_count = feature_layer.query(return_count_only=True)
                feature_layer.delete_features(where="objectid >= 0")
                _log.info(
                    f"Deleted {feature_count} features from ItemID: {self.itemid}"
                )
                feature_layer.manager.truncate()
                _log.info("Data truncated successfully")
                return
            except Exception as e:
                _log.warning(f"Attempt {attempt + 1} failed: {e}")
                self.ago_connection.reconnect()
        raise RuntimeError(
            f"All {attempts} attempts failed. AGO update unsuccessful."
        )