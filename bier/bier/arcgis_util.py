"""
bier arcgis function library (bier.py)
Author: Michael Dykes (michael.dykes@gov.bc.ca)
Created: February 10, 2025

Description:
        Contains common classes and functions used by the Business Innovation and Emergency Response section at GeoBC
        for GIS data processing and interaction with ArcGIS Online (AGO).

Dependencies:
    - arcgis
    - tenacity
"""

import logging
import os
from arcgis.gis import GIS
from arcgis.features import FeatureLayer
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type

# Set script logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
_log = logging.getLogger("bier_arcgis_util")

def retry_on_failure():
    """Decorator for retrying functions that interact with ArcGIS Online."""
    return retry(
        stop=stop_after_attempt(5),
        wait=wait_fixed(2),
        retry=retry_if_exception_type(Exception),
        reraise=True
    )

# ArcGIS Online Connection Class
class AGO:
    """
    Handles authentication and connection to ArcGIS Online.

    Attributes:
        url (str): The ArcGIS Online portal URL.
        username (str): The ArcGIS Online username.
        connection (GIS): The active GIS connection object.
    """

    def __init__(self, url: str, username: str, password: str) -> None:
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
        self.url: str = url or os.getenv("AGO_PORTAL_URL")
        self.username = username or os.getenv("AGO_USER")
        self._password = password or os.getenv("AGO_PASS")
        self.connection: GIS = self._connect(self._password)

    @retry_on_failure()
    def _connect(self, password: str) -> GIS:
        """Attempts to establish a connection to ArcGIS Online."""
        try:
            connection = GIS(self.url, self.username, self._password, expiration=9999)
            _log.info("Connected to ArcGIS Online successfully")
            return connection
        except Exception as e:
            _log.error(f"Failed to connect to ArcGIS Online: {e}")
            raise

    def reconnect(self) -> None:
        """Re-establishes the ArcGIS Online connection."""
        _log.info("Reconnecting to ArcGIS Online...")
        self.connection = self._connect()

    def disconnect(self) -> None:
        """Logs out and disconnects from ArcGIS Online."""
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
        ago_connection (AGO): The ArcGIS Online connection object.
        itemid (str): The unique identifier of the ArcGIS Online item.
        item (Item): The ArcGIS Online item object.
    """

    def __init__(self, ago_connection: AGO, itemid: str) -> None:
        """
        Initializes an ArcGIS Online item.

        Args:
            ago_connection (AGO): An established ArcGIS Online connection.
            itemid (str): The ID of the ArcGIS Online item.
        """
        self.ago_connection = ago_connection
        self.itemid = itemid
        self.item = self.ago_connection.connection.content.get(itemid)
        if not self.item:
            raise ValueError(f"Item with ID {itemid} not found.")

    def get_layer(self, layer_num: int = 0) -> FeatureLayer:
        """Retrieves a specific layer from the item."""
        try:
            return self.item.layers[layer_num]
        except IndexError:
            raise ValueError(f"Layer {layer_num} does not exist in item {self.itemid}.")

    @retry_on_failure()
    def delete_and_truncate(self, layer_num: int = 0) -> None:
        """Deletes all features and truncates a layer."""
        try:
            feature_layer = self.get_layer(layer_num)
            feature_count = feature_layer.query(return_count_only=True)
            feature_layer.delete_features(where="1=1")
            _log.info(f"Deleted {feature_count} features from ItemID: {self.itemid}")
            feature_layer.manager.truncate()
            _log.info("Data truncated successfully")
        except Exception as e:
            _log.error(f"Failed to delete and truncate: {e}")
            self.ago_connection.reconnect()
            raise

    @retry_on_failure()
    def append_data(self, new_features_list, layer_num: int = 0) -> None:
        """Adds new features to a hosted feature layer."""
        if not new_features_list:
            _log.warning("No features provided for append operation.")
            return
        
        try:
            feature_layer = self.get_layer(layer_num)
            result = feature_layer.edit_features(adds=new_features_list)
            _log.debug(result)
            _log.info(f"Added {len(new_features_list)} new features to ItemID: {self.itemid}")
        except Exception as e:
            _log.error(f"Failed to append data: {e}")
            self.ago_connection.reconnect()
            raise

    @retry_on_failure()
    def add_field(self, new_field, layer_num: int = 0) -> None:
        """Adds a new field to the feature layer."""
        try:
            feature_layer = self.get_layer(layer_num)
            result = feature_layer.manager.add_to_definition({"fields": new_field})
            _log.debug(result)
            _log.info(f"Added new field to ItemID: {self.itemid}")
        except Exception as e:
            _log.error(f"Failed to add field: {e}")
            raise
