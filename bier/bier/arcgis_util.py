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
from arcgis.gis import GIS, Item
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type

# Set script logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
_log = logging.getLogger(os.path.basename(os.path.splitext(__file__)[0]))

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
        self.username: str = username or os.getenv("AGO_USER")
        self.password: str = password or os.environ.get("AGO_PASS")
        self.connection: GIS = self._connect(self.password)

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_fixed(2),
        retry=retry_if_exception_type(Exception),
        reraise=True
    )
    def _connect(self, password: str) -> GIS:
        """Attempts to establish a connection to ArcGIS Online."""
        try:
            connection: GIS = GIS(self.url, self.username, password, expiration=9999)
            _log.info("Connected to ArcGIS Online successfully")
            return connection
        except Exception as e:
            _log.error(f"Failed to connect to ArcGIS Online: {e}")
            raise

    def reconnect(self) -> None:
        """Re-establishes the ArcGIS Online connection."""
        try:
            self.connection: GIS = self._connect(self.password)
            _log.info("Reconnected to ArcGIS Online successfully")
        except Exception as e:
            _log.warning(f"Reconnection to ArcGIS Online failed: {e}")

    def disconnect(self) -> None:
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
        self.itemid: str = itemid
        self.item: Item = ago_connection.connection.content.get(itemid)

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_fixed(2),
        retry=retry_if_exception_type(Exception),
        reraise=True
    )
    def delete_and_truncate(self, layer_num: int = 0) -> None:
        """
        Deletes all features from a layer and truncates the data.
        """
        try:
            feature_layer = self.item.layers[layer_num]
            feature_count = feature_layer.query(return_count_only=True)
            feature_layer.delete_features(where="objectid >= 0")
            _log.info(f"Deleted {feature_count} features from ItemID: {self.itemid}")
            feature_layer.manager.truncate()
            _log.info("Data truncated successfully")
        except Exception as e:
            _log.warning(f"Delete and truncate attempt failed: {e}")
            self.ago_connection.reconnect()
            raise

    def append_data(self, new_features_list, layer_num=0, attempts=5):
        '''
        Add Features to Hosted Feature Layer

            Parameters:
                    new_features_list (list): List of New Features (ArcGIS API for Python Objects) to Append
                    layer_num (int): Index Number for Layer from Hosted Feature Layer  
                    attempts (int): Number of Attempts to Try to Append New Features
        '''
        attempt = 0
        success = False
        # 5 attempts to connect and update the layer 
        while attempt < attempts and not success:
            try:
                # Attempt to update ago feature layer
                result = self.item.layers[layer_num].edit_features(adds = new_features_list)
                _log.debug(result)
                success = True
                _log.info(f"Finished creating {len(new_features_list)} new features in AGO - ItemID: {self.itemid}")
            except:
                # If attempt fails, retry attempt (up to 5 times then exit script if unsuccessful)
                _log.warning(f"Re-Attempting AGO Update. Attempt Number {attempt}")
                attempt += 1
                self.ago_connection.reconnect()
                if attempt == 5:
                    _log.critical(f"***No More Attempts Left. AGO Update Failed***")
                    sys.exit(1)

    def add_field(self, new_field, layer_num=0):
        self.item.layers[layer_num].manager.add_to_definition({'fields':new_field})
