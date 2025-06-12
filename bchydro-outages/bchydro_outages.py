"""
BC Hydro Outages

Written by: Michael Dykes (michael.dykes@gov.bc.ca) and Paulina Marczak (paulina.marczak@gov.bc.ca)
Created: May 27, 2021

Purpose: Grab BC Hydro Web Content (from https://www.bchydro.com/power-outages/app/outage-map.html) and update ArcGIS Online Hosted Feature Layer
"""

# Import libraries/modules
import os
import sys
import datetime
import logging
from arcgis.gis import GIS
from arcgis.features import FeatureLayer
from arcgis import geometry, features
from dotenv import load_dotenv
from typing import Optional, Dict, Any
from tenacity import retry, stop_after_attempt, wait_fixed

# Set script logger
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
_log = logging.getLogger(os.path.basename(os.path.splitext(__file__)[0]))

# Function to check if exception should trigger a retry
def is_retryable_exception(exception: Exception) -> bool:
    """Determines if an exception should trigger a retry."""
    return isinstance(exception, (requests.exceptions.Timeout, requests.exceptions.ConnectionError))

# Retry decorator with exponential backoff
@retry(
    stop=stop_after_attempt(3),  # Maximum 3 retries
    wait=wait_exponential(multiplier=1, min=2, max=10),  # Exponential backoff (2s, 4s, 8s)
    retry=retry_if_exception_type(requests.exceptions.RequestException),  # Retry on request failures
    reraise=True,
)
def connect_to_api(
    url: str,
    method: str = "GET",
    encode: bool = False,
    headers: Optional[Dict[str, str]] = None,
    params: Optional[Dict[str, Any]] = None,
    data: Optional[Dict[str, Any]] = None,
    json: Optional[Dict[str, Any]] = None,
) -> Optional[Dict[str, Any]]:
    """
    Connects to an API and retrieves JSON data using GET or POST, with retry logic.

    Args:
        url (str): The API endpoint URL.
        method (str): HTTP method ("GET" or "POST"). Defaults to "GET".
        encode (bool): Whether to set response encoding. Defaults to False.
        headers (Optional[Dict[str, str]]): Custom headers for the request. Defaults to None.
        params (Optional[Dict[str, Any]]): Query parameters for GET requests. Defaults to None.
        data (Optional[Dict[str, Any]]): Form-encoded data for POST requests. Defaults to None.
        json (Optional[Dict[str, Any]]): JSON data for POST requests. Defaults to None.

    Returns:
        Optional[Dict[str, Any]]: The JSON response data if successful, otherwise None.
    """
    try:
        response: requests.Response
        if method.upper() == "GET":
            response = requests.get(url, headers=headers, params=params, timeout=10)
        elif method.upper() == "POST":
            response = requests.post(url, headers=headers, params=params, data=data, json=json, timeout=10)
        else:
            _log.error(f"Unsupported HTTP method: {method}")
            return None

        if encode:
            response.encoding = response.apparent_encoding

        response.raise_for_status()
        return response.json()

    except requests.exceptions.Timeout:
        _log.warning(f"API request timed out ({method} {url}), retrying...")
        raise  # Will trigger retry

    except requests.exceptions.HTTPError as http_err:
        if response and 500 <= response.status_code < 600:
            _log.warning(f"Server error ({response.status_code}) ({method} {url}), retrying...")
            raise  # Will trigger retry
        _log.error(f"HTTP error occurred ({method} {url}): {http_err}")

    except requests.exceptions.RequestException as req_err:
        _log.error(f"API connection failed ({method} {url}): {req_err}")
        raise  # Will trigger retry

    return None

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

def import_environment_variables_from_file():
    """
    Import Environment Variables from File (if necessary)
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))
    environment_file_path = os.path.join(script_dir, "environment.env")

    if os.path.exists(environment_file_path):
        load_dotenv(dotenv_path=environment_file_path)
        _log.info("Environment Variables Imported Successfully")
    else:
        _log.warning("Environment File Not Found, Using System Environment Variables")


def fetch_bchydro_data(api_url):
    """
    Fetch BC Hydro outage data from the API with retry logic
    """

    @retry(
        stop=stop_after_attempt(3), wait=wait_fixed(5)
    )  # Retry up to 3 times with 5-second wait between
    def get_data():
        try:
            response = connect_to_api(api_url)
            if response is None:
                _log.error("BC Hydro API returned None. Check API status.")
                return []
            return response
        except Exception as e:
            _log.error(f"Failed to fetch BC Hydro data: {e}")
            raise

    return get_data()


def update_bchydro_outages_ago(bchydro_data, bchydro_item):
    """
    Clear existing features and append new features to the BC Hydro Outages hosted feature layer in AGO
    """
    if not bchydro_data:
        _log.info("No BC Hydro Outages found")
        return

    try:
        _log.info(f"{len(bchydro_data)} BC Hydro Outages found. Updating AGO...")
        bchydro_item.delete_and_truncate()

        new_features = []
        for row in bchydro_data:
            try:
                # Extract and format outage polygon coordinates
                latlong_list = [
                    list(a) for a in zip(row["polygon"][::2], row["polygon"][1::2])
                ]
                geom = geometry.Geometry(
                    {
                        "type": "Polygon",
                        "rings": [latlong_list],
                        "spatialReference": {"wkid": 4326},
                    }
                )

                # Convert timestamps safely
                def convert_timestamp(ts):
                    return (
                        datetime.datetime.utcfromtimestamp(ts / 1000)
                        .replace(tzinfo=datetime.timezone.utc)
                        .astimezone(tz=None)
                        if ts
                        else None
                    )

                attributes = {
                    "OUTAGE_ID": row.get("id"),
                    "GIS_ID": row.get("gisId"),
                    "REGION_ID": row.get("regionId"),
                    "REGION": row.get("regionName"),
                    "MUNI": row.get("municipality"),
                    "DETAILS": str(row.get("area", "")),
                    "CAUSE": row.get("cause"),
                    "AFFECTED": row.get("numCustomersOut"),
                    "CREW_STATUS": row.get("crewStatusDescription"),
                    "EST_TIME_ON": convert_timestamp(row.get("dateOn")),
                    "OFFTIME": convert_timestamp(row.get("dateOff")),
                    "UPDATED": convert_timestamp(row.get("lastUpdated")),
                    "CREW_ETA": convert_timestamp(row.get("crewEta")),
                    "CREW_ETR": convert_timestamp(row.get("crewEtr")),
                    "SHOW_ETA": row.get("showEta"),
                    "SHOW_ETR": row.get("showEtr"),
                }
                new_features.append(features.Feature(geom, attributes))
            except Exception as fe:
                _log.error(f"Error processing outage data: {fe}")

        if new_features:
            # Batch processing for large datasets (break into chunks of 500)
            batch_size = 500
            for i in range(0, len(new_features), batch_size):
                bchydro_item.append_data(new_features[i : i + batch_size])
                _log.info(
                    f"Batch {i // batch_size + 1} of {len(new_features) // batch_size + 1} appended to AGO"
                )
        else:
            _log.warning("No valid features to update AGO")
    except Exception as e:
        _log.error(f"Error updating AGO: {e}")
        _log.exception("Stack Trace:")
        raise


def main():
    """
    Run the script
    """
    import_environment_variables_from_file()

    try:
        AGO_Portal_URL = os.getenv("AGO_PORTAL_URL")
        HydroOutages_ItemID = os.getenv("HYDROOUTAGES_ITEMID")
        HydroOutagesLFN_ItemID = os.getenv("HYDROOUTAGES_LFN_ITEMID")
        BCHYDRO_API_URL = os.getenv(
            "BCHYDRO_API_URL",
            "https://www.bchydro.com/power-outages/app/outages-map-data.json",
        )

        if not (AGO_Portal_URL and HydroOutages_ItemID and HydroOutagesLFN_ItemID):
            _log.critical("Missing required environment variables for AGO. Exiting.")
            sys.exit(1)

        AGO = arcgis_util.AGO(AGO_Portal_URL,os.getenv("AGO_USER"),os.getenv("AGO_PASS"))
        bchydro_data = fetch_bchydro_data(BCHYDRO_API_URL)

        if bchydro_data:
            HydroOutages_item = AGOItem(AGO, HydroOutages_ItemID)
            update_bchydro_outages_ago(bchydro_data, HydroOutages_item)

            HydroOutagesLFN_item = AGOItem(AGO, HydroOutagesLFN_ItemID)
            update_bchydro_outages_ago(bchydro_data, HydroOutagesLFN_item)

        AGO.disconnect()
        _log.info("**Script completed successfully**")
    except Exception as e:
        _log.critical(f"Script execution failed: {e}")
        _log.exception("Stack Trace:")
        sys.exit(1)


if __name__ == "__main__":
    main()
