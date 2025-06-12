"""More actions
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
import bier
from arcgis import geometry, features
from dotenv import load_dotenv
from tenacity import retry, stop_after_attempt, wait_fixed

# Set script logger
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
_log = logging.getLogger(os.path.basename(os.path.splitext(__file__)[0]))


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
            response = bier.connect_to_api(api_url)
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

        AGO = bier.AGO(AGO_Portal_URL,os.getenv("AGO_USER"),os.getenv("AGO_PASS"))
        bchydro_data = fetch_bchydro_data(BCHYDRO_API_URL)

        if bchydro_data:
            HydroOutages_item = bier.AGOItem(AGO, HydroOutages_ItemID)
            update_bchydro_outages_ago(bchydro_data, HydroOutages_item)

            HydroOutagesLFN_item = bier.AGOItem(AGO, HydroOutagesLFN_ItemID)
            update_bchydro_outages_ago(bchydro_data, HydroOutagesLFN_item)

        AGO.disconnect()
        _log.info("**Script completed successfully**")
    except Exception as e:
        _log.critical(f"Script execution failed: {e}")
        _log.exception("Stack Trace:")
        sys.exit(1)


if __name__ == "__main__":
    main()
