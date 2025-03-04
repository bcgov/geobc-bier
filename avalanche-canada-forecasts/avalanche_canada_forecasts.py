"""
Avalanche Canada Forecasts

Written by: Michael Dykes (michael.dykes@gov.bc.ca)
Created: January 24 2023

Purpose: Get avalanche forecast from Avalanche Canada API - https://api.avalanche.ca/ - for display in EM GeoHub
"""

"""
Avalanche Canada Forecasts (avalanche_canada_forecasts.py)
Author: Michael Dykes (michael.dykes@gov.bc.ca)
Created: January 24 2023

Description:
        Get avalanche forecast from Avalanche Canada API - https://api.avalanche.ca/ - for display in EM GeoHub

Dependencies:
    - arcgis
    - bier
"""

import os
import sys
import datetime
import logging
from typing import Dict, Any, Optional

import bier  # Ensure this module is installed and properly configured
from arcgis import geometry, features

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
_log = logging.getLogger(os.path.basename(os.path.splitext(__file__)[0]))


def format_avalanche_forecast_data(
    avalanche_geometry_data: Optional[Dict[str, Any]],
    avalanche_attribute_data: Optional[Any],
) -> Dict[str, Dict[str, Any]]:
    """Combine Avalanche Canada geometry and attribute data into a dictionary."""
    if not avalanche_geometry_data or "features" not in avalanche_geometry_data:
        _log.warning("No Avalanche Canada geometry data found.")
        sys.exit(1)

    _log.info(f"{len(avalanche_geometry_data['features'])} avalanche forecasts found.")
    avalanche_dict = {}

    for row in avalanche_geometry_data["features"]:
        avalanche_dict[row["id"]] = {"geometry": row["geometry"]}

    if avalanche_attribute_data:
        for row in avalanche_attribute_data:
            area_id = row.get("area", {}).get("id")
            url = row.get("url")
            if area_id in avalanche_dict:
                avalanche_dict[area_id]["attributes"] = row.get("report", {})
                avalanche_dict[area_id]["url"] = url

    return avalanche_dict


def parse_iso_datetime(date_str: Optional[str]) -> Optional[datetime.datetime]:
    """Parse an ISO date string to a datetime object, handling various formats."""
    if not date_str:
        return None
    try:
        if "00:00Z" in date_str:
            return (
                datetime.datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%SZ")
                .replace(tzinfo=datetime.timezone.utc)
                .astimezone()
            )
        return (
            datetime.datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%S.%fZ")
            .replace(tzinfo=datetime.timezone.utc)
            .astimezone()
        )
    except ValueError as e:
        _log.exception(f"Error parsing datetime string: {date_str}")
        return None


def update_avalanche_forecast(
    avalanche_dict: Dict[str, Dict[str, Any]], avalanche_item: Any
) -> None:
    """Clear existing features and append new features to the Avalanche Forecast hosted feature layer."""
    if not avalanche_dict:
        _log.info("No Avalanche Forecasts found.")
        return

    # Delete all existing features and reset OBJECTID/FID counter
    avalanche_item.delete_and_truncate()
    new_features = []

    for area_id, data, in avalanche_dict.items():
        url = data.get("url")
        geom = geometry.Geometry(data["geometry"])

        statements = data["attributes"].get("confidence", {}).get("statements", ["No confidence statement available"])

        # Remove empty strings or whitespace-only entries
        statements = [s for s in statements if s.strip()]

        # If the list is still empty, use the fallback
        if not statements:
            statements = ["No confidence statement available"]

        statement_string = " ".join(statements)


        attributes = {
            "id": area_id,
            "date_issued": parse_iso_datetime(data["attributes"].get("dateIssued")),
            "valid_until": parse_iso_datetime(data["attributes"].get("validUntil")),
            "forecaster": data["attributes"].get("forecaster", "Unknown"),  # <-- Accessing nested "forecaster"
            "title": data["attributes"].get("title","Unnamed"),
            "confidence": data["attributes"].get("confidence",{}).get("rating",{}).get("display"),
            "statement" : statement_string,
            "url": url,
            
        }

        for i in range(3):
            try:
                ratings = data["attributes"]["dangerRatings"][i]
                attributes.update(
                    {
                        f"danger_rating_{i + 1}": ratings["date"]["display"],
                        f"danger_rating_{i + 1}_alp": ratings["ratings"]["alp"][
                            "rating"
                        ]["display"],
                        f"danger_rating_{i + 1}_tln": ratings["ratings"]["tln"][
                            "rating"
                        ]["display"],
                        f"danger_rating_{i + 1}_btl": ratings["ratings"]["btl"][
                            "rating"
                        ]["display"],
                    }
                )
            except (IndexError, KeyError, TypeError):
                _log.warning(
                    f"Missing danger rating data for area ID {area_id} on day {i + 1}."
                )

        new_features.append(features.Feature(geom, attributes))

    avalanche_item.append_data(new_features)

def main():
    """Main script execution."""

    AGO_URL = os.getenv("AGO_PORTAL_URL")
    AGO_USER = os.getenv("AGO_USER")
    AGO_PASS = os.getenv("AGO_PASS")

    if not all([AGO_URL, AGO_USER, AGO_PASS]):
        _log.error("AGO credentials are missing. Check your environment variables.")
        sys.exit(1)

    AGO = bier.AGO(AGO_URL, AGO_USER, AGO_PASS)
    AvalancheForecast_ItemID = os.getenv("AVALANCHEFORECAST_ITEMID")

    try:
        avalanche_geometry_data = bier.connect_to_api(
            "https://api.avalanche.ca/forecasts/en/areas"
        )
        avalanche_attribute_data = bier.connect_to_api(
            "https://api.avalanche.ca/forecasts/en/products", encode=True
        )

        avalanche_dict = format_avalanche_forecast_data(
            avalanche_geometry_data, avalanche_attribute_data
        )
        AvalancheForecast_item = bier.AGOItem(AGO, AvalancheForecast_ItemID)
        update_avalanche_forecast(avalanche_dict, AvalancheForecast_item)
    except Exception as e:
        _log.exception("An error occurred during execution.")
    finally:
        AGO.disconnect()
        _log.info("** Script completed **")


if __name__ == "__main__":
    main()
