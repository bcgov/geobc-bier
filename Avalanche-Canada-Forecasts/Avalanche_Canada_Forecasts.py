'''
Avalanche Canada Forecasts

Written by: Michael Dykes (michael.dykes@gov.bc.ca)
Created: January 24 2023

Purpose: Get avalanche forecast from Avalanche Canada API - https://api.avalanche.ca/ - for display in EM GeoHub
'''

# Import libraries/modules
import os, sys, datetime, logging
from arcgis import geometry, features

# set script logger
_log = logging.getLogger(f"{os.path.basename(os.path.splitext(__file__)[0])}")

try:
    import bier
    _log.info(f"bier module imported")
except:
    sys.path.append(".")
    sys.path.append(sys.argv[1])
    import bier
    _log.info(f"bier module imported")

def Format_Avalanche_Forecast_Data(avalanche_geometry_data,avalanche_attribute_data):
    '''Combine Avalanche Canada geometry data and attribute data into a dictionary'''
    if avalanche_geometry_data:
        _log.info(f"{len(avalanche_geometry_data['features'])} avalanche forecasts from Avalanche Canada found")

        avalanche_dict = {}
        if avalanche_geometry_data:
            for row in avalanche_geometry_data["features"]:
                avalanche_dict[row['id']] = {"geometry":row['geometry']}

        if avalanche_attribute_data:
            for row in avalanche_attribute_data:
                avalanche_dict[row['area']['id']]["attributes"] = row['report']

        return avalanche_dict
    else:
        _log.warning(f"No Avalanche Canada data found")
        sys.exit(0)

def Update_Avalanche_Forecast_AGO(avalanche_dict,avalanche_item):
    '''Clear existing features, and append new features to the Avalanche Forecast hosted feature layer in AGO'''
    # Delete all existing feature layer features and reset OBJECTID/FID counter
    avalanche_item.delete_and_truncate()

    new_features = []
    if avalanche_dict:
        for k,v in avalanche_dict.items():
            # Create arcgis geometry object from GEOJSON
            geom = geometry.Geometry(v['geometry'])

            # Align attributes from AGO fields names to attributes from the GEOJSON
            dateIssued = None
            if v['attributes']['dateIssued']:
                if "00:00Z" in v['attributes']['dateIssued']:
                    dateIssued = datetime.datetime.strptime(v['attributes']['dateIssued'],"%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=datetime.timezone.utc).astimezone(tz=None)
                else:
                    dateIssued = datetime.datetime.strptime(v['attributes']['dateIssued'],"%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=datetime.timezone.utc).astimezone(tz=None)

            validUntil = None
            if v['attributes']['validUntil']:
                if "00:00Z" in v['attributes']['validUntil']:
                    validUntil = datetime.datetime.strptime(v['attributes']['validUntil'],"%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=datetime.timezone.utc).astimezone(tz=None)
                else:
                    validUntil = datetime.datetime.strptime(v['attributes']['validUntil'],"%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=datetime.timezone.utc).astimezone(tz=None)

            attributes = {"id": k,
                    "date_issued": dateIssued,
                    "valid_until": validUntil,
                    "danger_rating_1": v['attributes']['dangerRatings'][0]['date']['display'],
                    "danger_rating_1_alp": v['attributes']['dangerRatings'][0]['ratings']['alp']['rating']['display'],
                    "danger_rating_1_tln": v['attributes']['dangerRatings'][0]['ratings']['tln']['rating']['display'],
                    "danger_rating_1_btl": v['attributes']['dangerRatings'][0]['ratings']['btl']['rating']['display'],
                    "danger_rating_2": v['attributes']['dangerRatings'][1]['date']['display'],
                    "danger_rating_2_alp": v['attributes']['dangerRatings'][1]['ratings']['alp']['rating']['display'],
                    "danger_rating_2_tln": v['attributes']['dangerRatings'][1]['ratings']['tln']['rating']['display'],
                    "danger_rating_2_btl": v['attributes']['dangerRatings'][1]['ratings']['btl']['rating']['display'],
                    "danger_rating_3": v['attributes']['dangerRatings'][2]['date']['display'],
                    "danger_rating_3_alp": v['attributes']['dangerRatings'][2]['ratings']['alp']['rating']['display'],
                    "danger_rating_3_tln": v['attributes']['dangerRatings'][2]['ratings']['tln']['rating']['display'],
                    "danger_rating_3_btl": v['attributes']['dangerRatings'][2]['ratings']['btl']['rating']['display']}

            # Create new feature
            newfeature = features.Feature(geom,attributes)
            new_features.append(newfeature)

        avalanche_item.append_data(new_features)
    else:
        _log.info("No Avalanche Forecasts found")
        
def main():
    '''Run code'''
    bier.Set_Logging_Level()

    try:
        conf = bier.Find_Config_File(__file__)
        AGO_Portal_URL = conf['AGO_PORTAL_URL']
        AVALANCHEFORECAST_ITEMID = conf['AVALANCHEFORECAST_ITEMID']
    except:
        AGO_Portal_URL = os.environ['AGO_PORTAL_URL']
        AVALANCHEFORECAST_ITEMID = os.environ['AVALANCHEFORECAST_ITEMID']

    AGO = bier.AGO_Connection(AGO_Portal_URL)

    avalanche_geometry_url = r"https://api.avalanche.ca/forecasts/en/areas"
    avalanche_geometry_data = bier.Connect_to_Website_or_API_JSON(avalanche_geometry_url)
    avalanche_attribute_url = r"https://api.avalanche.ca/forecasts/en/products"
    avalanche_attribute_data = bier.Connect_to_Website_or_API_JSON(avalanche_attribute_url,encode=True)

    avalanche_dict = Format_Avalanche_Forecast_Data(avalanche_geometry_data,avalanche_attribute_data)
    AvalancheForecast_item = bier.AGO_Item(AGO,AVALANCHEFORECAST_ITEMID)
    Update_Avalanche_Forecast_AGO(avalanche_dict,AvalancheForecast_item)

    AGO.disconnect()
    _log.info("**Script completed**")

# main function of script (if script is not imported as a module)
if __name__ == "__main__":
    main()