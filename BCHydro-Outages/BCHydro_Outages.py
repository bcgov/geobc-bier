'''
BC Hydro Outages

Written by: Michael Dykes (michael.dykes@gov.bc.ca) and Paulina Marczak (paulina.marczak@gov.bc.ca)
Created: May 27, 2021

Purpose: Grab BC Hydro Web Content (from https://www.bchydro.com/power-outages/app/outage-map.html) and update ArcGIS Online Hosted Feature Layer
'''

# Import libraries/modules
import os, sys, datetime, logging
from arcgis import geometry, features
from dotenv import load_dotenv

# set script logger
_log = logging.getLogger(f"{os.path.basename(os.path.splitext(__file__)[0])}")

def import_environment_variables_from_file():
    '''
    Import Environment Variables from File (if necessary)
    '''
    script_dir = os.path.dirname(os.path.abspath(__file__))
    environment_file_path = os.path.join(script_dir, 'environment.env')

    try:
        load_dotenv(dotenv_path=environment_file_path)
        _log.info(f"Environment Variables Imported from File Successfully")
    except:
        _log.info(f"Environment Variables Imported Not Imported from File")

def Update_BCHydro_Outages_AGO(bchydro_data,bchydro_item):
    '''Clear existing features, and append new features to the BC Hydro Outages hosted feature layer in AGO'''
    if bchydro_data:
        _log.info(f"{len(bchydro_data)} BC Hydro Outages found")
        # Get Hosted Feature Layers to Update
        bchydro_item.delete_and_truncate()

        new_features = []
        for row in bchydro_data:
            # Build LAT/LONG list pairings from unseparated list of LAT/LONGS from website JSON
            latlong_list = [list(a) for a in zip(row["polygon"][::2],row["polygon"][1::2])]
            # Create Polygon Geometry WKID:4326 = WGS 1984
            geom = geometry.Geometry({"type": "Polygon", "rings" : [latlong_list],"spatialReference" : {"wkid" : 4326}})
            # Build attributes to populate feature attribute table, check for none values in the EST_TIME_ON, OFFTIME and UPDATED date fields
            attributes = {"OUTAGE_ID": row['id'], 
                    "GIS_ID": row['gisId'],
                    "REGION_ID": row['regionId'],
                    "REGION": row['regionName'],
                    "MUNI": row['municipality'],
                    "DETAILS": str(row['area']),
                    "CAUSE": row['cause'],
                    "AFFECTED":  row['numCustomersOut'],
                    "CREW_STATUS": row['crewStatusDescription'],
                    "EST_TIME_ON": datetime.datetime.utcfromtimestamp(row['dateOn']/1000).replace(tzinfo=datetime.timezone.utc).astimezone(tz=None) if row['dateOn'] else None,
                    "OFFTIME": datetime.datetime.utcfromtimestamp(row['dateOff']/1000).replace(tzinfo=datetime.timezone.utc).astimezone(tz=None) if row['dateOff'] else None,
                    "UPDATED": datetime.datetime.utcfromtimestamp(row['lastUpdated']/1000).replace(tzinfo=datetime.timezone.utc).astimezone(tz=None) if row['lastUpdated'] else None,
                    "CREW_ETA": datetime.datetime.utcfromtimestamp(row['crewEta']/1000).replace(tzinfo=datetime.timezone.utc).astimezone(tz=None) if row['crewEta'] else None,
                    "CREW_ETR": datetime.datetime.utcfromtimestamp(row['crewEtr']/1000).replace(tzinfo=datetime.timezone.utc).astimezone(tz=None) if row['crewEtr'] else None,
                    "SHOW_ETA": row['showEta'],
                    "SHOW_ETR": row['showEtr']}
            _log.debug(attributes)
            # Create new feature
            newfeature = features.Feature(geom,attributes)
            new_features.append(newfeature)

        bchydro_item.append_data(new_features)
    else:
        _log.info("No BC Hydro Outages found")

def main():
    '''Run code (if executed as script)'''
    import_environment_variables_from_file()

    # import bier module
    try:
        import bier
        _log.info(f"bier module imported")
    except:
        sys.path.append(".")
        sys.path.append(os.environ["BIER_PATH"])
        import bier
        _log.info(f"bier module imported")

    bier.Set_Logging_Level(logging.DEBUG)

    AGO_Portal_URL = os.environ["AGO_PORTAL_URL"]
    HydroOutages_ItemID = os.environ["HYDROOUTAGES_ITEMID"]
    HydroOutagesLFN_ItemID = os.environ["HYDROOUTAGES_LFN_ITEMID"]

    AGO = bier.AGO_Connection(AGO_Portal_URL)
    bchydro_outages_url = r"https://www.bchydro.com/power-outages/app/outages-map-data.json"
    bchydro_data = bier.Connect_to_Website_or_API_JSON(bchydro_outages_url)

    HydroOutages_item = bier.AGO_Item(AGO,HydroOutages_ItemID)
    Update_BCHydro_Outages_AGO(bchydro_data,HydroOutages_item)
    HydroOutagesLFN_item = bier.AGO_Item(AGO,HydroOutagesLFN_ItemID)
    Update_BCHydro_Outages_AGO(bchydro_data,HydroOutagesLFN_item)
    AGO.disconnect()
    _log.info("**Script completed**")

if __name__ == "__main__":
    main()
