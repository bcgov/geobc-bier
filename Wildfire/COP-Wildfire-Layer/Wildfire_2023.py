'''
COP Wildfire Updater

Written by: Michael Dykes (michael.dykes@gov.bc.ca)
Created: May 1 2023

Purpose: Update EM Common Operating Picture wildfire layer (contains extra info like interface fire status, etc)
'''

# Import libraries/modules
import arcpy, logging, os, sys, getpass, requests, datetime, json
from arcgis.gis import GIS
from arcgis import geometry, features
    
#Set logging level (NOTSET,DEBUG,INFO,WARNING,ERROR,CRITICAL)
logging_level = logging.DEBUG

def Set_Logging_Level(logging_level):
    '''Set logging level from logging_level variable to send messages to the console'''
    global log
    # Setup logger and set logging level based off logging_level parameter
    logging.basicConfig(level=logging_level)
    # Set logger name to script filename (for display in console)
    log = logging.getLogger(f"{os.path.basename(os.path.splitext(__file__)[0])}-logger")
    # Get logging level as string for display in log 
    logging_level_str = logging.getLevelName(logging_level)
    log.info(f"Script console logging level set to - {logging_level_str}")

def Find_Config_File():
    '''Find config.json file that holds secrets (paths/itemIDs/etc) to be used in this script'''
    try:
        # Find 'config.json' file that should be stored in the same directory as this script
        config_file = os.path.join(os.path.dirname(__file__),'config.json')
        # Open config file as json object and store as conf variable for use elsewhere in script
        with open(config_file) as json_conf :
            global conf 
            conf = json.load(json_conf)
        log.info("Config file found")
    except:
        # No config file is found, shutdown script
        log.critical("No config file found - Stopping script")
        sys.exit(1)

def Create_AGO_Connection():
    '''Create connection to ArcGIS Online, creates a global variable "gis"'''
    log.info("Creating connection to ArcGIS Online...")
    if "AGO_Portal_URL" in conf:
        portal_url = conf["AGO_Portal_URL"]
    else:
        log.critical("No AGO Portal URL found in config.json - Stopping script")
        sys.exit(1)
    if 'JENKINS_URL' in os.environ:
        portal_username = sys.argv[1]
        portal_password = sys.argv[2]
        log.info("System arguement AGO credentials found")
    else:
        try:
            portal_username = os.getenv('GEOHUB_USERNAME')
            portal_password = os.getenv('GEOHUB_PASSWORD')
            log.info("Environment variable AGO credentials found")
        except:
            log.info("*User input required*")
            portal_username = input("Enter AGO username:")
            portal_password = getpass.getpass(prompt='Enter AGO password:')
            
    global gis
    gis = GIS(portal_url, username=portal_username, password=portal_password, expiration=9999)
    log.info("Connection to ArcGIS Online created successfully")

def AGO_Delete_and_Truncate(AGO_data_item):
    '''Delete existing features and truncate (which resets ObjectID) a table or hosted feature layer'''
    attempts = 0
    success = False
    # 5 attempts to connect and update the layer 
    while attempts < 5 and not success:
        try:
            feature_layer = AGO_data_item.layers[0]
            feature_count = feature_layer.query(where="objectid >= 0", return_count_only=True)
            feature_layer.delete_features(where="objectid >= 0")
            log.info(f"Deleted {feature_count} existing features from - ItemID: {AGO_data_item.id}")
            success = True
            try:
                feature_layer.manager.truncate()
                log.info(f"Data truncated")
            except:
                log.warning("Truncate failed")
        except:
            log.warning(f"Re-Attempting Delete Existing Features. Attempt Number {attempts}")
            attempts += 1
            Create_AGO_Connection()
            if attempts == 5:
                log.critical(f"***No More Attempts Left. AGO Update Failed***")
                sys.exit(1)
            
def Connect_to_Wildfire_API(url):
    '''Connect to BCWS API to get BCWS Current Resource information (from https://wildfiresituation.nrs.gov.bc.ca/map)'''
    x = requests.get(url)
    # If good web return/connection
    if x.status_code == 200:
        # If data that can be read in json is returned
        try:
            if x and x.json():
                return x.json()
        except:
            log.warning(f"API Data Not Found - Stopping script")

def Update_COP_Wildfire_Layer_AGO(fires_sde_layer):
    '''Clear existing features, and append new features to the EM Cop Wildfire hosted feature layer in AGO'''
    fire_points_COP_item = gis.content.get(conf["FirePointsCOP_ItemID"])
    fire_fields = ["INCIDENT_GUID","INCIDENT_ID","WILDFIRE_YEAR","FIRE_CTR_ORG_UNIT_NAME","FIRE_ZONE_ORG_UNIT_NAME","INCIDENT_NUMBER_LABEL","IGNITION_DATE","FIRE_OUT_DATE",
                   "FIRE_SIZE_HA","INCIDENT_TYPE_DESC","GEOGRAPHIC_DESCRIPTION","LATITUDE","LONGITUDE","INCIDENT_STATUS","STAGE_OF_CONTROL_DESC","INTERFACE_FIRE_IND",
                   "ELEVATION_M","PublicStatus","WFNProjectID","WFNProjectStatus","WFNDisplayName","WFNURL","SHAPE@"]

    AGO_Delete_and_Truncate(fire_points_COP_item)

    n = 0
    newfeatures = []
    with arcpy.da.SearchCursor(fires_sde_layer,fire_fields) as cursor:
        for row in cursor:
            if row[14] != 'Out' and row[9] in ['Fire','Agency Assist'] and row[13] != 'Completed':
                attributes = {"INCIDENT_GUID": row[0],
                            "INCIDENT_ID": row[1],
                            "WILDFIRE_YEAR": row[2],
                            "FIRE_CTR_ORG_UNIT_NAME": row[3],
                            "FIRE_ZONE_ORG_UNIT_NAME": row[4],
                            "INCIDENT_NUMBER_LABEL": row[5],
                            "IGNITION_DATE": row[6],
                            "FIRE_OUT_DATE": row[7],
                            "FIRE_SIZE_HA": row[8],
                            "INCIDENT_TYPE_DESC": row[9],
                            "GEOGRAPHIC_DESCRIPTION": row[10],
                            "LATITUDE": row[11],
                            "LONGITUDE": row[12],
                            "INCIDENT_STATUS": row[13],
                            "STAGE_OF_CONTROL_DESC": row[14],
                            "INTERFACE_FIRE_IND": row[15],
                            "ELEVATION_M": row[16],
                            "PublicStatus": row[17],
                            "WFNProjectID": row[18],
                            "WFNProjectStatus": row[19],
                            "WFNDisplayName": row[20],
                            "WFNURL": row[21]}

                #pt = Point({"x" : -118.15, "y" : 33.80, "spatialReference" : {"wkid" : 4326}})
                geom = geometry.Geometry(row[-1])
                new_feature = features.Feature(geom,attributes)
                newfeatures.append(new_feature)
                n += 1

    # Add feature to existing hosted feature layer
    result = fire_points_COP_item.layers[0].edit_features(adds = newfeatures)
    log.debug(result)
    print(n)

def main():
    '''Run code (if executed as script)'''
    Set_Logging_Level(logging_level)
    Find_Config_File()
    Create_AGO_Connection()
    #fireID = 'C40128'
    #wildfire_url = f'https://wildfiresituation-api.nrs.gov.bc.ca/publicPublishedIncident/{fireID}'
    #data = Connect_to_Wildfire_API(wildfire_url)

    log_folder               = r'\\spatialfiles.bcgov\Work\ilmb\dss\dsswhse\Tools and Resources\Scripts\GeoBC Tools\EMBC\Fire Numbers COP Updater'
    BCWS_SDE_NAME            = 'Wildfire_TJ.sde'
    BCWS_SDE_CONNECTION_FILE = os.path.join(log_folder, BCWS_SDE_NAME)
    BCWS_DATASET_NAME        = 'sde.fmloader.IN_CURRENT_INCIDENT_POINTS_SVW'
    BCWS_DATASET_PATH        = os.path.join(BCWS_SDE_CONNECTION_FILE, BCWS_DATASET_NAME)

    Update_COP_Wildfire_Layer_AGO(BCWS_DATASET_PATH)

    gis._con.logout()
    log.info("**Script completed**")

if __name__ == "__main__":
    main()