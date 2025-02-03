'''
BC Weather Alerts ECCC

Written by: Michael Dykes (michael.dykes@gov.bc.ca)
Created: June 23 2022

Purpose: Get BC Weather Alerts from ECCC website (https://weather.gc.ca/?layers=alert&province=BC&zoom=5&center=54.98445087,-125.28692377) GeoJSON 
and update hosted feature layer in GeoHub
'''

# Import libraries/modules
import logging, os, sys, getpass, requests, datetime, json
from arcgis.gis import GIS
from arcgis import geometry, features

#Set logging level (NOTSET,DEBUG,INFO,WARNING,ERROR,CRITICAL)
logging_level = logging.INFO

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

def Connect_to_Website_or_API_JSON(url,encode=False):
    '''Connect to a website or API and retrieve data from it as JSON'''
    x = requests.get(url)
    if encode:
        x.encoding = x.apparent_encoding
    # If good web return/connection
    if x.status_code == 200:
        # If data that can be read in json is returned
        try:
            if x and x.json():
                return x.json()
        except:
            log.critical(f"Website or API Data Not Found - Stopping script")
            sys.exit(1)
    else:
        log.critical(f"Connection to Website or API Failed - Stopping script")
        sys.exit(1)

# URL to connect to the GEOJSON from ECCC Weather Alerts (Filtered down to BC only) and use request module to connect to website data
url = r"https://geo.weather.gc.ca/geomet?lang=en&SERVICE=WFS&REQUEST=GetFeature&layer=ALERTS&version=2.0.0&typenames=ALERTS&outputformat=application/json&filter=<ogc:Filter><ogc:PropertyIsLike%20wildCard='*'%20singleChar='.'%20escape='!'><ogc:PropertyName>identifier</ogc:PropertyName><ogc:Literal>*CWVR08*</ogc:Literal></ogc:PropertyIsLike></ogc:Filter>"
x = requests.get(url)

def Update_Weather_Alerts_AGO(weatheralert_data,weatheralerts_itemid):
    '''Clear existing features, and append new features to the BC Weather Alerts hosted feature layer in AGO'''
    if weatheralert_data:
        log.info(f"{len(weatheralert_data['features'])} BC Weather Alerts found")
        # Get Hosted Feature Layer to Update
        weatheralerts_item = gis.content.get(weatheralerts_itemid)

        # Delete all existing feature layer features and reset OBJECTID/FID counter
        AGO_Delete_and_Truncate(weatheralerts_item)

        attempts = 0
        success = False
        # 5 attempts to connect and update the layer 
        while attempts < 5 and not success:
            try:
                # Iterate through rows in JSON
                n = 0
                for row in weatheralert_data['features']:
                    log.debug(f"Append new feature #{n}")
                    if row['properties']['area']:
                        # Create arcgis geometry object from GEOJSON
                        geom = geometry.Geometry(row['geometry'])

                        # Figure out sorting field for alert_type:
                        Alert_Type_Sort_Dict = {"warning":0,"watch":1,"statement":2,"advisory":3}

                        # Align attributes from AGO fields names to attributes from the GEOJSON
                        attributes = {"identifier":row['properties']['identifier'], 
                                "area":row['properties']['area'],
                                "headline":row['properties']['headline'],
                                "status":row['properties']['status'],
                                "alert_type":row['properties']['alert_type'],
                                "descrip_en":row['properties']['descrip_en'],
                                "effective": datetime.datetime.strptime(row['properties']['effective'],"%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=datetime.timezone.utc).astimezone(tz=None) if row['properties']['effective'] else None,
                                "expires": datetime.datetime.strptime(row['properties']['expires'],"%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=datetime.timezone.utc).astimezone(tz=None) if row['properties']['expires'] else None,
                                "sort":Alert_Type_Sort_Dict[row['properties']['alert_type']]}
                        # Create new feature
                        newfeature = features.Feature(geom,attributes)
                        # Add feature to existing hosted feature layer
                        result = weatheralerts_item.layers[0].edit_features(adds = [newfeature])
                        log.debug(result)
                        n+=1
                log.info(f"Finished creating {n} new BC Weather Alert features in AGO - ItemID: {weatheralerts_itemid}")
                success = True
            # If attempt fails, retry attempt (up to 5 times then exit script if unsuccessful)
            except:
                log.warning(f"Re-Attempting AGO Update. Attempt Number {attempts}")
                attempts += 1
                Create_AGO_Connection()
                if attempts == 5:
                    log.critical(f"***No More Attempts Left. AGO Update Failed***")
                    sys.exit(1)
    else:
        log.warning(f"No BC Weather Alert data found")
        sys.exit(0)

def Update_SitRep_Dashboard():
    '''Update EMCR Sit Rep weather alert dashboard updated time'''
    # Get Sit Rep Dashboard item (to update "last updated" time text)
    dashboard_item = gis.content.get(conf["SitRepDashboard_ItemID"])
    dashboard_item_data = dashboard_item.get_data()

    # Find update text widget
    for row in dashboard_item_data["widgets"]:
        if row["name"] == "Update Text":
            updatetext_index = dashboard_item_data["widgets"].index(row)

    # Get current time and update widget text to "Last Update:" and datetime 
    today = datetime.datetime.now()
    dashboard_item_data["widgets"][updatetext_index]["text"] = '<div style="align-items:center; display:flex; justify-content:center; margin-bottom:auto; margin-left:auto; margin-right:auto; margin-top:auto"><h3 style="font-size:14px; text-align:center"><strong>Data Last Updated: ' + today.strftime("%B %#d, %Y, %H:%M hrs") + '</strong></h3></div>'
    result = dashboard_item.update(data=dashboard_item_data)
    log.debug(result)
    log.info(f"Finished updating EMCR Sit Rep dashboard time")

def main():
    '''Run code (if executed as script)'''
    Set_Logging_Level(logging_level)
    Find_Config_File()
    bc_weatheralerts_url = url = r"https://geo.weather.gc.ca/geomet?lang=en&SERVICE=WFS&REQUEST=GetFeature&layer=ALERTS&version=2.0.0&typenames=ALERTS&outputformat=application/json&filter=<ogc:Filter><ogc:PropertyIsLike%20wildCard='*'%20singleChar='.'%20escape='!'><ogc:PropertyName>identifier</ogc:PropertyName><ogc:Literal>*CWVR08*</ogc:Literal></ogc:PropertyIsLike></ogc:Filter>"
    weatheralert_data = Connect_to_Website_or_API_JSON(bc_weatheralerts_url)
    Create_AGO_Connection()
    weatheralerts_ItemID = conf["WeatherAlerts_ItemID"]
    Update_Weather_Alerts_AGO(weatheralert_data,weatheralerts_ItemID)
    #Update_SitRep_Dashboard()
    gis._con.logout()
    log.info("**Script completed**")

if __name__ == "__main__":
    main()