'''
BCSIMS Earthquakes

Written by: Michael Dykes (michael.dykes@gov.bc.ca)
Created: January 24 2023

Purpose: Get earthquake points and shakemap data from BC Smart Infrastructure Monitoring System API - http://api.bcsims.ca/index.html - for display in EM GeoHub
'''

import os, sys, datetime, logging, requests, socket, json, pytz, arcpy, csv
from dateutil.relativedelta import relativedelta
from datetime import datetime, timezone, timedelta
from arcgis.gis import GIS
from arcgis import geometry, features

#Set logging level (NOTSET,DEBUG,INFO,WARNING,ERROR,CRITICAL)
include_simulated = False
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

# Authenticate User in SIMS and get Bearer Token
def Authenticate_SIMS_User():
    url = 'http://api.bcsims.ca/User/login'
    BCSIMS_API_USERNAME = sys.argv[3]
    BCSIMS_API_PASSWORD = sys.argv[4]
    body = {'username': BCSIMS_API_USERNAME, 'password': BCSIMS_API_PASSWORD}
    x = requests.post(url, json = body)
    if x.status_code == 200:
        if x and x.json() and x.json()['data'] and x.json()['data']['token']:
            bearerToken = x.json()['data']['token']
            return bearerToken
    else:
        # Client IP Address incase of error connecting to BCSIMS API (IP Restricted)
        hostname=socket.gethostname()   
        IPAddr=socket.gethostbyname(hostname)
        logging.warning(f'{x.status_code} failure from {IPAddr} for {url} request')
        exit
        
#"2023-01-09T16:40:22.519Z"
def getEarthquakesBCSIMS(token,magnitude=3):
    datestr_past = (datetime.now(tz=pytz.UTC)- relativedelta(days=30)).strftime("%Y-%m-%dT%H:%M")
    datestr_present = (datetime.now(tz=pytz.UTC) + timedelta(hours=6)).strftime("%Y-%m-%dT%H:%M")

    url = 'http://api.bcsims.ca/Earthquakes/earthquakes/false'
    header = {"Authorization":"Bearer {}".format(token)}
    parameters = {"itemsPerPage":1000,"page":1,"includeSimulated":False}
    body = {"from":datestr_past,"to":datestr_present,"magnitude":magnitude}

    earthquakes_list = []
    shakemap_list = []

    x = requests.post(url, headers = header, params = parameters, json = body)
    if x.status_code == 200:
        if x and x.json() and x.json()['data'] and x.json()['data']['earthquakes']:
            shakemap_list = []
            for row in x.json()['data']['earthquakes']:
                if row["hasShakemap"]:
                    shakemap_list.append(row)
            earthquakes_list = x.json()['data']['earthquakes']
    else:
        logging.warning(f'{x.status_code} failure for {url} request')

    logging.info(f"{len(earthquakes_list)} - SIMS Earthquakes from {datestr_past} to {datestr_present}")
    logging.info(f"{len(shakemap_list)} - with SIMS Shakemap")
    logging.info(f"SIMS Earthquakes - {earthquakes_list}")
    logging.info(f"SIMS Shakemaps - {shakemap_list}")
    return earthquakes_list, shakemap_list

def Check_Earthquakes(earthquakes):
    HFS_item = gis.content.get(conf["BCSIMS_EVENTS_ITEM_ID"])
    if not HFS_item:
        logging.warning(f"no HFS found with id {HFS_item}")
        exit

    #query HFS for IDs (TODO: could be more efficient maybe by using dates?)
    HFS_fset = HFS_item.layers[0].query(out_fields='ID')
    hfs_eventsIds = []
    for hfs_feature in HFS_fset.features:
        hfs_eventsIds.append(hfs_feature.attributes['ID'])#['eventid'])

    for earthquake in earthquakes:
        if int(earthquake['id']) not in hfs_eventsIds:
            Add_Earthquakes(earthquake)

def Add_Earthquakes(earthquake):
    HFS_item = gis.content.get(conf["BCSIMS_EVENTS_ITEM_ID"])

    try:
        attributes = {
            "ID": earthquake['id'], 
            "EventTime": earthquake['eventTime'], 
            "Latitude": earthquake['latitude'],
            "Longitude": earthquake['longitude'],
            "Depth": earthquake['depth'], 
            "Magnitude": earthquake['magnitude'], 
            "Region": earthquake['region'], 
            "HasShakemap": earthquake['hasShakemap'],
            "IsSimulated": earthquake["isSimulatedEvent"]
        }

        event_dict = {"attributes": attributes, "geometry": {"x" : earthquake['longitude'], "y" : earthquake['latitude'], "spatialReference" : {"wkid" : 4326}}}

        logging.info(f"adding event id {earthquake['id']} to hfs")
        result = HFS_item.layers[0].edit_features(adds = [event_dict])
        logging.info(f"result: {result}")

    except Exception as e:
        logging.warning(f"exception {e} adding event id {earthquake['id']} to hfs")

# Convert shakemap csv to Feature Class
def Create_ShakeMap_Raster(earthquake):
    eventid = earthquake['id']
    eventtime = earthquake['eventTime']
    depth = earthquake['depth']
    magnitude = earthquake['magnitude'] 
    region = earthquake['region']
    simulated = earthquake["isSimulatedEvent"]

    HFS_item = gis.content.get(conf["BCSIMS_SHAKEMAP_ITEM_ID"])
    WebMercator = arcpy.SpatialReference(4326)

    # Check out Spatial Analysis extension
    arcpy.CheckOutExtension("Spatial")

    HFS_fset = HFS_item.layers[0].query(out_fields='EVENTID')
    hfs_eventsIds = []
    for hfs_feature in HFS_fset.features:
        hfs_eventsIds.append(hfs_feature.attributes['EVENTID'])#['eventid'])

    if int(eventid) not in hfs_eventsIds:
        logging.info(f"adding event id {eventid} to shakemaps")
        url = f'https://www.bcsims.ca/ShakemapImages/{eventid}/ShakeMapGridPGA_{eventid}.csv'
        x = requests.get(url)
        if x.status_code == 200:
            decoded_content = x.content.decode('utf-8')
            cr = csv.reader(decoded_content.splitlines(), delimiter=',')
            my_list = list(cr)

            gdb = r'\\spatialfiles.bcgov\work\ilmb\dss\dsswhse\Tools and Resources\Scripts\GeoBC Tools ArcPro\Business-Innovation-and-Emergency-Response\BCSIMS\shakemap.gdb'
            arcpy.env.workspace = gdb
            arcpy.env.overwriteOutput = True

            points = arcpy.CreateFeatureclass_management(gdb, f'shakemap_points_{eventid}', "POINT", spatial_reference=WebMercator)
            arcpy.management.AddField(points, "PGA", "DOUBLE")
            arcpy.management.AddField(points, "CLASS", "INTEGER")

            with arcpy.da.InsertCursor(points, ["PGA","CLASS","SHAPE@"]) as cursor:
                for row in my_list:
                    PGA = float(row[2])
                    SIMS_class = 0
                    if PGA <= 0.0005:
                        SIMS_class = 1
                    elif PGA <= 0.0030:
                        SIMS_class = 2
                    elif PGA <= 0.0280:
                        SIMS_class = 3
                    elif PGA <= 0.0620:
                        SIMS_class = 4
                    elif PGA <= 0.12:
                        SIMS_class = 5
                    elif PGA <= 0.22:
                        SIMS_class = 6
                    elif PGA <= 0.4:
                        SIMS_class = 7
                    elif PGA <= 0.75:
                        SIMS_class = 8
                    elif PGA <= 1.39:
                        SIMS_class = 9
                    else:
                        SIMS_class = 10
                    newrow = PGA,SIMS_class,arcpy.Point(row[0],row[1])
                    cursor.insertRow(newrow)

            ShakeRaster = arcpy.PointToRaster_conversion(points,"CLASS",r"\\spatialfiles.bcgov\work\ilmb\dss\dsswhse\Tools and Resources\Scripts\GeoBC Tools ArcPro\Business-Innovation-and-Emergency-Response\BCSIMS\ShakeRaster_" + str(eventid) + ".tif",cellsize=0.015)
            shake_poly = arcpy.RasterToPolygon_conversion(ShakeRaster, r'\\spatialfiles.bcgov\work\ilmb\dss\dsswhse\Tools and Resources\Scripts\GeoBC Tools ArcPro\Business-Innovation-and-Emergency-Response\BCSIMS\shakemap.gdb\Shake_poly_' + str(eventid), "NO_SIMPLIFY","Value","MULTIPLE_OUTER_PART")

            with arcpy.da.SearchCursor(shake_poly,["gridcode","SHAPE@"]) as cursor:
                for row in cursor:
                    geom = geometry.Geometry(row[1])

                    # Align attributes from AGO fields names to attributes from the GEOJSON
                    attributes = {"CLASS":row[0],
                                "EVENTID":int(eventid),
                                "EVENTTIME":eventtime,
                                "DEPTH": depth,
                                "MAGNITUDE": magnitude,
                                "REGION": region,
                                "ISSIMULATED": simulated
                                }
                    log.debug(attributes)
                    # Create new feature
                    newfeature = features.Feature(geom,attributes)
                    # Add feature to existing hosted feature layer
                    result = HFS_item.layers[0].edit_features(adds = [newfeature])
                    log.debug(result)
            del cursor

            arcpy.Delete_management(points)
            arcpy.Delete_management(shake_poly)
            arcpy.Delete_management(ShakeRaster)
            
            #logging.info(f"result: {raster_AGO_layer}")
            logging.info(f"Shakemap {eventid} - Raster Done")
        else:
            logging.warning(f'{x.status_code} failure for {url} request')

def Staff_Checkin_Report(token,earthquakes):
    HFS_item = gis.content.get(conf["BCSIMS_CHECKIN_TABLE_ITEM_ID"])

    for row in earthquakes:
        eventid = row['id']

        url = f'http://api.bcsims.ca/Staff/checkin-report/{eventid}'
        headers = {"Authorization":"Bearer {}".format(token)}
        print(url)
        x = requests.get(url,headers = headers)

        if x.status_code == 200:
            try:
                HFS_fset = HFS_item.tables[0].query()
                CheckedIn_list = []
                for hfs_feature in HFS_fset.features:
                    CheckedIn_list.append((hfs_feature.attributes['EVENTID'],hfs_feature.attributes['EMPLOYEEEMAIL']))

                for row in x.json()['data']:
                    attributes = {
                    "EVENTID":int(row['eventId']),
                    "EMPLOYEEEMAIL": row['employeeEmail'],
                    "REGISTERDATE": datetime.strptime(row['registerDate'],'%m/%d/%Y %H:%M:%S').replace(tzinfo=timezone.utc).astimezone(tz=None) if row['registerDate'] else None,
                    "ISAVAILABLETOWORK": row['isAvailableToWork'],
                    "CITY": row['city'],
                    "JOBTITLE": row['jobTitle']
                    }

                    if (row['eventId'],row['employeeEmail']) not in CheckedIn_list:
                        # Add feature to existing hosted feature layer
                        table = HFS_item.tables[0]
                        newfeature = features.Feature(geometry=None,attributes=attributes)
                        table.edit_features(adds = [newfeature])
            except:
                logging.warning(f'Staff Checkin failure for - {eventid}')
        else:
            logging.warning(f'{x.status_code} failure for {url} request')

def main():
    '''Run code (if executed as script)'''
    Set_Logging_Level(logging_level)
    Find_Config_File()
    token = Authenticate_SIMS_User()
    earthquakes_list, shakemap_list = getEarthquakesBCSIMS(token)
    Create_AGO_Connection()
    Check_Earthquakes(earthquakes_list)
    if shakemap_list:
        for earthquake in shakemap_list:
            Create_ShakeMap_Raster(earthquake)

    Staff_Checkin_Report(token,earthquakes_list)

    gis._con.logout()
    log.info("**Script completed**")

if __name__ == "__main__":
    main()