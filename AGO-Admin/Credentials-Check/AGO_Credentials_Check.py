import os, sys, logging, requests, json
from arcgis.gis import GIS

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

def main():
    '''Run code (if executed as script)'''
    Set_Logging_Level(logging_level)
    Find_Config_File()
    try:
        Create_AGO_Connection()
    except:
        url = conf['BIER_Teams_Webhook']

        payload = {"text": f"Warning - Automated attempt to connect to GeoHub from '{portal_username}' account failed. Credentials may have been altered."}
        headers = {'Content-Type': 'application/json'}
        response = requests.post(url, headers=headers, data=json.dumps(payload))
        log.info(response.text.encode('utf8'))

if __name__ == "__main__":
    main()