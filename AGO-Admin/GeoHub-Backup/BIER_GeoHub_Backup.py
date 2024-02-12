'''
BIER GeoHub Backup

Written by: Michael Dykes (michael.dykes@gov.bc.ca)
Created: July 26 2022

Purpose: Backup up JSON content from BIER team members in GeoHub into our S3 Object Storage
'''

# Import libraries/modules
import os, sys, logging, json, tempfile
from arcgis import GIS
from minio import Minio

# Create logger and set logging level (NOTSET,DEBUG,INFO,WARNING,ERROR,CRITICAL)
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("my-logger")

# Load config file to get AGO parameter values
config_file = os.path.join(os.path.dirname(__file__),'config.json')
with open(config_file) as json_conf : 
    CONF = json.load(json_conf)

# BC GeoHub parameters and credentials needed for connection
PORTAL_URL = CONF["AGO_Portal_URL"]
try:
    PORTAL_USERNAME = sys.argv[2]
    PORTAL_PASSWORD = sys.argv[3]
except:
    PORTAL_USERNAME = os.getenv('GEOHUB_USERNAME')
    PORTAL_PASSWORD = os.getenv('GEOHUB_PASSWORD')   

AGO_Max_Items = 5000

RESTEndpoint = CONF["S3_REST_Endpoint"]
AccessKeyID = sys.argv[4]
SecretKey = sys.argv[5]
S3Connection = Minio(RESTEndpoint,AccessKeyID,SecretKey)
S3URL = CONF["S3_URL"]

folderpath = CONF["S3_FolderPath"]
part_size = 15728640

backup_users = CONF["Backup_Users_List"]
backup_itemtypes = ["Web Map","Dashboard","Hub Page","Hub Site Application","StoryMap","Web Experience","Web Mapping Application"]

# Backup AGO JSON data into text files
def Create_TEMP_JSON_BackupFile(ItemID):
    item = gis.content.get(ItemID)
    item_data = item.get_data()
    tfile = tempfile.NamedTemporaryFile(mode="w",delete=False)   
    json.dump(item_data, tfile)
    tfile.flush()
    tfile.close() 
    log.debug(tfile.name)
    return tfile

for user in backup_users:
    gis = GIS(PORTAL_URL, username=PORTAL_USERNAME, password=PORTAL_PASSWORD, expiration=9999)
    for type in backup_itemtypes:
        for item in gis.content.search(query="* AND \  owner:" + user,item_type=type, max_items=AGO_Max_Items):
            itemID = item.id
            title = item.title
            if "backup" not in title.lower() and "copy" not in title.lower():
                if "/" in title:
                    title = title.replace("/","_")
                if "\\" in title:
                    title = title.replace("\\","_")
                log.debug(title)
                try:
                    json_file = Create_TEMP_JSON_BackupFile(itemID)
                    Object = S3Connection.fput_object("xedyjn", f"{folderpath}/{type}/{title}.json", json_file.name, part_size=part_size)
                    url = f"{S3URL}/{folderpath}/{type}/{title}"
                    os.remove(json_file.name)
                except:
                    log.info(f"{title} - Failed to Backup")