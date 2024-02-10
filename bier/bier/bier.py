'''
bier.py

Written by: Michael Dykes (michael.dykes@gov.bc.ca)
Created: May 29 2023

Purpose: Common classes + functions used by the Business Innovation and Emergency Response section at GeoBC
'''

import logging, os, sys, json, getpass, requests
from arcgis.gis import GIS

# Create logger for messaging to console
_log = logging.getLogger(__name__)

'''
-----------------------------------------------------------------------------
1. Generic Python
- Classes and functions useful and common to any python script
-----------------------------------------------------------------------------
'''
def Set_Logging_Level(logging_level=logging.INFO):
    '''
    Set logging level from logging_level variable and send messages to the console on what 
    logging level the script is set to.

            Parameters:
                    logging_level (int): A decimal integer (Default value is INFO)
    '''
    # Set logging level based off logging_level parameter
    logging.basicConfig(level=logging_level)
    # Get logging level as string to display in console what logging level the script is set to
    logging_level_str = logging.getLevelName(logging_level)
    _log.info(f"Script console logging level set to - {logging_level_str}")

def Find_Config_File(script_file_path):
    '''
    Find config.json file that holds secrets (paths/itemIDs/etc) to be used in script
    
            Parameters:
                    file_path (str): Path to config file (Optional)

            Returns:
                    config_dict (str): Dictionary of secrets for use in script
    '''
    config_file_path = os.path.join(os.path.dirname(script_file_path),'config.json')
    try:
        # Open config file as json object/dictionary
        _log.debug(config_file_path) 
        with open(config_file_path) as json_conf :
            config_dict = json.load(json_conf)
        _log.info("Config file found")
        return config_dict
    except:
        # No config file is found, shutdown script
        _log.warning("No config file found")
                
'''
-----------------------------------------------------------------------------
2. ArcGIS Online
- Classes and functions to interact with ArcGIS Online 
-----------------------------------------------------------------------------
'''
class AGO_Connection():
    def __init__(self, url, username=None, password=None, username_arg_pos=2, password_arg_pos=3):
        '''Create connection to ArcGIS Online, creates a global variable "gis"'''
        _log.info("Creating connection to ArcGIS Online...")
        self.url = url
        self.username = None
        self.password = None

        if username:
            self.username = username
            self.password = password
        else:
            try:
                self.username = os.environ['AGO_USER']
                self.password = os.environ['AGO_PASS']
                _log.info(self.username)
                _log.info("Environment AGO credentials found")
            except:
                if os.environ.get('GEOHUB_USERNAME') is not None:
                    self.username = os.getenv('GEOHUB_USERNAME')
                    self.password = os.getenv('GEOHUB_PASSWORD')
                    _log.info("Environment variable AGO credentials found")
                else:
                    _log.info("*User input required*")
                    self.username = input("Enter AGO username:")
                    self.password = getpass.getpass(prompt='Enter AGO password:')
                    
        try:
            self.connection = GIS(self.url, username=self.username, password=self.password, expiration=9999)
            _log.debug(self.connection)
            _log.info("Connection to ArcGIS Online created successfully")
        except:
            _log.warning(f"Connection to ArcGIS Online Failed")

    def reconnect(self):
        try:
            _log.info("Reconnected to ArcGIS Online successfully")
            self.connection = GIS(self.url, username=self.username, password=self.password, expiration=9999, verify_cert=False)
        except:
            _log.warning(f"Attempt to reconnect to ArcGIS Online Failed")

    def disconnect(self):
        self.connection._con.logout()
        _log.info("Disconnected from ArcGIS Online")

class AGO_Item():
    def __init__(self, ago_connection, itemid):
        self.ago_connection = ago_connection
        self.itemid = itemid
        self.item = ago_connection.connection.content.get(self.itemid)

    def delete_and_truncate(self, layer_num=0, attempts=5):
        '''Delete existing features and truncate (which resets ObjectID) a table or hosted feature layer'''
        attempt = 0
        success = False
        # 5 attempts to connect and update the layer 
        while attempt < attempts and not success:
            try:
                feature_layer = self.item.layers[layer_num]
                feature_count = feature_layer.query(where="objectid >= 0", return_count_only=True)
                feature_layer.delete_features(where="objectid >= 0")
                _log.info(f"Deleted {feature_count} existing features from - ItemID: {self.itemid}")
                success = True
                try:
                    feature_layer.manager.truncate()
                    _log.info(f"Data truncated")
                except:
                    _log.warning("Truncate failed")
            except:
                _log.warning(f"Re-Attempting to Delete Existing Features from - ItemID: {self.itemid}. Attempt Number {attempt}")
                attempt += 1
                self.ago_connection.reconnect()
                if attempt == 5:
                    _log.critical(f"***No More Attempts Left. AGO Update Failed***")
                    sys.exit(1)

    def append_data(self, new_features_list, layer_num=0, attempts=5):
        attempt = 0
        success = False
        # 5 attempts to connect and update the layer 
        while attempt < attempts and not success:
            try:
                # Attempt to update ago feature layer
                result = self.item.layers[layer_num].edit_features(adds = new_features_list)
                _log.debug(result)
                success = True
                _log.info(f"Finished creating {len(new_features_list)} new features in AGO - ItemID: {self.itemid}")
            except:
                # If attempt fails, retry attempt (up to 5 times then exit script if unsuccessful)
                _log.warning(f"Re-Attempting AGO Update. Attempt Number {attempt}")
                attempt += 1
                self.ago_connection.reconnect()
                if attempt == 5:
                    _log.critical(f"***No More Attempts Left. AGO Update Failed***")
                    sys.exit(1)

    def add_field(self, new_field, layer_num=0):
        self.item.layers[layer_num].manager.add_to_definition({'fields':new_field})

'''
-----------------------------------------------------------------------------
3. API/Website
- Classes and functions to access/interact with Websites and APIs
-----------------------------------------------------------------------------
'''
def Connect_to_Website_or_API_JSON(url, encode=False):
    '''
    Connect to a website or API and retrieve data from it as JSON
    
            Parameters:
                    url (str): URL to Website/API for connection 

            Returns:
                    conf (str): Dictionary of secrets for use in script
    '''
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
            _log.warning(f"Error loading Website or API JSON Data")
    else:
        _log.warning(f"Connection to Website or API Failed")

'''
-----------------------------------------------------------------------------
4. S3 Object Storage
- Classes and functions to access/interact with S3 Object Storage
-----------------------------------------------------------------------------
'''

def Create_S3_Connection(variable_endpoint, variable_id, variable_key):
    '''
    Create connection to S3 Object Storage
    
            Parameters:
                    env_variable_endpoint (str): REST endpoint for S3 storage
                    env_variable_id (str): Access key ID
                    env_variable_key (str): Secret access key

            Returns:
                    S3Connection (obj): Minio connection to S3 Object Storage bucket
    '''
    _log.info("Creating connection to S3 Object Storage...")
    try:
        self.connection = Minio(self.env_variable_endpoint,self.env_variable_id,self.env_variable_key)
        self.bucket = self.connection.list_buckets()[0]
        _log.info("Connection to S3 Object Storage created successfully")
    except:
        _log.warning(f"Connection to S3 Object Storage")

    def access_object(self, s3_path, filename, download=False, download_path=None):
        '''
        Access file from S3 Object Storage. Can choose to download or not. If downloaded returns the downloaded file path.
        
                Parameters:
                        s3_path (str): Path within S3 bucket where object resides
                        filename (str): Name of object to access
                        download (bool): Download to storage (otherwise object is accessed through memory)
                        download_path (str): Path to where you want to download the object

                Returns:
                        s3_object (obj): Minio object representing object/item in S3 Object Storage
        '''
        if download:
            if download_path:
                self.connection.get_object(self.bucket, f"{s3_path}/{filename}", f"{download_path}/{filename}")
                s3_object = f"{download_path}/{filename}"
            else:
                _log.warning("No download path set for S3 File")
        else:
            s3_object = self.connection.get_object(self.bucket, f"{s3_path}/{filename}")
        return s3_object

    def upload_object(self, s3_path, upload_file_path, content_type=None, public=False, part_size = 15728640):
        '''
        Upload file to S3 Object Storage. s3_path parameter must include filename. Objects to set content type and public read permission.
        
                Parameters:
                        s3_path (str): Path within S3 bucket to put new object
                        upload_file_path (str): Path on storage where file to upload is
                        content_type (str): Content-Type header for S3
                        public (bool): True if you want to automatically set object to public on upload
                        part_size (int): Objects in S3 are uploaded in parts, this sets the size for each part

                Returns:
                        s3_object (obj): Minio object representing object/item in S3 Object Storage
        '''
        if content_type and public:
            s3_object = self.connection.fput_object(self.bucket, s3_path, upload_file_path, content_type="video/mp4", metadata={"x-amz-acl": "public-read"},part_size=part_size)
        elif content_type:
            s3_object = self.connection.fput_object(self.bucket, s3_path, upload_file_path, content_type="video/mp4",part_size=part_size)
        elif public:
            s3_object = self.connection.fput_object(self.bucket, s3_path, upload_file_path, metadata={"x-amz-acl": "public-read"},part_size=part_size)
        else:
            s3_object = self.connection.fput_object(self.bucket, s3_path, upload_file_path ,part_size=part_size)
        return s3_object
