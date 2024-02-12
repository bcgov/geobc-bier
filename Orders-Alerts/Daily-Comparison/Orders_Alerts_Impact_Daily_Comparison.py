'''
Orders Alerts Impact Tables

Written by: Michael Dykes (michael.dykes@gov.bc.ca)
Created: July 15 2023

Purpose: Update Orders and Alerts Impact Tables in the COP
'''

# Import libraries/modules
import os, sys, datetime, logging, json
import numpy as np
from arcgis import geometry, features
from collections import defaultdict
from minio import Minio

# set script logger
_log = logging.getLogger(f"{os.path.basename(os.path.splitext(__file__)[0])}")

# append path to BIER module for import
if 'JENKINS_URL' in os.environ:
    sys.path.append(sys.argv[1])
else:
    sys.path.append(".")
try:
    import bier
    _log.info(f"bier module imported")
except:
    sys.path.append(sys.argv[1])
    import bier
    _log.info(f"bier module imported")

def delete_and_truncate(item):
    '''Delete existing features and truncate (which resets ObjectID) a table or hosted feature layer'''
    feature_layer = item.tables[0]
    feature_count = feature_layer.query(where="objectid >= 0", return_count_only=True)
    feature_layer.delete_features(where="objectid >= 0")
    _log.info(f"Deleted {feature_count} existing features")

def append_data(item, new_features_list):
    result = item.tables[0].edit_features(adds = new_features_list)
    _log.debug(result)
    _log.info(f"Finished creating {len(new_features_list)} new features in AGO")

def update_impacts_table(Orders_Alerts_Impact_Table_item, combined_dict):
    delete_and_truncate(Orders_Alerts_Impact_Table_item)

    new_features = []
    for k,v in combined_dict.items():
        # Build attributes to populate feature attribute table, check for none values in the EST_TIME_ON, OFFTIME and UPDATED date fields
        attributes = {"EVENT_YEAR": k[0], 
                "PREOC_CODE": k[1],
                "EVENT_TYPE": k[2],
                "ACTIVE": k[3],
                "SOLE_CHANGE": (None if v['SOLE_Count'] == 0 else v['SOLE_Count']),
                "BCR_CHANGE": (None if v['BCR_Count'] == 0 else v['BCR_Count']),
                "ORDER_CHANGE": (None if v['Order_Count'] == 0 else v['Order_Count']),
                "ALERT_CHANGE": (None if v['Alert_Count'] == 0 else v['Alert_Count']),
                "ORDER_POP_CHANGE": (None if v['Order_Pop'] == 0 else v['Order_Pop']),
                "ALERT_POP_CHANGE": (None if v['Alert_Pop'] == 0 else v['Alert_Pop']),
                "ORDER_HOME_CHANGE": (None if v['Order_Home'] == 0 else v['Order_Home']),
                "ALERT_HOME_CHANGE": (None if v['Alert_Home'] == 0 else v['Alert_Home']),
                "TOTAL_POP_CHANGE": (None if v['Total_Pop'] == 0 else v['Total_Pop']),
                "TOTAL_HOME_CHANGE": (None if v['Total_Home'] == 0 else v['Total_Home']),
                "TOTAL_COUNT_CHANGE": (None if v['Total_Count'] == 0 else v['Total_Count'])
                }
        # Create new feature
        newfeature = features.Feature(None,attributes)
        new_features.append(newfeature)

    append_data(Orders_Alerts_Impact_Table_item, new_features)

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
    S3Connection = Minio(variable_endpoint,variable_id,variable_key)
    _log.info("Connection to S3 Object Storage created successfully")
    return(S3Connection)

def Upload_S3_Object(S3Connection, bucket, s3_path, upload_file_path, content_type=None, public=False, part_size = 15728640):
    '''
    Upload file to S3 Object Storage. s3_path parameter must include filename. Objects to set content type and public read permission.
    
            Parameters:
                    S3Connection (obj): Minio connection to S3 Object Storage bucket
                    upload_file_path (str): Access key ID
                    s3_path (str): Secret access key
                    content_type (bool): Secret access key
                    public (str):
                    part_size (int):

            Returns:
                    s3_object (obj): Minio object representing object/item in S3 Object Storage
    '''
    if content_type and public:
        s3_object = S3Connection.fput_object(bucket, s3_path, upload_file_path, content_type="video/mp4", metadata={"x-amz-acl": "public-read"},part_size=part_size)
    elif content_type:
        s3_object = S3Connection.fput_object(bucket, s3_path, upload_file_path, content_type="video/mp4",part_size=part_size)
    elif public:
        s3_object = S3Connection.fput_object(bucket, s3_path, upload_file_path, metadata={"x-amz-acl": "public-read"},part_size=part_size)
    else:
        s3_object = S3Connection.fput_object(bucket, s3_path, upload_file_path ,part_size=part_size)
    return s3_object

def backup_impact_table(ago_item, s3_storage):
    flayer = ago_item.tables[0]
    feat_json = flayer.query(where="ACTIVE = 'Active'").to_json
    file_suffix = datetime.date.today().strftime("%y%m%d")
    temp_file = os.path.join(os.path.dirname(__file__),'temp',f'Impact_Table_{file_suffix}_0800.json')
    with open(temp_file, "w") as outfile:
        outfile.write(feat_json)

    Upload_S3_Object(s3_storage, "xedyjn", f"Data/OrdersAlerts/ImpactTable/Impact_Table_{file_suffix}.json", temp_file, content_type=None, public=False, part_size = 15728640)
    return temp_file

def set_update_time(item):
    update_time = datetime.date.today().strftime("%y-%m-%d") + " 08:00"
    dashboard_item_data = item.get_data()
    update_widgets = ["Data Changes Table"]

    if "widgets" in dashboard_item_data.keys():
        for row in dashboard_item_data["widgets"]:
            if row["name"] in update_widgets:
                text_index = dashboard_item_data["desktopView"]["widgets"].index(row)
                old_datetime_str = dashboard_item_data["desktopView"]["widgets"][text_index]["caption"].split("Data Updated: ")[1].split("</p>")[0]
                dashboard_text = dashboard_item_data["desktopView"]["widgets"][text_index]["caption"].replace(old_datetime_str,update_time)
                dashboard_item_data["desktopView"]["widgets"][text_index]["caption"] = dashboard_text

    elif "desktopView" in dashboard_item_data.keys():
        for row in dashboard_item_data["desktopView"]["widgets"]:
            if row["name"] in update_widgets:
                text_index = dashboard_item_data["desktopView"]["widgets"].index(row)
                old_datetime_str = dashboard_item_data["desktopView"]["widgets"][text_index]["caption"].split("Data Updated: ")[1].split("</p>")[0]
                dashboard_text = dashboard_item_data["desktopView"]["widgets"][text_index]["caption"].replace(old_datetime_str,update_time)
                dashboard_item_data["desktopView"]["widgets"][text_index]["caption"] = dashboard_text

    item.update(data=dashboard_item_data)

def main():
    '''Run code (if executed as script)'''
    bier.Set_Logging_Level()
    conf = bier.Find_Config_File(__file__)
    AGO = bier.AGO_Connection(conf["AGO_Portal_URL"])

    gis = AGO.connection
    Orders_Alerts_Impact_Table_item = gis.content.get(conf["Orders_Alerts_Impact_Table_ItemID"])
    Orders_Alerts_Change_Table_item = gis.content.get(conf["Orders_Alerts_Change_Table_ItemID"])

    s3_connection = Create_S3_Connection('nrs.objectstore.gov.bc.ca',"nr-geobclitton-prd","dd1BoAMZE7q+elf2zl3Oa+DqTDjeRa4UD21X684C")
    today_file = backup_impact_table(Orders_Alerts_Impact_Table_item,s3_connection)
    #today_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'temp', 'Impact_Table_230802_0800.json')

    yesterday = (datetime.date.today() - datetime.timedelta(days = 1)).strftime("%y%m%d")
    yesterday_file = None

    list_objects = s3_connection.list_objects("xedyjn", "Data/OrdersAlerts/ImpactTable/", recursive=False)
    for item in list_objects:
        filename = item.object_name.split("/")[-1]
        if ".json" in filename and yesterday in filename:
            yesterday_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'temp', item.object_name.split("/")[-1])
            s3_connection.fget_object("xedyjn", item.object_name, yesterday_file)

    todaydict = {}
    data_today = json.load(open(today_file))
    for row in data_today['features']:
        if row['attributes']['ACTIVE'] == 'Active': 
            todaydict[row['attributes']['EVENT_YEAR'],row['attributes']['PREOC_CODE'],row['attributes']['EVENT_TYPE']] = [row['attributes']['SOLE_COUNT'],row['attributes']['BCR_COUNT'],
            row['attributes']['ORDER_COUNT'],row['attributes']['ALERT_COUNT'],row['attributes']['ORDER_POP'],row['attributes']['ALERT_POP'],row['attributes']['ORDER_HOME'],
            row['attributes']['ALERT_HOME'],row['attributes']['TOTAL_POP'],row['attributes']['TOTAL_HOME'],row['attributes']['TOTAL_COUNT']]

    yesterdaydict = {}
    if yesterday_file:
        data_yesterday = json.load(open(yesterday_file))
        for row in data_yesterday['features']:
            if row['attributes']['ACTIVE'] == 'Active': 
                yesterdaydict[row['attributes']['EVENT_YEAR'],row['attributes']['PREOC_CODE'],row['attributes']['EVENT_TYPE']] = [row['attributes']['SOLE_COUNT'],row['attributes']['BCR_COUNT'],
                row['attributes']['ORDER_COUNT'],row['attributes']['ALERT_COUNT'],row['attributes']['ORDER_POP'],row['attributes']['ALERT_POP'],row['attributes']['ORDER_HOME'],
                row['attributes']['ALERT_HOME'],row['attributes']['TOTAL_POP'],row['attributes']['TOTAL_HOME'],row['attributes']['TOTAL_COUNT']]

    delete_and_truncate(Orders_Alerts_Change_Table_item)

    new_features = []
    for k,v in todaydict.items():
        if k in yesterdaydict:
            SOLE_CHANGE = (v[0] if v[0] else 0) - (yesterdaydict[k][0] if yesterdaydict[k][0] else 0)
            BCR_CHANGE = (v[1] if v[1] else 0) - (yesterdaydict[k][1] if yesterdaydict[k][1] else 0)
            ORDER_CHANGE = (v[2] if v[2] else 0) - (yesterdaydict[k][2] if yesterdaydict[k][2] else 0)
            ALERT_CHANGE = (v[3] if v[3] else 0) - (yesterdaydict[k][3] if yesterdaydict[k][3] else 0)
            ORDER_POP_CHANGE = (v[4] if v[4] else 0) - (yesterdaydict[k][4] if yesterdaydict[k][4] else 0)
            ALERT_POP_CHANGE = (v[5] if v[5] else 0) - (yesterdaydict[k][5] if yesterdaydict[k][5] else 0)
            ORDER_HOME_CHANGE = (v[6] if v[6] else 0) - (yesterdaydict[k][6] if yesterdaydict[k][6] else 0)
            ALERT_HOME_CHANGE = (v[7] if v[7] else 0) - (yesterdaydict[k][7] if yesterdaydict[k][7] else 0)
            TOTAL_POP_CHANGE = (v[8] if v[8] else 0) - (yesterdaydict[k][8] if yesterdaydict[k][8] else 0)
            TOTAL_HOME_CHANGE = (v[9] if v[9] else 0) - (yesterdaydict[k][9] if yesterdaydict[k][9] else 0)
            TOTAL_COUNT_CHANGE = (v[10] if v[10] else 0) - (yesterdaydict[k][10] if yesterdaydict[k][10] else 0)

            # Build attributes to populate feature attribute table, check for none values in the EST_TIME_ON, OFFTIME and UPDATED date fields
            attributes = {"EVENT_YEAR": k[0], 
                    "PREOC_CODE": k[1],
                    "EVENT_TYPE": k[2],
                    "ACTIVE": "Active",
                    "SOLE_CHANGE": (None if SOLE_CHANGE == 0 else SOLE_CHANGE),
                    "BCR_CHANGE": (None if BCR_CHANGE == 0 else BCR_CHANGE),
                    "ORDER_CHANGE": (None if ORDER_CHANGE == 0 else ORDER_CHANGE),
                    "ALERT_CHANGE": (None if ALERT_CHANGE == 0 else ALERT_CHANGE),
                    "ORDER_POP_CHANGE": (None if ORDER_POP_CHANGE == 0 else ORDER_POP_CHANGE),
                    "ALERT_POP_CHANGE": (None if ALERT_POP_CHANGE == 0 else ALERT_POP_CHANGE),
                    "ORDER_HOME_CHANGE": (None if ORDER_HOME_CHANGE == 0 else ORDER_HOME_CHANGE),
                    "ALERT_HOME_CHANGE": (None if ALERT_HOME_CHANGE == 0 else ALERT_HOME_CHANGE),
                    "TOTAL_POP_CHANGE": (None if TOTAL_POP_CHANGE == 0 else TOTAL_POP_CHANGE),
                    "TOTAL_HOME_CHANGE": (None if TOTAL_HOME_CHANGE == 0 else TOTAL_HOME_CHANGE),
                    "TOTAL_COUNT_CHANGE": (None if TOTAL_COUNT_CHANGE == 0 else TOTAL_COUNT_CHANGE)
                    }
            # Create new feature
            newfeature = features.Feature(None,attributes)
            new_features.append(newfeature)

    append_data(Orders_Alerts_Change_Table_item, new_features)

    Orders_Alerts_Impact_Dashboard = gis.content.get(conf["Orders_Alerts_Impact_Dashboard_ItemID"])
    set_update_time(Orders_Alerts_Impact_Dashboard)

    AGO.disconnect()
    _log.info("**Script completed**")

if __name__ == "__main__":
    main()