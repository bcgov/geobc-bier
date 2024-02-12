'''
Wildfire of Note Summary

Written by: Michael Dykes (michael.dykes@gov.bc.ca)
Created: June 23 2022

Purpose: Create summary table of wildfires of note and their fire status for EMCR Sit Rep Application
'''

import os, sys, pandas, json, pytz, datetime, requests, logging
import collections
from arcgis.gis import GIS
from arcgis import geometry, features

# set script logger
_log = logging.getLogger(f"{os.path.basename(os.path.splitext(__file__)[0])}")

# Load config file to get AGO parameter values
config_file = os.path.join(os.path.dirname(__file__),'config.json')
with open(config_file) as json_conf: 
    CONF = json.load(json_conf)

# BC GeoHub parameters and credentials needed for connection
PORTAL_URL = CONF["AGO_Portal_URL"]
PORTAL_USERNAME = sys.argv[1]
PORTAL_PASSWORD = sys.argv[2]

# Get ItemIDs from config file
FireCentre_ItemID = CONF["FireCentre_ItemID"]
FireLocation_ItemID = CONF["FirePointBCWS_ItemID"]
WildfireTable_ItemID = CONF["WildfireTable_ItemID"]
SitRepDashboard_Mobile_ItemID = CONF["SitRepDashboard_Mobile_ItemID"]
SitRepDashboard_Desktop_ItemID = CONF["SitRepDashboard_Desktop_ItemID"]

# Build AGO Connection
gis = GIS(PORTAL_URL,PORTAL_USERNAME,PORTAL_PASSWORD, expiration=9999) 

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

# Grab Fire Centre HFL and create spatial dataframe
FireCentre_item = gis.content.get(FireCentre_ItemID)
FireCentre_layer = FireCentre_item.layers[0]
FireCentre_fset = FireCentre_layer.query()
FireCentre_df = FireCentre_fset.sdf

# Grab Fire Location HFL and create spatial dataframe
Fire_item = gis.content.get(FireLocation_ItemID)
Fire_layer = Fire_item.layers[0]
Fire_fset = Fire_layer.query()
Fire_df = Fire_fset.sdf

# Get existing wildfire of note summary table in AGO for updating
WildfireTable_item = gis.content.get(WildfireTable_ItemID)
WildfireTable_layer = WildfireTable_item.layers[0]

# Wipe existing wildfire of note data from table
WildfireTable_layer.delete_features(where="objectid > 0")
#WildfireTable_layer.manager.truncate()

# Create dictionary with fire centres names as keys and empty lists as values (to be populated with fires of note)
fire_by_centre_dict = {f: [] for f in FireCentre_df['MOF_FIRE_CENTRE_NAME'].unique()}

# Iterate through fires of note from Fire Location HFL and add this data to dictionary created above
for index, row in Fire_df.iterrows():
    if row["FIRE_STATUS"] == "Fire of Note":
        wildfire_url = f'https://wildfiresituation-api.nrs.gov.bc.ca/publicPublishedIncident/{row["FIRE_NUMBER"]}'
        data = Connect_to_Wildfire_API(wildfire_url)
        stage_control = None
        if data['stageOfControlCode'] == "OUT_CNTRL":
            stage_control = "Out of Control"
        if data['stageOfControlCode'] == "HOLDING":
            stage_control = "Being Held"
        if data['stageOfControlCode'] == "UNDR_CNTRL":
            stage_control = "Under Control"
        if data['stageOfControlCode'] == "OUT":
            stage_control = "Out"

        toappend = row["GEOGRAPHIC_DESCRIPTION"],row["FIRE_NUMBER"],row["CURRENT_SIZE"],stage_control,row["FIRE_URL"]
        if row["FIRE_CENTRE"] == 7:
            fire_centre = "Cariboo Fire Centre"
        if row["FIRE_CENTRE"] == 2:
            fire_centre = "Coastal Fire Centre"
        if row["FIRE_CENTRE"] == 5:
            fire_centre = "Kamloops Fire Centre"
        if row["FIRE_CENTRE"] == 3:
            fire_centre = "Northwest Fire Centre"
        if row["FIRE_CENTRE"] == 4:
            fire_centre = "Prince George Fire Centre"
        if row["FIRE_CENTRE"] == 6:
            fire_centre = "Southeast Fire Centre"

        fire_by_centre_dict[fire_centre].append(toappend)

# Iterate through existing the fire centre HFL to get the geometry/polygons
for index, row in FireCentre_df.iterrows():
    geom = geometry.Geometry(row["SHAPE"])
    # Build attributes to populate feature attribute table, create  a sum of the fires of note based on their stage of control
    attributes = {"MOF_FIRE_CENTRE_ID":row['MOF_FIRE_CENTRE_ID'],
                 "MOF_FIRE_CENTRE_NAME":row['MOF_FIRE_CENTRE_NAME'],
                 "OutOfControl":sum(1 for elem in fire_by_centre_dict[row['MOF_FIRE_CENTRE_NAME']] if elem[3] == "Out of Control"),
                 "BeingHeld":sum(1 for elem in fire_by_centre_dict[row['MOF_FIRE_CENTRE_NAME']] if elem[3] == "Being Held"),
                 "UnderControl":sum(1 for elem in fire_by_centre_dict[row['MOF_FIRE_CENTRE_NAME']] if elem[3] == "Under Control"),
                 "Total":sum(1 for elem in fire_by_centre_dict[row['MOF_FIRE_CENTRE_NAME']] if elem[3] != "Out")}
    # Create new feature
    newfeature = features.Feature(geom,attributes)

    attempts = 5
    attempt = 0
    success = False
    # 5 attempts to connect and update the layer 
    while attempt < attempts and not success:
        try:
            # Attempt to update ago feature layer
            result = WildfireTable_layer.edit_features(adds = [newfeature])
            success = True
        except:
            # If attempt fails, retry attempt (up to 5 times then exit script if unsuccessful)
            _log.warning(f"Re-Attempting AGO Update. Attempt Number {attempt}")
            attempt += 1
            if attempt == 5:
                _log.critical(f"***No More Attempts Left. AGO Update Failed***")
                sys.exit(1)

firecentre_list = []
for firecentre,firelist in fire_by_centre_dict.items():
    toappend = firecentre,len(firelist)
    firecentre_list.append(toappend)

firecentre_sorted_list = sorted(sorted(firecentre_list, key=lambda x: x[0]),key=lambda x: x[1],reverse=True)

# Build HTML table to update in Wildfire of Note dashboard in the Situation Report Application
dashboard_text = ""
for firecentre in firecentre_sorted_list:
    header = '<p><span style="font-size:11pt"><strong>' + firecentre[0] + '</strong></span></p>'
    if fire_by_centre_dict[firecentre[0]]:
        header = header + "<ul>"
        for fire in fire_by_centre_dict[firecentre[0]]:
            header = header + f'<li><a href="{str(fire[4])}" style="color: #99ccff; font-weight: bold;">{str(fire[0])}</a> ({str(fire[1])}), {str(fire[2])} hectares, {str(fire[3])}</li>'
        header = header + '</ul>'
    else:
        header = header + "<ul><li>None</li></ul>"
    dashboard_text = dashboard_text + header

# Get Situation Report Application items from AGO
dashboard_item_mobile = gis.content.get(SitRepDashboard_Mobile_ItemID)
dashboard_item_desktop = gis.content.get(SitRepDashboard_Desktop_ItemID)

dashboard_toupdate = [dashboard_item_mobile,dashboard_item_desktop]

# Update Sit Rep Dashboards with new HTML tables
for dashboard in dashboard_toupdate:
    dashboard_item_data = dashboard.get_data()

    if "widgets" in dashboard_item_data.keys():
        for row in dashboard_item_data["widgets"]:
            if row["name"] == "Fires of Note Text":
                firetext_index = dashboard_item_data["widgets"].index(row)
                dashboard_item_data["widgets"][firetext_index]["text"] = dashboard_text
            if row["name"] == "Update Text":
                updatetext_index = dashboard_item_data["widgets"].index(row)
                today = datetime.datetime.now()
                dashboard_item_data["widgets"][updatetext_index]["text"] = '<div style="align-items:center; display:flex; justify-content:center; margin-bottom:auto; margin-left:auto; margin-right:auto; margin-top:auto"><h3 style="font-size:14px; text-align:center"><strong>Data Last Updated: ' + today.strftime("%B %#d, %Y, %H:%M hrs") + '</strong></h3></div>'

    elif "desktopView" in dashboard_item_data.keys():
        for row in dashboard_item_data["desktopView"]["widgets"]:
            if row["name"] == "Fires of Note Text":
                firetext_index = dashboard_item_data["desktopView"]["widgets"].index(row)
                dashboard_item_data["desktopView"]["widgets"][firetext_index]["text"] = dashboard_text
            if row["name"] == "Update Text":
                updatetext_index = dashboard_item_data["desktopView"]["widgets"].index(row)
                today = datetime.datetime.now()
                dashboard_item_data["desktopView"]["widgets"][updatetext_index]["text"] = '<div style="align-items:center; display:flex; justify-content:center; margin-bottom:auto; margin-left:auto; margin-right:auto; margin-top:auto"><h3 style="font-size:14px; text-align:center"><strong>Data Last Updated: ' + today.strftime("%B %#d, %Y, %H:%M hrs") + '</strong></h3></div>'

    dashboard.update(data=dashboard_item_data)