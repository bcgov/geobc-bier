#******************************************************************************************************************************************************************************************
#
# Title: Template_Script.py
# Dependencies: environment.json
# Created by: Julia Lax (julia.lax@gov.bc.ca)
# Created on: August 26th, 2024
# Purpose: The purpose of this script is to determine the fire hazards within a specified distance of the facilities of interest. The example
#          used within this script are health care facilities within 25km of wildfires, or under fire alert/order or under State of Local Emergency
#          / Band Council Resolution. This information is then written back to the health facilities layer in AGOL in designated columns, and updated on
#          Jenkins every hour. This AGOL layer is then used within the configuration of an ArcGIS Dashboard. 
#
#******************************************************************************************************************************************************************************************

# Importing libraries
import sys, os
from arcgis.gis import GIS
from arcgis.features import FeatureLayer
from arcgis.geometry import Geometry
import json
import arcpy

def find_config_file():
    '''
    This function finds the configuration or environment file within the same folder as the script. This json file holds the
    AGOL item IDs of the layers of interest, as well as the ArcGIS url being logged into. 
    '''
    script_dir = os.path.dirname(os.path.abspath(__file__))
    config_file_path = os.path.join(script_dir, 'environment.json')
    
    try:
        # Open config file as json object/dictionary
        print(config_file_path) 
        with open(config_file_path) as json_conf :
            config_dict = json.load(json_conf)
        print("Config file found")
        return config_dict
    except:
        # No config file is found, shutdown script
        print("No config file found")

def create_ago_connection(config_env):
    '''
    This function creates a connection to AGOL using system credentials manually added on the machine running the script 
    (i.e., windows system menu --> edit system variables --> add --> "ago_user" and "ago_password") or the credentials within Jenkins. 
    The script then signs into the AGOL url within the environment file with these credentials.
    '''
    try: # For signing in when running on Jenkins
        if "JENKINS_URL" in os.environ:
            username = sys.argv[1]
            password = sys.argv[2]
        else: # For signing on manually with user credentials in the environment variables
            username = os.environ["ago_user"]
            password = os.environ["ago_password"]
            
        gis = GIS(config_env["AGO_PORTAL_URL"], username, password, verify_cert=False)
        print("Connection to ArcGIS Online created successfully")
        arcpy.SignInToPortal(config_env["AGO_PORTAL_URL"], username, password)

        return gis
    
    except Exception as e:
        print(f"Failed to create a connection to ArcGIS Online. Error: {str(e)}")
        sys.exit(1)

def copy_feature_layers(Orders_Alerts_COP_item, Facilities_item, Fire_Locations_item, SOLE_BCR_item, token):
    '''
    This function copies the AGOL layers to a feature layer within a designated geodatabase. The function then filters the points within the facilities layer 
    based on the spatial relationship with the other layer of interest (e.g., SOLE/BCR or Orders/Alerts or Fires) using the Select by Location arcpy function. 
    These filtered layers are then copied to the geodatabase. 
    '''
    #**********************************************************************
    # 
    # For adapting this section of the script, you will need to complete the following: 
    #    - Create ArcGIS Pro Project and replace aprx variable with project location
    #    - Create a geodatabase within a Jenkins accessible location, replace workspace location with this geodatabase pathway
    #    - Set facilities_within_orderalerts_dist, facilities_within_solebcr_dist, facilities_within_fire_dist to the Jenkins accessible gdb, give logical fc names
    #    - Replace the rest api links for the facilities and hazard feature layers of interest, name appropriately and ensure SQL is adjusted as needed
    #    - Within the SelectLayerByLocation processes, ensure the parameters are set for desired output (e.g., within 25km = WITHIN_A_DISTANCE_GEODESIC vs. under order/alert = WITHIN) - see here for more details: https://pro.arcgis.com/en/pro-app/latest/tool-reference/data-management/select-layer-by-location.htm
    #
    #**********************************************************************

    # Set the ArcGIS project location
    aprx = arcpy.mp.ArcGISProject(r"\\spatialfiles.bcgov\ilmb\bier\PROJECTS\2024\24-015 - HLTH - Community Care and Assisted Living Dashboard - 20240602\ARCPROJECTS\JULAX_Wk_24015HLTH\CC_AL.aprx")
    
    # Set the workspace location (geodatabase accessible to Jenkins)
    arcpy.env.workspace = r"\\spatialfiles.bcgov\ilmb\dss\dsswhse\Resources\Scripts\Tools_ArcPro\BIER\dev\CCAL\CCAL_Jenkins.gdb"
    arcpy.env.overwriteOutput = True # Set the overwriting to true in order to continously update the copy layers

    # Set the geodatabase location and layer names for the copies of the facilities layers based on their spatial relationships to the other layers (e.g., the filtered facilities layers within 25km of each hazard)
    facilities_within_orderalerts_dist = r"\\spatialfiles.bcgov\ilmb\dss\dsswhse\Resources\Scripts\Tools_ArcPro\BIER\dev\CCAL\CCAL_Jenkins.gdb\Health_Facilities_OrderAlerts"
    facilities_within_solebcr_dist = r"\\spatialfiles.bcgov\ilmb\dss\dsswhse\Resources\Scripts\Tools_ArcPro\BIER\dev\CCAL\CCAL_Jenkins.gdb\Health_Facilities_SOLEBCR"
    facilities_within_fire_dist = r"\\spatialfiles.bcgov\ilmb\dss\dsswhse\Resources\Scripts\Tools_ArcPro\BIER\dev\CCAL\CCAL_Jenkins.gdb\Health_Facilities_25km"
    
    print("Successfully found the ArcGIS project and set the workspace location.")
    
    # Create feature layers of the AGOL layers of interest (with their SQL filter criteria) within the workspace. 
    arcpy.management.MakeFeatureLayer(f"https://services1.arcgis.com/xeMpV7tU1t4KD3Ei/arcgis/rest/services/CCAL_CareLocations/FeatureServer/0", "facilities_layer")
    arcpy.management.MakeFeatureLayer(f"https://services1.arcgis.com/xeMpV7tU1t4KD3Ei/arcgis/rest/services/Orders_Alerts_COP/FeatureServer/0", "orderalerts_layer", "EVENT_TYPE = 'Fire'")
    arcpy.management.MakeFeatureLayer(f"https://services1.arcgis.com/xeMpV7tU1t4KD3Ei/arcgis/rest/services/SOLE_BCR/FeatureServer/0", "solebcr_layer", "STATUS = 'Active' AND EVENT_TYPE = 'Fire'")
    arcpy.management.MakeFeatureLayer(f"https://services6.arcgis.com/ubm4tcTYICKBpist/arcgis/rest/services/BCWS_ActiveFires_PublicView/FeatureServer/0", "fire_locations_layer", "FIRE_STATUS <> 'Out' AND FIRE_STATUS <> 'Under Control' AND FIRE_STATUS <> 'Being Held'")

    print("Successfully made feature layers of each AGOL layer of interest.")

#---------------------------------------------------------------------------------------------------------------------
#                                        Orders and Alerts
#---------------------------------------------------------------------------------------------------------------------
    # Select the facilities within order/alert polygons, and copy the features to the feature layer.
    arcpy.management.SelectLayerByLocation(
        in_layer="facilities_layer", 
        overlap_type="WITHIN", 
        select_features="orderalerts_layer", 
        selection_type="NEW_SELECTION"
    )
    arcpy.management.CopyFeatures("facilities_layer", facilities_within_orderalerts_dist)
    
    # Print the number of facilities within threshold dist to Orders/Alerts to the terminal. 
    result_OA = arcpy.management.GetCount(facilities_within_orderalerts_dist)
    count_OA = int(result_OA.getOutput(0))
    print(f"Number of facilities within threshold distance to order or alert: {count_OA}")

#---------------------------------------------------------------------------------------------------------------------
#                                        SOLE/BCR
#---------------------------------------------------------------------------------------------------------------------
    # Select the facilities within SOLE/BCR polygons, and copy the features to the feature layer.
    arcpy.management.SelectLayerByLocation(
        in_layer="facilities_layer", 
        select_features="solebcr_layer", 
        overlap_type="WITHIN", 
        selection_type="NEW_SELECTION"
    )
    arcpy.management.CopyFeatures("facilities_layer", facilities_within_solebcr_dist)
    
    # Print the number of facilities within threshold distance SOLE/BCR to the terminal. 
    result_sole = arcpy.management.GetCount(facilities_within_solebcr_dist)
    count_sole = int(result_sole.getOutput(0))
    print(f"Number of facilities within threshold distance to sole/bcr: {count_sole}")

#---------------------------------------------------------------------------------------------------------------------
#                                        Wildfires
#---------------------------------------------------------------------------------------------------------------------
    # Select facilities within a geodesic threshold distance from fire locations
    arcpy.management.SelectLayerByLocation(
        in_layer="facilities_layer", 
        overlap_type="WITHIN_A_DISTANCE_GEODESIC", 
        select_features="fire_locations_layer", 
        search_distance="25 Kilometers",  
        selection_type="NEW_SELECTION"
    )

    arcpy.management.CopyFeatures("facilities_layer", facilities_within_fire_dist)

    # Print the number of selected facilities
    result_fire = arcpy.management.GetCount(facilities_within_fire_dist)
    count_fire = int(result_fire.getOutput(0))
    print(f"Number of facilities within threshold distance of fire locations: {count_fire}")

    # Return the dictionary with the filtered facilities layers by their spatial relationship to the layers of interest.
    return {
        "facilities_within_orderalerts_dist": facilities_within_orderalerts_dist,
        "facilities_within_solebcr_dist": facilities_within_solebcr_dist,
        "facilities_within_fire_dist": facilities_within_fire_dist
    }

def process_fire_data(facility_geom, fire_locations):
    '''
    This function process the wildfire data only, so that the output columns for the fire information (i.e., FIRE_25KM_NUMBERS, FIRE_25KM_GEOGRAPHICDESCRIP, etc.) 
    have the information for all fires within the threshold distance together seperated by a comma.  
    '''
    #**********************************************************************
    # Change threshold value here if adapting script

    threshold = 25000 # Distance threshold (e.g., 25km)
    
    #**********************************************************************

    # Set the variables to be updated by the script
    fire_geographic_descriptions = set() # Set ensures it's a unique list of descriptions
    fire_numbers = [] # List of the fire numbers (IDs) for all the fires within threshold dist of a facility
    fire_status = [] # List of the fire status for all fires within threshold dist of a facility 
    fire_count = 0 # Count of how many fires are within threshold dist of the facility
     
    # Loop through all the fire locations 
    for fire in fire_locations:
        fire_geom = Geometry(fire[0]).project_as(spatial_reference="3005")
        distance = facility_geom.distance_to(fire_geom)
        if distance <= threshold: 
            fire_count += 1 # Increase the fire count by 1

                # ** Note: This is the format for the fire_locations being sent to this function: ['SHAPE@', 'FIRE_NUMBER', 'GEOGRAPHIC_DESCRIPTION', 'FIRE_STATUS'] **

            fire_numbers.append(fire[1] if fire[1] is not None else '') # If there is information for the FIRE_NUMBER, append this information to the fire num list or else leave blank
            fire_geographic_descriptions.add(fire[2] if fire[2] is not None else '') # If there is information for the GEOGRAPHIC_DESCRIPTION, append this information to the fire geodesrip list or else leave blank
            fire_status.append(fire[3] if fire[3] is not None else '') # If there is information for the FIRE_STATUS, append this information to the fire status list or else leave blank
        
    fire_count_within_threshold = len(fire_numbers) # Use the length of the fire numbers list to count the number of fires within threshold distance of the facility
    fire_geographic_description_str = ', '.join(fire_geographic_descriptions) # Join all the fire descriptions within threshold dist of the facility into one string, seperated by comma
    fire_numbers_str = ', '.join(f"{num} ({status})" for num, status in zip(fire_numbers, fire_status) if num and status) # Join all the fire numbers (IDs) within threshold dist of the facility into one string, seperated by comma

    # Return the dictionary with the fire information for that facility (fires within threshold distance)
    return {
        'fire_count_within_threshold': fire_count_within_threshold,
        'fire_geographic_description_str': fire_geographic_description_str,
        'fire_numbers_str': fire_numbers_str
    }

def process_oa_data(oa_locations):
    '''
    This function process the order and alert data only, so that the output columns for the order and alert information 
    (i.e., ALERT_DETAILS and ORDER_DETAILS etc.) have the information for all orders and alerts within the distance 
    threshold together seperated by a comma.  
    '''
    #**********************************************************************
    # Change threshold value here if adapting script

    threshold = 25000 # Distance threshold (e.g., 25km)

    #**********************************************************************
  
    oa_names = []
    oa_status = []
    oa_types = []
    oa_count = 0
    oa_details = []
    oa_distance = []

    # ** 
    # Note: 
    # The following information from the order_distance_list: ORDER_ALERT_NAME, ORDER_ALERT_STATUS, MULTI_SOURCED_HOMES, MULTI_SOURCED_POPULATION, EVENT_TYPE, distance, MIN_START_ORDER_DATE, EVENT_NAME
    # The following information from the alert_distance_list: ORDER_ALERT_NAME, ORDER_ALERT_STATUS, MULTI_SOURCED_HOMES, MULTI_SOURCED_POPULATION, EVENT_TYPE, distance, MIN_START_ALERT_DATE, EVENT_NAME
    # **

    # Loop through each order/alert in the list being sent with the oa_locations, create lists of lists in the variables above 
    for oa in oa_locations:
        distance = oa[5] # Distance in meters from the order/alert to the facility point
        distance_kms = distance / 1000 # Convert distance to km
        formatted_distance = f"{distance_kms:.2f} km" # Format the distance in km for writing to field
        if distance <= threshold: # If the distance between facility and order/alert is within the threshold distance, continue
            oa_count += 1
            oa_names.append(oa[7] if oa[7] is not None else '') # Append the EVENT_NAME information to the list
            oa_status.append(oa[1] if oa[1] is not None else '') # Append the ORDER_ALERT_STATUS to the list
            oa_types.append(oa[4] if oa[4] is not None else '') # Append the EVENT_TYPE to the list
            oa_distance.append(formatted_distance) # Append the formatted distance (kms) to the list
    
    # Return within the same field a combination of information about all the orders and all the alerts within the threshold distance of that facility 
    # Note: These will be populating two different columns, one for ORDER_DETAILS and one for ORDER_ALERTS
    # E.g. of the result format for facility with 3 orders: ORDER DETAILS = Order - Flood (Nooaitch Flooding, 1.59 km away), Order - Flood (Shackan Indian Band Flooding, 12.11 km away), Order - Flood (Shackan Mudslide, 12.16 km away)
    oa_details = ', '.join(f"{status or ''} - {oa_types or ''} ({name or ''}, {distance or ''} away)" for name, status, oa_types, distance in zip(oa_names, oa_status, oa_types, oa_distance))
    
    # Return a dictionary with these results (oa_count_within_threshold = INT, oa_details = STR)
    return {
        'oa_details': oa_details
    }

def facility_hazard_distances(copied_layers):
    '''
    This function updates the output layer (the existing facilities layer in AGOL) with the selected information from the layers of interest based on the facility unique ID (LicReg). 
    Note, it is important that the required columns that are to be updated with information have been added to the AGOL layer before hand, and that the field names match exactly. 
    
    Other notes: It is also important that the number of characters allowed have been adjusted accordingly (e.g., FIRE_25KM_GEOGRAPHICDESCRIP will have many characters). 
    For dashboard purposes, it is also important that the numeric fields to be used are in DOUBLE format, not Big Int. 
    '''
    #**********************************************************************
    # 
    # For adapting this section of the script, you will need to complete the following: 
    #       - Change the threshold distance value
    #       - Verify the rest api links and SQL details within each "SearchCursor" line
    #       - Ensure the Unique ID for your facilities layer is being used (replace LicReg)
    #       - Update dictionary with your field names in AGOL (that make sense for your analysis, e.g., replace ...25KM)

    threshold = 25000 # Distance threshold (e.g., 25km)
    
    #**********************************************************************

    facilities_within_orderalerts_dist = copied_layers["facilities_within_orderalerts_dist"] # Access the subset facilites layer for facilites within threshold dist to order/alert
    facilities_within_fire_dist = copied_layers["facilities_within_fire_dist"] # Access the subset facilities layer for facilites within threshold dist of a fire
    facilities_within_solebcr_dist = copied_layers["facilities_within_solebcr_dist"] # Access the subset facilities layer for facilities within threshold dist to sole/bcr

    Facilities_Hazard_Dict = {} # Dictionary to hold data updates for the facilities AGOL layer to update

    # Loop through the current health facilities layer on AGOL using the unique ID (LicReg)
    with arcpy.da.SearchCursor("https://services1.arcgis.com/xeMpV7tU1t4KD3Ei/arcgis/rest/services/CCAL_CareLocations/FeatureServer/0", ['LicReg']) as all_facilities:
        for facility in all_facilities: # For each facility in the facilities AGOL layer
            lic_reg = facility[0] # The LicReg is the first field within the AGOL layer
            
            # Set all columns listed below to their default state of None, empty strings or 0 -- the field type determines which default variable to set to (e.g., dates set to None, int set to 0, etc.)
            Facilities_Hazard_Dict[lic_reg] = {
                'CLOSEST_ALERT_DATE': None,
                'CLOSEST_ORDER_DATE': None,
                'CLOSE_ORDER_CNT': 0,
                'CLOSEST_ORDER_NAME': None,
                'CLOSEST_ORDER_TYPE': None,
                'CLOSE_ALERT_CNT': 0,
                'CLOSEST_ALERT_NAME': None,
                'CLOSEST_ALERT_TYPE': None,
                'CLOSEST_ALERT_DIST': None,
                'CLOSEST_ORDER_DIST': None,
                'FIRE_COUNT_25KM': 0,
                'HAS_FIRE_25KM': 0,
                'FIRE_25KM_GEOGRAPHICDESCRIP': '',
                'FIRE_25KM_NUMBERS': '',
                'SOLE_TYPECODES': '',
                'SOLE_STRTDATE': None,
                'SOLE_COMMUNITY': '',
                'SOLE_MUNI': '', 
                'ORDER_DETAILS': '', 
                'ALERT_DETAILS': ''
            }
            # * Note: If the field names are not appropriate for your analysis (e.g., ...25KM), adjust accordingly in the dictionaries and AGOL field names

#---------------------------------------------------------------------------------------------------------------------
#                                        Orders/Alerts
#---------------------------------------------------------------------------------------------------------------------
    # Prepare the subset layer of facilities within threshold dist to order/alert to populate the dictionary and eventually update the AGOL layer by linking the following order/alert information to the unique ID of the facility (LicReg)
    with arcpy.da.SearchCursor(facilities_within_orderalerts_dist, ['SHAPE@', 'LicReg']) as facilities: # Searching the subset layer of facilities within threshold dist to order/alert
        for facility in facilities: # For each facility within the subset layer
            lic_reg = facility[1] # The unique ID (LicReg) is the second item in the layer details list being reviewed as stated above (index 1)
            facility_geom = Geometry(facility[0]).project_as(spatial_reference="3005")
            order_distance_list = [] # All orders that enclose a facility point will have their information added to this list (this is a list of lists)
            alert_distance_list = [] # All alerts that enclose a facility point will have their information added to this list (this is a list of lists)

            # Looping through the orders/alerts layer from AGOL, looking at only the fields from this layer that are included in the SearchCursor list
            with arcpy.da.SearchCursor("https://services1.arcgis.com/xeMpV7tU1t4KD3Ei/arcgis/rest/services/Orders_Alerts_COP/FeatureServer/0", ['SHAPE@', 'ORDER_ALERT_STATUS', 'ORDER_ALERT_NAME', 'MULTI_SOURCED_HOMES', 'MULTI_SOURCED_POPULATION', 'EVENT_TYPE', 'MIN_START_ORDER_DATE', 'MIN_START_ALERT_DATE'], where_clause="EVENT_TYPE = 'Fire'") as orderalerts:
                
                for oa in orderalerts: # For each order and alert
                    oa_geom = Geometry(oa[0]).project_as(spatial_reference="3005")
                    distance = facility_geom.distance_to(oa_geom)
                    if distance <= threshold:
                        if oa[1] == "Order": # Extract only orders/alerts where ORDER_ALERT_STATUS is equal to Order
                            order_distance_list.append([oa[2], oa[1], oa[3], oa[4], oa[5], distance, oa[6], oa[8]]) # Append the following information to the order list: ORDER_ALERT_NAME, ORDER_ALERT_STATUS, MULTI_SOURCED_HOMES, MULTI_SOURCED_POPULATION, EVENT_TYPE, distance, MIN_START_ORDER_DATE, EVENT_NAME
                        elif oa[1] == "Alert": # Extract only orders/alerts where ORDER_ALERT_STATUS is equal to Alert
                            alert_distance_list.append([oa[2], oa[1], oa[3], oa[4], oa[5], distance, oa[7], oa[8]]) # Append the following information to the order list: ORDER_ALERT_NAME, ORDER_ALERT_STATUS, MULTI_SOURCED_HOMES, MULTI_SOURCED_POPULATION, EVENT_TYPE, distance, MIN_START_ALERT_DATE, EVENT_NAME
            
            # Updating dictionary with oa_details
            half_key = Facilities_Hazard_Dict[lic_reg] # Reduce redundancy with abbreviating code
            
            order_data = process_oa_data(order_distance_list) # With the list of orders that are within the threshold distance, send this list of orders to the process_oa_data function
            half_key['ORDER_DETAILS'] = order_data['oa_details'] # Write the string that is returned from the process_oa_data function for the oa_details to the ORDER_DETAILS dictionary item
            

            alert_data = process_oa_data(alert_distance_list) # With the list of alerts that are within the threshold distance, send this list of alerts to the process_oa_data function
            half_key['ALERT_DETAILS'] = alert_data['oa_details'] # Write the string that is returned from the process_oa_data function for the oa_details to the ALERT_DETAILS dictionary item

            # Process orders
            close_order_cnt = len(order_distance_list) # The number of orders that are within the threshold distance can be determined by counting the items in the order_distance_list
            if close_order_cnt > 0: # If there are orders that are within the threshold distance, continue
                closest_order = min(order_distance_list, key=lambda x: x[5]) # The closest order is the order within the order_distance list that as the minimum "distance" value
                if closest_order[5] <= threshold: # If the distance value is less than or equal to the threshold value, continue
                    # *Note: This is the format: ORDER_ALERT_NAME, ORDER_ALERT_STATUS, MULTI_SOURCED_HOMES, MULTI_SOURCED_POPULATION, EVENT_TYPE, distance, MIN_START_ORDER_DATE, EVENT_NAME
                    data_dict_halfkey = Facilities_Hazard_Dict[lic_reg]
                    data_dict_halfkey['CLOSE_ORDER_CNT'] = close_order_cnt
                    data_dict_halfkey['CLOSEST_ORDER_NAME'] = closest_order[0] # ORDER_ALERT_NAME
                    data_dict_halfkey['CLOSEST_ORDER_TYPE'] = closest_order[4] # EVENT_TYPE
                    data_dict_halfkey['CLOSEST_ORDER_DIST'] = closest_order[5] # distance
                    data_dict_halfkey['CLOSEST_ORDER_DATE'] = closest_order[6] # MIN_START_ORDER_DATE

            # Process alerts
            close_alert_cnt = len(alert_distance_list) # The number of alerts that are within the threshold distance can be determined by counting the items in the alert_distance_list
            if close_alert_cnt > 0: # If there are alerts that are within the threshold distance, continue
                closest_alert = min(alert_distance_list, key=lambda x: x[5]) # The closest alert is the alert within the alert_distance list that as the minimum "distance" value
                if closest_alert[5] < threshold: # If the distance value is less than or equal to the threshold value, continue
                    # *Note: This is the format: ORDER_ALERT_NAME, ORDER_ALERT_STATUS, MULTI_SOURCED_HOMES, MULTI_SOURCED_POPULATION, EVENT_TYPE, distance, MIN_START_ALERT_DATE, EVENT_NAME
                    data_dict_halfkey['CLOSE_ALERT_CNT'] = close_alert_cnt
                    data_dict_halfkey['CLOSEST_ALERT_NAME'] = closest_alert[0] # ORDER_ALERT_NAME
                    data_dict_halfkey['CLOSEST_ALERT_TYPE'] = closest_alert[4] # EVENT_TYPE
                    data_dict_halfkey['CLOSEST_ALERT_DIST'] = closest_alert[5] # distance
                    data_dict_halfkey['CLOSEST_ALERT_DATE'] = closest_alert[6] # MIN_START_ALERT_DATE

#---------------------------------------------------------------------------------------------------------------------
#                                        Wildfires
#---------------------------------------------------------------------------------------------------------------------
    # Populate Fire Information
    facility_count = 0 # Initialize the count variable

    # Looping through the subset of facilities that are within the threshold distance to a fire point
    with arcpy.da.SearchCursor(facilities_within_fire_dist, ['SHAPE@', 'LicReg']) as facilities:
        # For each facility in the facilities subset
        for facility in facilities:
            facility_geom = Geometry(facility[0]).project_as(spatial_reference="3005") # Convert to BC Albers
            lic_reg = facility[1]
            data_dict_halfkey['HAS_FIRE_25KM'] = 1 # Since we know these facilities are within threshold distance with SelectByLocation, set HAS_FIRE_25KM equal to 1 to indicate true
            facility_count += 1 # Increase the facility count (facilities within threshold distance to fire)

            # Retrieve fire data
            with arcpy.da.SearchCursor("https://services6.arcgis.com/ubm4tcTYICKBpist/arcgis/rest/services/BCWS_ActiveFires_PublicView/FeatureServer/0", ['SHAPE@', 'FIRE_NUMBER', 'GEOGRAPHIC_DESCRIPTION', 'FIRE_STATUS'], where_clause="FIRE_STATUS <> 'Out' AND FIRE_STATUS <> 'Being Held' AND FIRE_STATUS <> 'Under Control'") as fire_locations:
                # Send the list of fire locations to the process_fire_data function to assess distances to facility and prepare information written to AGOL fields
                fire_data = process_fire_data(facility_geom, fire_locations)
                # Extract the information from the dictionary returned by the process_fire_data to write to AGOL updating dictionary
                data_dict_halfkey['FIRE_COUNT_25KM'] = fire_data['fire_count_within_threshold']
                data_dict_halfkey['FIRE_25KM_GEOGRAPHICDESCRIP'] = fire_data['fire_geographic_description_str']
                data_dict_halfkey['FIRE_25KM_NUMBERS'] = fire_data['fire_numbers_str']

#---------------------------------------------------------------------------------------------------------------------
#                                        SOLE/BCR
#---------------------------------------------------------------------------------------------------------------------
    # Loop through the subset of facilities that are within the threshold distance to a SOLE/BCR (that was already determined with the SelectByLocation)
    with arcpy.da.SearchCursor(facilities_within_solebcr_dist, ['SHAPE@', 'LicReg']) as facilities:
        # For each facility within the subset layer of facilities
        for facility in facilities:
            lic_reg = facility[1]
            facility_geom = Geometry(facility[0]).project_as(spatial_reference="3005") # Convert to BC Albers
            bcr_FN_within_threshold = [] # List of Lists, format = SOLE_TYPE_CODE, START_DATE, COMMUNITY, MUNICIPALITY, distance
            sole_LG_within_threshold = [] # List of Lists, format = SOLE_TYPE_CODE, START_DATE, COMMUNITY, MUNICIPALITY, distance

            # Loop through all the SOLE/BCR data from AGOL, looking at specific fields
            with arcpy.da.SearchCursor("https://services1.arcgis.com/xeMpV7tU1t4KD3Ei/arcgis/rest/services/SOLE_BCR/FeatureServer/0", ['SHAPE@', 'SOLE_TYPE_CODE', 'START_DATE', 'COMMUNITY', 'MUNICIPALITY'], where_clause="STATUS = 'Active' AND EVENT_TYPE = 'Fire'") as sole_bcrs:
                # For each SOLE/BCR that 
                for sole_bcr in sole_bcrs:
                    solebcr_geom = Geometry(sole_bcr[0]).project_as(spatial_reference="3005") # Convert to BC Albers
                    distance = facility_geom.distance_to(solebcr_geom) # Distance between SOLE/BCR and facility (in meters)
                    if distance <= threshold: # If the SOLE/BCR is within the threshold, continue
                        if sole_bcr[1] == "FN": # Seperate Band Council Resolutions
                            bcr_FN_within_threshold.append([sole_bcr[1], sole_bcr[2], sole_bcr[3], sole_bcr[4], distance])
                        elif sole_bcr[1] == "LG": # Seperate State of Local Emergency
                            sole_LG_within_threshold.append([sole_bcr[1], sole_bcr[2], sole_bcr[3], sole_bcr[4], distance])

            # Process SOLE/BCR data
            if bcr_FN_within_threshold: # If there are entries within the sole_FN_within_threshold list, continue
                closest_BCR = min(bcr_FN_within_threshold, key=lambda x: x[4]) # Closest BCR is the BCR in the bcr_FN_within_threshold with the minimum "distance" value
                data_dict_halfkey['SOLE_TYPECODES'] = closest_BCR[0] # SOLE_TYPE_CODE
                data_dict_halfkey['SOLE_STRTDATE'] = closest_BCR[1] # START_DATE
                data_dict_halfkey['SOLE_COMMUNITY'] = closest_BCR[2] # COMMUNITY
                data_dict_halfkey['SOLE_MUNI'] = closest_BCR[3] # MUNICIPALITY

            if sole_LG_within_threshold: # If there are entries within the sole_LG_within_threshold list, continue
                closest_sole_LG = min(sole_LG_within_threshold, key=lambda x: x[4]) # Closest SOLE is the SOLE in the sole_LG_within_threshold with the minimum "distance" value
                data_dict_halfkey['SOLE_TYPECODES'] = closest_sole_LG[0] # SOLE_TYPE_CODE
                data_dict_halfkey['SOLE_STRTDATE'] = closest_sole_LG[1] # START_DATE
                data_dict_halfkey['SOLE_COMMUNITY'] = closest_sole_LG[2] # COMMUNITY
                data_dict_halfkey['SOLE_MUNI'] = closest_sole_LG[3] # MUNICIPALITY

    # Return entire dictionary with updated field information (Note, this includes the reset values of None, '' and 0 if the LicReg hasn't been used to update information in the other functions)
    return Facilities_Hazard_Dict

def update_agol_layer(Facilities_Hazard_Dict, Facilities_item):
    '''
    This function updates the AGOL layer of the facilities of interest with the dictionary information that has been added throughout the other calculation functions. 
    '''
    try:
        # Access the feature layer in AGOL
        flayer = Facilities_item.layers[0]
        
        # Fetch all features from the layer
        fset = flayer.query()
        features = fset.features
        
        # Create a list for the updates
        updates = []
        
        # For each feature in the AGOL layer
        for feature in features:
            # Retrieve the LicReg (the unique ID being used)
            lic_reg = feature.attributes.get('LicReg', 'Unknown')
            # If there is a matching LicReg within the dictionary with the updates and the AGOL layer, continue
            if lic_reg in Facilities_Hazard_Dict:
                # Abbreviate code to reduce redundancy
                data_dict_halfkey = Facilities_Hazard_Dict[lic_reg]
                
                # Prepare the update dictionary
                updated_attributes = {
                    'CLOSE_ORDER_CNT': data_dict_halfkey['CLOSE_ORDER_CNT'],
                    'CLOSEST_ORDER_NAME': data_dict_halfkey['CLOSEST_ORDER_NAME'],
                    'CLOSEST_ORDER_TYPE': data_dict_halfkey['CLOSEST_ORDER_TYPE'],
                    'CLOSEST_ORDER_DIST': data_dict_halfkey['CLOSEST_ORDER_DIST'],
                    'CLOSEST_ORDER_DATE': data_dict_halfkey['CLOSEST_ORDER_DATE'],
                    'CLOSE_ALERT_CNT': data_dict_halfkey['CLOSE_ALERT_CNT'],
                    'CLOSEST_ALERT_NAME': data_dict_halfkey['CLOSEST_ALERT_NAME'],
                    'CLOSEST_ALERT_TYPE': data_dict_halfkey['CLOSEST_ALERT_TYPE'],
                    'CLOSEST_ALERT_DIST': data_dict_halfkey['CLOSEST_ALERT_DIST'],
                    'CLOSEST_ALERT_DATE': data_dict_halfkey['CLOSEST_ALERT_DATE'],
                    'FIRE_COUNT_25KM': data_dict_halfkey['FIRE_COUNT_25KM'],
                    'HAS_FIRE_25KM': data_dict_halfkey['HAS_FIRE_25KM'],
                    'FIRE_25KM_GEOGRAPHICDESCRIP': data_dict_halfkey['FIRE_25KM_GEOGRAPHICDESCRIP'],
                    'FIRE_25KM_NUMBERS': data_dict_halfkey['FIRE_25KM_NUMBERS'],
                    'SOLE_TYPECODES': data_dict_halfkey['SOLE_TYPECODES'],
                    'SOLE_STRTDATE': data_dict_halfkey['SOLE_STRTDATE'],
                    'SOLE_COMMUNITY': data_dict_halfkey['SOLE_COMMUNITY'],
                    'SOLE_MUNI': data_dict_halfkey['SOLE_MUNI'], 
                    'ORDER_DETAILS': data_dict_halfkey['ORDER_DETAILS'],
                    'ALERT_DETAILS': data_dict_halfkey['ALERT_DETAILS']
                }
                
                # Append the updated feature
                updates.append({
                    'attributes': {**feature.attributes, **updated_attributes},
                    'objectId': feature.attributes['OBJECTID']
                })
        
        # Update the feature layer with all changes
        if updates: # If there is information within the updates list, continue
            flayer.edit_features(updates=updates)
            print("Feature layer updated successfully.")
        else:
            print("No updates to apply.")
    
    except Exception as e:
        print(f"Failed to update ArcGIS Online layer. Error: {str(e)}")

def main():
    '''
    This function runs the processing for the configuration, calculations and AGOL updates. 
    '''
    try:
        print("Starting script execution...")
        
        config_env = find_config_file() # Find the config environment file for AGOL information

        # From the environment.json file, retrieve the item IDs for the AGOL layers of interest
        Orders_Alerts_COP_ItemID = config_env["ORDERSALERTSCOP_ITEMID"]
        Facilities_ItemID = config_env["FACILITIES_ITEMID"]
        Fire_Locations_ItemID = config_env["FIRESLOCATIONS_ITEMID"]
        SOLE_BCR_ItemID = config_env["SOLEBCR_ITEMID"]

        # Create AGOL connection
        print("Creating ArcGIS Online connection...")
        gis = create_ago_connection(config_env)
        print("Connected successfully to ArcGIS Online.")
        token = gis._con.token

        # Get items from AGOL with item IDs
        print("Fetching ArcGIS Online items...")
        Orders_Alerts_COP_item = gis.content.get(Orders_Alerts_COP_ItemID)
        Facilities_item = gis.content.get(Facilities_ItemID)
        Fire_Locations_item = gis.content.get(Fire_Locations_ItemID)
        SOLE_BCR_item = gis.content.get(SOLE_BCR_ItemID)
        #Fire_Polygons_item = gis.content.get(Fire_Polygons_ItemID)
        print("Items fetched successfully.")

        # Call the copy_feature_layers function to make feature layers from the AGOL content
        copied_layers = copy_feature_layers(Orders_Alerts_COP_item, Facilities_item, Fire_Locations_item, SOLE_BCR_item, token)

        # Calculate distances and extract the necessary attributes of the hazards to include in the facility AGOL layer
        print("Calculating distances and attributes for Health Facilities...")
        facilities_hazards_dict = facility_hazard_distances(copied_layers)
        print("Distances and attributes calculated successfully.")

        # Update AGOL layer with calculated data using the dictionary with stored information
        print("Updating AGOL layer with calculated data...")
        update_agol_layer(facilities_hazards_dict, Facilities_item)
        print("AGOL layer updated successfully.")

        print("Script execution completed.")

    except Exception as e:
        print(f"Script execution failed. Error: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
