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

def get_update_time(item):
    data_modified = max(i.properties.editingInfo.lastEditDate for i in item.layers + item.tables)
    update_time = datetime.datetime.fromtimestamp(data_modified/1000).strftime("%y-%m-%d %H:%M")
    return update_time

def set_update_time(item, update_time):
    dashboard_item_data = item.get_data()
    update_widgets = ["Population and Properties Table","SOLE/BCR/Order/Alert Table","All Data Table"]

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

def current_OA_groupby_dictionary(item, hazards_list, query=None, layer_num=0):
    flayer = item.layers[layer_num]
    if query:
        feat_dict = flayer.query(query).to_dict()
    else:
        feat_dict = flayer.query().to_dict()

    orders_count = defaultdict(int)
    order_population_sum = defaultdict(int)
    order_properties_sum = defaultdict(int)
    alerts_count = defaultdict(int)
    alert_population_sum = defaultdict(int)
    alert_properties_sum = defaultdict(int)
    total_pop_sum = defaultdict(int)
    total_home_sum = defaultdict(int) 
    total_count = defaultdict(int)
    
    for feature in feat_dict['features']:
        hazards_list.append(feature['attributes']['EVENT_TYPE'])
        if feature['attributes']['MIN_EVENT_START_DATE']:
            year = datetime.datetime.fromtimestamp(feature['attributes']['MIN_EVENT_START_DATE']/1000).year
        else:
            year = 2023
        if feature['attributes']['MIN_START_ORDER_DATE'] and feature['attributes']['ORDER_ALERT_STATUS'] == 'Order':
            orders_count[year,feature['attributes']['PREOC_CODE'],feature['attributes']['EVENT_TYPE'],"Active","Order_Count"] += 1
            order_population_sum[year,feature['attributes']['PREOC_CODE'],feature['attributes']['EVENT_TYPE'],"Active","Order_Pop"] += (feature['attributes']['MULTI_SOURCED_POPULATION'] if feature['attributes']['MULTI_SOURCED_POPULATION'] else 0)
            order_properties_sum[year,feature['attributes']['PREOC_CODE'],feature['attributes']['EVENT_TYPE'],"Active","Order_Home"] += (feature['attributes']['MULTI_SOURCED_HOMES'] if feature['attributes']['MULTI_SOURCED_HOMES'] else 0)
            total_pop_sum[year,feature['attributes']['PREOC_CODE'],feature['attributes']['EVENT_TYPE'],"Active","Total_Pop"] += (feature['attributes']['MULTI_SOURCED_POPULATION'] if feature['attributes']['MULTI_SOURCED_POPULATION'] else 0)
            total_home_sum[year,feature['attributes']['PREOC_CODE'],feature['attributes']['EVENT_TYPE'],"Active","Total_Home"] += (feature['attributes']['MULTI_SOURCED_HOMES'] if feature['attributes']['MULTI_SOURCED_HOMES'] else 0)
            total_count[year,feature['attributes']['PREOC_CODE'],feature['attributes']['EVENT_TYPE'],"Active","Total_Count"] += 1

        elif feature['attributes']['MIN_START_ORDER_DATE'] and feature['attributes']['ORDER_ALERT_STATUS'] == 'Alert':
            orders_count[year,feature['attributes']['PREOC_CODE'],feature['attributes']['EVENT_TYPE'],"Inactive","Order_Count"] += 1
            order_population_sum[year,feature['attributes']['PREOC_CODE'],feature['attributes']['EVENT_TYPE'],"Inactive","Order_Pop"] += (feature['attributes']['MULTI_SOURCED_POPULATION'] if feature['attributes']['MULTI_SOURCED_POPULATION'] else 0)
            order_properties_sum[year,feature['attributes']['PREOC_CODE'],feature['attributes']['EVENT_TYPE'],"Inactive","Order_Home"] += (feature['attributes']['MULTI_SOURCED_HOMES'] if feature['attributes']['MULTI_SOURCED_HOMES'] else 0)
            total_pop_sum[year,feature['attributes']['PREOC_CODE'],feature['attributes']['EVENT_TYPE'],"Inactive","Total_Pop"] += (feature['attributes']['MULTI_SOURCED_POPULATION'] if feature['attributes']['MULTI_SOURCED_POPULATION'] else 0)
            total_home_sum[year,feature['attributes']['PREOC_CODE'],feature['attributes']['EVENT_TYPE'],"Inactive","Total_Home"] += (feature['attributes']['MULTI_SOURCED_HOMES'] if feature['attributes']['MULTI_SOURCED_HOMES'] else 0)
            total_count[year,feature['attributes']['PREOC_CODE'],feature['attributes']['EVENT_TYPE'],"Active","Total_Count"] += 1

        if feature['attributes']['MIN_START_ALERT_DATE'] and feature['attributes']['ORDER_ALERT_STATUS'] == 'Alert':
            alerts_count[year,feature['attributes']['PREOC_CODE'],feature['attributes']['EVENT_TYPE'],"Active","Alert_Count"] += 1
            alert_population_sum[year,feature['attributes']['PREOC_CODE'],feature['attributes']['EVENT_TYPE'],"Active","Alert_Pop"] += (feature['attributes']['MULTI_SOURCED_POPULATION'] if feature['attributes']['MULTI_SOURCED_POPULATION'] else 0)
            alert_properties_sum[year,feature['attributes']['PREOC_CODE'],feature['attributes']['EVENT_TYPE'],"Active","Alert_Home"] += (feature['attributes']['MULTI_SOURCED_HOMES'] if feature['attributes']['MULTI_SOURCED_HOMES'] else 0)
            total_pop_sum[year,feature['attributes']['PREOC_CODE'],feature['attributes']['EVENT_TYPE'],"Active","Total_Pop"] += (feature['attributes']['MULTI_SOURCED_POPULATION'] if feature['attributes']['MULTI_SOURCED_POPULATION'] else 0)
            total_home_sum[year,feature['attributes']['PREOC_CODE'],feature['attributes']['EVENT_TYPE'],"Active","Total_Home"] += (feature['attributes']['MULTI_SOURCED_HOMES'] if feature['attributes']['MULTI_SOURCED_HOMES'] else 0)
            total_count[year,feature['attributes']['PREOC_CODE'],feature['attributes']['EVENT_TYPE'],"Active","Total_Count"] += 1

        elif feature['attributes']['MIN_START_ALERT_DATE'] and feature['attributes']['ORDER_ALERT_STATUS'] == 'Order':
            alerts_count[year,feature['attributes']['PREOC_CODE'],feature['attributes']['EVENT_TYPE'],"Inactive","Alert_Count"] += 1
            alert_population_sum[year,feature['attributes']['PREOC_CODE'],feature['attributes']['EVENT_TYPE'],"Inactive","Alert_Pop"] += (feature['attributes']['MULTI_SOURCED_POPULATION'] if feature['attributes']['MULTI_SOURCED_POPULATION'] else 0)
            alert_properties_sum[year,feature['attributes']['PREOC_CODE'],feature['attributes']['EVENT_TYPE'],"Inactive","Alert_Home"] += (feature['attributes']['MULTI_SOURCED_HOMES'] if feature['attributes']['MULTI_SOURCED_HOMES'] else 0)
            total_pop_sum[year,feature['attributes']['PREOC_CODE'],feature['attributes']['EVENT_TYPE'],"Inactive","Total_Pop"] += (feature['attributes']['MULTI_SOURCED_POPULATION'] if feature['attributes']['MULTI_SOURCED_POPULATION'] else 0)
            total_home_sum[year,feature['attributes']['PREOC_CODE'],feature['attributes']['EVENT_TYPE'],"Inactive","Total_Home"] += (feature['attributes']['MULTI_SOURCED_HOMES'] if feature['attributes']['MULTI_SOURCED_HOMES'] else 0)
            total_count[year,feature['attributes']['PREOC_CODE'],feature['attributes']['EVENT_TYPE'],"Active","Total_Count"] += 1

    oa_current_dict = {}
    for d in [orders_count,alerts_count,order_population_sum,order_properties_sum,alert_population_sum,alert_properties_sum,total_pop_sum,total_home_sum,total_count]:
        for k, v in d.items():  # d.items() in Python 3+
            oa_current_dict.setdefault(k[:-1], {})
            value_dict = {k[-1]:v}
            oa_current_dict[k[:-1]].update(value_dict)

    return oa_current_dict

def historical_OA_groupby_dictionary(item, hazards_list, query=None, layer_num=0):
    flayer = item.layers[layer_num]
    if query:
        feat_dict = flayer.query(query).to_dict()
    else:
        feat_dict = flayer.query().to_dict()

    orders_count = defaultdict(int)
    order_population_sum = defaultdict(int)
    order_properties_sum = defaultdict(int)
    alerts_count = defaultdict(int)
    alert_population_sum = defaultdict(int)
    alert_properties_sum = defaultdict(int) 
    total_pop_sum = defaultdict(int)
    total_home_sum = defaultdict(int)
    total_count = defaultdict(int)
    
    for feature in feat_dict['features']:
        hazards_list.append(feature['attributes']['EVENT_TYPE'])
        preoc_code = feature['attributes']['PREOC_CODE']
        if preoc_code == "VIR":
            preoc_code = "VIC"
        if feature['attributes']['EVENT_START_DATE']:
            year = datetime.datetime.fromtimestamp(feature['attributes']['EVENT_START_DATE']/1000).year
        else:
            year = None
        if feature['attributes']['START_ORDER_DATE']:
            orders_count[year,preoc_code,feature['attributes']['EVENT_TYPE'],"Inactive","Order_Count"] += 1
            order_population_sum[year,preoc_code,feature['attributes']['EVENT_TYPE'],"Inactive","Order_Pop"] += (feature['attributes']['MULTI_SOURCED_POPULATION'] if feature['attributes']['MULTI_SOURCED_POPULATION'] else 0)
            order_properties_sum[year,preoc_code,feature['attributes']['EVENT_TYPE'],"Inactive","Order_Home"] += (feature['attributes']['MULTI_SOURCED_HOMES'] if feature['attributes']['MULTI_SOURCED_HOMES'] else 0)
            total_pop_sum[year,preoc_code,feature['attributes']['EVENT_TYPE'],"Inactive","Total_Pop"] += (feature['attributes']['MULTI_SOURCED_HOMES'] if feature['attributes']['MULTI_SOURCED_HOMES'] else 0)
            total_home_sum[year,preoc_code,feature['attributes']['EVENT_TYPE'],"Inactive","Total_Home"] += (feature['attributes']['MULTI_SOURCED_HOMES'] if feature['attributes']['MULTI_SOURCED_HOMES'] else 0)
            total_count[year,preoc_code,feature['attributes']['EVENT_TYPE'],"Inactive","Total_Count"] += 1

        if feature['attributes']['START_ALERT_DATE']:
            alerts_count[year,preoc_code,feature['attributes']['EVENT_TYPE'],"Inactive","Alert_Count"] += 1
            alert_population_sum[year,preoc_code,feature['attributes']['EVENT_TYPE'],"Inactive","Alert_Pop"] += (feature['attributes']['MULTI_SOURCED_POPULATION'] if feature['attributes']['MULTI_SOURCED_POPULATION'] else 0)
            alert_properties_sum[year,preoc_code,feature['attributes']['EVENT_TYPE'],"Inactive","Alert_Home"] += (feature['attributes']['MULTI_SOURCED_HOMES'] if feature['attributes']['MULTI_SOURCED_HOMES'] else 0)
            total_pop_sum[year,preoc_code,feature['attributes']['EVENT_TYPE'],"Inactive","Total_Pop"] += (feature['attributes']['MULTI_SOURCED_POPULATION'] if feature['attributes']['MULTI_SOURCED_POPULATION'] else 0)
            total_home_sum[year,preoc_code,feature['attributes']['EVENT_TYPE'],"Inactive","Total_Home"] += (feature['attributes']['MULTI_SOURCED_HOMES'] if feature['attributes']['MULTI_SOURCED_HOMES'] else 0)
            total_count[year,preoc_code,feature['attributes']['EVENT_TYPE'],"Inactive","Total_Count"] += 1

    oa_historical_dict = {}
    for d in [orders_count,alerts_count,order_population_sum,order_properties_sum,alert_population_sum,alert_properties_sum,total_pop_sum,total_home_sum,total_count]:
        for k, v in d.items():  # d.items() in Python 3+
            oa_historical_dict.setdefault(k[:-1], {})
            value_dict = {k[-1]:v}
            oa_historical_dict[k[:-1]].update(value_dict)

    return oa_historical_dict

def SOLE_BCR_groupby_dictionary(item, hazards_list, query=None, layer_num=0):
    flayer = item.layers[layer_num]
    if query:
        feat_dict = flayer.query(query).to_dict()
    else:
        feat_dict = flayer.query().to_dict()

    SOLE_count = defaultdict(int)
    BCR_count = defaultdict(int)
    total_count = defaultdict(int)
    
    for feature in feat_dict['features']:
        hazards_list.append(feature['attributes']['EVENT_TYPE'])
        if feature['attributes']['START_DATE']:
            year = datetime.datetime.fromtimestamp(feature['attributes']['START_DATE']/1000).year
        else:
            year = None
        if feature['attributes']['SOLE_TYPE_CODE'] == "LG":
            SOLE_count[year,feature['attributes']['PREOC_CODE'],feature['attributes']['EVENT_TYPE'],"Active","SOLE_Count"] += 1
            total_count[year,feature['attributes']['PREOC_CODE'],feature['attributes']['EVENT_TYPE'],"Active","Total_Count"] += 1

        if feature['attributes']['SOLE_TYPE_CODE'] == "FN":
            BCR_count[year,feature['attributes']['PREOC_CODE'],feature['attributes']['EVENT_TYPE'],"Active","BCR_Count"] += 1
            total_count[year,feature['attributes']['PREOC_CODE'],feature['attributes']['EVENT_TYPE'],"Active","Total_Count"] += 1

    sole_bcr_dict = {}
    for d in [SOLE_count,BCR_count,total_count]:
        for k, v in d.items():  # d.items() in Python 3+
            sole_bcr_dict.setdefault(k[:-1], {})
            value_dict = {k[-1]:v}
            sole_bcr_dict[k[:-1]].update(value_dict)

    return sole_bcr_dict

def new_oa_table(hazards_list,current_OA, historical_OA, SOLE_BCR):
    years_list = [None,2017,2018,2019,2020,2021,2022,2023,2024]
    active_list = ['Active','Inactive']
    preoc_list = ['CTL','NEA','NWE','SEA','SWE','VIC']

    combined_dict = {}
    for year in years_list:
        for preoc in preoc_list:
            for hazard in hazards_list:
                for active in active_list:
                    #SOLE_count, BC_count, Order_count, Alert_count, Pop_Order, Pop_Alert, Home_Order, Home_Alert
                    combined_dict[year,preoc,hazard,active] = {'SOLE_Count': None,'BCR_Count': None,'Order_Count': None,'Alert_Count': None,'Order_Pop': None,'Alert_Pop': None,'Order_Home': None,'Alert_Home': None,'Total_Pop': None,'Total_Home': None, 'Total_Count': None}

    for oa_dict in [current_OA,historical_OA]:
        for k,v in oa_dict.items():
            if 'Order_Count' in v:
                if combined_dict[k]['Order_Count']:
                    combined_dict[k]['Order_Count'] += v['Order_Count']
                else:
                    combined_dict[k]['Order_Count'] = v['Order_Count']

            if 'Alert_Count' in v:
                if combined_dict[k]['Alert_Count']:
                    combined_dict[k]['Alert_Count'] += v['Alert_Count']
                else:
                    combined_dict[k]['Alert_Count'] = v['Alert_Count']

            if 'Order_Pop' in v:
                if combined_dict[k]['Order_Pop']:
                    combined_dict[k]['Order_Pop'] += v['Order_Pop']
                else:
                    combined_dict[k]['Order_Pop'] = v['Order_Pop']

            if 'Alert_Pop' in v:
                if combined_dict[k]['Alert_Pop']:
                    combined_dict[k]['Alert_Pop'] += v['Alert_Pop']
                else:
                    combined_dict[k]['Alert_Pop'] = v['Alert_Pop']

            if 'Order_Home' in v:
                if combined_dict[k]['Order_Home']:
                    combined_dict[k]['Order_Home'] += v['Order_Home']
                else:
                    combined_dict[k]['Order_Home'] = v['Order_Home']

            if 'Alert_Home' in v:
                if combined_dict[k]['Alert_Home']:
                    combined_dict[k]['Alert_Home'] += v['Alert_Home']
                else:
                    combined_dict[k]['Alert_Home'] = v['Alert_Home']

            if 'Total_Pop' in v:
                if combined_dict[k]['Total_Pop']:
                    combined_dict[k]['Total_Pop'] += v['Total_Pop']
                else:
                    combined_dict[k]['Total_Pop'] = v['Total_Pop']

            if 'Total_Home' in v:
                if combined_dict[k]['Total_Home']:
                    combined_dict[k]['Total_Home'] += v['Total_Home']
                else:
                    combined_dict[k]['Total_Home'] = v['Total_Home']

            if 'Total_Count' in v:
                if combined_dict[k]['Total_Count']:
                    combined_dict[k]['Total_Count'] += v['Total_Count']
                else:
                    combined_dict[k]['Total_Count'] = v['Total_Count']

    for k,v in SOLE_BCR.items():
        if 'SOLE_Count' in v:
            if combined_dict[k]['SOLE_Count']:
                combined_dict[k]['SOLE_Count'] += v['SOLE_Count']
            else:
                combined_dict[k]['SOLE_Count'] = v['SOLE_Count']

        if 'BCR_Count' in v:
            if combined_dict[k]['BCR_Count']:
                combined_dict[k]['BCR_Count'] += v['BCR_Count']
            else:
                combined_dict[k]['BCR_Count'] = v['BCR_Count']

        if 'Total_Count' in v:
            if combined_dict[k]['Total_Count']:
                combined_dict[k]['Total_Count'] += v['Total_Count']
            else:
                combined_dict[k]['Total_Count'] = v['Total_Count']

    return combined_dict

def update_impacts_table(Orders_Alerts_Impact_Table_item, combined_dict):
    delete_and_truncate(Orders_Alerts_Impact_Table_item)

    new_features = []
    for k,v in combined_dict.items():
        # Build attributes to populate feature attribute table, check for none values in the EST_TIME_ON, OFFTIME and UPDATED date fields
        attributes = {"EVENT_YEAR": k[0], 
                "PREOC_CODE": k[1],
                "EVENT_TYPE": k[2],
                "ACTIVE": k[3],
                "SOLE_COUNT": (None if v['SOLE_Count'] == 0 else v['SOLE_Count']),
                "BCR_COUNT": (None if v['BCR_Count'] == 0 else v['BCR_Count']),
                "ORDER_COUNT": (None if v['Order_Count'] == 0 else v['Order_Count']),
                "ALERT_COUNT": (None if v['Alert_Count'] == 0 else v['Alert_Count']),
                "ORDER_POP": (None if v['Order_Pop'] == 0 else v['Order_Pop']),
                "ALERT_POP": (None if v['Alert_Pop'] == 0 else v['Alert_Pop']),
                "ORDER_HOME": (None if v['Order_Home'] == 0 else v['Order_Home']),
                "ALERT_HOME": (None if v['Alert_Home'] == 0 else v['Alert_Home']),
                "TOTAL_POP": (None if v['Total_Pop'] == 0 else v['Total_Pop']),
                "TOTAL_HOME": (None if v['Total_Home'] == 0 else v['Total_Home']),
                "TOTAL_COUNT": (None if v['Total_Count'] == 0 else v['Total_Count'])
                }
        # Create new feature
        newfeature = features.Feature(None,attributes)
        new_features.append(newfeature)

    append_data(Orders_Alerts_Impact_Table_item, new_features)

def main():
    '''Run code (if executed as script)'''
    bier.Set_Logging_Level()
    conf = bier.Find_Config_File(__file__)
    AGO = bier.AGO_Connection(conf["AGO_Portal_URL"])

    gis = AGO.connection
    OrdersAlerts_item = gis.content.get(conf["Orders_Alerts_COP_ItemID"])
    SOLE_BCR_item = gis.content.get(conf["SOLE_BCR_ItemID"])
    Historical_Orders_Alerts_item = gis.content.get(conf["Historical_Orders_Alerts_ItemID"])
    Orders_Alerts_Impact_Table_item = gis.content.get(conf["Orders_Alerts_Impact_Table_ItemID"])
    Orders_Alerts_Impact_Dashboard = gis.content.get(conf["Orders_Alerts_Impact_Dashboard_ItemID"])

    update_time = get_update_time(OrdersAlerts_item)
    set_update_time(Orders_Alerts_Impact_Dashboard, update_time)

    hazards_list = []
    current_OA = current_OA_groupby_dictionary(OrdersAlerts_item,hazards_list)
    historical_OA = historical_OA_groupby_dictionary(Historical_Orders_Alerts_item,hazards_list)
    SOLE_BCR = SOLE_BCR_groupby_dictionary(SOLE_BCR_item,hazards_list)

    combined_dict = new_oa_table(hazards_list,current_OA, historical_OA, SOLE_BCR)
    update_impacts_table(Orders_Alerts_Impact_Table_item, combined_dict)

    AGO.disconnect()
    _log.info("**Script completed**")

if __name__ == "__main__":
    main()