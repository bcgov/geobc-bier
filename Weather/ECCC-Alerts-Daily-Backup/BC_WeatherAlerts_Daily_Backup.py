'''
BC Weather Alerts Daily Backup

Written by: Michael Dykes (michael.dykes@gov.bc.ca)
Created: September 2 2022

Purpose: Get BC Weather Alerts from ECCC website (https://weather.gc.ca/?layers=alert&province=BC&zoom=5&center=54.98445087,-125.28692377) GeoJSON 
and append to historic hosted feature layer in GeoHub
'''

import os, sys, logging, xmltodict, json, requests
from bs4 import BeautifulSoup
from arcgis.gis import GIS
from arcgis import geometry, features
from datetime import datetime, timezone, timedelta

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

HistoricAlerts_itemID = CONF["HistoricAlerts_itemID"]

todaydate_str = datetime.now().strftime("%Y%m%d")

#missed_list = ["20240124","20240126","20240127","20240128"]

url = "https://dd.weather.gc.ca/alerts/cap/"
r = requests.get(url)

gis = GIS(PORTAL_URL, username=PORTAL_USERNAME, password=PORTAL_PASSWORD, expiration=9999)
HFS_item = gis.content.get(HistoricAlerts_itemID)

soup = BeautifulSoup(r.content, 'html.parser')

for date_item in soup.findAll('a'):
    if date_item["href"][:-1].isnumeric():
        if date_item["href"][:-1] == todaydate_str:
            #if date_item["href"][:-1] in missed_list:
            subdir = url + date_item["href"]
            sub_r = requests.get(subdir)
            sub_soup = BeautifulSoup(sub_r.content, 'html.parser')
            for area_item in sub_soup.findAll('a'):
                if "CWVR" in area_item["href"]:
                    areadir = subdir + area_item["href"]
                    area_r = requests.get(areadir)
                    area_soup = BeautifulSoup(area_r.content, 'html.parser')
                    for time_item in area_soup.findAll('a'):
                        if time_item["href"][:-1].isnumeric():
                            timedir = areadir + time_item["href"]
                            time_r = requests.get(timedir)
                            time_soup = BeautifulSoup(time_r.content, 'html.parser')
                            for download_item in time_soup.findAll('a'):
                                if ".cap" in download_item["href"]:
                                    filename = download_item["href"]
                                    log.info(filename)
                                    download_url = timedir + filename
                                    download_r = requests.get(download_url)
                                    o = xmltodict.parse(download_r.content)
                                    data = json.dumps(o)

                                    for row in o["alert"]["info"]:
                                        if row["language"] == "en-CA":
                                            Category = row["category"][1]
                                            Event = row["event"]
                                            responseType = row["responseType"]
                                            urgency = row["urgency"]
                                            severity = row["severity"]
                                            effective = datetime.strptime(row["effective"], '%Y-%m-%dT%H:%M:%S-00:00')
                                            expires = datetime.strptime(row["expires"], '%Y-%m-%dT%H:%M:%S-00:00')
                                            headline = row["headline"]
                                            description = row["description"]

                                            geom = row["area"]
                                            Alert_Type = row["parameter"][0]["value"]
                                            Alert_Name = row["parameter"][7]["value"]
                                            Alert_Coverage = row["parameter"][8]["value"]
                                            try:
                                                for area in geom:
                                                    areaDesc = area["areaDesc"]
                                                    polygon_str = area["polygon"]
                                                    polygon_array = []
                                                    
                                                    for row in polygon_str.split():
                                                        y,x = row.split(",")
                                                        polygon_array.append([x,y])
                                                    
                                                    geom = geometry.Geometry({"type": "Polygon", "rings" : [polygon_array],"spatialReference" : {"wkid" : 4326}})
                                                    attributes = {"Alert_Type": Alert_Type, 
                                                                    "Alert_Name": Alert_Name,
                                                                    "Alert_Coverage": Alert_Coverage,
                                                                    "areaDesc": areaDesc,
                                                                    "Headline": headline,
                                                                    "Description": description,
                                                                    "Effective": effective.replace(tzinfo=timezone.utc).astimezone(tz=None) if effective else None,
                                                                    "Expires":  expires.replace(tzinfo=timezone.utc).astimezone(tz=None) if expires else None,
                                                                    "Category": Category,
                                                                    "Event": Event,
                                                                    "responseType": responseType,
                                                                    "urgency": urgency,
                                                                    "severity": severity,
                                                                    "MSC_Source":filename}
                                                    newfeature = features.Feature(geom,attributes)
                                                    result = HFS_item.layers[0].edit_features(adds = [newfeature])
                                                    log.info(result)

                                            except:
                                                areaDesc = geom["areaDesc"]
                                                polygon_str = geom["polygon"]

                                                polygon_array = []
                                                for row in polygon_str.split():
                                                    y,x = row.split(",")
                                                    polygon_array.append([x,y])
                                                    
                                                geom = geometry.Geometry({"type": "Polygon", "rings" : [polygon_array],"spatialReference" : {"wkid" : 4326}})
                                                attributes = {"Alert_Type": Alert_Type, 
                                                                "Alert_Name": Alert_Name,
                                                                "Alert_Coverage": Alert_Coverage,
                                                                "areaDesc": areaDesc,
                                                                "Headline": headline,
                                                                "Description": description,
                                                                "Effective": effective.replace(tzinfo=timezone.utc).astimezone(tz=None) + timedelta(hours=7) if effective else None,
                                                                "Expires":  expires.replace(tzinfo=timezone.utc).astimezone(tz=None) + timedelta(hours=7) if expires else None,
                                                                "Category": Category,
                                                                "Event": Event,
                                                                "responseType": responseType,
                                                                "urgency": urgency,
                                                                "severity": severity}
                                                newfeature = features.Feature(geom,attributes)
                                                result = HFS_item.layers[0].edit_features(adds = [newfeature])
                                                log.info(result)