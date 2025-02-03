import os, sys, json, logging, datetime
import pandas as pd
from datetime import datetime
from arcgis.gis import GIS

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

gis = GIS(PORTAL_URL, username=PORTAL_USERNAME, password=PORTAL_PASSWORD, expiration=999999)

Hub_Items ={"Main Pages":{"Homepage":"12869505897c4a73b62ac2ff3bda8394","Floods":"0f3221d1d85842aeac585827f33505f6","Wildfires":"1cd2e5ba401d4cd3b3ebd829abb0ed1f",
"Extreme Heat":"a8168463200b4ba593677161af375223","Tsunamis":"4acf3b0f91854e3381472c38dd68c5f4","Earthquakes":"816ec7fde6f64ff4a673d02e0b1c96d5","Drought and water scarcity":"5118256793504bbd925d654001e53049",
"Risk Data":"506b1780c3954fe9a5e58d3047b98aa2","Extreme cold and winter storms":"f4c3a60fd02c4a94b35b41ca79fe313d","Resources":"ad9878be03b34390abba0ff8aa3b34fe","About":"b8af7d55931d4c2b835fcc61c2976295"}}

today = datetime.today().strftime('%Y%m%d')
output_folder = os.path.join(os.path.dirname(__file__),'ClimateReadyBC_UsageFiles')
writer = pd.ExcelWriter(f"{output_folder}\\ClimateReadyBC_Usage_{today}.xlsx", engine='xlsxwriter')
workbook = writer.book

for k,v in Hub_Items.items():
    num = 1
    row = 2
    max_row = 9
    for title, ItemID in v.items():
        Item = gis.content.get(ItemID)
        print(Item.usage("7D",True))
        df = Item.usage("7D",True)
        print(title)
        print(df)
        df['PageName'] = title
        df['ItemID'] = ItemID
        df['Date'] = pd.to_datetime(df['Date']).dt.date
        if num == 1:
            ClimateUsage = df
        else:
            ClimateUsage = pd.concat([ClimateUsage, df])
        num +=1

        xlsx_file = ClimateUsage.to_excel(writer, sheet_name=k, index=False)
        worksheet = writer.sheets[k]
        for row_num in range(1, 200):
            worksheet.set_row(row_num, 30)
        worksheet.set_column(0, 2, 20)
        worksheet.set_column(3, 3, 35)

        # Create a chart object.
        chart = workbook.add_chart({'type': 'column','name':title})

        # Configure the chart axes.
        chart.set_y_axis({'visible': False, 'major_gridlines': {'visible': False}})
        chart.set_x_axis({'date_axis': True})

        # Turn off chart legend. It is on by default in Excel.
        chart.set_legend({'position': 'none'})

        # Configure the series of the chart from the dataframe data.
        chart.add_series({'categories': [k, row-1, 0, max_row-1, 0],'values': [k, row-1, 1, max_row-1, 1],'data_labels': {'value': True}})
        chart.set_title({'name': title})

        # Insert the chart into the worksheet.
        worksheet.insert_chart(row, 5, chart)

        row += 8
        max_row += 8

# Close the Pandas Excel writer and output the Excel file.
writer.close()