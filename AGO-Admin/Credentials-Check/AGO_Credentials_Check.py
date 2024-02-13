'''
AGO Credentials Check

Written by: Michael Dykes (michael.dykes@gov.bc.ca)
Created: February 13 2024

Purpose: Check that AGO credentials are working, if not send message to MS Teams channel
'''

import os, sys, logging, requests, json
from arcgis.gis import GIS

# set script logger
_log = logging.getLogger(f"{os.path.basename(os.path.splitext(__file__)[0])}")

try:
    import bier
    _log.info(f"bier module imported")
except:
    sys.path.append(".")
    sys.path.append(sys.argv[1])
    import bier
    _log.info(f"bier module imported")

def Send_Teams_WebHook_Message():
        url = conf['BIER_Teams_Webhook']
        payload = {"text": f"Warning - Automated attempt to connect to GeoHub from '{portal_username}' account failed. Credentials may have been altered."}
        headers = {'Content-Type': 'application/json'}
        response = requests.post(url, headers=headers, data=json.dumps(payload))
        _log.info(response.text.encode('utf8'))



def main():
    '''Run code'''
    bier.Set_Logging_Level()

    try:
        conf = bier.Find_Config_File(__file__)
        AGO_Portal_URL = conf['AGO_PORTAL_URL']
        AVALANCHEFORECAST_ITEMID = conf['AVALANCHEFORECAST_ITEMID']
    except:
        AGO_Portal_URL = os.environ['AGO_PORTAL_URL']
        AVALANCHEFORECAST_ITEMID = os.environ['AVALANCHEFORECAST_ITEMID']

    AGO = bier.AGO_Connection(AGO_Portal_URL)

    avalanche_geometry_url = r"https://api.avalanche.ca/forecasts/en/areas"
    avalanche_geometry_data = bier.Connect_to_Website_or_API_JSON(avalanche_geometry_url)
    avalanche_attribute_url = r"https://api.avalanche.ca/forecasts/en/products"
    avalanche_attribute_data = bier.Connect_to_Website_or_API_JSON(avalanche_attribute_url,encode=True)

    avalanche_dict = Format_Avalanche_Forecast_Data(avalanche_geometry_data,avalanche_attribute_data)
    AvalancheForecast_item = bier.AGO_Item(AGO,AVALANCHEFORECAST_ITEMID)
    Update_Avalanche_Forecast_AGO(avalanche_dict,AvalancheForecast_item)

    AGO.disconnect()
    _log.info("**Script completed**")

# main function of script (if script is not imported as a module)
if __name__ == "__main__":
    main()



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