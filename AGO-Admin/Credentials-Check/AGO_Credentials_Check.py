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
    url = os.environ['BIER_TEAMS_WEBHOOK']
    payload = {"text": f"Warning - Automated attempt to connect to GeoHub from '{os.environ['AGO_USER']}' account failed. Credentials may have been altered."}
    headers = {'Content-Type': 'application/json'}
    response = requests.post(url, headers=headers, data=json.dumps(payload))
    _log.info(response.text.encode('utf8'))

def main():
    '''Run code'''
    bier.Set_Logging_Level()

    try:
        conf = bier.Find_Config_File(__file__)
        AGO_Portal_URL = conf['AGO_PORTAL_URL']
    except:
        AGO_Portal_URL = os.environ['AGO_PORTAL_URL']

    try:
        AGO = bier.AGO_Connection(AGO_Portal_URL)
    except:
        Send_Teams_WebHook_Message()

    AGO.disconnect()
    _log.info("**Script completed**")

# main function of script (if script is not imported as a module)
if __name__ == "__main__":
    main()