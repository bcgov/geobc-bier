'''
AGO Credentials Check

Written by: Michael Dykes (michael.dykes@gov.bc.ca)
Created: February 13 2024

Purpose: Check that AGO credentials are working, if not send message to MS Teams channel
'''

import os, sys, logging, requests, json
from arcgis.gis import GIS
from dotenv import load_dotenv

# set script logger
_log = logging.getLogger(f"{os.path.basename(os.path.splitext(__file__)[0])}")

def import_environment_variables_from_file():
    '''
    Import Environment Variables from File (if necessary)
    '''
    script_dir = os.path.dirname(os.path.abspath(__file__))
    environment_file_path = os.path.join(script_dir, 'environment.env')

    try:
        load_dotenv(dotenv_path=environment_file_path)
        _log.info(f"Environment Variables Imported from File Successfully")
    except:
        _log.info(f"Environment Variables Not Imported from File")

def send_msteams_webhook_message(url):
    payload = {"text": f"Warning - Automated attempt to connect to GeoHub from '{os.environ['AGO_USER']}' account failed. Credentials may have been altered."}
    headers = {'Content-Type': 'application/json'}
    response = requests.post(url, headers=headers, data=json.dumps(payload))
    _log.info(response.text.encode('utf8'))

def main():
    '''Run code'''
    # import bier module
    try:
        import bier
        _log.info(f"bier module imported")
    except:
        sys.path.append(".")
        sys.path.append(os.environ["BIER_PATH"])
        import bier
        _log.info(f"bier module imported")
        
    bier.Set_Logging_Level()

    AGO_Portal_URL = os.environ["AGO_PORTAL_URL"]
    webhook_url = os.environ['BIER_TEAMS_WEBHOOK']

    try:
        AGO = bier.AGO_Connection(AGO_Portal_URL)
    except:
        send_msteams_webhook_message(webhook_url)

    AGO.disconnect()
    _log.info("**Script completed**")

# main function of script (if script is not imported as a module)
if __name__ == "__main__":
    main()
