"""
bier function library (bier.py)
Author: Michael Dykes (michael.dykes@gov.bc.ca)
Created: May 29, 2023

Description:
        Contains common classes and functions used by the Business Innovation and Emergency Response section at GeoBC
        for GIS data processing and interaction with ArcGIS Online (AGO) and other GIS tools.

Dependencies:
    - arcgis
    - minio
    - tenacity
"""

import logging
import os
import json

# Set script logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
_log = logging.getLogger(os.path.basename(os.path.splitext(__file__)[0]))

# Generic Python Utilities
def find_config_file(script_file_path: str) -> dict:
    """
    Locate and load a configuration file named `config.json`.

    Args:
        script_file_path (str): The full path of the script file.

    Returns:
        dict or None: The parsed JSON configuration dictionary if found,
                      otherwise None.
    """
    config_file_path: str = os.path.join(
        os.path.dirname(script_file_path), "config.json"
    )
    try:
        with open(config_file_path, encoding="utf-8") as json_conf:
            config_dict: dict = json.load(json_conf)
        _log.info("Config file found")
        return config_dict
    except FileNotFoundError:
        _log.warning("Config file not found.")
    except json.JSONDecodeError as e:
        _log.warning(f"Invalid JSON format: {e}")
    except Exception as e:
        _log.warning(f"Unexpected error: {e}")
    return None

def get_env_variable(var_name: str, default: str = "") -> str:
    """
    Get the value of an environment variable.

    Args:
        var_name (str): The name of the environment variable.
        default (str): The default value if the environment variable is not set.

    Returns:
        str: The value of the environment variable or the default value.
    """
    return os.getenv(var_name, default)