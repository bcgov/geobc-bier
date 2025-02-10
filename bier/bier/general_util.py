"""
bier general function library (bier.py)
Author: Michael Dykes (michael.dykes@gov.bc.ca)
Created: February 10, 2025

Description:
        Contains common classes and functions used by the Business Innovation and Emergency Response section at GeoBC
        for general python script functionality.
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

# Cache for config file
_config_cache = None

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
    global _config_cache
    if _config_cache is not None:
        _log.info("Returning cached config.")
        return _config_cache

    config_file_path: str = os.path.join(
        os.path.dirname(script_file_path), "config.json"
    )
    try:
        with open(config_file_path, encoding="utf-8") as json_conf:
            config_dict: dict = json.load(json_conf)
        _config_cache = config_dict
        _log.info(f"Config file found at {config_file_path}")
        return config_dict
    except FileNotFoundError:
        _log.warning(f"Config file not found at {config_file_path}.")
    except json.JSONDecodeError as e:
        _log.warning(f"Invalid JSON format in config file: {e}")
    except Exception as e:
        _log.warning(f"Unexpected error loading config file: {e}")
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
    value = os.getenv(var_name, default)
    if value == default:
        _log.warning(f"Environment variable {var_name} not found. Using default value.")
    else:
        _log.info(f"Environment variable {var_name} found with value: {value}")
    return value
