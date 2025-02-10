"""
bier api function library (bier.py)
Author: Michael Dykes (michael.dykes@gov.bc.ca)
Created: February 10, 2025

Description:
        Contains common classes and functions used by the Business Innovation and Emergency Response section at GeoBC
        for working with APIs.

Dependencies:
    - tenacity
"""

import logging
import os
import requests
from typing import Optional, Dict, Any
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

# Set script logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
_log = logging.getLogger(os.path.basename(os.path.splitext(__file__)[0]))

# Function to check if exception should trigger a retry
def is_retryable_exception(exception: Exception) -> bool:
    """Determines if an exception should trigger a retry."""
    return isinstance(exception, (requests.exceptions.Timeout, requests.exceptions.ConnectionError))

# Retry decorator with exponential backoff
@retry(
    stop=stop_after_attempt(3),  # Maximum 3 retries
    wait=wait_exponential(multiplier=1, min=2, max=10),  # Exponential backoff (2s, 4s, 8s)
    retry=retry_if_exception_type(requests.exceptions.RequestException),  # Retry on request failures
    reraise=True,
)
def connect_to_api(
    url: str,
    method: str = "GET",
    encode: bool = False,
    headers: Optional[Dict[str, str]] = None,
    params: Optional[Dict[str, Any]] = None,
    data: Optional[Dict[str, Any]] = None,
    json: Optional[Dict[str, Any]] = None,
) -> Optional[Dict[str, Any]]:
    """
    Connects to an API and retrieves JSON data using GET or POST, with retry logic.

    Args:
        url (str): The API endpoint URL.
        method (str): HTTP method ("GET" or "POST"). Defaults to "GET".
        encode (bool): Whether to set response encoding. Defaults to False.
        headers (Optional[Dict[str, str]]): Custom headers for the request. Defaults to None.
        params (Optional[Dict[str, Any]]): Query parameters for GET requests. Defaults to None.
        data (Optional[Dict[str, Any]]): Form-encoded data for POST requests. Defaults to None.
        json (Optional[Dict[str, Any]]): JSON data for POST requests. Defaults to None.

    Returns:
        Optional[Dict[str, Any]]: The JSON response data if successful, otherwise None.
    """
    try:
        response: requests.Response
        if method.upper() == "GET":
            response = requests.get(url, headers=headers, params=params, timeout=10)
        elif method.upper() == "POST":
            response = requests.post(url, headers=headers, params=params, data=data, json=json, timeout=10)
        else:
            _log.error(f"Unsupported HTTP method: {method}")
            return None

        if encode:
            response.encoding = response.apparent_encoding

        response.raise_for_status()
        return response.json()

    except requests.exceptions.Timeout:
        _log.warning(f"API request timed out ({method} {url}), retrying...")
        raise  # Will trigger retry

    except requests.exceptions.HTTPError as http_err:
        if response and 500 <= response.status_code < 600:
            _log.warning(f"Server error ({response.status_code}) ({method} {url}), retrying...")
            raise  # Will trigger retry
        _log.error(f"HTTP error occurred ({method} {url}): {http_err}")

    except requests.exceptions.RequestException as req_err:
        _log.error(f"API connection failed ({method} {url}): {req_err}")
        raise  # Will trigger retry

    return None
