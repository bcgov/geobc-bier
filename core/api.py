"""
Core API module for handling HTTP requests with built-in retry logic.

This module provides a robust way to make HTTP requests that automatically
retries on transient failures, ensuring reliable communication with external APIs.
"""

import requests
import logging
from typing import Optional, Dict, Any
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

# Set up a logger specific to this module for better debugging and monitoring
_log = logging.getLogger("core.api")

# Create a shared session for connection reuse, improving performance for multiple requests
_session = requests.Session()


@retry(
    stop=stop_after_attempt(3),  # Retry up to 3 times
    wait=wait_exponential(multiplier=1, min=2, max=10),  # Exponential backoff: 2s, 4s, 8s
    retry=retry_if_exception_type(
        (requests.exceptions.Timeout, requests.exceptions.RequestException)
    ),  # Only retry on timeout or general request exceptions
    reraise=True,  # Re-raise the exception after retries are exhausted
)
def request_json(
    url: str,
    method: str = "GET",
    headers: Optional[Dict[str, str]] = None,
    params: Optional[Dict[str, Any]] = None,
    data: Optional[Dict[str, Any]] = None,
    json: Optional[Dict[str, Any]] = None,
    timeout: int = 10,
) -> Dict[str, Any]:
    """
    Make an HTTP request and return the JSON response with automatic retries.

    This function supports GET and POST methods, handles common request parameters,
    and includes retry logic for transient network issues.

    Args:
        url (str): The URL to request.
        method (str): HTTP method, either 'GET' or 'POST'. Defaults to 'GET'.
        headers (Optional[Dict[str, str]]): HTTP headers to include.
        params (Optional[Dict[str, Any]]): URL parameters for GET requests.
        data (Optional[Dict[str, Any]]): Form data for POST requests.
        json (Optional[Dict[str, Any]]): JSON data for POST requests.
        timeout (int): Request timeout in seconds. Defaults to 10.

    Returns:
        Dict[str, Any]: The JSON response from the server.

    Raises:
        ValueError: If an unsupported HTTP method is provided.
        requests.exceptions.HTTPError: If the response status is not 2xx.
        Other exceptions: As raised by the underlying requests library after retries.
    """
    response = None

    try:
        # Perform the HTTP request based on the method
        if method.upper() == "GET":
            response = _session.get(url, headers=headers, params=params, timeout=timeout)
        elif method.upper() == "POST":
            response = _session.post(url, headers=headers, params=params, data=data, json=json, timeout=timeout)
        else:
            raise ValueError(f"Unsupported HTTP method: {method}")

        # Raise an exception for bad status codes (4xx, 5xx)
        response.raise_for_status()
        # Parse and return the JSON response
        return response.json()

    except Exception as e:
        # Log server errors (5xx) for monitoring purposes
        if response is not None and 500 <= response.status_code < 600:
            _log.warning(f"Server error {response.status_code}: {url}")
        # Re-raise the exception to let the caller handle it
        raise