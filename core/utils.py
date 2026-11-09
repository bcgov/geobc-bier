"""
Core utility functions for the application.

This module contains helper functions for environment variable handling
and date/time parsing, commonly used across scripts.
"""

import os
import datetime
from typing import Optional


def _get_env(key: str) -> str:
    """
    Retrieve the value of a required environment variable.

    This function ensures that critical environment variables are set
    before the application proceeds, raising an error if missing.

    Args:
        key (str): The name of the environment variable.

    Returns:
        str: The value of the environment variable.

    Raises:
        EnvironmentError: If the environment variable is not set.
    """
    value = os.getenv(key)
    if not value:
        raise EnvironmentError(f"Required environment variable '{key}' is not set.")
    return value


def parse_iso_datetime(date_str: Optional[str]) -> Optional[datetime.datetime]:
    """
    Parse an ISO 8601 datetime string into a datetime object.

    Handles two common ISO formats: one with seconds precision and one with microseconds.
    Converts to the local timezone after parsing as UTC.

    Args:
        date_str (Optional[str]): The ISO datetime string to parse, or None.

    Returns:
        Optional[datetime.datetime]: The parsed datetime object in local timezone,
        or None if parsing fails or input is None.
    """
    if not date_str:
        return None

    try:
        # Handle format without microseconds (e.g., "2023-10-01T12:00:00Z")
        if "00:00Z" in date_str:
            return (
                datetime.datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%SZ")
                .replace(tzinfo=datetime.timezone.utc)
                .astimezone()
            )

        # Handle format with microseconds (e.g., "2023-10-01T12:00:00.123456Z")
        return (
            datetime.datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%S.%fZ")
            .replace(tzinfo=datetime.timezone.utc)
            .astimezone()
        )
    except ValueError:
        # Return None if the string doesn't match expected formats
        return None
