"""
Date utilities for standardized ISO 8601 week calculations.
Ensures consistency across all processors (flows, dex, supply).
"""

from datetime import datetime
from typing import Union
import pandas as pd


def get_iso_week(date_obj: Union[datetime, pd.Timestamp]) -> str:
    """
    Convert any datetime-like object to ISO 8601 week format: YYYY-W##

    Args:
        date_obj: datetime or pd.Timestamp object

    Returns:
        str: ISO week in format "YYYY-W##" (e.g., "2026-W04")

    Raises:
        TypeError: If input is not datetime or pd.Timestamp

    Examples:
        >>> get_iso_week(datetime(2026, 1, 1))
        '2026-W01'
        >>> get_iso_week(datetime(2026, 1, 31))
        '2026-W05'
    """
    # Convert pd.Timestamp to datetime if needed
    if isinstance(date_obj, pd.Timestamp):
        date_obj = date_obj.to_pydatetime()

    if not isinstance(date_obj, datetime):
        raise TypeError(
            f"Expected datetime or pd.Timestamp, got {type(date_obj).__name__}"
        )

    iso = date_obj.isocalendar()
    return f"{iso.year}-W{iso.week:02d}"


def validate_week_format(week_str: str) -> bool:
    """
    Validate that string matches ISO 8601 week format YYYY-W##

    Args:
        week_str: String to validate

    Returns:
        bool: True if valid format, False otherwise

    Examples:
        >>> validate_week_format("2026-W04")
        True
        >>> validate_week_format("2026W04")
        False
    """
    import re
    pattern = r'^\d{4}-W\d{2}$'
    return bool(re.match(pattern, week_str))


def extract_week_from_series(series: pd.Series) -> str:
    """
    Extract single week from a pandas Series of datetime objects.
    Validates that all non-null rows belong to the same week.

    Args:
        series: pandas Series of datetime objects

    Returns:
        str: ISO week in format "YYYY-W##"

    Raises:
        ValueError: If series contains multiple different weeks
        ValueError: If series is empty or all null

    Examples:
        >>> dates = pd.Series([datetime(2026, 1, 5), datetime(2026, 1, 6)])
        >>> extract_week_from_series(dates)
        '2026-W02'
    """
    # Remove null values
    non_null = series.dropna()

    if len(non_null) == 0:
        raise ValueError("Series is empty or all null values")

    # Get unique weeks
    weeks = set(get_iso_week(dt) for dt in non_null)

    if len(weeks) > 1:
        raise ValueError(
            f"Series contains multiple weeks: {sorted(weeks)}. "
            f"All rows must belong to same week."
        )

    return weeks.pop()


def get_previous_week(week_str: str) -> str:
    """
    Get the previous ISO week.

    Args:
        week_str: ISO week string in format "YYYY-W##"

    Returns:
        str: Previous week in same format

    Raises:
        ValueError: If input is not valid ISO week format

    Examples:
        >>> get_previous_week("2026-W04")
        '2026-W03'
        >>> get_previous_week("2026-W01")
        '2025-W52'
    """
    if not validate_week_format(week_str):
        raise ValueError(f"Invalid week format: {week_str}. Expected YYYY-W##")

    year, week_num = map(int, week_str.split('-W'))

    if week_num == 1:
        return f"{year - 1}-W52"
    else:
        return f"{year}-W{week_num - 1:02d}"


def week_to_date_range(week_str: str) -> tuple:
    """
    Convert ISO week string to (start_date, end_date) tuple.
    Start is Monday, end is Sunday.

    Args:
        week_str: ISO week string in format "YYYY-W##"

    Returns:
        tuple: (Monday datetime, Sunday datetime)

    Raises:
        ValueError: If input is not valid ISO week format

    Examples:
        >>> start, end = week_to_date_range("2026-W01")
        >>> start.weekday() == 0  # Monday
        True
    """
    if not validate_week_format(week_str):
        raise ValueError(f"Invalid week format: {week_str}. Expected YYYY-W##")

    year, week_num = map(int, week_str.split('-W'))

    # ISO week 1 is the week containing the first Thursday
    # Monday of that week is: Jan 4 of that year - (day of week of Jan 4 - 1)
    jan_4 = datetime(year, 1, 4)
    week_1_monday = jan_4 - pd.Timedelta(days=jan_4.weekday())

    # Add weeks
    start_date = week_1_monday + pd.Timedelta(weeks=week_num - 1)
    end_date = start_date + pd.Timedelta(days=6)  # Sunday

    return (start_date, end_date)