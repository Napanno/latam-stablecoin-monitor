import pandas as pd
from datetime import datetime


def get_iso_week(date: pd.Timestamp) -> str:
    """Convert date to ISO week string (YYYY_W##)"""
    iso = date.isocalendar()
    return f"{iso.year}_W{iso.week:02d}"


def get_week_date_range(week_str: str) -> tuple:
    """
    Convert ISO week string to date range

    Args:
        week_str: Format "2025_W52"

    Returns:
        (start_date, end_date) tuple
    """
    year, week = week_str.split('_W')
    year, week = int(year), int(week)

    # ISO week 1 is the week with Thursday in it
    jan4 = datetime(year, 1, 4)
    week_one_monday = jan4 - pd.Timedelta(days=jan4.weekday())

    start = week_one_monday + pd.Timedelta(weeks=week - 1)
    end = start + pd.Timedelta(days=6)

    return (start.date(), end.date())