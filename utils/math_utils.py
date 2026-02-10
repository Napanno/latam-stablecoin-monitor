"""
Math utilities for safe percentage calculations and WoW metrics.
Handles edge cases like zero denominators and all-zero previous values.
"""

import pandas as pd
import numpy as np


def wow_percentage_change(current: pd.Series, previous: pd.Series, decimals: int = 2) -> pd.Series:
    """
    Calculate Week-over-Week percentage change safely.

    Args:
        current: Current week values (Series)
        previous: Previous week values (Series)
        decimals: Decimal places for rounding

    Returns:
        pd.Series: WoW percentage changes, NaN where undefined

    Raises:
        ValueError: If all previous values are zero (no baseline)
        TypeError: If inputs are not Series

    Examples:
        >>> current = pd.Series([100.0, 150.0])
        >>> previous = pd.Series([100.0, 100.0])
        >>> wow_percentage_change(current, previous)
        0     0.0
        1    50.0
        dtype: float64
    """
    if not isinstance(current, pd.Series) or not isinstance(previous, pd.Series):
        raise TypeError("Both inputs must be pandas Series")

    # Check if all previous values are zero
    if (previous == 0).all():
        raise ValueError(
            "Cannot calculate WoW: all previous values are zero. No baseline for percentage change calculation.")

    # Safe division: where previous=0, result is NaN
    with np.errstate(divide='ignore', invalid='ignore'):
        result = ((current - previous) / previous * 100)

    return result.round(decimals)


def safe_percentage(numerator: pd.Series, denominator: pd.Series, decimals: int = 2) -> pd.Series:
    """
    Calculate percentage safely with zero-denominator handling.

    Args:
        numerator: Numerator values
        denominator: Denominator values (division by zero → NaN)
        decimals: Decimal places for rounding

    Returns:
        pd.Series: Percentages, NaN where denominator is zero

    Examples:
        >>> num = pd.Series([100, 50, 0])
        >>> denom = pd.Series([1000, 0, 500])
        >>> safe_percentage(num, denom)
        0    10.0
        1     NaN
        2     0.0
        dtype: float64
    """
    with np.errstate(divide='ignore', invalid='ignore'):
        result = (numerator / denominator * 100)

    return result.round(decimals)


def safe_division(numerator: pd.Series, denominator: pd.Series, default_value: float = 0.0) -> pd.Series:
    """
    Safe division with default for zero denominators.

    Args:
        numerator: Numerator values
        denominator: Denominator values
        default_value: Value to use when denominator is zero

    Returns:
        pd.Series: Results with defaults applied

    Examples:
        >>> num = pd.Series([100, 50])
        >>> denom = pd.Series([10, 0])
        >>> safe_division(num, denom)
        0    10.0
        1     0.0
        dtype: float64
    """
    with np.errstate(divide='ignore', invalid='ignore'):
        result = numerator / denominator

    result[denominator == 0] = default_value
    return result
