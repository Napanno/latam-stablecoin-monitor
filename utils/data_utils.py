import pandas as pd
from typing import List


def validate_dataframe(df: pd.DataFrame, required_cols: List[str]) -> bool:
    """Check if DataFrame has all required columns"""
    missing = set(required_cols) - set(df.columns)
    if missing:
        raise ValueError(f"Missing columns: {missing}")
    return True


def handle_null_address(address_str: str) -> bool:
    """Check if address is null address (0x0000...)"""
    if not address_str:
        return False
    return address_str.lower() == "0x0000000000000000000000000000000000000000"


def safe_divide(numerator: pd.Series, denominator: pd.Series, fill_value: float = 0.0) -> pd.Series:
    """Safely divide Series, handling zero division"""
    return pd.Series(
        [n / d if d != 0 else fill_value for n, d in zip(numerator, denominator)]
    )