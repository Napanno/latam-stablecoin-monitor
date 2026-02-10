from abc import ABC, abstractmethod
from typing import Dict, Optional
import pandas as pd
from pathlib import Path
from datetime import datetime
from utils.logger import get_logger
from utils.date_utils import extract_week_from_series

logger = get_logger(__name__)


class BaseProcessor(ABC):
    """Abstract base class for all KPI processors"""

    def __init__(self, domain_name: str):
        """
        Initialize processor with domain-specific metadata

        Args:
            domain_name: Human-readable name (e.g., "Supply", "Flows", "DEX")
        """
        self.domain_name = domain_name
        self.domain_num = self._extract_domain_num(domain_name)
        self.kpi_data = {}
        self.export_dir = Path("./data/kpi")
        self.export_dir.mkdir(parents=True, exist_ok=True)

    def _extract_domain_num(self, name: str) -> int:
        """Map domain name to number for output filenames"""
        mapping = {"supply": 1, "flows": 2, "dex": 3}
        return mapping.get(name.lower(), 0)

    @abstractmethod
    def process_all(self, raw_data: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        """
        Process raw data into all KPIs for this domain

        Args:
            raw_data: DataFrame from Dune query

        Returns:
            Dictionary of KPI DataFrames, keyed by KPI name
        """
        pass

    def export_kpis(self, timestamp: Optional[str] = None) -> Dict[str, Path]:
        """
        Export all KPIs to CSV with consistent naming

        Args:
            timestamp: Optional timestamp override (YYYYMMDD_HHMMSS format)

        Returns:
            Dictionary mapping KPI names to file paths
        """
        if not timestamp:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        exported_files = {}

        for kpi_name, df in self.kpi_data.items():
            # Extract week from data if present
            week_str = self._extract_week_from_data(df)

            # Construct filename: domain_X_{kpi_name}_{week}_{timestamp}.csv
            filename = f"domain_{self.domain_num}_{kpi_name}_{week_str}_{timestamp}.csv"
            filepath = self.export_dir / filename

            df.to_csv(filepath, index=False)
            logger.info(f"✓ Exported: {filename}")
            exported_files[kpi_name] = filepath

        return exported_files

    def _extract_week_from_data(self, df: pd.DataFrame) -> str:
        """
        Extract ISO week string from DataFrame using standardized utility

        Returns:
            ISO week format (e.g., "2026-W04") or "UNKNOWN"
        """
        if df.empty:
            return "UNKNOWN"

        # Try common week column names
        for col in ["week", "period", "iso_week"]:
            if col in df.columns:
                try:
                    # Get first non-null value
                    week_series = df[col].dropna()
                    if not week_series.empty:
                        # NEW: Use standardized extract_week_from_series
                        week_val = extract_week_from_series(week_series)
                        return week_val.replace("-", "_")
                except (ValueError, TypeError):
                    # If extraction fails, continue to next column
                    continue

        return "UNKNOWN"

    def clean_numeric_columns(self, df: pd.DataFrame, columns: list) -> pd.DataFrame:
        """
        Safely convert columns to numeric, handling string inputs from Dune

        Args:
            df: Input DataFrame
            columns: List of column names to convert

        Returns:
            DataFrame with numeric columns converted
        """
        df = df.copy()

        for col in columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        return df

    def log_processing_summary(self, df: pd.DataFrame, stage: str = "input"):
        """Log data processing checkpoint"""
        logger.debug(
            f"[{self.domain_name}] {stage}: "
            f"{len(df):,} rows, {len(df.columns)} cols"
        )
