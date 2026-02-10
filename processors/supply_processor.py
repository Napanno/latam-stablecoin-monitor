"""
Supply KPI Processor - Domain 3
Processes supply data derived from flows (mint/burn events)
"""

import pandas as pd
from pathlib import Path
from datetime import datetime
from typing import Dict, Optional
from utils.logger import get_logger
from utils.date_utils import get_iso_week
from utils.math_utils import wow_percentage_change, safe_division

logger = get_logger(__name__)


class SupplyKPIProcessor:
    """Process supply data to calculate supply change KPIs"""

    def __init__(self, output_dir: str = 'data/kpi'):
        """Initialize processor"""
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        logger.info("SupplyKPIProcessor initialized")

    def process_all(self, flows_df: pd.DataFrame) -> Dict:
        """
        Process supply data (derived from flows)

        Args:
            flows_df: DataFrame from flows query with mint/burn data

        Returns:
            Dictionary with Supply KPIs
        """
        if flows_df is None or flows_df.empty:
            logger.error("Empty flows data received")
            return {}

        logger.info(f"Processing supply data from flows: {len(flows_df)} rows")

        # Clean data
        df = self._clean_data(flows_df)

        # Calculate KPIs
        results = {
            'supply_change': self._kpi1_supply_change(df),
            'issuance_rate': self._kpi2_issuance_rate(df),
            'token_metrics': self._kpi3_token_metrics(df),
            'wow_supply_change': self._kpi4_wow_supply_change(df),
        }

        logger.info("✅ Supply processing complete")
        return results

    def _clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean and prepare supply data (from flows)

        Args:
            df: Raw supply/flows DataFrame

        Returns:
            Cleaned DataFrame
        """
        df = df.copy()

        # ===================================================================
        # STANDARDIZED DATE HANDLING (I4 Fix)
        # Convert any date column to both datetime and ISO week format
        # ===================================================================
        date_col_found = False

        if 'block_time' in df.columns:
            df['block_time'] = pd.to_datetime(df['block_time'], errors='coerce')
            df['week'] = df['block_time'].apply(get_iso_week)
            date_col_found = True
            logger.debug("Using 'block_time' as primary date column")

        elif 'week_start' in df.columns:
            df['week_start'] = pd.to_datetime(df['week_start'], errors='coerce')
            df['block_time'] = df['week_start']
            df['week'] = df['week_start'].apply(get_iso_week)
            date_col_found = True
            logger.debug("Using 'week_start' as primary date column (mapped to block_time)")

        elif 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'], errors='coerce')
            df['block_time'] = df['date']
            df['week'] = df['date'].apply(get_iso_week)
            date_col_found = True
            logger.debug("Using 'date' as primary date column (mapped to block_time)")

        if not date_col_found:
            logger.error("No date column found in supply data")
            raise ValueError("Missing date column (expected 'block_time', 'week_start', or 'date')")

        if 'week' not in df.columns:
            logger.warning("'week' column not created - using fallback")
            df['week'] = df['block_time'].apply(get_iso_week)
        # ===================================================================

        # Convert numeric columns
        numeric_columns = ['mint_volume_usd', 'burn_volume_usd', 'mint_count',
                           'burn_count', 'total_supply', 'circulating_supply',
                           'amount', 'amount_usd']
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        # Fill NaN with 0
        df = df.fillna(0)

        # Remove rows with missing critical data
        df = df.dropna(subset=['week', 'symbol'])

        logger.info(f"✓ Cleaned supply data: {len(df)} rows")
        logger.debug(f"  Date column: block_time, Week column: week (ISO format)")

        return df

    def _kpi1_supply_change(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        KPI 3.1: Weekly supply change by token and blockchain

        Returns:
            DataFrame with weekly supply changes
        """
        # Check if we have pre-aggregated mint/burn data
        if 'mint_volume_usd' in df.columns and 'burn_volume_usd' in df.columns:
            agg_dict = {
                'mint_volume_usd': 'sum',
                'burn_volume_usd': 'sum',
            }

            # Include counts if available
            if 'mint_count' in df.columns:
                agg_dict['mint_count'] = 'sum'
            if 'burn_count' in df.columns:
                agg_dict['burn_count'] = 'sum'

            kpi = df.groupby(['week', 'symbol', 'blockchain']).agg(agg_dict).reset_index()

            # Calculate net issuance
            kpi['net_issuance_usd'] = kpi['mint_volume_usd'] - kpi['burn_volume_usd']

            return kpi.sort_values('week', ascending=False)

        # Fallback: no pre-aggregated data available
        logger.warning("No pre-aggregated mint/burn data found")
        return pd.DataFrame()

    def _kpi2_issuance_rate(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        KPI 3.2: Token issuance rate (mints vs burns)

        Returns:
            DataFrame with issuance rates
        """
        if 'mint_volume_usd' not in df.columns or 'burn_volume_usd' not in df.columns:
            logger.warning("Missing mint/burn volume columns")
            return pd.DataFrame()

        agg_dict = {
            'mint_volume_usd': 'sum',
            'burn_volume_usd': 'sum',
        }

        # Include counts if available
        if 'mint_count' in df.columns:
            agg_dict['mint_count'] = 'sum'
        if 'burn_count' in df.columns:
            agg_dict['burn_count'] = 'sum'

        kpi = df.groupby(['week', 'symbol']).agg(agg_dict).reset_index()

        # Rename for clarity
        kpi.rename(columns={
            'mint_volume_usd': 'total_mints_usd',
            'burn_volume_usd': 'total_burns_usd',
        }, inplace=True)

        # Calculate net issuance
        kpi['net_issuance_usd'] = kpi['total_mints_usd'] - kpi['total_burns_usd']

        # Calculate issuance rate (net / gross)
        total_activity = kpi['total_mints_usd'] + kpi['total_burns_usd']
        kpi['issuance_rate_pct'] = safe_division(
            kpi['net_issuance_usd'],
            total_activity,
            default_value=0.0
        ) * 100

        return kpi.sort_values('week', ascending=False)

    def _kpi3_token_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        KPI 3.3: Per-token supply metrics

        Returns:
            DataFrame with token-level statistics
        """
        if 'mint_volume_usd' not in df.columns or 'burn_volume_usd' not in df.columns:
            logger.warning("Missing mint/burn volume columns")
            return pd.DataFrame()

        agg_dict = {
            'mint_volume_usd': 'sum',
            'burn_volume_usd': 'sum',
        }

        # Include counts and calculate averages
        if 'mint_count' in df.columns:
            agg_dict['mint_count'] = 'sum'
        if 'burn_count' in df.columns:
            agg_dict['burn_count'] = 'sum'

        kpi = df.groupby(['week', 'symbol']).agg(agg_dict).reset_index()

        # Rename columns
        kpi.rename(columns={
            'mint_volume_usd': 'total_mint_usd',
            'burn_volume_usd': 'total_burn_usd',
            'mint_count': 'mint_events',
            'burn_count': 'burn_events',
        }, inplace=True)

        # Calculate net supply change
        kpi['net_supply_change_usd'] = kpi['total_mint_usd'] - kpi['total_burn_usd']

        # Calculate total events
        if 'mint_events' in kpi.columns and 'burn_events' in kpi.columns:
            kpi['total_supply_events'] = kpi['mint_events'] + kpi['burn_events']

            # Calculate average event sizes
            kpi['mint_event_avg_size'] = safe_division(
                kpi['total_mint_usd'],
                kpi['mint_events'],
                default_value=0.0
            )

            kpi['burn_event_avg_size'] = safe_division(
                kpi['total_burn_usd'],
                kpi['burn_events'],
                default_value=0.0
            )

        return kpi.sort_values('week', ascending=False)

    def _kpi4_wow_supply_change(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        KPI 3.4: Week-over-week supply change

        Returns:
            DataFrame with WoW supply metrics
        """
        # Get base supply metrics
        weekly = self._kpi2_issuance_rate(df)

        if weekly.empty:
            return pd.DataFrame()

        # Sort by symbol/week
        weekly = weekly.sort_values(['symbol', 'week'])

        # Calculate previous week values
        weekly['mints_prev_week'] = weekly.groupby('symbol')['total_mints_usd'].shift(1)
        weekly['burns_prev_week'] = weekly.groupby('symbol')['total_burns_usd'].shift(1)
        weekly['net_prev_week'] = weekly.groupby('symbol')['net_issuance_usd'].shift(1)

        # Calculate WoW percentage changes
        try:
            weekly['mints_wow_pct'] = wow_percentage_change(
                weekly['total_mints_usd'],
                weekly['mints_prev_week']
            )
        except ValueError:
            weekly['mints_wow_pct'] = None

        try:
            weekly['burns_wow_pct'] = wow_percentage_change(
                weekly['total_burns_usd'],
                weekly['burns_prev_week']
            )
        except ValueError:
            weekly['burns_wow_pct'] = None

        try:
            weekly['net_wow_pct'] = wow_percentage_change(
                weekly['net_issuance_usd'],
                weekly['net_prev_week']
            )
        except ValueError:
            weekly['net_wow_pct'] = None

        return weekly[[
            'week', 'symbol',
            'total_mints_usd', 'mints_prev_week', 'mints_wow_pct',
            'total_burns_usd', 'burns_prev_week', 'burns_wow_pct',
            'net_issuance_usd', 'net_prev_week', 'net_wow_pct'
        ]].sort_values('week', ascending=False)

    def export_kpis(self, results: Dict, timestamp: str = None) -> Dict[str, Path]:
        """
        Export KPI results to CSV files

        Args:
            results: Dictionary with Supply KPI DataFrames
            timestamp: Optional timestamp for filenames

        Returns:
            Dictionary with filenames of exported files
        """
        if timestamp is None:
            timestamp = datetime.now().strftime('%Y%m%d%H%M%S')

        exported_files = {}

        # Extract week from data
        week = "2026-W01"  # Default fallback
        for key in ['supply_change', 'issuance_rate', 'token_metrics', 'wow_supply_change']:
            if key in results and results[key] is not None and not results[key].empty:
                if 'week' in results[key].columns:
                    week = results[key]['week'].iloc[0]
                    break

        # Export each KPI
        kpi_mapping = {
            'supply_change': 'supply_kpi1_supply_change',
            'issuance_rate': 'supply_kpi2_issuance_rate',
            'token_metrics': 'supply_kpi3_token_metrics',
            'wow_supply_change': 'supply_kpi4_wow_supply_change',
        }

        for key, filename_prefix in kpi_mapping.items():
            if key in results and results[key] is not None:
                df = results[key]
                if not df.empty:
                    filename = self.output_dir / f"{filename_prefix}_{week}_{timestamp}.csv"
                    df.to_csv(filename, index=False)
                    logger.info(f"✓ Exported: {filename}")
                    exported_files[key] = filename

        return exported_files

    def generate_summary(self, results: Dict) -> Dict:
        """
        Generate summary metrics for reporting

        Args:
            results: Dictionary with Supply KPI DataFrames

        Returns:
            Dictionary with summary statistics
        """
        summary = {
            'total_mints_usd': 0,
            'total_burns_usd': 0,
            'net_supply_change_usd': 0,
            'total_tokens_tracked': 0,
            'tokens_with_mints': 0,
            'tokens_with_burns': 0,
        }

        if 'issuance_rate' in results and results['issuance_rate'] is not None:
            df = results['issuance_rate']
            summary['total_mints_usd'] = df['total_mints_usd'].sum()
            summary['total_burns_usd'] = df['total_burns_usd'].sum()
            summary['net_supply_change_usd'] = df['net_issuance_usd'].sum()
            summary['total_tokens_tracked'] = df['symbol'].nunique()
            summary['tokens_with_mints'] = len(df[df['total_mints_usd'] > 0])
            summary['tokens_with_burns'] = len(df[df['total_burns_usd'] > 0])

        return summary
