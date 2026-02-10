"""
Flows KPI Processor - Domain 2
Processes mint/burn flow data from token transfers
"""

import pandas as pd
from pathlib import Path
from datetime import datetime
from typing import Dict, Optional
from utils.logger import get_logger
from utils.date_utils import get_iso_week
from utils.math_utils import wow_percentage_change, safe_division

logger = get_logger(__name__)


class FlowsKPIProcessor:
    """Process mint/burn flows data to calculate supply change KPIs"""

    def __init__(self, output_dir: str = 'data/kpi'):
        """Initialize processor"""
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        logger.info("FlowsKPIProcessor initialized")

    def process_all(self, df: pd.DataFrame) -> Dict:
        """
        Process flows data with mint/burn analysis

        Args:
            df: DataFrame from flows query with mint/burn data

        Returns:
            Dictionary with Flows KPIs
        """
        if df is None or df.empty:
            logger.error("Empty flows data received")
            return {}

        logger.info(f"Processing flows data: {len(df)} rows")

        # Clean data
        df = self._clean_data(df)

        # Calculate KPIs
        results = {
            'daily_activity': self._kpi1_daily_activity(df),
            'weekly_aggregates': self._kpi2_weekly_aggregates(df),
            'net_issuance': self._kpi3_net_issuance(df),
            'wow_change': self._kpi4_wow_change(df),
            'network_health': self._kpi5_network_health(df),  # NEW
        }

        logger.info("✅ Flows processing complete")
        return results

    def _clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean and prepare flows data

        Args:
            df: Raw flows DataFrame

        Returns:
            Cleaned DataFrame
        """
        df = df.copy()

        # ===================================================================
        # STANDARDIZED DATE HANDLING (I4 Fix)
        # Convert any date column to both datetime and ISO week format
        # ===================================================================
        date_col_found = False

        # Priority order: block_time > week_start > date
        if 'block_time' in df.columns:
            df['block_time'] = pd.to_datetime(df['block_time'], errors='coerce')
            df['week'] = df['block_time'].apply(get_iso_week)
            date_col_found = True
            logger.debug("Using 'block_time' as primary date column")

        elif 'week_start' in df.columns:
            df['week_start'] = pd.to_datetime(df['week_start'], errors='coerce')
            # Create standard datetime column for consistency
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
            logger.error("No date column found in flows data")
            raise ValueError("Missing date column (expected 'block_time', 'week_start', or 'date')")

        # Ensure 'week' column exists (should be created above)
        if 'week' not in df.columns:
            logger.warning("'week' column not created - using fallback")
            df['week'] = df['block_time'].apply(get_iso_week)
        # ===================================================================

        # Classify mint/burn using zero address OR handle pre-aggregated data
        null_address = "0x0000000000000000000000000000000000000000"

        # Check if data already has mint/burn volumes (pre-aggregated from SQL)
        if 'mint_volume_usd' in df.columns and 'burn_volume_usd' in df.columns:
            logger.info("  Using pre-aggregated mint/burn data from SQL")
            df['flow_type'] = 'aggregated'
        elif 'from_address' in df.columns:
            # Ensure from/to columns exist and handle NaN
            df['from_address'] = df['from_address'].fillna('').astype(str).str.lower()
            df['to_address'] = df['to_address'].fillna('').astype(str).str.lower()

            # Classify flow type
            df['flow_type'] = 'transfer'
            df.loc[df['from_address'] == null_address.lower(), 'flow_type'] = 'mint'
            df.loc[df['to_address'] == null_address.lower(), 'flow_type'] = 'burn'

            # Validate no row is both mint and burn
            both_mint_burn = df[(df['from_address'] == null_address.lower()) &
                                (df['to_address'] == null_address.lower())]
            if len(both_mint_burn) > 0:
                logger.warning(f"⚠ {len(both_mint_burn)} rows classified as both mint and burn")

        # Convert numeric columns
        numeric_columns = ['amount', 'amount_usd', 'mint_volume_usd', 'burn_volume_usd',
                           'total_volume_usd', 'transfer_count', 'mint_count', 'burn_count',
                           'unique_senders', 'unique_receivers', 'avg_transfer_usd',
                           'max_transfer_usd', 'total_amount_normalized']
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        # Fill NaN with 0 for volume metrics
        df = df.fillna(0)

        # Remove rows with missing critical data
        # Use standardized 'week' column
        df = df.dropna(subset=['week', 'symbol'])

        logger.info(f"✓ Cleaned flows data: {len(df)} rows")
        logger.debug(f"  Date column: block_time, Week column: week (ISO format)")

        return df

    def _kpi1_daily_activity(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        KPI 2.1: Daily mint/burn activity

        Returns:
            DataFrame with daily flow metrics
        """
        date_col = 'block_time' if 'block_time' in df.columns else (
            'week_start' if 'week_start' in df.columns else 'date')

        # Handle pre-aggregated data differently
        if 'mint_volume_usd' in df.columns and 'burn_volume_usd' in df.columns:
            # Data already has mint/burn split
            kpi = df.groupby([date_col, 'symbol', 'blockchain']).agg({
                'mint_volume_usd': 'sum',
                'burn_volume_usd': 'sum',
            }).reset_index()
            return kpi.sort_values(date_col, ascending=False)

        # Transaction-level data
        kpi = df.groupby([date_col, 'symbol', 'blockchain', 'flow_type']).agg({
            'amount_usd': 'sum',
            'amount': 'sum',
        }).reset_index()

        # Pivot to separate mint/burn columns
        kpi_pivot = kpi.pivot_table(
            index=[date_col, 'symbol', 'blockchain'],
            columns='flow_type',
            values='amount_usd',
            aggfunc='sum',
            fill_value=0
        ).reset_index()

        return kpi_pivot.sort_values(date_col, ascending=False)

    def _kpi2_weekly_aggregates(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        KPI 2.2: Weekly aggregated mint/burn volumes

        Returns:
            DataFrame with weekly flow metrics
        """
        # Handle pre-aggregated data (already weekly from SQL)
        if 'mint_volume_usd' in df.columns and 'burn_volume_usd' in df.columns:
            agg_dict = {
                'mint_volume_usd': 'sum',
                'burn_volume_usd': 'sum',
            }

            # Include burn_count if available (NEW from optimized query)
            if 'burn_count' in df.columns:
                agg_dict['burn_count'] = 'sum'
            if 'mint_count' in df.columns:
                agg_dict['mint_count'] = 'sum'

            kpi = df.groupby(['week', 'symbol', 'blockchain']).agg(agg_dict).reset_index()

            # Calculate net issuance
            kpi['net_issuance_usd'] = kpi['mint_volume_usd'] - kpi['burn_volume_usd']
            return kpi.sort_values('week', ascending=False)

        # Transaction-level data
        kpi = df.groupby(['week', 'symbol', 'blockchain', 'flow_type']).agg({
            'amount_usd': 'sum',
            'amount': 'sum',
        }).reset_index()

        # Separate mint and burn
        mints = kpi[kpi['flow_type'] == 'mint'][['week', 'symbol', 'blockchain', 'amount_usd']].copy()
        mints.rename(columns={'amount_usd': 'mint_volume_usd'}, inplace=True)

        burns = kpi[kpi['flow_type'] == 'burn'][['week', 'symbol', 'blockchain', 'amount_usd']].copy()
        burns.rename(columns={'amount_usd': 'burn_volume_usd'}, inplace=True)

        # Merge on week/symbol/blockchain
        kpi_merged = mints.merge(burns, on=['week', 'symbol', 'blockchain'], how='outer')
        kpi_merged = kpi_merged.fillna(0)

        # Calculate net issuance
        kpi_merged['net_issuance_usd'] = kpi_merged['mint_volume_usd'] - kpi_merged['burn_volume_usd']

        return kpi_merged.sort_values('week', ascending=False)

    def _kpi3_net_issuance(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        KPI 2.3: Net issuance by token

        Returns:
            DataFrame with net supply changes
        """
        weekly = self._kpi2_weekly_aggregates(df)

        agg_dict = {
            'mint_volume_usd': 'sum',
            'burn_volume_usd': 'sum',
            'net_issuance_usd': 'sum',
        }

        # Include event counts if available
        if 'mint_count' in weekly.columns:
            agg_dict['mint_count'] = 'sum'
        if 'burn_count' in weekly.columns:
            agg_dict['burn_count'] = 'sum'

        kpi = weekly.groupby(['week', 'symbol']).agg(agg_dict).reset_index()

        return kpi.sort_values('week', ascending=False)

    def _kpi4_wow_change(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        KPI 2.4: Week-over-week supply change

        Returns:
            DataFrame with WoW metrics
        """
        weekly = self._kpi2_weekly_aggregates(df)

        # Sort by symbol/blockchain/week
        weekly = weekly.sort_values(['symbol', 'blockchain', 'week'])

        # Calculate WoW change
        weekly['mint_prev_week'] = weekly.groupby(['symbol', 'blockchain'])['mint_volume_usd'].shift(1)
        weekly['burn_prev_week'] = weekly.groupby(['symbol', 'blockchain'])['burn_volume_usd'].shift(1)
        weekly['net_prev_week'] = weekly.groupby(['symbol', 'blockchain'])['net_issuance_usd'].shift(1)

        # Use safe wow_percentage_change for mints (but handle zero case)
        try:
            weekly['mint_wow_pct'] = wow_percentage_change(
                weekly['mint_volume_usd'],
                weekly['mint_prev_week']
            )
        except ValueError:
            weekly['mint_wow_pct'] = None

        try:
            weekly['burn_wow_pct'] = wow_percentage_change(
                weekly['burn_volume_usd'],
                weekly['burn_prev_week']
            )
        except ValueError:
            weekly['burn_wow_pct'] = None

        try:
            weekly['net_wow_pct'] = wow_percentage_change(
                weekly['net_issuance_usd'],
                weekly['net_prev_week']
            )
        except ValueError:
            weekly['net_wow_pct'] = None

        return weekly[[
            'week', 'symbol', 'blockchain',
            'mint_volume_usd', 'mint_prev_week', 'mint_wow_pct',
            'burn_volume_usd', 'burn_prev_week', 'burn_wow_pct',
            'net_issuance_usd', 'net_prev_week', 'net_wow_pct'
        ]].sort_values('week', ascending=False)

    def _kpi5_network_health(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        KPI 2.5: Network health and distribution metrics (NEW)

        Analyzes on-chain activity patterns:
        - Transfer frequency (network usage)
        - Wallet distribution (decentralization)
        - Whale activity (max transfer size)
        - Average transfer size (retail vs institutional)

        Returns:
            DataFrame with network health metrics
        """
        # Check if required columns exist
        required_cols = ['transfer_count', 'unique_senders', 'unique_receivers',
                         'avg_transfer_usd', 'max_transfer_usd']

        available_cols = [col for col in required_cols if col in df.columns]

        if not available_cols:
            logger.warning("⚠ Network health columns not available in data")
            return pd.DataFrame()

        # Build aggregation dict dynamically based on available columns
        agg_dict = {}
        if 'transfer_count' in df.columns:
            agg_dict['transfer_count'] = 'sum'
        if 'unique_senders' in df.columns:
            agg_dict['unique_senders'] = 'sum'
        if 'unique_receivers' in df.columns:
            agg_dict['unique_receivers'] = 'sum'
        if 'avg_transfer_usd' in df.columns:
            agg_dict['avg_transfer_usd'] = 'mean'
        if 'max_transfer_usd' in df.columns:
            agg_dict['max_transfer_usd'] = 'max'
        if 'total_volume_usd' in df.columns:
            agg_dict['total_volume_usd'] = 'sum'

        kpi = df.groupby(['week', 'symbol', 'blockchain']).agg(agg_dict).reset_index()

        # Calculate derived metrics
        if 'unique_senders' in kpi.columns and 'unique_receivers' in kpi.columns:
            # Network balance: ratio of receivers to senders (>1 = expanding, <1 = contracting)
            kpi['receiver_sender_ratio'] = safe_division(
                kpi['unique_receivers'],
                kpi['unique_senders'],
                default_value=0.0
            )

        if 'max_transfer_usd' in kpi.columns and 'avg_transfer_usd' in kpi.columns:
            # Whale concentration: how much larger is the biggest transfer vs average
            kpi['whale_concentration_ratio'] = safe_division(
                kpi['max_transfer_usd'],
                kpi['avg_transfer_usd'],
                default_value=0.0
            )

        if 'transfer_count' in kpi.columns and 'unique_senders' in kpi.columns:
            # Transfer frequency per sender (velocity indicator)
            kpi['avg_transfers_per_sender'] = safe_division(
                kpi['transfer_count'],
                kpi['unique_senders'],
                default_value=0.0
            )

        return kpi.sort_values('week', ascending=False)

    def export_kpis(self, results: Dict, timestamp: str = None) -> Dict[str, Path]:
        """
        Export KPI results to CSV files

        Args:
            results: Dictionary with Flows KPI DataFrames
            timestamp: Optional timestamp for filenames

        Returns:
            Dictionary with filenames of exported files
        """
        if timestamp is None:
            timestamp = datetime.now().strftime('%Y%m%d%H%M%S')

        exported_files = {}

        # Extract week from data
        week = "2026-W01"  # Default fallback
        for key in ['weekly_aggregates', 'net_issuance', 'wow_change', 'network_health']:
            if key in results and results[key] is not None and not results[key].empty:
                if 'week' in results[key].columns:
                    week = results[key]['week'].iloc[0]
                    logger.debug(f"Extracted week from {key}: {week}")
                    break

        # Export each KPI
        kpi_mapping = {
            'daily_activity': 'flows_kpi1_daily_activity',
            'weekly_aggregates': 'flows_kpi2_weekly_aggregates',
            'net_issuance': 'flows_kpi3_net_issuance',
            'wow_change': 'flows_kpi4_wow_change',
            'network_health': 'flows_kpi5_network_health',  # NEW
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
        ENHANCED: Now includes network health metrics

        Args:
            results: Dictionary with Flows KPI DataFrames

        Returns:
            Dictionary with summary statistics
        """
        summary = {
            'total_mints': 0,
            'total_burns': 0,
            'net_issuance': 0,
            'mint_count': 0,
            'burn_count': 0,
            'tokens_with_mints': 0,
            'tokens_with_burns': 0,
            # NEW network health metrics
            'total_transfers': 0,
            'total_unique_senders': 0,
            'total_unique_receivers': 0,
            'avg_transfer_size_usd': 0,
            'max_whale_transfer_usd': 0,
        }

        if 'net_issuance' in results and results['net_issuance'] is not None:
            df = results['net_issuance']
            summary['total_mints'] = df['mint_volume_usd'].sum()
            summary['total_burns'] = df['burn_volume_usd'].sum()
            summary['net_issuance'] = df['net_issuance_usd'].sum()
            summary['tokens_with_mints'] = (df['mint_volume_usd'] > 0).sum()
            summary['tokens_with_burns'] = (df['burn_volume_usd'] > 0).sum()

            # NEW: Add event counts if available
            if 'mint_count' in df.columns:
                summary['mint_count'] = df['mint_count'].sum()
            if 'burn_count' in df.columns:
                summary['burn_count'] = df['burn_count'].sum()

        # NEW: Network health summary
        if 'network_health' in results and results['network_health'] is not None:
            df = results['network_health']
            if 'transfer_count' in df.columns:
                summary['total_transfers'] = df['transfer_count'].sum()
            if 'unique_senders' in df.columns:
                summary['total_unique_senders'] = df['unique_senders'].sum()
            if 'unique_receivers' in df.columns:
                summary['total_unique_receivers'] = df['unique_receivers'].sum()
            if 'avg_transfer_usd' in df.columns:
                summary['avg_transfer_size_usd'] = df['avg_transfer_usd'].mean()
            if 'max_transfer_usd' in df.columns:
                summary['max_whale_transfer_usd'] = df['max_transfer_usd'].max()

        return summary
