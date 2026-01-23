"""
Supply KPI Processor - Domain 1
Processes flows data to calculate on-chain supply metrics with mint/burn verification
"""

import pandas as pd
from pathlib import Path
from datetime import datetime
from typing import Dict, Optional
from utils.logger import get_logger

logger = get_logger(__name__)


class SupplyKPIProcessor:
    """Process flows data to calculate weekly supply KPIs with mint/burn tracking"""

    def __init__(self, output_dir: str = 'data/kpi'):
        """Initialize processor"""
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        logger.info("SupplyKPIProcessor initialized")

    def process_all(self, df: pd.DataFrame) -> Dict:
        """
        Process flows data with supply verification through mint/burn tracking

        Args:
            df: DataFrame from flows query with mint/burn columns

        Returns:
            Dictionary with supply KPIs and verification metrics
        """
        if df is None or df.empty:
            logger.error("Empty flows data received")
            return {}

        logger.info(f"Processing supply data: {len(df)} rows")

        # Clean data
        df = self._clean_data(df)

        # Calculate net supply changes from mint/burn
        supply_changes = self._calculate_supply_changes(df)

        # Calculate mint/burn metrics
        mint_burn_metrics = self._calculate_mint_burn_metrics(df)

        # Combine results
        results = {
            'supply_changes': supply_changes,
            'mint_burn_metrics': mint_burn_metrics,
            'mint_activity': df[df['mint_count'] > 0],
            'burn_activity': df[df['burn_volume_usd'] > 0]
        }

        logger.info("✅ Supply processing complete")
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

        # Ensure required columns exist
        required_columns = [
            'blockchain', 'symbol', 'week_start', 'transfer_count',
            'unique_senders', 'unique_receivers', 'total_amount_raw',
            'total_amount_normalized', 'total_volume_usd', 'avg_transfer_usd',
            'min_transfer_usd', 'max_transfer_usd', 'mint_count',
            'mint_volume_usd', 'burn_volume_usd'
        ]

        missing = [col for col in required_columns if col not in df.columns]
        if missing:
            logger.warning(f"Missing columns: {missing}")
            logger.warning("Make sure econ_flows_query.sql has been updated with mint/burn columns")

        # Convert date column
        if 'week_start' in df.columns:
            df['week_start'] = pd.to_datetime(df['week_start'], errors='coerce')

        # Convert numeric columns
        numeric_columns = [
            'transfer_count', 'unique_senders', 'unique_receivers',
            'total_amount_raw', 'total_amount_normalized', 'total_volume_usd',
            'avg_transfer_usd', 'min_transfer_usd', 'max_transfer_usd',
            'mint_count', 'mint_volume_usd', 'burn_volume_usd'
        ]

        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        # Fill NaN with 0 for volume metrics
        df = df.fillna(0)
        df = df.infer_objects(copy=False)

        return df

    def _calculate_supply_changes(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate net supply changes from mint/burn activity

        Args:
            df: Cleaned flows DataFrame

        Returns:
            DataFrame with supply change metrics
        """
        # Group by blockchain and symbol for cumulative analysis
        supply_data = df.groupby(['blockchain', 'symbol']).agg({
            'mint_volume_usd': 'sum',
            'burn_volume_usd': 'sum',
            'total_volume_usd': 'sum',
            'mint_count': 'sum',
            'transfer_count': 'sum',
            'unique_senders': 'sum',
            'unique_receivers': 'sum'
        }).reset_index()

        # Calculate net supply change
        supply_data['net_supply_change'] = (
            supply_data['mint_volume_usd'] - supply_data['burn_volume_usd']
        ).round(2)

        # Calculate mint/burn ratio (with protection for division by zero)
        supply_data['mint_to_burn_ratio'] = (
            supply_data['mint_volume_usd'] /
            (supply_data['burn_volume_usd'] + 1)
        ).round(2)

        # Calculate inflation rate (net supply change as % of total volume)
        supply_data['inflation_rate_pct'] = (
            (supply_data['net_supply_change'] / (supply_data['total_volume_usd'] + 1)) * 100
        ).round(2)

        # Determine supply trend
        supply_data['supply_trend'] = supply_data['net_supply_change'].apply(
            lambda x: 'EXPANSION' if x > 0 else 'CONTRACTION' if x < 0 else 'STABLE'
        )

        # Calculate burn activity percentage
        supply_data['burn_activity_pct'] = (
            (supply_data['burn_volume_usd'] / (supply_data['total_volume_usd'] + 1)) * 100
        ).round(2)

        # Rank by net supply change
        supply_data['rank_by_net_supply'] = supply_data['net_supply_change'].rank(
            ascending=False, method='min'
        ).astype(int)

        return supply_data

    def _calculate_mint_burn_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate detailed mint/burn activity metrics

        Args:
            df: Cleaned flows DataFrame

        Returns:
            DataFrame with mint/burn metrics
        """
        # Weekly aggregated mint/burn
        weekly_metrics = df.groupby(['blockchain', 'symbol', 'week_start']).agg({
            'mint_count': 'sum',
            'mint_volume_usd': 'sum',
            'burn_volume_usd': 'sum',
            'transfer_count': 'sum',
            'total_volume_usd': 'sum'
        }).reset_index()

        # Calculate net flows
        weekly_metrics['net_flow_usd'] = (
            weekly_metrics['mint_volume_usd'] - weekly_metrics['burn_volume_usd']
        ).round(2)

        # Calculate burn to mint ratio
        weekly_metrics['burn_to_mint_ratio'] = (
            weekly_metrics['burn_volume_usd'] /
            (weekly_metrics['mint_volume_usd'] + 1)
        ).round(2)

        # Identify minting weeks (where minting occurred)
        weekly_metrics['had_mints'] = (weekly_metrics['mint_count'] > 0).astype(int)
        weekly_metrics['had_burns'] = (weekly_metrics['burn_volume_usd'] > 0).astype(int)

        # Calculate mint concentration (mint volume as % of total transfers)
        weekly_metrics['mint_concentration_pct'] = (
            (weekly_metrics['mint_volume_usd'] / (weekly_metrics['total_volume_usd'] + 1)) * 100
        ).round(2)

        # Sort by date descending
        weekly_metrics = weekly_metrics.sort_values('week_start', ascending=False)

        return weekly_metrics

    def export_kpis(self, results: Dict, timestamp: str = None) -> Dict[str, Path]:
        """
        Export KPI results to CSV files

        Args:
            results: Dictionary with supply KPI DataFrames
            timestamp: Optional timestamp for filenames

        Returns:
            Dictionary with filenames of exported files
        """
        if timestamp is None:
            timestamp = datetime.now().strftime('%Y%m%d%H%M%S')

        exported_files = {}

        # Export supply changes
        if 'supply_changes' in results and results['supply_changes'] is not None:
            df = results['supply_changes']
            # Extract week from data if available
            week = "2026W04"  # Placeholder - extract from actual data in production
            filename = self.output_dir / f"supply_kpi_changes_{week}_{timestamp}.csv"
            df.to_csv(filename, index=False)
            logger.info(f"✓ Exported: {filename}")
            exported_files['supply_changes'] = filename

        # Export mint/burn metrics
        if 'mint_burn_metrics' in results and results['mint_burn_metrics'] is not None:
            df = results['mint_burn_metrics']
            week = "2026W04"  # Placeholder - extract from actual data in production
            filename = self.output_dir / f"supply_kpi_mint_burn_metrics_{week}_{timestamp}.csv"
            df.to_csv(filename, index=False)
            logger.info(f"✓ Exported: {filename}")
            exported_files['mint_burn_metrics'] = filename

        # Export mint activity detail
        if 'mint_activity' in results and results['mint_activity'] is not None:
            df = results['mint_activity']
            if not df.empty:
                week = "2026W04"  # Placeholder
                filename = self.output_dir / f"supply_kpi_mint_activity_{week}_{timestamp}.csv"
                df.to_csv(filename, index=False)
                logger.info(f"✓ Exported: {filename}")
                exported_files['mint_activity'] = filename

        # Export burn activity detail
        if 'burn_activity' in results and results['burn_activity'] is not None:
            df = results['burn_activity']
            if not df.empty:
                week = "2026W04"  # Placeholder
                filename = self.output_dir / f"supply_kpi_burn_activity_{week}_{timestamp}.csv"
                df.to_csv(filename, index=False)
                logger.info(f"✓ Exported: {filename}")
                exported_files['burn_activity'] = filename

        return exported_files

    def generate_summary(self, results: Dict) -> Dict:
        """
        Generate summary metrics for reporting

        Args:
            results: Dictionary with supply KPI DataFrames

        Returns:
            Dictionary with summary statistics
        """
        summary = {
            'total_tokens_tracked': 0,
            'total_blockchains': 0,
            'total_mints_usd': 0,
            'total_burns_usd': 0,
            'net_supply_change_usd': 0,
            'average_inflation_rate': 0,
            'tokens_in_expansion': 0,
            'tokens_in_contraction': 0,
            'tokens_stable': 0
        }

        if 'supply_changes' in results and results['supply_changes'] is not None:
            df = results['supply_changes']

            summary['total_tokens_tracked'] = df['symbol'].nunique()
            summary['total_blockchains'] = df['blockchain'].nunique()
            summary['total_mints_usd'] = df['mint_volume_usd'].sum()
            summary['total_burns_usd'] = df['burn_volume_usd'].sum()
            summary['net_supply_change_usd'] = df['net_supply_change'].sum()
            summary['average_inflation_rate'] = df['inflation_rate_pct'].mean()

            # Count by trend
            summary['tokens_in_expansion'] = (df['supply_trend'] == 'EXPANSION').sum()
            summary['tokens_in_contraction'] = (df['supply_trend'] == 'CONTRACTION').sum()
            summary['tokens_stable'] = (df['supply_trend'] == 'STABLE').sum()

        return summary
