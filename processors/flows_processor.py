"""
Process mint/burn transaction data and calculate KPIs
"""

import pandas as pd
from pathlib import Path
from typing import Dict
import yaml

from utils.logger import setup_logger

logger = setup_logger(__name__)


class MintBurnKPIProcessor:
    """Calculate mint/burn activity KPIs"""

    def __init__(self, config_path: str = 'config.yaml'):
        """Initialize processor with config"""
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)

        self.kpi_dir = Path(self.config['paths']['kpi_export'])
        self.kpi_dir.mkdir(parents=True, exist_ok=True)

        self.kpis = {}

    def load_raw_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Load and prepare raw mint/burn data

        Args:
            df: Raw mint/burn data from Dune

        Returns:
            Cleaned DataFrame
        """
        logger.info("Loading and cleaning mint/burn data...")

        df = df.copy()
        df['block_date'] = pd.to_datetime(df['block_date'])

        # Fill NaN values
        df = df.fillna({
            'mint_count': 0,
            'burn_count': 0,
            'mint_volume': 0,
            'burn_volume': 0,
            'net_volume': 0
        }).infer_objects(copy=False)

        logger.info(f"✓ Loaded {len(df):,} rows")
        return df

    def calculate_kpi1_daily_activity(self, df: pd.DataFrame) -> pd.DataFrame:
        """KPI 1: Daily Mint/Burn Activity"""
        logger.info("Calculating Mint/Burn KPI 1: Daily Activity...")

        kpi1 = df[[
            'block_date', 'stablecoin', 'blockchain',
            'mint_count', 'burn_count', 'net_mint_count',
            'mint_volume', 'burn_volume', 'net_volume'
        ]].copy()

        kpi1 = kpi1.sort_values(['block_date', 'net_volume'], ascending=[False, False])

        self.kpis['mintburn_kpi1_daily_activity'] = kpi1
        logger.info(f"✓ Mint/Burn KPI 1 calculated: {len(kpi1):,} rows")

        return kpi1

    def calculate_kpi2_weekly_aggregates(self, df: pd.DataFrame) -> pd.DataFrame:
        """KPI 2: Weekly Aggregated Mint/Burn"""
        logger.info("Calculating Mint/Burn KPI 2: Weekly Aggregates...")

        df_weekly = df.copy()
        df_weekly['week'] = pd.to_datetime(df_weekly['block_date']).dt.to_period('W').dt.start_time

        kpi2 = df_weekly.groupby(['week', 'stablecoin', 'blockchain']).agg({
            'mint_count': 'sum',
            'burn_count': 'sum',
            'mint_volume': 'sum',
            'burn_volume': 'sum',
            'net_volume': 'sum'
        }).reset_index()

        kpi2 = kpi2.sort_values(['week', 'net_volume'], ascending=[False, False])

        self.kpis['mintburn_kpi2_weekly_aggregates'] = kpi2
        logger.info(f"✓ Mint/Burn KPI 2 calculated: {len(kpi2):,} rows")

        return kpi2

    def calculate_kpi3_net_issuance(self, kpi2: pd.DataFrame) -> pd.DataFrame:
        """KPI 3: Net Issuance by Token"""
        logger.info("Calculating Mint/Burn KPI 3: Net Issuance...")

        kpi3 = kpi2.groupby(['week', 'stablecoin']).agg({
            'mint_volume': 'sum',
            'burn_volume': 'sum',
            'net_volume': 'sum'
        }).reset_index()

        kpi3.rename(columns={
            'mint_volume': 'total_mints',
            'burn_volume': 'total_burns',
            'net_volume': 'net_issuance'
        }, inplace=True)

        # Add trend classification
        kpi3['trend'] = kpi3['net_issuance'].apply(
            lambda x: 'EXPANSION' if x > 0 else 'CONTRACTION' if x < 0 else 'NEUTRAL'
        )

        kpi3 = kpi3.sort_values(['week', 'net_issuance'], ascending=[False, False])

        self.kpis['mintburn_kpi3_net_issuance'] = kpi3
        logger.info(f"✓ Mint/Burn KPI 3 calculated: {len(kpi3):,} rows")

        return kpi3

    def calculate_kpi4_wow_change(self, kpi2: pd.DataFrame) -> pd.DataFrame:
        """KPI 4: Week-over-Week Activity Change"""
        logger.info("Calculating Mint/Burn KPI 4: WoW Change...")

        kpi4 = kpi2.copy()
        kpi4 = kpi4.sort_values(['stablecoin', 'blockchain', 'week'])

        # Calculate WoW change
        kpi4['mint_volume_wow_pct'] = (
                kpi4.groupby(['stablecoin', 'blockchain'])['mint_volume']
                .pct_change() * 100
        )

        kpi4['burn_volume_wow_pct'] = (
                kpi4.groupby(['stablecoin', 'blockchain'])['burn_volume']
                .pct_change() * 100
        )

        kpi4['net_volume_wow_pct'] = (
                kpi4.groupby(['stablecoin', 'blockchain'])['net_volume']
                .pct_change() * 100
        )

        kpi4 = kpi4.sort_values(['week', 'mint_volume_wow_pct'], ascending=[False, False])

        self.kpis['mintburn_kpi4_wow_change'] = kpi4
        logger.info(f"✓ Mint/Burn KPI 4 calculated: {len(kpi4):,} rows")

        return kpi4

    def process_all(self, df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        """
        Process all mint/burn KPIs

        Args:
            df: Raw mint/burn data

        Returns:
            Dictionary of KPI DataFrames
        """
        logger.info("=" * 80)
        logger.info("PROCESSING MINT/BURN KPIs")
        logger.info("=" * 80)

        # Clean data
        df_clean = self.load_raw_data(df)

        # Calculate KPIs
        self.calculate_kpi1_daily_activity(df_clean)
        kpi2 = self.calculate_kpi2_weekly_aggregates(df_clean)
        self.calculate_kpi3_net_issuance(kpi2)
        self.calculate_kpi4_wow_change(kpi2)

        logger.info("\n✅ All Mint/Burn KPIs calculated successfully")
        return self.kpis

    def export_kpis(self, timestamp: str = None) -> Dict[str, Path]:
        """Export KPIs to CSV with week in filename"""
        if timestamp is None:
            from datetime import datetime
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

        logger.info("Exporting KPI CSVs...")
        exported = {}

        for kpi_name, kpi_df in self.kpis.items():
            # Extract week from the data
            if 'week' in kpi_df.columns:
                latest_week = kpi_df['week'].max()
                week_str = latest_week.strftime('%Y_W%W')  # e.g., 2025_W52
                filename = f"{kpi_name}_{week_str}_{timestamp}.csv"
            else:
                filename = f"{kpi_name}_{timestamp}.csv"

            filepath = self.kpi_dir / filename
            kpi_df.to_csv(filepath, index=False)
            exported[kpi_name] = filepath
            logger.info(f"✓ {filepath}")

        logger.info(f"✓ Exported {len(exported)} KPI files")
        return exported

