"""
Process DEX trading volume data and calculate KPIs
"""

import pandas as pd
from pathlib import Path
from typing import Dict
import yaml

from utils.logger import setup_logger

logger = setup_logger(__name__)


class DexVolumeKPIProcessor:
    """Calculate DEX volume KPIs"""

    def __init__(self, config_path: str = 'config.yaml'):
        """Initialize processor with config"""
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)

        self.kpi_dir = Path(self.config['paths']['kpi_export'])
        self.kpi_dir.mkdir(parents=True, exist_ok=True)

        self.kpis = {}

    def load_raw_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Load and prepare raw DEX volume data

        Args:
            df: Raw DEX volume data from Dune

        Returns:
            Cleaned DataFrame
        """
        logger.info("Loading and cleaning DEX volume data...")

        df = df.copy()
        df['block_date'] = pd.to_datetime(df['block_date'])

        # Fill NaN values
        df = df.fillna({
            'volume_usd': 0,
            'trade_count': 0
        }).infer_objects(copy=False)

        logger.info(f"✓ Loaded {len(df):,} rows")
        return df

    def calculate_kpi1_daily_volume(self, df: pd.DataFrame) -> pd.DataFrame:
        """KPI 1: Daily DEX Trading Volume"""
        logger.info("Calculating DEX KPI 1: Daily Volume...")

        kpi1 = df[[
            'block_date', 'blockchain', 'stablecoin_symbol',
            'volume_usd', 'trade_count'
        ]].copy()

        kpi1.rename(columns={'stablecoin_symbol': 'stablecoin'}, inplace=True)
        kpi1 = kpi1.sort_values(['block_date', 'volume_usd'], ascending=[False, False])

        self.kpis['dex_kpi1_daily_volume'] = kpi1
        logger.info(f"✓ DEX KPI 1 calculated: {len(kpi1):,} rows")

        return kpi1

    def calculate_kpi2_weekly_volume(self, df: pd.DataFrame) -> pd.DataFrame:
        """KPI 2: Weekly DEX Volume Aggregates"""
        logger.info("Calculating DEX KPI 2: Weekly Volume...")

        df_weekly = df.copy()
        df_weekly['week'] = pd.to_datetime(df_weekly['block_date']).dt.to_period('W').dt.start_time

        kpi2 = df_weekly.groupby(['week', 'blockchain', 'stablecoin_symbol']).agg({
            'volume_usd': 'sum',
            'trade_count': 'sum'
        }).reset_index()

        kpi2.rename(columns={'stablecoin_symbol': 'stablecoin'}, inplace=True)
        kpi2 = kpi2.sort_values(['week', 'volume_usd'], ascending=[False, False])

        self.kpis['dex_kpi2_weekly_volume'] = kpi2
        logger.info(f"✓ DEX KPI 2 calculated: {len(kpi2):,} rows")

        return kpi2

    def calculate_kpi3_token_breakdown(self, kpi2: pd.DataFrame) -> pd.DataFrame:
        """KPI 3: Token-Level Volume Breakdown"""
        logger.info("Calculating DEX KPI 3: Token Breakdown...")

        kpi3 = kpi2.groupby(['week', 'stablecoin']).agg({
            'volume_usd': 'sum',
            'trade_count': 'sum'
        }).reset_index()

        kpi3.rename(columns={
            'volume_usd': 'total_volume_usd',
            'trade_count': 'total_trades'
        }, inplace=True)

        # Calculate market share
        kpi3['market_share_pct'] = kpi3.groupby('week')['total_volume_usd'].transform(
            lambda x: (x / x.sum() * 100) if x.sum() > 0 else 0
        )

        kpi3 = kpi3.sort_values(['week', 'total_volume_usd'], ascending=[False, False])

        self.kpis['dex_kpi3_token_breakdown'] = kpi3
        logger.info(f"✓ DEX KPI 3 calculated: {len(kpi3):,} rows")

        return kpi3

    def calculate_kpi4_wow_volume_change(self, kpi2: pd.DataFrame) -> pd.DataFrame:
        """KPI 4: Week-over-Week Volume Change"""
        logger.info("Calculating DEX KPI 4: WoW Volume Change...")

        kpi4 = kpi2.copy()
        kpi4 = kpi4.sort_values(['stablecoin', 'blockchain', 'week'])

        # Calculate WoW change
        kpi4['volume_wow_pct'] = (
                kpi4.groupby(['stablecoin', 'blockchain'])['volume_usd']
                .pct_change() * 100
        )

        kpi4['trades_wow_pct'] = (
                kpi4.groupby(['stablecoin', 'blockchain'])['trade_count']
                .pct_change() * 100
        )

        # Absolute change
        kpi4['volume_wow_abs'] = (
            kpi4.groupby(['stablecoin', 'blockchain'])['volume_usd']
            .diff()
        )

        kpi4 = kpi4.sort_values(['week', 'volume_wow_pct'], ascending=[False, False])

        self.kpis['dex_kpi4_wow_volume_change'] = kpi4
        logger.info(f"✓ DEX KPI 4 calculated: {len(kpi4):,} rows")

        return kpi4

    def calculate_kpi5_chain_distribution(self, kpi2: pd.DataFrame) -> pd.DataFrame:
        """KPI 5: Volume Distribution by Chain"""
        logger.info("Calculating DEX KPI 5: Chain Distribution...")

        latest_week = kpi2['week'].max()
        kpi5 = kpi2[kpi2['week'] == latest_week].copy()

        # Calculate chain share per token
        kpi5.loc[:, 'chain_share_pct'] = kpi5.groupby('stablecoin')['volume_usd'].transform(
            lambda x: (x / x.sum() * 100) if x.sum() > 0 else 0
        )

        kpi5 = kpi5.sort_values('volume_usd', ascending=False)

        self.kpis['dex_kpi5_chain_distribution'] = kpi5
        logger.info(f"✓ DEX KPI 5 calculated: {len(kpi5):,} rows")

        return kpi5

    def process_all(self, df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        """
        Process all DEX volume KPIs

        Args:
            df: Raw DEX volume data

        Returns:
            Dictionary of KPI DataFrames
        """
        logger.info("=" * 80)
        logger.info("PROCESSING DEX VOLUME KPIs")
        logger.info("=" * 80)

        # Clean data
        df_clean = self.load_raw_data(df)

        # Calculate KPIs
        self.calculate_kpi1_daily_volume(df_clean)
        kpi2 = self.calculate_kpi2_weekly_volume(df_clean)
        self.calculate_kpi3_token_breakdown(kpi2)
        self.calculate_kpi4_wow_volume_change(kpi2)
        self.calculate_kpi5_chain_distribution(kpi2)

        logger.info("\n✅ All DEX Volume KPIs calculated successfully")
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

