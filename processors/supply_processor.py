"""
Process circulating supply data and calculate KPIs
"""

import pandas as pd
from pathlib import Path
from typing import Dict
import yaml

from utils.logger import setup_logger

logger = setup_logger(__name__)


class SupplyKPIProcessor:
    """Calculate supply-related KPIs"""

    def __init__(self, config_path: str = 'config.yaml'):
        """Initialize processor with config"""
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)

        self.kpi_dir = Path(self.config['paths']['kpi_export'])
        self.kpi_dir.mkdir(parents=True, exist_ok=True)

        self.kpis = {}

    def load_raw_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Load and prepare raw supply data

        Args:
            df: Raw supply data from Dune

        Returns:
            Cleaned DataFrame
        """
        logger.info("Loading and cleaning supply data...")

        df = df.copy()
        df['week'] = pd.to_datetime(df['week'])

        # Fill NaN values
        df = df.fillna({
            'circulating_supply_tokens': 0,
            'weekly_minted_tokens': 0,
            'weekly_burned_tokens': 0,
            'weekly_net_flow_tokens': 0
        }).infer_objects(copy=False)

        logger.info(f"✓ Loaded {len(df):,} rows")
        return df

    def calculate_kpi1_weekly_supply(self, df: pd.DataFrame) -> pd.DataFrame:
        """KPI 1: Weekly On-chain Circulating Supply"""
        logger.info("Calculating KPI 1: Weekly On-chain Circulating Supply...")

        kpi1 = df[[
            'week', 'stablecoin', 'blockchain',
            'circulating_supply_tokens',
            'weekly_minted_tokens', 'weekly_burned_tokens',
            'weekly_net_flow_tokens'
        ]].copy()

        kpi1 = kpi1.sort_values(['week', 'circulating_supply_tokens'], ascending=[False, False])

        self.kpis['kpi1_weekly_supply'] = kpi1
        logger.info(f"✓ KPI 1 calculated: {len(kpi1):,} rows")

        return kpi1

    def calculate_kpi2_supply_by_chain(self, df: pd.DataFrame) -> pd.DataFrame:
        """KPI 2: Cumulative Supply by Chain"""
        logger.info("Calculating KPI 2: Cumulative Supply by Chain...")

        latest_week = df['week'].max()
        kpi2 = df[df['week'] == latest_week].copy()

        # Calculate chain share
        kpi2.loc[:, 'chain_share_pct'] = kpi2.groupby('stablecoin')['circulating_supply_tokens'].transform(
            lambda x: (x / x.sum() * 100) if x.sum() > 0 else 0
        )

        kpi2 = kpi2.sort_values('circulating_supply_tokens', ascending=False)

        self.kpis['kpi2_supply_by_chain'] = kpi2
        logger.info(f"✓ KPI 2 calculated: {len(kpi2):,} rows")

        return kpi2

    def calculate_kpi3_total_supply(self, df: pd.DataFrame) -> pd.DataFrame:
        """KPI 3: Total Supply Across All Chains"""
        logger.info("Calculating KPI 3: Total Supply Across All Chains...")

        kpi3 = df.groupby(['week', 'stablecoin']).agg({
            'circulating_supply_tokens': 'sum',
            'weekly_minted_tokens': 'sum',
            'weekly_burned_tokens': 'sum',
            'weekly_net_flow_tokens': 'sum'
        }).reset_index()

        kpi3.rename(columns={
            'circulating_supply_tokens': 'total_supply_all_chains',
            'weekly_minted_tokens': 'total_mints_all_chains',
            'weekly_burned_tokens': 'total_burns_all_chains',
            'weekly_net_flow_tokens': 'total_net_flow_all_chains'
        }, inplace=True)

        kpi3 = kpi3.sort_values(['week', 'total_supply_all_chains'], ascending=[False, False])

        # Calculate market share
        kpi3['market_share_pct'] = kpi3.groupby('week')['total_supply_all_chains'].transform(
            lambda x: (x / x.sum() * 100) if x.sum() > 0 else 0
        )

        self.kpis['kpi3_total_supply'] = kpi3
        logger.info(f"✓ KPI 3 calculated: {len(kpi3):,} rows")

        return kpi3

    def calculate_kpi4_growth_rate(self, kpi3: pd.DataFrame) -> pd.DataFrame:
        """KPI 4: Supply Growth Rate"""
        logger.info("Calculating KPI 4: Supply Growth Rate...")

        kpi4 = kpi3.copy()
        kpi4 = kpi4.sort_values(['stablecoin', 'week'])

        # WoW growth rate
        kpi4['supply_growth_rate_pct'] = (
                kpi4.groupby('stablecoin')['total_supply_all_chains']
                .pct_change() * 100
        )

        # Absolute change
        kpi4['supply_change_abs'] = (
            kpi4.groupby('stablecoin')['total_supply_all_chains']
            .diff()
        )

        # Classification
        thresholds = self.config['processing']['growth_thresholds']

        def classify_growth(rate):
            if pd.isna(rate):
                return 'N/A'
            elif rate > thresholds['high_growth']:
                return 'HIGH_GROWTH'
            elif rate > thresholds['moderate_growth']:
                return 'MODERATE_GROWTH'
            elif rate > thresholds['moderate_decline']:
                return 'STABLE'
            elif rate > thresholds['high_decline']:
                return 'MODERATE_DECLINE'
            else:
                return 'HIGH_DECLINE'

        kpi4['growth_classification'] = kpi4['supply_growth_rate_pct'].apply(classify_growth)

        self.kpis['kpi4_growth_rate'] = kpi4
        logger.info(f"✓ KPI 4 calculated: {len(kpi4):,} rows")

        return kpi4

    def process_all(self, df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        """
        Process all supply KPIs

        Args:
            df: Raw supply data

        Returns:
            Dictionary of KPI DataFrames
        """
        logger.info("=" * 80)
        logger.info("PROCESSING SUPPLY KPIs")
        logger.info("=" * 80)

        # Clean data
        df_clean = self.load_raw_data(df)

        # Calculate KPIs
        self.calculate_kpi1_weekly_supply(df_clean)
        self.calculate_kpi2_supply_by_chain(df_clean)
        kpi3 = self.calculate_kpi3_total_supply(df_clean)
        self.calculate_kpi4_growth_rate(kpi3)

        logger.info("\n✅ All Supply KPIs calculated successfully")
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