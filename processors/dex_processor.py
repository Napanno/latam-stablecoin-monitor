import pandas as pd
from typing import Dict
from processors.base_processor import BaseProcessor
from utils.logger import get_logger

logger = get_logger(__name__)


class DexProcessor(BaseProcessor):
    """Domain 3: DEX Trading Volume"""

    def __init__(self):
        super().__init__(domain_name="DEX")

    def process_all(self, raw_data: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        """Process dex.trades data for volume metrics"""
        logger.info(f"[{self.domain_name}] Processing {len(raw_data):,} DEX trades")

        # Step 1: Data cleaning
        df = self._clean_data(raw_data)
        self.log_processing_summary(df, "after_cleaning")

        # Step 2: Generate KPIs
        self.kpi_data = {
            "daily_volume": self._kpi1_daily_volume(df),
            "weekly_volume": self._kpi2_weekly_volume(df),
            "token_breakdown": self._kpi3_token_breakdown(df),
            "wow_change": self._kpi4_wow_change(df),
            "chain_distribution": self._kpi5_chain_distribution(df),
        }

        return self.kpi_data

    def _clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Prepare raw data for processing"""
        df = df.copy()

        # Convert date columns
        if 'block_time' in df.columns:
            df['block_time'] = pd.to_datetime(df['block_time'], errors='coerce')

        # Extract dates
        df['block_date'] = df['block_time'].dt.date
        df['week'] = df['block_time'].dt.isocalendar().apply(
            lambda x: f"{x.year}_W{x.week:02d}", axis=1
        )

        # Convert numeric columns
        numeric_cols = ['volume_usd']
        df = self.clean_numeric_columns(df, numeric_cols)

        # Remove rows with missing critical data
        df = df.dropna(subset=['block_time', 'blockchain'])

        return df

    def _kpi1_daily_volume(self, df: pd.DataFrame) -> pd.DataFrame:
        """KPI 3.1 & 3.2: Daily DEX volume and trade count"""
        kpi = df.groupby(['block_date', 'blockchain', 'symbol']).agg({
            'volume_usd': 'sum',
            'tx_hash': 'count'
        }).reset_index().rename(columns={'tx_hash': 'trade_count'})

        return kpi

    def _kpi2_weekly_volume(self, df: pd.DataFrame) -> pd.DataFrame:
        """KPI 3.1 & 3.2: Weekly aggregated volume"""
        kpi = df.groupby(['week', 'blockchain', 'symbol']).agg({
            'volume_usd': 'sum',
            'tx_hash': 'count'
        }).reset_index().rename(columns={'tx_hash': 'trade_count'})

        return kpi

    def _kpi3_token_breakdown(self, df: pd.DataFrame) -> pd.DataFrame:
        """KPI 3.3 & 3.5: Volume breakdown by token"""
        latest_week = df['week'].max()
        weekly = df[df['week'] == latest_week]

        kpi = weekly.groupby('symbol').agg({
            'volume_usd': 'sum',
            'tx_hash': 'count'
        }).reset_index()

        total_volume = kpi['volume_usd'].sum()
        kpi['market_share_pct'] = (kpi['volume_usd'] / total_volume * 100).round(2)
        kpi['week'] = latest_week

        return kpi.rename(columns={'tx_hash': 'trade_count'})

    def _kpi4_wow_change(self, df: pd.DataFrame) -> pd.DataFrame:
        """KPI 3.7: Week-over-week volume change"""
        weekly = self._kpi2_weekly_volume(df)

        # Calculate WoW
        weekly = weekly.sort_values(['symbol', 'week'])
        weekly['volume_prev_week'] = weekly.groupby('symbol')['volume_usd'].shift(1)
        weekly['volume_wow_pct'] = (
            (weekly['volume_usd'] - weekly['volume_prev_week']) /
            weekly['volume_prev_week'] * 100
        ).round(2)

        return weekly[['week', 'symbol', 'blockchain', 'volume_wow_pct']]

    def _kpi5_chain_distribution(self, df: pd.DataFrame) -> pd.DataFrame:
        """KPI 3.4 & 3.6: Volume distribution by chain"""
        latest_week = df['week'].max()
        weekly = df[df['week'] == latest_week]

        kpi = weekly.groupby('blockchain').agg({
            'volume_usd': 'sum'
        }).reset_index()

        total_volume = kpi['volume_usd'].sum()
        kpi['chain_share_pct'] = (kpi['volume_usd'] / total_volume * 100).round(2)
        kpi['week'] = latest_week

        return kpi