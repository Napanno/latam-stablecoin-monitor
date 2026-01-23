import pandas as pd
from typing import Dict
from datetime import datetime
import numpy as np
from processors.base_processor import BaseProcessor
from utils.logger import get_logger
from utils.date_utils import get_iso_week

logger = get_logger(__name__)


class SupplyProcessor(BaseProcessor):
    """Domain 1: On-Chain Supply KPIs"""

    def __init__(self):
        super().__init__(domain_name="Supply")

    def process_all(self, raw_data: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        """
        Process tokens.transfers data for supply metrics

        Args:
            raw_data: tokens.transfers raw data from Dune

        Returns:
            Dict with KPIs:
            - weekly_supply
            - supply_by_chain
            - supply_by_token
            - supply_growth_rate
        """
        logger.info(f"[{self.domain_name}] Processing {len(raw_data):,} transfer events")

        # Step 1: Data cleaning
        df = self._clean_data(raw_data)
        self.log_processing_summary(df, "after_cleaning")

        # Step 2: Calculate cumulative supply (mints - burns)
        df = self._calculate_cumulative_supply(df)
        self.log_processing_summary(df, "after_cumulative")

        # Step 3: Generate KPIs
        self.kpi_data = {
            "weekly_supply": self._kpi1_weekly_supply(df),
            "supply_by_chain": self._kpi2_supply_by_chain(df),
            "supply_by_token": self._kpi3_supply_by_token(df),
            "growth_rate": self._kpi4_growth_rate(df),
        }

        return self.kpi_data

    def _clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Prepare raw data for processing"""
        df = df.copy()

        # Convert date columns
        if 'block_time' in df.columns:
            df['block_time'] = pd.to_datetime(df['block_time'], errors='coerce')

        # Convert numeric columns
        numeric_cols = ['amount', 'amount_usd']
        df = self.clean_numeric_columns(df, numeric_cols)

        # Add week identifier
        df['week'] = df['block_time'].dt.isocalendar().apply(
            lambda x: f"{x.year}_W{x.week:02d}", axis=1
        )

        # Remove rows with missing critical data
        df = df.dropna(subset=['block_time', 'blockchain', 'contract_address'])

        return df

    def _calculate_cumulative_supply(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate cumulative supply by mint/burn logic"""
        df = df.copy()

        # Identify mints and burns
        null_address = "0x0000000000000000000000000000000000000000"
        df['is_mint'] = df['from'].str.lower() == null_address.lower()
        df['is_burn'] = df['to'].str.lower() == null_address.lower()

        # Calculate supply impact (mints add, burns subtract)
        df['supply_impact'] = 0
        df.loc[df['is_mint'], 'supply_impact'] = df['amount']
        df.loc[df['is_burn'], 'supply_impact'] = -df['amount']

        return df

    def _kpi1_weekly_supply(self, df: pd.DataFrame) -> pd.DataFrame:
        """KPI 1.1 & 1.2: Weekly supply by token/chain + total"""
        kpi = df.groupby(['week', 'blockchain', 'symbol']).agg({
            'supply_impact': 'sum',
            'block_time': 'max'
        }).reset_index()

        # Cumulative supply (group level)
        kpi['cumulative_supply'] = kpi.groupby(['blockchain', 'symbol'])['supply_impact'].cumsum()

        # Rename for clarity
        kpi = kpi.rename(columns={'cumulative_supply': 'circulating_supply_tokens'})

        return kpi[['week', 'blockchain', 'symbol', 'circulating_supply_tokens']]

    def _kpi2_supply_by_chain(self, df: pd.DataFrame) -> pd.DataFrame:
        """KPI 1.4: Supply share by blockchain"""
        # Get latest week's supply
        latest_week = df['week'].max()
        latest = df[df['week'] == latest_week]

        chain_supply = latest.groupby('blockchain').agg({
            'supply_impact': 'sum'
        }).reset_index()

        # Calculate total and share
        total_supply = chain_supply['supply_impact'].sum()
        chain_supply['chain_share_pct'] = (chain_supply['supply_impact'] / total_supply * 100).round(2)

        return chain_supply.rename(columns={
            'supply_impact': 'circulating_supply_tokens'
        })

    def _kpi3_supply_by_token(self, df: pd.DataFrame) -> pd.DataFrame:
        """KPI 1.3: Supply share by token"""
        latest_week = df['week'].max()
        latest = df[df['week'] == latest_week]

        token_supply = latest.groupby('symbol').agg({
            'supply_impact': 'sum'
        }).reset_index()

        # Calculate total and share
        total_supply = token_supply['supply_impact'].sum()
        token_supply['token_share_pct'] = (token_supply['supply_impact'] / total_supply * 100).round(2)

        token_supply['week'] = latest_week

        return token_supply.rename(columns={
            'supply_impact': 'circulating_supply_tokens'
        })[['week', 'symbol', 'circulating_supply_tokens', 'token_share_pct']]

    def _kpi4_growth_rate(self, df: pd.DataFrame) -> pd.DataFrame:
        """KPI 1.5: Week-over-week supply growth"""
        weekly_totals = df.groupby('week').agg({
            'supply_impact': 'sum'
        }).reset_index().rename(columns={'supply_impact': 'weekly_supply'})

        # Cumulative supply
        weekly_totals['cumulative_supply'] = weekly_totals['weekly_supply'].cumsum()

        # WoW growth rate
        weekly_totals['prev_week_supply'] = weekly_totals['cumulative_supply'].shift(1)
        weekly_totals['growth_rate_pct'] = (
                (weekly_totals['cumulative_supply'] - weekly_totals['prev_week_supply']) /
                weekly_totals['prev_week_supply'] * 100
        ).round(2)

        return weekly_totals[['week', 'cumulative_supply', 'growth_rate_pct']]