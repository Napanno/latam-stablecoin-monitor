import pandas as pd
from typing import Dict
from processors.base_processor import BaseProcessor
from utils.logger import get_logger

logger = get_logger(__name__)


class FlowsProcessor(BaseProcessor):
    """Domain 2: Mint/Burn Transaction Flows"""

    def __init__(self):
        super().__init__(domain_name="Flows")

    def process_all(self, raw_data: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        """
        Process tokens.transfers data for flow metrics (Daily aggregation)

        Args:
            raw_data: tokens.transfers raw data from Dune

        Returns:
            Dict with KPIs:
            - daily_flows
            - weekly_aggregates
            - net_issuance
            - wow_change
        """
        logger.info(f"[{self.domain_name}] Processing {len(raw_data):,} transfer events")

        # Step 1: Data cleaning
        df = self._clean_data(raw_data)
        self.log_processing_summary(df, "after_cleaning")

        # Step 2: Identify mints and burns
        df = self._classify_flows(df)
        self.log_processing_summary(df, "after_classification")

        # Step 3: Generate KPIs
        self.kpi_data = {
            "daily_flows": self._kpi1_daily_flows(df),
            "weekly_aggregates": self._kpi2_weekly_aggregates(df),
            "net_issuance": self._kpi3_net_issuance(df),
            "wow_change": self._kpi4_wow_change(df),
        }

        return self.kpi_data

    def _clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Prepare raw data for flow processing"""
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
        numeric_cols = ['amount', 'amount_usd']
        df = self.clean_numeric_columns(df, numeric_cols)

        # Remove rows with missing critical data
        df = df.dropna(subset=['block_time', 'blockchain', 'symbol'])

        return df

    def _classify_flows(self, df: pd.DataFrame) -> pd.DataFrame:
        """Classify transfers as mints or burns"""
        df = df.copy()

        null_address = "0x0000000000000000000000000000000000000000"
        df['flow_type'] = 'transfer'  # Default
        df.loc[df['from'].str.lower() == null_address.lower(), 'flow_type'] = 'mint'
        df.loc[df['to'].str.lower() == null_address.lower(), 'flow_type'] = 'burn'

        return df

    def _kpi1_daily_flows(self, df: pd.DataFrame) -> pd.DataFrame:
        """KPI 2.1 & 2.2 & 2.3: Daily mint/burn counts and volumes"""
        # Filter for mints and burns only
        flows = df[df['flow_type'].isin(['mint', 'burn'])].copy()

        kpi = flows.groupby(['block_date', 'blockchain', 'symbol', 'flow_type']).agg({
            'tx_hash': 'count',  # Transaction count
            'amount_usd': 'sum'  # Volume
        }).reset_index()

        # Pivot to separate mint/burn
        kpi = kpi.pivot_table(
            index=['block_date', 'blockchain', 'symbol'],
            columns='flow_type',
            values=['tx_hash', 'amount_usd'],
            fill_value=0
        ).reset_index()

        # Flatten multi-level columns
        kpi.columns = [f"{col[0]}_{col[1]}" if col[1] else col[0] for col in kpi.columns.values]

        # Calculate net
        kpi['net_volume_usd'] = kpi['amount_usd_mint'] - kpi['amount_usd_burn']

        return kpi

    def _kpi2_weekly_aggregates(self, df: pd.DataFrame) -> pd.DataFrame:
        """KPI 2.3 & 2.4: Weekly aggregated flows"""
        flows = df[df['flow_type'].isin(['mint', 'burn'])].copy()

        kpi = flows.groupby(['week', 'blockchain', 'symbol', 'flow_type']).agg({
            'amount_usd': 'sum',
            'tx_hash': 'count'
        }).reset_index()

        # Pivot
        kpi = kpi.pivot_table(
            index=['week', 'blockchain', 'symbol'],
            columns='flow_type',
            values=['amount_usd', 'tx_hash'],
            fill_value=0
        ).reset_index()

        # Flatten columns
        kpi.columns = [f"{col[0]}_{col[1]}" if col[1] else col[0] for col in kpi.columns.values]

        kpi['net_volume_usd'] = kpi['amount_usd_mint'] - kpi['amount_usd_burn']

        return kpi

    def _kpi3_net_issuance(self, df: pd.DataFrame) -> pd.DataFrame:
        """KPI 2.4: Net issuance trend (expansion/contraction)"""
        flows = df[df['flow_type'].isin(['mint', 'burn'])].copy()

        kpi = flows.groupby(['week', 'symbol']).agg({
            'flow_type': lambda x: (x == 'mint').sum() - (x == 'burn').sum(),
            'amount_usd': lambda x: x[flows.loc[x.index, 'flow_type'] == 'mint'].sum() -
                                    x[flows.loc[x.index, 'flow_type'] == 'burn'].sum()
        }).reset_index()

        kpi.columns = ['week', 'symbol', 'net_transaction_count', 'net_issuance_usd']
        kpi['trend'] = kpi['net_issuance_usd'].apply(
            lambda x: 'EXPANSION' if x > 0 else 'CONTRACTION' if x < 0 else 'NEUTRAL'
        )

        return kpi

    def _kpi4_wow_change(self, df: pd.DataFrame) -> pd.DataFrame:
        """KPI 2.5: Week-over-week activity change"""
        # Get weekly totals
        weekly = self._kpi2_weekly_aggregates(df)

        # Calculate WoW change
        weekly = weekly.sort_values(['symbol', 'week'])

        # Group by symbol to calculate WoW
        weekly['mint_volume_prev_week'] = weekly.groupby('symbol')['amount_usd_mint'].shift(1)
        weekly['mint_volume_wow_pct'] = (
                (weekly['amount_usd_mint'] - weekly['mint_volume_prev_week']) /
                weekly['mint_volume_prev_week'] * 100
        ).round(2)

        weekly['burn_volume_prev_week'] = weekly.groupby('symbol')['amount_usd_burn'].shift(1)
        weekly['burn_volume_wow_pct'] = (
                (weekly['amount_usd_burn'] - weekly['burn_volume_prev_week']) /
                weekly['burn_volume_prev_week'] * 100
        ).round(2)

        return weekly[['week', 'symbol', 'blockchain', 'mint_volume_wow_pct', 'burn_volume_wow_pct']]