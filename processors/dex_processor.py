"""
DEX KPI Processor - Domain 1
Processes decentralized exchange trade data
"""

import pandas as pd
from pathlib import Path
from datetime import datetime
from typing import Dict, Optional
from utils.logger import get_logger
from utils.date_utils import get_iso_week
from utils.math_utils import safe_percentage, wow_percentage_change, safe_division

logger = get_logger(__name__)


class DexKPIProcessor:
    """Process DEX trade data to calculate trading KPIs"""

    def __init__(self, output_dir: str = 'data/kpi'):
        """Initialize processor"""
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        logger.info("DexKPIProcessor initialized")

    def process_all(self, df: pd.DataFrame) -> Dict:
        """
        Process DEX data with trading analysis

        Args:
            df: DataFrame from DEX query with trade data

        Returns:
            Dictionary with DEX KPIs
        """
        if df is None or df.empty:
            logger.error("Empty DEX data received")
            return {}

        logger.info(f"Processing DEX data: {len(df)} rows")

        # Clean data
        df = self._clean_data(df)

        # Calculate KPIs
        results = {
            'daily_volume': self._kpi1_daily_volume(df),
            'weekly_aggregates': self._kpi2_weekly_aggregates(df),
            'token_trading': self._kpi3_token_trading(df),
            'wow_change': self._kpi4_wow_change(df),
            'liquidity_analysis': self._kpi5_liquidity_analysis(df),  # NEW
            'raw_data': df  # Store cleaned data for summary calculations
        }

        logger.info("✅ DEX processing complete")
        return results

    def _clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean and prepare DEX data
        Handle column names from SQL query (date, symbol, blockchain)

        Args:
            df: Raw DEX DataFrame

        Returns:
            Cleaned DataFrame
        """
        df = df.copy()

        # Map SQL column names to expected processor names
        column_mapping = {
            'date': 'block_time',
            'symbol': 'token_symbol',
            'total_volume_usd': 'amount_usd'
        }

        # Apply column renaming
        for old_name, new_name in column_mapping.items():
            if old_name in df.columns and new_name not in df.columns:
                df[new_name] = df[old_name]
                logger.debug(f"Mapped column: {old_name} → {new_name}")

        # ===================================================================
        # STANDARDIZED DATE HANDLING (I4 Fix)
        # Convert any date column to both datetime and ISO week format
        # ===================================================================
        date_col_found = False

        # Priority order: block_time > block_date > date
        if 'block_time' in df.columns:
            df['block_time'] = pd.to_datetime(df['block_time'], errors='coerce')
            df['week'] = df['block_time'].apply(get_iso_week)
            date_col_found = True
            logger.debug("Using 'block_time' as primary date column")

        elif 'block_date' in df.columns:
            df['block_date'] = pd.to_datetime(df['block_date'], errors='coerce')
            df['block_time'] = df['block_date']
            df['week'] = df['block_date'].apply(get_iso_week)
            date_col_found = True
            logger.debug("Using 'block_date' as primary date column (mapped to block_time)")

        elif 'date' in df.columns:
            # This should have been mapped to block_time above, but check anyway
            if 'block_time' not in df.columns:
                df['date'] = pd.to_datetime(df['date'], errors='coerce')
                df['block_time'] = df['date']
                df['week'] = df['date'].apply(get_iso_week)
                date_col_found = True
                logger.debug("Using 'date' as primary date column (mapped to block_time)")

        if not date_col_found:
            logger.error("No date column found in DEX data")
            raise ValueError("Missing date column (expected 'block_time', 'block_date', or 'date')")

        # Ensure 'week' column exists
        if 'week' not in df.columns:
            logger.warning("'week' column not created - using fallback")
            df['week'] = df['block_time'].apply(get_iso_week)
        # ===================================================================

        # Convert numeric columns
        numeric_columns = ['amount_usd', 'trade_count', 'buy_volume_usd', 'sell_volume_usd',
                           'buy_pressure_pct', 'avg_trade_size_usd', 'buy_count', 'sell_count',
                           'net_buy_pressure_usd', 'max_trade_usd', 'unique_dex_count']
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        # Fill NaN with 0 for volume metrics
        df = df.fillna(0)

        # Remove rows with missing critical data
        # Use standardized columns
        df = df.dropna(subset=['block_time', 'token_symbol'])

        logger.info(f"✓ Cleaned DEX data: {len(df)} rows")
        logger.debug(f"  Date column: block_time, Week column: week (ISO format)")

        return df

    def _kpi1_daily_volume(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        KPI 1.1: Daily trading volume by token

        Returns:
            DataFrame with daily volume metrics
        """
        # If data is already aggregated daily (from SQL), use it directly
        if 'trade_count' in df.columns and 'amount_usd' in df.columns:
            kpi = df[['block_time', 'token_symbol', 'blockchain', 'amount_usd', 'trade_count']].copy()

            # Include buy pressure if available
            if 'buy_pressure_pct' in df.columns:
                kpi['buy_pressure_pct'] = df['buy_pressure_pct']

            # Calculate average trade size if not present
            if 'avg_trade_size' not in kpi.columns:
                kpi['avg_trade_size'] = safe_percentage(
                    kpi['amount_usd'],
                    kpi['trade_count']
                )

            return kpi.sort_values('block_time', ascending=False)

        # Fallback: aggregate from raw trades
        kpi = df.groupby(['block_time', 'token_symbol', 'blockchain']).agg({
            'amount_usd': ['sum', 'count'],
        }).reset_index()

        kpi.columns = ['block_time', 'token_symbol', 'blockchain', 'volume_usd', 'trade_count']

        kpi['avg_trade_size'] = safe_percentage(
            kpi['volume_usd'],
            kpi['trade_count']
        )

        return kpi.sort_values('block_time', ascending=False)

    def _kpi2_weekly_aggregates(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        KPI 1.2: Weekly aggregated trading volume

        Returns:
            DataFrame with weekly volume metrics
        """
        agg_dict = {
            'amount_usd': 'sum',
            'trade_count': 'sum',
        }

        # Include buy/sell volumes if available
        if 'buy_volume_usd' in df.columns:
            agg_dict['buy_volume_usd'] = 'sum'
        if 'sell_volume_usd' in df.columns:
            agg_dict['sell_volume_usd'] = 'sum'

        kpi = df.groupby(['week', 'token_symbol', 'blockchain']).agg(agg_dict).reset_index()

        # Rename for consistency
        kpi.rename(columns={'amount_usd': 'volume_usd'}, inplace=True)

        # Calculate average trade size
        kpi['avg_trade_size'] = safe_percentage(
            kpi['volume_usd'],
            kpi['trade_count']
        )

        # Calculate buy pressure percentage if buy/sell volumes available
        if 'buy_volume_usd' in kpi.columns and 'sell_volume_usd' in kpi.columns:
            total_volume = kpi['buy_volume_usd'] + kpi['sell_volume_usd']
            kpi['buy_pressure_pct'] = safe_percentage(
                kpi['buy_volume_usd'],
                total_volume
            )

        return kpi.sort_values('week', ascending=False)

    def _kpi3_token_trading(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        KPI 1.3: Token-level trading metrics

        Returns:
            DataFrame with per-token statistics
        """
        kpi = df.groupby(['week', 'token_symbol']).agg({
            'amount_usd': ['sum', 'mean', 'std'],
            'trade_count': 'sum',
            'blockchain': 'nunique',
        }).reset_index()

        kpi.columns = ['week', 'token_symbol', 'volume_usd',
                       'avg_trade_size', 'trade_volatility',
                       'trade_count', 'blockchains_traded']

        # Calculate market share (volume as % of total)
        total_volume = kpi.groupby('week')['volume_usd'].transform('sum')
        kpi['market_share_pct'] = safe_percentage(
            kpi['volume_usd'],
            total_volume
        )

        return kpi.sort_values('week', ascending=False)

    def _kpi4_wow_change(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        KPI 1.4: Week-over-week trading change

        Returns:
            DataFrame with WoW metrics
        """
        weekly = self._kpi2_weekly_aggregates(df)

        # Sort by token/blockchain/week
        weekly = weekly.sort_values(['token_symbol', 'blockchain', 'week'])

        # Calculate WoW change
        weekly['volume_prev_week'] = weekly.groupby(['token_symbol', 'blockchain'])['volume_usd'].shift(1)
        weekly['trades_prev_week'] = weekly.groupby(['token_symbol', 'blockchain'])['trade_count'].shift(1)

        # Use safe wow_percentage_change (handles zero case gracefully)
        try:
            weekly['volume_wow_pct'] = wow_percentage_change(
                weekly['volume_usd'],
                weekly['volume_prev_week']
            )
        except ValueError:
            weekly['volume_wow_pct'] = None

        try:
            weekly['trades_wow_pct'] = wow_percentage_change(
                weekly['trade_count'],
                weekly['trades_prev_week']
            )
        except ValueError:
            weekly['trades_wow_pct'] = None

        return weekly[[
            'week', 'token_symbol', 'blockchain',
            'volume_usd', 'volume_prev_week', 'volume_wow_pct',
            'trade_count', 'trades_prev_week', 'trades_wow_pct',
            'avg_trade_size'
        ]].sort_values('week', ascending=False)

    def _kpi5_liquidity_analysis(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        KPI 1.5: Liquidity and market depth analysis (NEW)

        Analyzes DEX liquidity characteristics:
        - Whale activity (max trade size)
        - Trade frequency imbalance (buy vs sell counts)
        - Net pressure magnitude (absolute buying/selling)
        - DEX fragmentation (liquidity spread)

        Returns:
            DataFrame with liquidity metrics
        """
        # Check if required columns exist
        required_cols = ['max_trade_usd', 'buy_count', 'sell_count',
                         'net_buy_pressure_usd', 'unique_dex_count']

        available_cols = [col for col in required_cols if col in df.columns]

        if not available_cols:
            logger.warning("⚠ Liquidity analysis columns not available in data")
            return pd.DataFrame()

        # Build aggregation dict dynamically (FIXED)
        agg_dict = {}
        if 'max_trade_usd' in df.columns:
            agg_dict['max_trade_usd'] = 'max'
        if 'avg_trade_size_usd' in df.columns:
            agg_dict['avg_trade_size_usd'] = 'mean'
        if 'buy_count' in df.columns:
            agg_dict['buy_count'] = 'sum'
        if 'sell_count' in df.columns:
            agg_dict['sell_count'] = 'sum'
        if 'net_buy_pressure_usd' in df.columns:
            agg_dict['net_buy_pressure_usd'] = 'sum'
        if 'unique_dex_count' in df.columns:
            agg_dict['unique_dex_count'] = 'sum'
        if 'trade_count' in df.columns:
            agg_dict['trade_count'] = 'sum'
        # FIXED: Changed from tuple to string
        if 'amount_usd' in df.columns:
            agg_dict['amount_usd'] = 'sum'

        kpi = df.groupby(['week', 'token_symbol', 'blockchain']).agg(agg_dict).reset_index()

        # Rename amount_usd to total_volume_usd if it exists
        if 'amount_usd' in kpi.columns:
            kpi.rename(columns={'amount_usd': 'total_volume_usd'}, inplace=True)

        # Calculate derived metrics

        # 1. Whale concentration ratio (how dominant are large trades)
        if 'max_trade_usd' in kpi.columns and 'avg_trade_size_usd' in kpi.columns:
            kpi['whale_concentration_ratio'] = safe_division(
                kpi['max_trade_usd'],
                kpi['avg_trade_size_usd'],
                default_value=0.0
            )

        # 2. Trade frequency imbalance (buy/sell activity ratio)
        if 'buy_count' in kpi.columns and 'sell_count' in kpi.columns:
            kpi['buy_sell_count_ratio'] = safe_division(
                kpi['buy_count'],
                kpi['sell_count'],
                default_value=1.0
            )

            # Total trade activity
            kpi['total_trade_count'] = kpi['buy_count'] + kpi['sell_count']

            # Buy frequency percentage
            kpi['buy_frequency_pct'] = safe_percentage(
                kpi['buy_count'],
                kpi['total_trade_count']
            )

        # 3. Net pressure intensity (pressure per trade)
        if 'net_buy_pressure_usd' in kpi.columns and 'trade_count' in kpi.columns:
            kpi['net_pressure_per_trade'] = safe_division(
                kpi['net_buy_pressure_usd'],
                kpi['trade_count'],
                default_value=0.0
            )

        # 4. DEX fragmentation score (lower = more concentrated)
        if 'unique_dex_count' in kpi.columns and 'total_volume_usd' in kpi.columns:
            # Average volume per DEX
            kpi['avg_volume_per_dex'] = safe_division(
                kpi['total_volume_usd'],
                kpi['unique_dex_count'],
                default_value=0.0
            )

        return kpi.sort_values('week', ascending=False)

    def export_kpis(self, results: Dict, timestamp: str = None) -> Dict[str, Path]:
        """
        Export KPI results to CSV files

        Args:
            results: Dictionary with DEX KPI DataFrames
            timestamp: Optional timestamp for filenames

        Returns:
            Dictionary with filenames of exported files
        """
        if timestamp is None:
            timestamp = datetime.now().strftime('%Y%m%d%H%M%S')

        exported_files = {}

        # Extract week from data
        week = "2026-W01"  # Default fallback
        for key in ['weekly_aggregates', 'token_trading', 'wow_change', 'liquidity_analysis']:
            if key in results and results[key] is not None and not results[key].empty:
                if 'week' in results[key].columns:
                    week = results[key]['week'].iloc[0]
                    break

        # Export each KPI
        kpi_mapping = {
            'daily_volume': 'dex_kpi1_daily_volume',
            'weekly_aggregates': 'dex_kpi2_weekly_aggregates',
            'token_trading': 'dex_kpi3_token_trading',
            'wow_change': 'dex_kpi4_wow_change',
            'liquidity_analysis': 'dex_kpi5_liquidity_analysis',  # NEW
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
        ENHANCED: Now includes liquidity analysis metrics

        Args:
            results: Dictionary with DEX KPI DataFrames

        Returns:
            Dictionary with summary statistics
        """
        summary = {
            'total_volume_usd': 0,
            'total_trades': 0,
            'avg_trade_size': 0,
            'unique_tokens': 0,
            'unique_blockchains': 0,
            'avg_buy_pressure_pct': 0,
            'total_buy_volume_usd': 0,
            'total_sell_volume_usd': 0,
            # NEW liquidity metrics
            'max_whale_trade_usd': 0,
            'avg_whale_concentration': 0,
            'total_unique_dexs': 0,
            'net_buy_pressure_usd': 0,
        }

        if 'weekly_aggregates' in results and results['weekly_aggregates'] is not None:
            df = results['weekly_aggregates']
            summary['total_volume_usd'] = df['volume_usd'].sum()
            summary['total_trades'] = df['trade_count'].sum()
            summary['avg_trade_size'] = safe_percentage(
                pd.Series([summary['total_volume_usd']]),
                pd.Series([summary['total_trades']])
            ).values[0] if summary['total_trades'] > 0 else 0
            summary['unique_tokens'] = df['token_symbol'].nunique()
            summary['unique_blockchains'] = df['blockchain'].nunique()

            # Calculate buy/sell volumes and average buy pressure
            if 'buy_volume_usd' in df.columns:
                summary['total_buy_volume_usd'] = df['buy_volume_usd'].sum()
            if 'sell_volume_usd' in df.columns:
                summary['total_sell_volume_usd'] = df['sell_volume_usd'].sum()

            if 'buy_pressure_pct' in df.columns:
                valid_pressures = df[df['buy_pressure_pct'] > 0]['buy_pressure_pct']
                if len(valid_pressures) > 0:
                    summary['avg_buy_pressure_pct'] = valid_pressures.mean()

        # Alternative: Calculate from raw data if weekly doesn't have buy_pressure_pct
        if summary['avg_buy_pressure_pct'] == 0 and 'raw_data' in results:
            raw_df = results['raw_data']
            if 'buy_pressure_pct' in raw_df.columns:
                total_volume = raw_df['amount_usd'].sum()
                if total_volume > 0:
                    weighted_pressure = (raw_df['buy_pressure_pct'] * raw_df['amount_usd']).sum()
                    summary['avg_buy_pressure_pct'] = weighted_pressure / total_volume
                else:
                    valid_pressures = raw_df[raw_df['buy_pressure_pct'] > 0]['buy_pressure_pct']
                    if len(valid_pressures) > 0:
                        summary['avg_buy_pressure_pct'] = valid_pressures.mean()

        # NEW: Liquidity analysis summary
        if 'liquidity_analysis' in results and results['liquidity_analysis'] is not None:
            df = results['liquidity_analysis']

            if 'max_trade_usd' in df.columns:
                summary['max_whale_trade_usd'] = df['max_trade_usd'].max()

            if 'whale_concentration_ratio' in df.columns:
                valid_ratios = df[df['whale_concentration_ratio'] > 0]['whale_concentration_ratio']
                if len(valid_ratios) > 0:
                    summary['avg_whale_concentration'] = valid_ratios.mean()

            if 'unique_dex_count' in df.columns:
                summary['total_unique_dexs'] = df['unique_dex_count'].sum()

            if 'net_buy_pressure_usd' in df.columns:
                summary['net_buy_pressure_usd'] = df['net_buy_pressure_usd'].sum()

        return summary
