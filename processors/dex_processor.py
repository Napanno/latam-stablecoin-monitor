"""
DEX KPI Processor - Domain 3
Processes DEX trading data with market sentiment analysis
"""

import pandas as pd
from pathlib import Path
from datetime import datetime
from typing import Dict, Optional
from utils.logger import get_logger

logger = get_logger(__name__)


class DexKPIProcessor:
    """Process DEX trading data to calculate volume and sentiment KPIs"""

    def __init__(self, output_dir: str = 'data/kpi'):
        """Initialize processor"""
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        logger.info("DexKPIProcessor initialized")

    def process_all(self, df: pd.DataFrame) -> Dict:
        """
        Process DEX data with market sentiment analysis

        Args:
            df: DataFrame from DEX query with buy/sell metrics

        Returns:
            Dictionary with DEX KPIs and sentiment metrics
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
            'weekly_volume': self._kpi2_weekly_volume(df),
            'token_breakdown': self._kpi3_token_breakdown(df),
            'wow_change': self._kpi4_wow_change(df),
            'chain_distribution': self._kpi5_chain_distribution(df),
            'market_sentiment': self._kpi6_market_sentiment(df),
            'sentiment_summary': self._kpi7_sentiment_summary(df)
        }

        logger.info("✅ DEX processing complete")
        return results

    def _clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean and prepare DEX data

        Args:
            df: Raw DEX DataFrame

        Returns:
            Cleaned DataFrame
        """
        df = df.copy()

        # Ensure required columns exist
        required_columns = [
            'blockchain', 'symbol', 'date', 'trade_count',
            'total_volume_usd', 'avg_trade_size_usd', 'min_trade_usd', 'max_trade_usd',
            'buy_volume_usd', 'sell_volume_usd', 'buy_count', 'sell_count',
            'net_buy_pressure_usd', 'buy_pressure_pct', 'unique_dex_count'
        ]

        missing = [col for col in required_columns if col not in df.columns]
        if missing:
            logger.warning(f"Missing columns: {missing}")
            logger.warning("Make sure econ_dex_query.sql has been updated with sentiment columns")

        # Convert date column
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'], errors='coerce')
            df['week'] = df['date'].dt.to_period('W').apply(lambda x: f"{x.year}W{x.week:02d}")

        # Convert numeric columns
        numeric_columns = [
            'trade_count', 'total_volume_usd', 'avg_trade_size_usd',
            'min_trade_usd', 'max_trade_usd', 'buy_volume_usd', 'sell_volume_usd',
            'buy_count', 'sell_count', 'net_buy_pressure_usd',
            'buy_pressure_pct', 'unique_dex_count'
        ]

        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        # Fill NaN with 0 for volume metrics
        df = df.fillna(0)
        df = df.infer_objects(copy=False)

        # Remove rows with missing critical data
        df = df.dropna(subset=['date', 'blockchain', 'symbol'])

        return df

    def _kpi1_daily_volume(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        KPI 3.1 & 3.2: Daily DEX volume and trade count

        Returns:
            DataFrame with daily volume metrics
        """
        kpi = df.groupby(['date', 'blockchain', 'symbol']).agg({
            'total_volume_usd': 'sum',
            'trade_count': 'sum',
            'buy_volume_usd': 'sum',
            'sell_volume_usd': 'sum',
            'buy_count': 'sum',
            'sell_count': 'sum',
            'unique_dex_count': 'max'
        }).reset_index()

        return kpi.sort_values('date', ascending=False)

    def _kpi2_weekly_volume(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        KPI 3.1 & 3.2: Weekly aggregated volume

        Returns:
            DataFrame with weekly volume metrics
        """
        kpi = df.groupby(['week', 'blockchain', 'symbol']).agg({
            'total_volume_usd': 'sum',
            'trade_count': 'sum',
            'buy_volume_usd': 'sum',
            'sell_volume_usd': 'sum',
            'buy_count': 'sum',
            'sell_count': 'sum',
            'net_buy_pressure_usd': 'sum',
            'unique_dex_count': 'max'
        }).reset_index()

        # Calculate weekly average buy pressure
        kpi['avg_buy_pressure_pct'] = (
            (kpi['buy_volume_usd'] / (kpi['total_volume_usd'] + 1)) * 100
        ).round(2)

        return kpi.sort_values('week', ascending=False)

    def _kpi3_token_breakdown(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        KPI 3.3 & 3.5: Volume breakdown by token with market share

        Returns:
            DataFrame with token-level metrics
        """
        latest_week = df['week'].max()
        weekly = df[df['week'] == latest_week]

        kpi = weekly.groupby('symbol').agg({
            'total_volume_usd': 'sum',
            'trade_count': 'sum',
            'buy_volume_usd': 'sum',
            'sell_volume_usd': 'sum'
        }).reset_index()

        # Calculate market share
        total_volume = kpi['total_volume_usd'].sum()
        kpi['market_share_pct'] = (
            (kpi['total_volume_usd'] / (total_volume + 1)) * 100
        ).round(2)

        # Calculate buy/sell ratio
        kpi['buy_sell_ratio'] = (
            kpi['buy_volume_usd'] / (kpi['sell_volume_usd'] + 1)
        ).round(2)

        kpi['week'] = latest_week

        return kpi.sort_values('total_volume_usd', ascending=False)

    def _kpi4_wow_change(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        KPI 3.7: Week-over-week volume change

        Returns:
            DataFrame with WoW change metrics
        """
        weekly = self._kpi2_weekly_volume(df)

        # Calculate WoW for volume
        weekly = weekly.sort_values(['blockchain', 'symbol', 'week'])
        weekly['volume_prev_week'] = weekly.groupby(['blockchain', 'symbol'])['total_volume_usd'].shift(1)
        weekly['volume_wow_pct'] = (
            (weekly['total_volume_usd'] - weekly['volume_prev_week']) /
            (weekly['volume_prev_week'] + 1) * 100
        ).round(2)

        # Calculate WoW for trades
        weekly['trades_prev_week'] = weekly.groupby(['blockchain', 'symbol'])['trade_count'].shift(1)
        weekly['trades_wow_pct'] = (
            (weekly['trade_count'] - weekly['trades_prev_week']) /
            (weekly['trades_prev_week'] + 1) * 100
        ).round(2)

        # Calculate absolute change
        weekly['volume_wow_abs'] = (weekly['total_volume_usd'] - weekly['volume_prev_week']).round(2)

        return weekly[[
            'week', 'blockchain', 'symbol',
            'total_volume_usd', 'volume_prev_week', 'volume_wow_pct', 'volume_wow_abs',
            'trade_count', 'trades_prev_week', 'trades_wow_pct'
        ]].sort_values('week', ascending=False)

    def _kpi5_chain_distribution(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        KPI 3.4 & 3.6: Volume distribution by chain

        Returns:
            DataFrame with chain-level metrics
        """
        latest_week = df['week'].max()
        weekly = df[df['week'] == latest_week]

        kpi = weekly.groupby(['blockchain', 'symbol']).agg({
            'total_volume_usd': 'sum',
            'trade_count': 'sum',
            'unique_dex_count': 'max'
        }).reset_index()

        # Calculate chain share (per symbol)
        kpi['chain_share_pct'] = kpi.groupby('symbol')['total_volume_usd'].transform(
            lambda x: (x / (x.sum() + 1)) * 100
        ).round(2)

        # Overall chain share
        total_volume = kpi['total_volume_usd'].sum()
        kpi['overall_chain_share_pct'] = (
            (kpi['total_volume_usd'] / (total_volume + 1)) * 100
        ).round(2)

        kpi['week'] = latest_week

        return kpi.sort_values('total_volume_usd', ascending=False)

    def _kpi6_market_sentiment(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        NEW KPI: Daily market sentiment analysis with buy pressure classification

        Returns:
            DataFrame with sentiment metrics
        """
        kpi = df.groupby(['date', 'blockchain', 'symbol']).agg({
            'buy_pressure_pct': 'mean',
            'net_buy_pressure_usd': 'sum',
            'buy_volume_usd': 'sum',
            'sell_volume_usd': 'sum',
            'total_volume_usd': 'sum'
        }).reset_index()

        # Classify sentiment based on buy pressure
        def classify_sentiment(pct):
            if pct >= 75:
                return 'STRONG_BULLISH'
            elif pct >= 55:
                return 'BULLISH'
            elif pct >= 45:
                return 'NEUTRAL'
            elif pct >= 25:
                return 'BEARISH'
            else:
                return 'STRONG_BEARISH'

        kpi['sentiment'] = kpi['buy_pressure_pct'].apply(classify_sentiment)

        # Calculate sentiment strength (distance from 50%)
        kpi['sentiment_strength'] = abs(kpi['buy_pressure_pct'] - 50).round(2)

        return kpi.sort_values('date', ascending=False)

    def _kpi7_sentiment_summary(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        NEW KPI: Weekly sentiment summary by token

        Returns:
            DataFrame with aggregated sentiment metrics
        """
        latest_week = df['week'].max()
        weekly = df[df['week'] == latest_week]

        kpi = weekly.groupby(['blockchain', 'symbol']).agg({
            'buy_pressure_pct': 'mean',
            'net_buy_pressure_usd': 'sum',
            'total_volume_usd': 'sum',
            'trade_count': 'sum'
        }).reset_index()

        # Round buy pressure
        kpi['avg_buy_pressure_pct'] = kpi['buy_pressure_pct'].round(2)
        kpi = kpi.drop('buy_pressure_pct', axis=1)

        # Classify overall sentiment
        def classify_sentiment(pct):
            if pct >= 60:
                return 'ACCUMULATION'
            elif pct >= 40:
                return 'BALANCED'
            else:
                return 'DISTRIBUTION'

        kpi['sentiment_classification'] = kpi['avg_buy_pressure_pct'].apply(classify_sentiment)

        # Calculate net flow indicator
        kpi['net_flow_indicator'] = (
            kpi['net_buy_pressure_usd'] / (kpi['total_volume_usd'] + 1) * 100
        ).round(2)

        kpi['week'] = latest_week

        return kpi.sort_values('total_volume_usd', ascending=False)

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
        week = "2026W04"  # Placeholder - extract from actual data in production

        # Export each KPI
        kpi_mapping = {
            'daily_volume': 'dex_kpi1_daily_volume',
            'weekly_volume': 'dex_kpi2_weekly_volume',
            'token_breakdown': 'dex_kpi3_token_breakdown',
            'wow_change': 'dex_kpi4_wow_change',
            'chain_distribution': 'dex_kpi5_chain_distribution',
            'market_sentiment': 'dex_kpi6_market_sentiment',
            'sentiment_summary': 'dex_kpi7_sentiment_summary'
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
            results: Dictionary with DEX KPI DataFrames

        Returns:
            Dictionary with summary statistics
        """
        summary = {
            'total_volume_usd': 0,
            'total_trades': 0,
            'unique_tokens': 0,
            'unique_blockchains': 0,
            'avg_buy_pressure_pct': 0,
            'net_buy_pressure_usd': 0,
            'bullish_tokens': 0,
            'bearish_tokens': 0,
            'neutral_tokens': 0,
            'most_active_token': None,
            'most_active_blockchain': None
        }

        if 'sentiment_summary' in results and results['sentiment_summary'] is not None:
            df = results['sentiment_summary']

            summary['total_volume_usd'] = df['total_volume_usd'].sum()
            summary['total_trades'] = df['trade_count'].sum()
            summary['unique_tokens'] = df['symbol'].nunique()
            summary['unique_blockchains'] = df['blockchain'].nunique()
            summary['avg_buy_pressure_pct'] = df['avg_buy_pressure_pct'].mean()
            summary['net_buy_pressure_usd'] = df['net_buy_pressure_usd'].sum()

            # Count sentiment classifications
            summary['bullish_tokens'] = (df['sentiment_classification'] == 'ACCUMULATION').sum()
            summary['bearish_tokens'] = (df['sentiment_classification'] == 'DISTRIBUTION').sum()
            summary['neutral_tokens'] = (df['sentiment_classification'] == 'BALANCED').sum()

            # Most active
            if not df.empty:
                most_active = df.nlargest(1, 'total_volume_usd').iloc[0]
                summary['most_active_token'] = most_active['symbol']
                summary['most_active_blockchain'] = most_active['blockchain']

        return summary
