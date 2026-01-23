"""
Dune Data Extractor with smart execution/cached fallback + Mint/Burn Validation
"""

import os
import pandas as pd
import yaml
from pathlib import Path
from datetime import datetime
from typing import Dict, Optional
from dune_client.client import DuneClient
from dune_client.query import QueryBase
from utils.logger import get_logger

logger = get_logger(__name__)


class DuneDataExtractor:
    """Extract data from Dune Analytics with smart execution strategy and data validation"""

    def __init__(self, config_path='config/config.yaml'):
        """Initialize Dune client"""
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)

        api_key = os.getenv('DUNE_API_KEY')
        if not api_key:
            raise EnvironmentError("DUNE_API_KEY environment variable not set")

        self.client = DuneClient(api_key)
        self.raw_data_dir = Path(self.config['output']['raw_data_dir'])
        self.raw_data_dir.mkdir(parents=True, exist_ok=True)

        logger.info("DuneDataExtractor initialized")

    def fetch_query(self, query_name: str, query_id: int, use_cached: bool = False) -> pd.DataFrame:
        """
        Fetch query results from Dune with smart strategy

        Strategy:
        1. Try fresh execution (if use_cached=False)
        2. If execution fails (401/402), fallback to cached results
        3. If cached fails, raise error

        Args:
            query_name: Name of query for logging
            query_id: Dune query ID
            use_cached: Force using cached results (skip execution)

        Returns:
            DataFrame with query results
        """
        logger.info(f"Fetching {query_name} (Query ID: {query_id})...")

        # If use_cached is forced, skip execution
        if use_cached:
            return self._fetch_cached(query_name, query_id)

        # Try fresh execution first
        try:
            logger.debug(f"Attempting fresh execution for {query_name}...")
            query = QueryBase(query_id=query_id)
            df = self.client.run_query_dataframe(query)
            logger.info(f"✓ {query_name}: Fresh execution - {len(df)} rows, {len(df.columns)} columns")
            return df

        except Exception as e:
            error_str = str(e)

            # Check if it's an execution limit error (401/402)
            if '401' in error_str or '402' in error_str or 'Unauthorized' in error_str or 'Payment' in error_str:
                logger.warning(f"⚠ {query_name}: Execution not available ({error_str})")
                logger.info(f"→ Falling back to cached results for {query_name}...")
                return self._fetch_cached(query_name, query_id)
            else:
                # Other error, raise it
                logger.error(f"✗ Failed to fetch {query_name}: {e}")
                raise

    def _fetch_cached(self, query_name: str, query_id: int) -> pd.DataFrame:
        """
        Fetch cached query results (doesn't execute)

        Args:
            query_name: Name of query for logging
            query_id: Dune query ID

        Returns:
            DataFrame with cached results
        """
        try:
            logger.debug(f"Fetching cached results for {query_name}...")
            result = self.client.get_latest_result(query_id)

            if result and result.result and result.result.rows:
                df = pd.DataFrame(result.result.rows)
                logger.info(f"✓ {query_name}: Cached results - {len(df)} rows, {len(df.columns)} columns")
                return df
            else:
                error_msg = f"No cached results found for {query_name}. Run query manually on Dune first: https://dune.com/queries/{query_id}"
                logger.error(error_msg)
                raise ValueError(error_msg)

        except Exception as e:
            logger.error(f"✗ Failed to fetch cached {query_name}: {e}")
            raise

    def save_raw_data(self, df: pd.DataFrame, query_name: str, timestamp: str):
        """Save raw data to CSV"""
        filename = self.raw_data_dir / f"{query_name}_raw_{timestamp}.csv"
        df.to_csv(filename, index=False)
        logger.info(f"✓ Saved raw data: {filename}")
        return filename

    def validate_supply_data(self, flows_df: pd.DataFrame) -> Dict:
        """
        Validate mint/burn data from flows query.

        Spot-checks the new mint/burn columns to ensure:
        - Mint and burn events are detected
        - Data quality is reasonable
        - No obvious anomalies

        Args:
            flows_df: DataFrame from flows query (should have mint_count, mint_volume_usd, burn_volume_usd)

        Returns:
            Dictionary with validation metrics
        """
        # Check if required columns exist
        required_columns = ['mint_volume_usd', 'burn_volume_usd', 'mint_count', 'symbol', 'blockchain']
        missing_columns = [col for col in required_columns if col not in flows_df.columns]

        if missing_columns:
            logger.warning(f"⚠ Missing columns in flows data: {missing_columns}")
            logger.warning("   Make sure your econ_flows_query.sql has been updated with mint/burn columns")
            return {
                'status': 'FAILED',
                'error': f'Missing required columns: {missing_columns}',
                'total_mints': 0,
                'total_burns': 0,
                'net_supply': 0,
                'tokens_with_activity': [],
                'tokens_with_burns': []
            }

        # Calculate validation metrics
        total_mints = flows_df['mint_volume_usd'].sum()
        total_burns = flows_df['burn_volume_usd'].sum()
        net_supply = total_mints - total_burns

        # Find tokens with minting activity
        tokens_with_mints = flows_df[flows_df['mint_count'] > 0]['symbol'].unique().tolist()
        tokens_with_burns = flows_df[flows_df['burn_volume_usd'] > 0]['symbol'].unique().tolist()

        # Count mint/burn events
        total_mint_events = flows_df['mint_count'].sum()
        total_burn_events = (flows_df['burn_volume_usd'] > 0).sum()

        # Identify tokens with zero activity (potential data quality issue)
        all_tokens = flows_df['symbol'].unique().tolist()
        inactive_tokens = [t for t in all_tokens if t not in tokens_with_mints and t not in tokens_with_burns]

        validation = {
            'status': 'SUCCESS',
            'total_mints': round(total_mints, 2),
            'total_burns': round(total_burns, 2),
            'net_supply': round(net_supply, 2),
            'total_mint_events': int(total_mint_events),
            'total_burn_events': int(total_burn_events),
            'tokens_with_mints': tokens_with_mints,
            'tokens_with_burns': tokens_with_burns,
            'all_tokens': all_tokens,
            'inactive_tokens': inactive_tokens,  # Potential data quality issues
            'lookback_period': f"{flows_df['week_start'].min()} to {flows_df['week_start'].max()}" if 'week_start' in flows_df.columns else 'Unknown'
        }

        # Print validation report
        logger.info("=" * 70)
        logger.info("✅ FLOWS DATA VALIDATION REPORT")
        logger.info("=" * 70)
        logger.info(f"Status:                {validation['status']}")
        logger.info(f"Total Mints:           ${validation['total_mints']:,.2f}")
        logger.info(f"Total Burns:           ${validation['total_burns']:,.2f}")
        logger.info(f"Net Supply Change:     ${validation['net_supply']:,.2f}")
        logger.info(f"Total Mint Events:     {validation['total_mint_events']}")
        logger.info(f"Total Burn Events:     {validation['total_burn_events']}")
        logger.info(f"Lookback Period:       {validation['lookback_period']}")
        logger.info("")
        logger.info(f"Tokens with Mint Activity:  {validation['tokens_with_mints']}")
        logger.info(f"Tokens with Burn Activity:  {validation['tokens_with_burns']}")
        logger.info(f"All Tracked Tokens:         {validation['all_tokens']}")

        if validation['inactive_tokens']:
            logger.warning(f"⚠ Tokens with NO mint/burn activity: {validation['inactive_tokens']}")
            logger.warning("  → These tokens may have no minting/burning activity OR data quality issue")

        logger.info("=" * 70)

        return validation

    def extract_all(self, use_cached: bool = False) -> Dict[str, Optional[pd.DataFrame]]:
        """
        Extract all queries and validate flows data

        Args:
            use_cached: Force using cached results for all queries

        Returns:
            Dictionary with 'flows' and 'dex' DataFrames
        """
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S')

        # Extract flows data
        flows_query_id = self.config['dune']['query_ids']['flows']
        try:
            logger.info("=" * 70)
            logger.info("EXECUTING QUERY 1: FLOWS (tokens.transfers)")
            logger.info("=" * 70)
            flows_df = self.fetch_query('flows', flows_query_id, use_cached=use_cached)
            self.save_raw_data(flows_df, 'flows', timestamp)

            # NEW: Validate mint/burn data
            logger.info("")
            self.validate_supply_data(flows_df)

        except Exception as e:
            logger.error(f"✗ Failed to fetch flows data: {e}")
            flows_df = None

        # Extract DEX data
        logger.info("")
        logger.info("=" * 70)
        logger.info("EXECUTING QUERY 2: DEX (dex.trades)")
        logger.info("=" * 70)
        dex_query_id = self.config['dune']['query_ids']['dex']
        try:
            dex_df = self.fetch_query('dex', dex_query_id, use_cached=use_cached)
            self.save_raw_data(dex_df, 'dex', timestamp)
            logger.info(f"✓ DEX data ready: {len(dex_df)} rows")

        except Exception as e:
            logger.error(f"✗ Failed to fetch DEX data: {e}")
            dex_df = None

        logger.info("=" * 70)
        logger.info("EXTRACTION COMPLETE")
        logger.info("=" * 70)

        return {
            'flows': flows_df,
            'dex': dex_df
        }
