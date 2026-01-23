"""
Dune Data Extractor with smart execution/cached fallback
"""

import os
import pandas as pd
import yaml
from pathlib import Path
from datetime import datetime
from dune_client.client import DuneClient
from dune_client.query import QueryBase
from utils.logger import get_logger

logger = get_logger(__name__)


class DuneDataExtractor:
    """Extract data from Dune Analytics with smart execution strategy"""

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

    def extract_all(self, use_cached: bool = False):
        """
        Extract all queries

        Args:
            use_cached: Force using cached results for all queries

        Returns:
            Dictionary with 'flows' and 'dex' DataFrames
        """
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S')

        # Extract flows data
        flows_query_id = self.config['dune']['query_ids']['flows']
        try:
            flows_df = self.fetch_query('flows', flows_query_id, use_cached=use_cached)
            self.save_raw_data(flows_df, 'flows', timestamp)
        except Exception as e:
            logger.error(f"✗ Failed to fetch flows data: {e}")
            flows_df = None

        # Extract DEX data
        dex_query_id = self.config['dune']['query_ids']['dex']
        try:
            dex_df = self.fetch_query('dex', dex_query_id, use_cached=use_cached)
            self.save_raw_data(dex_df, 'dex', timestamp)
        except Exception as e:
            logger.error(f"✗ Failed to fetch DEX data: {e}")
            dex_df = None

        return {
            'flows': flows_df,
            'dex': dex_df
        }
