"""
Extract data from Dune Analytics
"""

import pandas as pd
from pathlib import Path
from datetime import datetime
from typing import Dict, Optional
from dune_client.client import DuneClient
from dune_client.query import QueryBase
import yaml
from utils.logger import setup_logger

logger = setup_logger(__name__)


class DuneDataExtractor:
    """Extract data from Dune Analytics and save to raw files"""

    def __init__(self, config_path: str = 'config.yaml'):
        """Initialize extractor with config"""
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)

        self.api_key = self.config['dune']['api_key']
        self.queries = self.config['dune']['queries']
        self.raw_dir = Path(self.config['paths']['raw_data'])
        self.raw_dir.mkdir(parents=True, exist_ok=True)

        self.client = DuneClient(self.api_key)
        self.timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

    def fetch_query(self, query_name: str, query_id: int) -> Optional[pd.DataFrame]:
        """
        Execute query on Dune and fetch fresh results

        Args:
            query_name: Name of the query (for logging/saving)
            query_id: Dune query ID

        Returns:
            DataFrame with results or None if error
        """
        logger.info(f"🔄 Executing {query_name} (Query ID: {query_id})...")

        try:
            # Create query object (required by dune_client API)
            query = QueryBase(query_id=query_id)

            # Execute query and get DataFrame directly
            df = self.client.run_query_dataframe(query)

            logger.info(f"✓ {query_name}: {len(df):,} rows, {len(df.columns)} columns")
            return df

        except Exception as e:
            logger.error(f"✗ Failed to execute {query_name}: {e}")
            logger.exception("Full traceback:")
            return None

    def save_raw_data(self, df: pd.DataFrame, query_name: str) -> Path:
        """
        Save raw data to CSV

        Args:
            df: DataFrame to save
            query_name: Name for the file

        Returns:
            Path to saved file
        """
        filename = f"{query_name}_raw_{self.timestamp}.csv"
        filepath = self.raw_dir / filename
        df.to_csv(filepath, index=False)
        logger.info(f"✓ Saved raw data: {filepath}")
        return filepath

    def extract_all(self) -> Dict[str, pd.DataFrame]:
        """
        Extract all configured queries

        Returns:
            Dictionary of {query_name: DataFrame}
        """
        logger.info("=" * 80)
        logger.info("EXECUTING ALL DUNE QUERIES (FRESH DATA)")
        logger.info("=" * 80)

        results = {}

        for query_name, query_id in self.queries.items():
            df = self.fetch_query(query_name, query_id)
            if df is not None:
                self.save_raw_data(df, query_name)
                results[query_name] = df

        logger.info(f"\n✅ Extraction complete: {len(results)}/{len(self.queries)} queries successful")
        return results


def main():
    """Standalone execution"""
    extractor = DuneDataExtractor()
    extractor.extract_all()


if __name__ == "__main__":
    main()
