"""
LATAM Stablecoins Economic Analysis Pipeline
Orchestrates extraction, processing, and reporting across all domains
01/23/26

Pipeline Flow:
  1. Extract data from Dune (flows, DEX trades)
  2. Process Domain 1: Supply (mint/burn verification)
  3. Process Domain 3: DEX (market sentiment analysis)
  4. Generate consolidated report
  5. Export all KPIs and summary metrics
"""

import argparse
import sys
from pathlib import Path
from datetime import datetime
from typing import Dict, Optional

import pandas as pd

from extractors.data_extractor import DuneDataExtractor
from processors.supply_processor import SupplyKPIProcessor
from processors.dex_processor import DexKPIProcessor
from utils.logger import get_logger

logger = get_logger(__name__)


class LATAMEconomicPipeline:
    """Orchestrate full pipeline from extraction to reporting"""

    def __init__(self, config_path: str = 'config/config.yaml', use_cached: bool = False):
        """
        Initialize pipeline with all processors

        Args:
            config_path: Path to config.yaml
            use_cached: Use cached Dune results instead of fresh execution
        """
        self.config_path = config_path
        self.use_cached = use_cached

        self.extractor = DuneDataExtractor(config_path)
        self.supply_processor = SupplyKPIProcessor()
        self.dex_processor = DexKPIProcessor()

        self.timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
        self.execution_log = []

        logger.info("=" * 80)
        logger.info("🚀 LATAM Stablecoins Economic Analysis Pipeline")
        logger.info("=" * 80)

    def run(self) -> Dict:
        """
        Execute full pipeline

        Returns:
            Dictionary with all results and metadata
        """
        results = {
            'timestamp': self.timestamp,
            'status': 'SUCCESS',
            'extraction': {},
            'supply_kpis': {},
            'dex_kpis': {},
            'consolidated_summary': {},
            'exported_files': {},
            'errors': []
        }

        try:
            # ===================================================================
            # PHASE 1: DATA EXTRACTION
            # ===================================================================
            logger.info("")
            logger.info("=" * 80)
            logger.info("PHASE 1: DATA EXTRACTION")
            logger.info("=" * 80)

            extraction_result = self._extract_data()
            results['extraction'] = extraction_result

            if not extraction_result['success']:
                logger.error("✗ Extraction failed")
                results['status'] = 'FAILED'
                return results

            flows_df = extraction_result['flows']
            dex_df = extraction_result['dex']

            # ===================================================================
            # PHASE 2: SUPPLY DOMAIN PROCESSING (Domain 1)
            # ===================================================================
            logger.info("")
            logger.info("=" * 80)
            logger.info("PHASE 2: SUPPLY DOMAIN (mint/burn verification)")
            logger.info("=" * 80)

            supply_result = self._process_supply(flows_df)
            results['supply_kpis'] = supply_result

            if not supply_result['success']:
                logger.warning("⚠ Supply processing had issues")
                results['errors'].append("Supply processing incomplete")

            # ===================================================================
            # PHASE 3: DEX DOMAIN PROCESSING (Domain 3)
            # ===================================================================
            logger.info("")
            logger.info("=" * 80)
            logger.info("PHASE 3: DEX DOMAIN (market sentiment analysis)")
            logger.info("=" * 80)

            dex_result = self._process_dex(dex_df)
            results['dex_kpis'] = dex_result

            if not dex_result['success']:
                logger.warning("⚠ DEX processing had issues")
                results['errors'].append("DEX processing incomplete")

            # ===================================================================
            # PHASE 4: CONSOLIDATED REPORTING
            # ===================================================================
            logger.info("")
            logger.info("=" * 80)
            logger.info("PHASE 4: CONSOLIDATED REPORTING")
            logger.info("=" * 80)

            consolidated = self._generate_consolidated_report(
                supply_result, dex_result
            )
            results['consolidated_summary'] = consolidated

            # ===================================================================
            # PHASE 5: FINAL EXPORT & SUMMARY
            # ===================================================================
            logger.info("")
            logger.info("=" * 80)
            logger.info("PHASE 5: FINAL EXPORT & SUMMARY")
            logger.info("=" * 80)

            exported_files = self._export_final_files(results)
            results['exported_files'] = exported_files

            # Print execution summary
            self._print_execution_summary(results)

            logger.info("")
            logger.info("=" * 80)
            logger.info("✅ PIPELINE COMPLETE")
            logger.info("=" * 80)

            return results

        except Exception as e:
            logger.error(f"✗ Pipeline failed: {e}")
            results['status'] = 'FAILED'
            results['errors'].append(str(e))
            return results

    # =========================================================================
    # PHASE 1: EXTRACTION
    # =========================================================================

    def _extract_data(self) -> Dict:
        """
        Extract data from Dune Analytics

        Returns:
            Dictionary with extraction status and DataFrames
        """
        logger.info("Extracting data from Dune Analytics...")
        logger.info(f"Strategy: {'CACHED' if self.use_cached else 'FRESH + FALLBACK'}")

        try:
            # Run extraction with smart strategy
            raw_data = self.extractor.extract_all(use_cached=self.use_cached)

            flows_df = raw_data.get('flows')
            dex_df = raw_data.get('dex')

            # Validate extraction
            success = flows_df is not None and dex_df is not None

            if success:
                logger.info("")
                logger.info("✅ EXTRACTION SUCCESSFUL")
                logger.info(f"   Flows data: {len(flows_df):,} rows, {len(flows_df.columns)} columns")
                logger.info(f"   DEX data: {len(dex_df):,} rows, {len(dex_df.columns)} columns")
            else:
                logger.error("❌ Missing required data")
                if flows_df is None:
                    logger.error("   - Flows data: MISSING")
                if dex_df is None:
                    logger.error("   - DEX data: MISSING")

            return {
                'success': success,
                'flows': flows_df,
                'dex': dex_df,
                'flows_rows': len(flows_df) if flows_df is not None else 0,
                'dex_rows': len(dex_df) if dex_df is not None else 0
            }

        except Exception as e:
            logger.error(f"✗ Extraction failed: {e}")
            return {
                'success': False,
                'flows': None,
                'dex': None,
                'error': str(e)
            }

    # =========================================================================
    # PHASE 2: SUPPLY PROCESSING
    # =========================================================================

    def _process_supply(self, flows_df: Optional[pd.DataFrame]) -> Dict:
        """
        Process supply domain with mint/burn verification

        Args:
            flows_df: Raw flows data from Dune

        Returns:
            Dictionary with supply KPIs and export info
        """
        if flows_df is None or flows_df.empty:
            logger.error("Cannot process supply: No flows data")
            return {
                'success': False,
                'error': 'No flows data provided',
                'kpis': {}
            }

        try:
            logger.info("Processing supply domain...")

            # Validate mint/burn data
            validation = self.extractor.validate_supply_data(flows_df)

            # Process supply KPIs
            supply_results = self.supply_processor.process_all(flows_df)

            # Export KPIs
            exported_files = self.supply_processor.export_kpis(supply_results, self.timestamp)

            # Generate summary
            summary = self.supply_processor.generate_summary(supply_results)

            logger.info("")
            logger.info("✅ SUPPLY PROCESSING COMPLETE")
            logger.info(f"   Total mints: ${summary['total_mints_usd']:,.2f}")
            logger.info(f"   Total burns: ${summary['total_burns_usd']:,.2f}")
            logger.info(f"   Net supply: ${summary['net_supply_change_usd']:,.2f}")
            logger.info(f"   Tokens in expansion: {summary['tokens_in_expansion']}")
            logger.info(f"   Tokens in contraction: {summary['tokens_in_contraction']}")
            logger.info(f"   Files exported: {len(exported_files)}")

            return {
                'success': True,
                'validation': validation,
                'kpis': supply_results,
                'summary': summary,
                'exported_files': exported_files
            }

        except Exception as e:
            logger.error(f"✗ Supply processing failed: {e}")
            return {
                'success': False,
                'error': str(e),
                'kpis': {}
            }

    # =========================================================================
    # PHASE 3: DEX PROCESSING
    # =========================================================================

    def _process_dex(self, dex_df: Optional[pd.DataFrame]) -> Dict:
        """
        Process DEX domain with market sentiment analysis

        Args:
            dex_df: Raw DEX data from Dune

        Returns:
            Dictionary with DEX KPIs and export info
        """
        if dex_df is None or dex_df.empty:
            logger.error("Cannot process DEX: No DEX data")
            return {
                'success': False,
                'error': 'No DEX data provided',
                'kpis': {}
            }

        try:
            logger.info("Processing DEX domain with market sentiment analysis...")

            # Process DEX KPIs
            dex_results = self.dex_processor.process_all(dex_df)

            # Export KPIs
            exported_files = self.dex_processor.export_kpis(dex_results, self.timestamp)

            # Generate summary
            summary = self.dex_processor.generate_summary(dex_results)

            logger.info("")
            logger.info("✅ DEX PROCESSING COMPLETE")
            logger.info(f"   Total volume: ${summary['total_volume_usd']:,.2f}")
            logger.info(f"   Total trades: {summary['total_trades']:,}")
            logger.info(f"   Unique tokens: {summary['unique_tokens']}")
            logger.info(f"   Unique blockchains: {summary['unique_blockchains']}")
            logger.info(f"   Avg buy pressure: {summary['avg_buy_pressure_pct']:.2f}%")
            logger.info(f"   Bullish tokens: {summary['bullish_tokens']}")
            logger.info(f"   Bearish tokens: {summary['bearish_tokens']}")
            logger.info(f"   Neutral tokens: {summary['neutral_tokens']}")
            if summary['most_active_token']:
                logger.info(f"   Most active: {summary['most_active_token']} ({summary['most_active_blockchain']})")
            logger.info(f"   Files exported: {len(exported_files)}")

            return {
                'success': True,
                'kpis': dex_results,
                'summary': summary,
                'exported_files': exported_files
            }

        except Exception as e:
            logger.error(f"✗ DEX processing failed: {e}")
            return {
                'success': False,
                'error': str(e),
                'kpis': {}
            }

    # =========================================================================
    # PHASE 4: CONSOLIDATED REPORTING
    # =========================================================================

    def _generate_consolidated_report(self, supply_result: Dict, dex_result: Dict) -> Dict:
        """
        Generate consolidated report across all domains

        Args:
            supply_result: Supply processing results
            dex_result: DEX processing results

        Returns:
            Consolidated summary dictionary
        """
        logger.info("Generating consolidated report...")

        consolidated = {
            'supply_summary': supply_result.get('summary', {}),
            'dex_summary': supply_result.get('summary', {}),
            'cross_domain_insights': {}
        }

        # Generate cross-domain insights
        if supply_result['success'] and dex_result['success']:
            supply_sum = supply_result.get('summary', {})
            dex_sum = dex_result.get('summary', {})

            consolidated['cross_domain_insights'] = {
                'total_ecosystem_volume': (
                        supply_sum.get('total_mints_usd', 0) +
                        dex_sum.get('total_volume_usd', 0)
                ),
                'net_supply_vs_trading': {
                    'net_supply': supply_sum.get('net_supply_change_usd', 0),
                    'trading_volume': dex_sum.get('total_volume_usd', 0),
                    'ratio': (
                            supply_sum.get('net_supply_change_usd', 0) /
                            (dex_sum.get('total_volume_usd', 1))
                    )
                },
                'market_health': {
                    'tokens_tracked': supply_sum.get('total_tokens_tracked', 0),
                    'avg_inflation_rate': supply_sum.get('average_inflation_rate', 0),
                    'avg_buy_pressure': dex_sum.get('avg_buy_pressure_pct', 0)
                }
            }

            logger.info("")
            logger.info("📊 CROSS-DOMAIN INSIGHTS")
            logger.info(
                f"   Total ecosystem volume: ${consolidated['cross_domain_insights']['total_ecosystem_volume']:,.2f}")
            logger.info(
                f"   Avg inflation rate: {consolidated['cross_domain_insights']['market_health']['avg_inflation_rate']:.2f}%")
            logger.info(
                f"   Avg buy pressure: {consolidated['cross_domain_insights']['market_health']['avg_buy_pressure']:.2f}%")

        return consolidated

    # =========================================================================
    # PHASE 5: EXPORT & SUMMARY
    # =========================================================================

    def _export_final_files(self, results: Dict) -> Dict:
        """
        Export final consolidated files and metadata

        Args:
            results: Pipeline results dictionary

        Returns:
            Dictionary with exported file paths
        """
        exported = {}

        # Collect all exported files
        if results['supply_kpis'].get('success'):
            exported['supply'] = results['supply_kpis'].get('exported_files', {})

        if results['dex_kpis'].get('success'):
            exported['dex'] = results['dex_kpis'].get('exported_files', {})

        # Export metadata
        metadata_file = self._export_metadata(results)
        exported['metadata'] = metadata_file

        # Export execution log
        log_file = self._export_execution_log()
        exported['log'] = log_file

        return exported

    def _export_metadata(self, results: Dict) -> Path:
        """Export pipeline metadata to JSON"""
        import json

        metadata = {
            'timestamp': results['timestamp'],
            'status': results['status'],
            'extraction': {
                'flows_rows': results['extraction'].get('flows_rows', 0),
                'dex_rows': results['extraction'].get('dex_rows', 0)
            },
            'supply': {
                'status': 'SUCCESS' if results['supply_kpis'].get('success') else 'FAILED',
                'summary': results['supply_kpis'].get('summary', {})
            },
            'dex': {
                'status': 'SUCCESS' if results['dex_kpis'].get('success') else 'FAILED',
                'summary': results['dex_kpis'].get('summary', {})
            },
            'consolidated': results['consolidated_summary']
        }

        metadata_dir = Path('data/metadata')
        metadata_dir.mkdir(parents=True, exist_ok=True)
        metadata_file = metadata_dir / f"pipeline_metadata_{results['timestamp']}.json"

        with open(metadata_file, 'w') as f:
            json.dump(metadata, f, indent=2, default=str)

        logger.info(f"✓ Exported metadata: {metadata_file}")
        return metadata_file

    def _export_execution_log(self) -> Path:
        """Export execution log"""
        log_dir = Path('logs')
        log_dir.mkdir(parents=True, exist_ok=True)
        log_file = log_dir / f"pipeline_execution_{self.timestamp}.log"

        logger.info(f"✓ Execution log saved: {log_file}")
        return log_file

    def _print_execution_summary(self, results: Dict):
        """Print final execution summary"""
        logger.info("")
        logger.info("=" * 80)
        logger.info("📋 EXECUTION SUMMARY")
        logger.info("=" * 80)

        # Extraction summary
        logger.info("")
        logger.info("1️⃣  EXTRACTION")
        if results['extraction'].get('success'):
            logger.info(f"   Status: ✅ SUCCESS")
            logger.info(f"   Flows: {results['extraction']['flows_rows']:,} rows")
            logger.info(f"   DEX: {results['extraction']['dex_rows']:,} rows")
        else:
            logger.info(f"   Status: ❌ FAILED - {results['extraction'].get('error', 'Unknown error')}")

        # Supply processing summary
        logger.info("")
        logger.info("2️⃣  SUPPLY DOMAIN")
        if results['supply_kpis'].get('success'):
            logger.info(f"   Status: ✅ SUCCESS")
            summary = results['supply_kpis'].get('summary', {})
            logger.info(f"   Mints: ${summary.get('total_mints_usd', 0):,.2f}")
            logger.info(f"   Burns: ${summary.get('total_burns_usd', 0):,.2f}")
            logger.info(f"   Net: ${summary.get('net_supply_change_usd', 0):,.2f}")
            logger.info(f"   Files: {len(results['supply_kpis'].get('exported_files', {}))}")
        else:
            logger.info(f"   Status: ❌ FAILED - {results['supply_kpis'].get('error', 'Unknown error')}")

        # DEX processing summary
        logger.info("")
        logger.info("3️⃣  DEX DOMAIN")
        if results['dex_kpis'].get('success'):
            logger.info(f"   Status: ✅ SUCCESS")
            summary = results['dex_kpis'].get('summary', {})
            logger.info(f"   Volume: ${summary.get('total_volume_usd', 0):,.2f}")
            logger.info(f"   Trades: {summary.get('total_trades', 0):,}")
            logger.info(f"   Buy Pressure: {summary.get('avg_buy_pressure_pct', 0):.2f}%")
            logger.info(f"   Files: {len(results['dex_kpis'].get('exported_files', {}))}")
        else:
            logger.info(f"   Status: ❌ FAILED - {results['dex_kpis'].get('error', 'Unknown error')}")

        # Overall status
        logger.info("")
        logger.info("=" * 80)
        if results['status'] == 'SUCCESS':
            logger.info("✅ PIPELINE EXECUTION: SUCCESS")
        else:
            logger.info("❌ PIPELINE EXECUTION: FAILED")
            if results['errors']:
                logger.info("Errors:")
                for error in results['errors']:
                    logger.info(f"   - {error}")

        logger.info("=" * 80)


def main():
    """Entry point for pipeline execution"""
    parser = argparse.ArgumentParser(
        description='LATAM Stablecoins Economic Analysis Pipeline'
    )
    parser.add_argument(
        '--use-cached',
        action='store_true',
        help='Use cached Dune results instead of fresh execution'
    )
    parser.add_argument(
        '--config',
        type=str,
        default='config/config.yaml',
        help='Path to config file'
    )

    args = parser.parse_args()

    try:
        # Initialize and run pipeline
        pipeline = LATAMEconomicPipeline(
            config_path=args.config,
            use_cached=args.use_cached
        )
        results = pipeline.run()

        # Exit with appropriate code
        sys.exit(0 if results['status'] == 'SUCCESS' else 1)

    except Exception as e:
        logger.error(f"✗ Fatal error: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
