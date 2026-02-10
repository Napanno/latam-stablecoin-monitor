"""
LATAM Stablecoins Economic Analysis Pipeline

Orchestrates extraction, processing, and reporting across all domains

Pipeline Flow:
1. Load data from local raw CSV files (data/raw/)
2. Validate data consistency (dates, coverage, quality)
3. Process Domain 1: DEX (market sentiment analysis)
4. Process Domain 2: Flows (mint/burn transaction analysis)
5. Process Domain 3: Supply (mint/burn verification)
6. Generate consolidated JSON report (FIXED)
7. Export all KPIs and summary metrics
"""

import argparse
import sys
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, Optional, Tuple
import pandas as pd
import glob

from processors.supply_processor import SupplyKPIProcessor
from processors.dex_processor import DexKPIProcessor
from processors.flows_processor import FlowsKPIProcessor
from utils.logger import get_logger

logger = get_logger(__name__)


class LATAMEconomicPipeline:
    """Orchestrate full pipeline from extraction to reporting"""

    def __init__(self, config_path: str = 'config/config.yaml'):
        """
        Initialize pipeline with all processors

        Args:
            config_path: Path to config.yaml
        """
        self.config_path = config_path
        self.supply_processor = SupplyKPIProcessor()
        self.dex_processor = DexKPIProcessor()
        self.flows_processor = FlowsKPIProcessor()
        self.timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
        self.execution_log = []

        logger.info("=" * 80)
        logger.info("🚀 LATAM Stablecoins Economic Analysis Pipeline v2.0")
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
            'validation': {},
            'dex_kpis': {},
            'flows_kpis': {},
            'supply_kpis': {},
            'consolidated_summary': {},
            'report': {},
            'exported_files': {},
            'errors': [],
            'warnings': []
        }

        try:
            # ===================================================================
            # PHASE 1: DATA EXTRACTION (FROM LOCAL FILES)
            # ===================================================================
            logger.info("")
            logger.info("=" * 80)
            logger.info("PHASE 1: DATA LOADING (FROM LOCAL CSV FILES)")
            logger.info("=" * 80)

            extraction_result = self._extract_data()
            results['extraction'] = extraction_result

            if not extraction_result['success']:
                logger.error("✗ Data loading failed")
                results['status'] = 'FAILED'
                return results

            flows_df = extraction_result['flows']
            dex_df = extraction_result['dex']

            # ===================================================================
            # PHASE 2: DATA VALIDATION
            # ===================================================================
            logger.info("")
            logger.info("=" * 80)
            logger.info("PHASE 2: DATA VALIDATION & CONSISTENCY CHECKS")
            logger.info("=" * 80)

            validation_result = self._validate_data_consistency(flows_df, dex_df)
            results['validation'] = validation_result

            if not validation_result['passed']:
                logger.error("✗ Data validation failed")
                results['status'] = 'FAILED'
                results['errors'].extend(validation_result.get('errors', []))
                return results

            if validation_result.get('warnings'):
                results['warnings'].extend(validation_result['warnings'])

            # ===================================================================
            # PHASE 3: DEX DOMAIN PROCESSING
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
            # PHASE 4: FLOWS DOMAIN PROCESSING
            # ===================================================================
            logger.info("")
            logger.info("=" * 80)
            logger.info("PHASE 4: FLOWS DOMAIN (mint/burn transaction analysis)")
            logger.info("=" * 80)

            flows_result = self._process_flows(flows_df)
            results['flows_kpis'] = flows_result

            if not flows_result['success']:
                logger.warning("⚠ Flows processing had issues")
                results['errors'].append("Flows processing incomplete")

            # ===================================================================
            # PHASE 5: SUPPLY DOMAIN PROCESSING
            # ===================================================================
            logger.info("")
            logger.info("=" * 80)
            logger.info("PHASE 5: SUPPLY DOMAIN (supply verification)")
            logger.info("=" * 80)

            supply_result = self._process_supply(flows_df)
            results['supply_kpis'] = supply_result

            if not supply_result['success']:
                logger.warning("⚠ Supply processing had issues")
                results['errors'].append("Supply processing incomplete")

            # ===================================================================
            # PHASE 6: JSON REPORT GENERATION (FIXED)
            # ===================================================================
            logger.info("")
            logger.info("=" * 80)
            logger.info("PHASE 6: CONSOLIDATED JSON REPORT GENERATION")
            logger.info("=" * 80)

            report_result = self._generate_json_report(dex_result, flows_result, supply_result)
            results['report'] = report_result

            if not report_result.get('success'):
                logger.warning("⚠ Report generation had issues")
                results['warnings'].append(f"Report generation: {report_result.get('error', 'Unknown')}")

            # Also generate summary for backward compatibility
            consolidated = self._generate_consolidated_report(dex_result, flows_result, supply_result)
            results['consolidated_summary'] = consolidated

            # ===================================================================
            # PHASE 7: FINAL EXPORT & SUMMARY
            # ===================================================================
            logger.info("")
            logger.info("=" * 80)
            logger.info("PHASE 7: FINAL EXPORT & SUMMARY")
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
            logger.error(f"✗ Pipeline failed: {e}", exc_info=True)
            results['status'] = 'FAILED'
            results['errors'].append(str(e))
            return results

    # =========================================================================
    # PHASE 1: DATA EXTRACTION
    # =========================================================================

    def _extract_data(self) -> Dict:
        """
        Load data from local raw CSV files in data/raw/

        Returns:
            Dictionary with extraction status and DataFrames
        """
        logger.info("Loading data from local raw files...")
        logger.info("Source: data/raw/")

        try:
            raw_dir = Path('data/raw')

            # Find latest flows and dex files
            flows_files = sorted(glob.glob(str(raw_dir / 'flows_raw_*.csv')))
            dex_files = sorted(glob.glob(str(raw_dir / 'dex_raw_*.csv')))

            if not flows_files or not dex_files:
                logger.error("Missing raw data files in data/raw/")
                logger.error(f"  Flows files found: {len(flows_files)}")
                logger.error(f"  DEX files found: {len(dex_files)}")
                return {'success': False, 'flows': None, 'dex': None}

            flows_file = flows_files[-1]  # Latest
            dex_file = dex_files[-1]  # Latest

            logger.info(f"Loading flows: {flows_file}")
            logger.info(f"Loading dex: {dex_file}")

            # Load data with error handling
            try:
                flows_df = pd.read_csv(flows_file, on_bad_lines='skip')
                logger.info(f"  Flows loaded with on_bad_lines='skip'")
            except Exception as e:
                logger.warning(f"  Error loading flows: {e}, retrying without skip...")
                flows_df = pd.read_csv(flows_file)

            try:
                dex_df = pd.read_csv(dex_file, on_bad_lines='skip')
                logger.info(f"  DEX loaded with on_bad_lines='skip'")
            except Exception as e:
                logger.warning(f"  Error loading dex: {e}, retrying without skip...")
                dex_df = pd.read_csv(dex_file)

            # Ensure datetime columns
            if 'block_time' in flows_df.columns:
                flows_df['block_time'] = pd.to_datetime(flows_df['block_time'], errors='coerce')
            elif 'week_start' in flows_df.columns:
                flows_df['week_start'] = pd.to_datetime(flows_df['week_start'], errors='coerce')

            if 'block_time' in dex_df.columns:
                dex_df['block_time'] = pd.to_datetime(dex_df['block_time'], errors='coerce')
            elif 'date' in dex_df.columns:
                dex_df['date'] = pd.to_datetime(dex_df['date'], errors='coerce')

            # Validate
            success = flows_df is not None and dex_df is not None

            if success:
                logger.info("")
                logger.info("✅ DATA LOADING SUCCESSFUL")
                logger.info(f"  Flows: {len(flows_df):,} rows, {len(flows_df.columns)} columns")
                logger.info(f"  DEX: {len(dex_df):,} rows, {len(dex_df.columns)} columns")
            else:
                logger.error("❌ Missing required data")

            return {
                'success': success,
                'flows': flows_df,
                'dex': dex_df,
                'flows_rows': len(flows_df) if flows_df is not None else 0,
                'dex_rows': len(dex_df) if dex_df is not None else 0,
                'flows_file': str(flows_file),
                'dex_file': str(dex_file)
            }

        except Exception as e:
            logger.error(f"✗ Data loading failed: {e}")
            return {
                'success': False,
                'flows': None,
                'dex': None,
                'error': str(e)
            }

    # =========================================================================
    # PHASE 2: DATA VALIDATION
    # =========================================================================

    def _validate_data_consistency(self, flows_df: pd.DataFrame, dex_df: pd.DataFrame) -> Dict:
        """
        Validate data consistency and quality

        Checks:
        1. Date range overlap between datasets
        2. Data freshness (not too old)
        3. Minimum data requirements
        4. Column existence

        Args:
            flows_df: Flows DataFrame
            dex_df: DEX DataFrame

        Returns:
            Dictionary with validation results
        """
        logger.info("Validating data consistency...")

        validation = {
            'passed': True,
            'errors': [],
            'warnings': [],
            'checks': {}
        }

        try:
            # CHECK 1: Date Range Overlap
            logger.info("  Check 1: Date range overlap between datasets")
            flows_date_col, flows_dates = self._extract_dates(flows_df, 'Flows')
            dex_date_col, dex_dates = self._extract_dates(dex_df, 'DEX')

            if flows_dates is None or dex_dates is None:
                validation['passed'] = False
                validation['errors'].append("Cannot extract dates from one or both datasets")
                validation['checks']['date_overlap'] = 'FAILED'
            else:
                overlap_dates = flows_dates.intersection(dex_dates)
                if len(overlap_dates) == 0:
                    validation['passed'] = False
                    validation['errors'].append(
                        f"No overlapping dates between datasets! "
                        f"Flows: {min(flows_dates)} to {max(flows_dates)}, "
                        f"DEX: {min(dex_dates)} to {max(dex_dates)}"
                    )
                    validation['checks']['date_overlap'] = 'FAILED'
                else:
                    overlap_pct = len(overlap_dates) / max(len(flows_dates), len(dex_dates)) * 100
                    logger.info(f"  ✓ Found {len(overlap_dates)} overlapping dates ({overlap_pct:.1f}% coverage)")
                    validation['checks']['date_overlap'] = {
                        'status': 'PASSED',
                        'overlapping_dates': len(overlap_dates),
                        'overlap_percentage': round(overlap_pct, 1),
                        'flows_date_range': f"{min(flows_dates)} to {max(flows_dates)}",
                        'dex_date_range': f"{min(dex_dates)} to {max(dex_dates)}"
                    }

                    if overlap_pct < 50:
                        validation['warnings'].append(
                            f"Low date overlap ({overlap_pct:.1f}%). Cross-domain analysis may be limited."
                        )

            # CHECK 2: Data Freshness
            logger.info("  Check 2: Data freshness")
            if flows_dates and dex_dates:
                most_recent_flows = max(flows_dates)
                most_recent_dex = max(dex_dates)
                days_old_flows = (datetime.now().date() - most_recent_flows).days
                days_old_dex = (datetime.now().date() - most_recent_dex).days

                logger.info(f"  Flows data: {days_old_flows} days old (latest: {most_recent_flows})")
                logger.info(f"  DEX data: {days_old_dex} days old (latest: {most_recent_dex})")

                validation['checks']['data_freshness'] = {
                    'flows_days_old': days_old_flows,
                    'dex_days_old': days_old_dex,
                    'flows_latest_date': str(most_recent_flows),
                    'dex_latest_date': str(most_recent_dex)
                }

                if days_old_flows > 7 or days_old_dex > 7:
                    validation['warnings'].append(
                        f"Stale data detected: Flows is {days_old_flows}d old, DEX is {days_old_dex}d old"
                    )
                else:
                    logger.info(f"  ✓ Data is fresh (< 7 days old)")

            # CHECK 3: Minimum Data Requirements
            logger.info("  Check 3: Minimum data requirements")
            min_rows_required = 10
            if len(flows_df) < min_rows_required:
                validation['passed'] = False
                validation['errors'].append(
                    f"Insufficient flows data: {len(flows_df)} rows (minimum: {min_rows_required})"
                )
                validation['checks']['min_data'] = 'FAILED'
            elif len(dex_df) < min_rows_required:
                validation['passed'] = False
                validation['errors'].append(
                    f"Insufficient DEX data: {len(dex_df)} rows (minimum: {min_rows_required})"
                )
                validation['checks']['min_data'] = 'FAILED'
            else:
                logger.info(f"  ✓ Sufficient data: Flows={len(flows_df)}, DEX={len(dex_df)}")
                validation['checks']['min_data'] = 'PASSED'

            # CHECK 4: Required Columns
            logger.info("  Check 4: Required columns existence")

            flows_required = ['blockchain', 'symbol', 'mint_volume_usd', 'burn_volume_usd']
            flows_missing = [col for col in flows_required if col not in flows_df.columns]

            dex_required = ['blockchain', 'symbol', 'total_volume_usd', 'buy_volume_usd', 'sell_volume_usd']
            dex_missing = []
            for col in dex_required:
                if col not in dex_df.columns:
                    if col == 'symbol' and 'token_symbol' in dex_df.columns:
                        continue
                    if col == 'total_volume_usd' and 'amount_usd' in dex_df.columns:
                        continue
                    dex_missing.append(col)

            if flows_missing:
                validation['warnings'].append(f"Flows missing columns: {flows_missing}")
                logger.warning(f"  ⚠ Flows missing columns: {flows_missing}")

            if dex_missing:
                validation['warnings'].append(f"DEX missing columns: {dex_missing}")
                logger.warning(f"  ⚠ DEX missing columns: {dex_missing}")

            if not flows_missing and not dex_missing:
                logger.info(f"  ✓ All required columns present")
                validation['checks']['required_columns'] = 'PASSED'
            else:
                validation['checks']['required_columns'] = 'PASSED_WITH_WARNINGS'

            # FINAL STATUS
            logger.info("")
            if validation['passed']:
                logger.info("✅ DATA VALIDATION PASSED")
                if validation['warnings']:
                    logger.info(f"  ⚠ {len(validation['warnings'])} warnings (non-critical)")
            else:
                logger.error("❌ DATA VALIDATION FAILED")
                logger.error(f"  Errors: {len(validation['errors'])}")
                for error in validation['errors']:
                    logger.error(f"    - {error}")

            return validation

        except Exception as e:
            logger.error(f"✗ Validation error: {e}", exc_info=True)
            validation['passed'] = False
            validation['errors'].append(f"Validation exception: {str(e)}")
            return validation

    def _extract_dates(self, df: pd.DataFrame, dataset_name: str) -> Tuple[Optional[str], Optional[set]]:
        """
        Extract date column and unique dates from DataFrame

        Args:
            df: DataFrame to extract dates from
            dataset_name: Name for logging

        Returns:
            Tuple of (column_name, set of dates)
        """
        date_columns = ['block_time', 'date', 'block_date', 'week_start']

        for col in date_columns:
            if col in df.columns:
                try:
                    dates_series = pd.to_datetime(df[col], errors='coerce')
                    unique_dates = set(dates_series.dt.date.dropna())
                    if unique_dates:
                        logger.debug(f"  {dataset_name}: Found {len(unique_dates)} unique dates in '{col}' column")
                        return col, unique_dates
                except Exception as e:
                    logger.debug(f"  {dataset_name}: Error parsing '{col}' as dates: {e}")
                    continue

        logger.error(f"  {dataset_name}: No valid date column found. Tried: {date_columns}")
        return None, None

    # =========================================================================
    # PHASE 3: DEX PROCESSING
    # =========================================================================

    def _process_dex(self, dex_df: Optional[pd.DataFrame]) -> Dict:
        """
        Process DEX domain with market sentiment analysis

        Args:
            dex_df: Raw DEX data

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

            dex_results = self.dex_processor.process_all(dex_df)
            summary = self.dex_processor.generate_summary(dex_results)

            logger.info("")
            logger.info("✅ DEX PROCESSING COMPLETE")
            logger.info(f"  Total volume: ${summary.get('total_volume_usd', 0):,.2f}")
            logger.info(f"  Total trades: {summary.get('total_trades', 0):,}")
            logger.info(f"  Unique tokens: {summary.get('unique_tokens', 0)}")
            logger.info(f"  Unique blockchains: {summary.get('unique_blockchains', 0)}")
            logger.info(f"  Avg buy pressure: {summary.get('avg_buy_pressure_pct', 0):.2f}%")

            return {
                'success': True,
                'kpis': dex_results,
                'summary': summary,
                'exported_files': {}
            }

        except Exception as e:
            logger.error(f"✗ DEX processing failed: {e}", exc_info=True)
            return {
                'success': False,
                'error': str(e),
                'kpis': {}
            }

    # =========================================================================
    # PHASE 4: FLOWS PROCESSING
    # =========================================================================

    def _process_flows(self, flows_df: Optional[pd.DataFrame]) -> Dict:
        """
        Process flows domain with mint/burn transaction analysis

        Args:
            flows_df: Raw flows data

        Returns:
            Dictionary with Flows KPIs and export info
        """
        if flows_df is None or flows_df.empty:
            logger.error("Cannot process Flows: No flows data")
            return {
                'success': False,
                'error': 'No flows data provided',
                'kpis': {}
            }

        try:
            logger.info("Processing flows domain with mint/burn transaction analysis...")

            flows_results = self.flows_processor.process_all(flows_df)
            summary = self.flows_processor.generate_summary(flows_results)

            logger.info("")
            logger.info("✅ FLOWS PROCESSING COMPLETE")
            logger.info(f"  Total mints: ${summary.get('total_mints', 0):,.2f}")
            logger.info(f"  Total burns: ${summary.get('total_burns', 0):,.2f}")
            logger.info(f"  Net issuance: ${summary.get('net_issuance', 0):,.2f}")
            logger.info(f"  Tokens with mints: {summary.get('tokens_with_mints', 0)}")
            logger.info(f"  Tokens with burns: {summary.get('tokens_with_burns', 0)}")

            return {
                'success': True,
                'kpis': flows_results,
                'summary': summary,
                'exported_files': {}
            }

        except Exception as e:
            logger.error(f"✗ Flows processing failed: {e}", exc_info=True)
            return {
                'success': False,
                'error': str(e),
                'kpis': {}
            }

    # =========================================================================
    # PHASE 5: SUPPLY PROCESSING
    # =========================================================================

    def _process_supply(self, flows_df: Optional[pd.DataFrame]) -> Dict:
        """
        Process supply domain with mint/burn verification

        Args:
            flows_df: Raw flows data

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

            supply_results = self.supply_processor.process_all(flows_df)
            summary = self.supply_processor.generate_summary(supply_results)

            logger.info("")
            logger.info("✅ SUPPLY PROCESSING COMPLETE")
            logger.info(f"  Total mints: ${summary.get('total_mints_usd', 0):,.2f}")
            logger.info(f"  Total burns: ${summary.get('total_burns_usd', 0):,.2f}")
            logger.info(f"  Net supply: ${summary.get('net_supply_change_usd', 0):,.2f}")

            return {
                'success': True,
                'kpis': supply_results,
                'summary': summary,
                'exported_files': {}
            }

        except Exception as e:
            logger.error(f"✗ Supply processing failed: {e}", exc_info=True)
            return {
                'success': False,
                'error': str(e),
                'kpis': {}
            }

    # =========================================================================
    # PHASE 6: JSON REPORT GENERATION (NEW - FIXED)
    # =========================================================================

    def _generate_json_report(self, dex_result: Dict, flows_result: Dict, supply_result: Dict) -> Dict:
        """Generate consolidated JSON report using ReportGenerator"""
        logger.info("Generating consolidated JSON report...")

        try:
            from generators.report_generator import ReportGenerator
            from generators.markdown_exporter import MarkdownExporter

            # Get the exported KPI file paths from each processor
            # These should be the CSV file paths that were already exported
            supply_kpis = {}
            flows_kpis = {}
            dex_kpis = {}

            # Export KPIs to get file paths
            if supply_result.get('success') and supply_result.get('kpis'):
                supply_kpi_dict = supply_result['kpis']
                exported_supply = self.supply_processor.export_kpis(supply_kpi_dict, self.timestamp)
                # exported_supply is a dict: {kpi_name: file_path}
                supply_kpis = exported_supply

            if flows_result.get('success') and flows_result.get('kpis'):
                flows_kpi_dict = flows_result['kpis']
                exported_flows = self.flows_processor.export_kpis(flows_kpi_dict, self.timestamp)
                flows_kpis = exported_flows

            if dex_result.get('success') and dex_result.get('kpis'):
                dex_kpi_dict = dex_result['kpis']
                exported_dex = self.dex_processor.export_kpis(dex_kpi_dict, self.timestamp)
                dex_kpis = exported_dex

            # Initialize report generator
            report_gen = ReportGenerator()

            # Generate report with file paths
            report_path = report_gen.generate_consolidated_report(
                supply_kpis=supply_kpis,
                flows_kpis=flows_kpis,
                dex_kpis=dex_kpis,
                timestamp=self.timestamp
            )

            logger.info("")
            logger.info("✅ REPORT GENERATION COMPLETE")
            logger.info(f"   Report: {report_path}")

            # Read report to get health score
            import json
            with open(report_path, 'r') as f:
                report_data = json.load(f)

            health_score = report_data.get('market_health', {}).get('overall_score', 'N/A')
            logger.info(f"   Market Health Score: {health_score}")

            try:
                md_exporter = MarkdownExporter()
                md_path = md_exporter.export_report(report_path)
                logger.info(f"   Markdown: {md_path}")
            except Exception as e:
                logger.warning(f"⚠ Markdown export failed: {e}")

            return {
                'success': True,
                'file_path': str(report_path),
                'timestamp': self.timestamp,
                'health_score': health_score
            }

        except Exception as e:
            logger.error(f"✗ Report generation failed: {e}", exc_info=True)
            return {
                'success': False,
                'error': str(e)
            }

        #except Exception as e:
        #    logger.error(f"✗ Report generation failed: {e}", exc_info=True)
        #    return {
        #        'success': False,
        #        'error': str(e)
        #    }

    def _generate_consolidated_report(self, dex_result: Dict, flows_result: Dict, supply_result: Dict) -> Dict:
        """
        Generate consolidated report summary (backward compatibility)

        Args:
            dex_result: DEX processing results
            flows_result: Flows processing results
            supply_result: Supply processing results

        Returns:
            Consolidated summary dictionary
        """
        logger.info("Generating consolidated summary...")

        consolidated = {
            'dex_summary': dex_result.get('summary', {}),
            'flows_summary': flows_result.get('summary', {}),
            'supply_summary': supply_result.get('summary', {}),
            'cross_domain_insights': {}
        }

        # Generate cross-domain insights
        if dex_result['success'] and flows_result['success'] and supply_result['success']:
            dex_sum = dex_result.get('summary', {})
            flows_sum = flows_result.get('summary', {})
            supply_sum = supply_result.get('summary', {})

            consolidated['cross_domain_insights'] = {
                'total_ecosystem_volume': (
                        dex_sum.get('total_volume_usd', 0) +
                        flows_sum.get('total_mints', 0)
                ),
                'net_supply_vs_trading': {
                    'net_supply_from_supply': supply_sum.get('net_supply_change_usd', 0),
                    'net_issuance_from_flows': flows_sum.get('net_issuance', 0),
                    'trading_volume': dex_sum.get('total_volume_usd', 0)
                },
                'market_health': {
                    'tokens_tracked': supply_sum.get('total_tokens_tracked', 0),
                    'avg_buy_pressure': dex_sum.get('avg_buy_pressure_pct', 0),
                    'tokens_with_mint_activity': flows_sum.get('tokens_with_mints', 0),
                    'tokens_with_burn_activity': flows_sum.get('tokens_with_burns', 0)
                }
            }

            logger.info("")
            logger.info("📊 CROSS-DOMAIN INSIGHTS")
            logger.info(
                f"  Total ecosystem volume: ${consolidated['cross_domain_insights']['total_ecosystem_volume']:,.2f}")
            logger.info(
                f"  Avg buy pressure: {consolidated['cross_domain_insights']['market_health']['avg_buy_pressure']:.2f}%")
            logger.info(
                f"  Net issuance (flows): ${consolidated['cross_domain_insights']['net_supply_vs_trading']['net_issuance_from_flows']:,.2f}")

        return consolidated

    # =========================================================================
    # PHASE 7: EXPORT & SUMMARY
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

        logger.info("")
        logger.info("PHASE 7: EXPORTING KPI FILES")
        logger.info("-" * 80)

        # Export DEX KPI CSVs
        if 'dex_kpis' in results and results['dex_kpis'].get('success'):
            logger.info("Exporting DEX KPIs...")
            dex_kpis = results['dex_kpis']['kpis']
            dex_exports = self.dex_processor.export_kpis(dex_kpis, self.timestamp)
            exported['dex'] = dex_exports
            logger.info(f"✅ DEX exports: {len(dex_exports)} files")
        else:
            logger.warning("⚠️ DEX KPIs not available for export")
            exported['dex'] = []

        # Export Flows KPI CSVs
        if 'flows_kpis' in results and results['flows_kpis'].get('success'):
            logger.info("Exporting Flows KPIs...")
            flows_kpis = results['flows_kpis']['kpis']
            flows_exports = self.flows_processor.export_kpis(flows_kpis, self.timestamp)
            exported['flows'] = flows_exports
            logger.info(f"✅ Flows exports: {len(flows_exports)} files")
        else:
            logger.warning("⚠️ Flows KPIs not available for export")
            exported['flows'] = []

        # Export Supply KPI CSVs
        if 'supply_kpis' in results and results['supply_kpis'].get('success'):
            logger.info("Exporting Supply KPIs...")
            supply_kpis = results['supply_kpis']['kpis']
            supply_exports = self.supply_processor.export_kpis(supply_kpis, self.timestamp)
            exported['supply'] = supply_exports
            logger.info(f"✅ Supply exports: {len(supply_exports)} files")
        else:
            logger.warning("⚠️ Supply KPIs not available for export")
            exported['supply'] = []

        logger.info("")
        logger.info("Exporting consolidated metadata...")

        # Export metadata
        metadata_file = self._export_metadata(results)
        exported['metadata'] = str(metadata_file)
        logger.info(f"✅ Metadata exported: {metadata_file}")

        logger.info("")
        total_files = len(exported.get('dex', [])) + len(exported.get('flows', [])) + len(exported.get('supply', []))
        logger.info(f"📁 Total files exported: {total_files} KPI CSVs + 1 metadata JSON")

        return exported

    def _export_metadata(self, results: Dict) -> Path:
        """Export pipeline metadata to JSON"""
        import json

        metadata = {
            'timestamp': results['timestamp'],
            'status': results['status'],
            'extraction': {
                'flows_rows': results['extraction'].get('flows_rows', 0),
                'dex_rows': results['extraction'].get('dex_rows', 0),
                'flows_file': results['extraction'].get('flows_file', ''),
                'dex_file': results['extraction'].get('dex_file', '')
            },
            'validation': results.get('validation', {}),
            'dex': {
                'status': 'SUCCESS' if results['dex_kpis'].get('success') else 'FAILED',
                'summary': results['dex_kpis'].get('summary', {})
            },
            'flows': {
                'status': 'SUCCESS' if results['flows_kpis'].get('success') else 'FAILED',
                'summary': results['flows_kpis'].get('summary', {})
            },
            'supply': {
                'status': 'SUCCESS' if results['supply_kpis'].get('success') else 'FAILED',
                'summary': results['supply_kpis'].get('summary', {})
            },
            'consolidated': results['consolidated_summary'],
            'errors': results.get('errors', []),
            'warnings': results.get('warnings', [])
        }

        metadata_dir = Path('data/metadata')
        metadata_dir.mkdir(parents=True, exist_ok=True)

        metadata_file = metadata_dir / f"pipeline_metadata_{results['timestamp']}.json"
        with open(metadata_file, 'w') as f:
            json.dump(metadata, f, indent=2, default=str)

        logger.info(f"✓ Exported metadata: {metadata_file}")

        return metadata_file

    def _print_execution_summary(self, results: Dict):
        """Print final execution summary"""
        logger.info("")
        logger.info("=" * 80)
        logger.info("📋 EXECUTION SUMMARY")
        logger.info("=" * 80)

        # Data loading summary
        logger.info("")
        logger.info("1️⃣ DATA LOADING")
        if results['extraction'].get('success'):
            logger.info(f"  Status: ✅ SUCCESS")
            logger.info(f"  Flows: {results['extraction']['flows_rows']:,} rows")
            logger.info(f"  DEX: {results['extraction']['dex_rows']:,} rows")
        else:
            logger.info(f"  Status: ❌ FAILED - {results['extraction'].get('error', 'Unknown error')}")

        # Validation summary
        logger.info("")
        logger.info("2️⃣ DATA VALIDATION")
        if results.get('validation', {}).get('passed'):
            logger.info(f"  Status: ✅ PASSED")
            if results['validation'].get('warnings'):
                logger.info(f"  Warnings: {len(results['validation']['warnings'])}")
        else:
            logger.info(f"  Status: ❌ FAILED")
            if results['validation'].get('errors'):
                logger.info(f"  Errors: {len(results['validation']['errors'])}")

        # DEX processing summary
        logger.info("")
        logger.info("3️⃣ DEX DOMAIN")
        if results['dex_kpis'].get('success'):
            logger.info(f"  Status: ✅ SUCCESS")
            summary = results['dex_kpis'].get('summary', {})
            logger.info(f"  Volume: ${summary.get('total_volume_usd', 0):,.2f}")
            logger.info(f"  Trades: {summary.get('total_trades', 0):,}")
            logger.info(f"  Buy Pressure: {summary.get('avg_buy_pressure_pct', 0):.2f}%")
        else:
            logger.info(f"  Status: ❌ FAILED - {results['dex_kpis'].get('error', 'Unknown error')}")

        # Flows processing summary
        logger.info("")
        logger.info("4️⃣ FLOWS DOMAIN")
        if results['flows_kpis'].get('success'):
            logger.info(f"  Status: ✅ SUCCESS")
            summary = results['flows_kpis'].get('summary', {})
            logger.info(f"  Mints: ${summary.get('total_mints', 0):,.2f}")
            logger.info(f"  Burns: ${summary.get('total_burns', 0):,.2f}")
            logger.info(f"  Net Issuance: ${summary.get('net_issuance', 0):,.2f}")
        else:
            logger.info(f"  Status: ❌ FAILED - {results['flows_kpis'].get('error', 'Unknown error')}")

        # Supply processing summary
        logger.info("")
        logger.info("5️⃣ SUPPLY DOMAIN")
        if results['supply_kpis'].get('success'):
            logger.info(f"  Status: ✅ SUCCESS")
            summary = results['supply_kpis'].get('summary', {})
            logger.info(f"  Mints: ${summary.get('total_mints_usd', 0):,.2f}")
            logger.info(f"  Burns: ${summary.get('total_burns_usd', 0):,.2f}")
            logger.info(f"  Net: ${summary.get('net_supply_change_usd', 0):,.2f}")
        else:
            logger.info(f"  Status: ❌ FAILED - {results['supply_kpis'].get('error', 'Unknown error')}")

        # Overall status
        logger.info("")
        logger.info("=" * 80)
        if results['status'] == 'SUCCESS':
            logger.info("✅ PIPELINE EXECUTION: SUCCESS")
            if results.get('warnings'):
                logger.info(f"⚠️ Warnings: {len(results['warnings'])}")
                for warning in results['warnings'][:3]:  # Show first 3
                    logger.info(f"  - {warning}")
        else:
            logger.info("❌ PIPELINE EXECUTION: FAILED")
            if results['errors']:
                logger.info("Errors:")
                for error in results['errors']:
                    logger.info(f"  - {error}")
        logger.info("=" * 80)


def main():
    """Entry point for pipeline execution"""
    parser = argparse.ArgumentParser(
        description='LATAM Stablecoins Economic Analysis Pipeline v2.0'
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
        pipeline = LATAMEconomicPipeline(config_path=args.config)
        results = pipeline.run()

        # Exit with appropriate code
        sys.exit(0 if results['status'] == 'SUCCESS' else 1)

    except Exception as e:
        logger.error(f"✗ Fatal error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    main()
