"""
Report Generator for LATAM Stablecoin Weekly Report v3.0
Generates consolidated JSON reports from processed KPI data across 3 domains
"""

import json
import pandas as pd
from pathlib import Path
from datetime import datetime
from utils.logger import get_logger

logger = get_logger(__name__)


class ReportGenerator:
    """
    Generates consolidated reports from KPI data across multiple domains

    Features:
    - Loads KPI CSVs from all 3 domains (Supply, Flows, DEX)
    - Creates consolidated JSON report
    - Generates executive summary metrics
    - Handles missing data gracefully
    """

    def __init__(self, output_dir='./data/reports'):
        """
        Initialize report generator

        Args:
            output_dir (str): Directory for report output
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"ReportGenerator initialized: {self.output_dir}")

    def generate_consolidated_report(self, supply_kpis, flows_kpis, dex_kpis, timestamp):
        """
        Generate consolidated JSON report from all domain KPIs

        Args:
            supply_kpis (dict): Dictionary of supply KPI file paths
            flows_kpis (dict): Dictionary of flows KPI file paths
            dex_kpis (dict): Dictionary of DEX KPI file paths
            timestamp (str): Timestamp string for filename (YYYYMMDDHHMMSS)

        Returns:
            Path: Path to generated report file
        """
        logger.info("Starting consolidated report generation...")

        # Load all KPI data
        supply_data = self._load_domain_kpis(supply_kpis, 'Supply')
        flows_data = self._load_domain_kpis(flows_kpis, 'Flows')
        dex_data = self._load_domain_kpis(dex_kpis, 'DEX')

        # Extract week from data (assuming all KPIs have 'week' column)
        week = self._extract_week(supply_data, flows_data, dex_data)

        # Build executive summary
        executive_summary = self._build_executive_summary(supply_data, flows_data, dex_data)

        # Build complete report structure
        report_data = {
            'report_metadata': {
                'report_type': 'LATAM Stablecoin Weekly Report - Consolidated',
                'report_version': '3.0',
                'week': week,
                'generated_at': datetime.now().isoformat(),
                'timestamp': timestamp,
                'domains_included': ['supply', 'flows', 'dex'],
                'total_kpis': len(supply_kpis) + len(flows_kpis) + len(dex_kpis)
            },
            'executive_summary': executive_summary,
            'domains': {
                'supply': {
                    'description': 'On-chain supply metrics (Domain 1)',
                    'kpis': supply_data
                },
                'flows': {
                    'description': 'Mint/Burn flow metrics (Domain 2)',
                    'kpis': flows_data
                },
                'dex': {
                    'description': 'DEX trading volume metrics (Domain 3)',
                    'kpis': dex_data
                }
            }
        }

        # Generate filename
        report_filename = self.output_dir / f"consolidated_report_{timestamp}.json"

        # Save to JSON with pretty formatting
        with open(report_filename, 'w', encoding='utf-8') as f:
            json.dump(report_data, f, indent=2, ensure_ascii=False, default=str)

        logger.info(f"✓ Consolidated report saved: {report_filename}")
        logger.info(f"  - Week: {week}")
        logger.info(f"  - Total KPIs: {len(supply_kpis) + len(flows_kpis) + len(dex_kpis)}")
        logger.info(f"  - File size: {report_filename.stat().st_size / 1024:.1f} KB")

        return report_filename

    def _load_domain_kpis(self, kpi_dict, domain_name):
        """
        Load KPI data from file paths for a specific domain

        Args:
            kpi_dict (dict): Dictionary mapping KPI names to file paths
            domain_name (str): Name of domain (for logging)

        Returns:
            dict: Dictionary with KPI data
        """
        domain_data = {}
        loaded_count = 0
        failed_count = 0

        for kpi_name, file_path in kpi_dict.items():
            try:
                df = pd.read_csv(file_path)

                # Convert DataFrame to list of dictionaries
                domain_data[kpi_name] = {
                    'row_count': len(df),
                    'columns': list(df.columns),
                    'data': df.to_dict(orient='records')
                }

                loaded_count += 1
                logger.debug(f"✓ {domain_name} - {kpi_name}: {len(df)} rows, {len(df.columns)} cols")

            except FileNotFoundError:
                logger.warning(f"✗ {domain_name} - {kpi_name}: File not found at {file_path}")
                domain_data[kpi_name] = {
                    'row_count': 0,
                    'columns': [],
                    'data': [],
                    'error': 'File not found'
                }
                failed_count += 1

            except Exception as e:
                logger.warning(f"✗ {domain_name} - {kpi_name}: Error loading - {str(e)}")
                domain_data[kpi_name] = {
                    'row_count': 0,
                    'columns': [],
                    'data': [],
                    'error': str(e)
                }
                failed_count += 1

        logger.info(f"  {domain_name}: {loaded_count} KPIs loaded, {failed_count} failed")
        return domain_data

    def _extract_week(self, supply_data, flows_data, dex_data):
        """
        Extract week identifier from KPI data

        Args:
            supply_data (dict): Supply KPI data
            flows_data (dict): Flows KPI data
            dex_data (dict): DEX KPI data

        Returns:
            str: Week identifier (e.g., '2026-W01') or 'Unknown'
        """
        # Try to find week in any KPI that has data
        for domain_data in [supply_data, flows_data, dex_data]:
            for kpi_name, kpi_info in domain_data.items():
                if kpi_info.get('data') and len(kpi_info['data']) > 0:
                    first_row = kpi_info['data'][0]
                    if 'week' in first_row:
                        return first_row['week']

        # Fallback: generate current week
        current_week = datetime.now().strftime('%Y-W%V')
        logger.warning(f"Could not extract week from data, using current week: {current_week}")
        return current_week

    def _build_executive_summary(self, supply_data, flows_data, dex_data):
        """
        Build executive summary with key metrics

        Args:
            supply_data (dict): Supply KPI data
            flows_data (dict): Flows KPI data
            dex_data (dict): DEX KPI data

        Returns:
            dict: Executive summary metrics
        """
        summary = {
            'supply_metrics': self._extract_supply_summary(supply_data),
            'flows_metrics': self._extract_flows_summary(flows_data),
            'dex_metrics': self._extract_dex_summary(dex_data)
        }

        return summary

    def _extract_supply_summary(self, supply_data):
        """Extract key supply metrics for executive summary"""
        summary = {
            'total_supply': None,
            'supply_growth_wow': None,
            'top_token': None,
            'top_chain': None
        }

        try:
            # Try to get total supply from KPI3 (total supply)
            if 'supply_kpi3_total_supply' in supply_data:
                kpi3_data = supply_data['supply_kpi3_total_supply']['data']
                if kpi3_data:
                    # Get latest week data
                    latest = kpi3_data[-1]
                    summary['total_supply'] = latest.get('total_supply_all_chains')
                    summary['supply_growth_wow'] = latest.get('supply_growth_rate_pct')
                    summary['top_token'] = latest.get('stablecoin')

            # Try to get top chain from KPI2 (supply by chain)
            if 'supply_kpi2_supply_by_chain' in supply_data:
                kpi2_data = supply_data['supply_kpi2_supply_by_chain']['data']
                if kpi2_data:
                    # First entry should be largest
                    summary['top_chain'] = kpi2_data[0].get('blockchain')

        except Exception as e:
            logger.debug(f"Could not extract supply summary: {e}")

        return summary

    def _extract_flows_summary(self, flows_data):
        """Extract key flows metrics for executive summary"""
        summary = {
            'total_mints': None,
            'total_burns': None,
            'net_issuance': None,
            'mint_count': None,
            'burn_count': None
        }

        try:
            # Try to get from KPI3 (net issuance)
            if 'flows_kpi3_net_issuance' in flows_data:
                kpi3_data = flows_data['flows_kpi3_net_issuance']['data']
                if kpi3_data:
                    # Sum across all tokens for latest week
                    total_mints = sum(row.get('total_mints', 0) for row in kpi3_data)
                    total_burns = sum(row.get('total_burns', 0) for row in kpi3_data)

                    summary['total_mints'] = total_mints
                    summary['total_burns'] = total_burns
                    summary['net_issuance'] = total_mints - total_burns

            # Try to get counts from KPI2 (weekly aggregates)
            if 'flows_kpi2_weekly_aggregates' in flows_data:
                kpi2_data = flows_data['flows_kpi2_weekly_aggregates']['data']
                if kpi2_data:
                    mint_count = sum(row.get('mint_count_sum', 0) for row in kpi2_data)
                    burn_count = sum(row.get('burn_count_sum', 0) for row in kpi2_data)

                    summary['mint_count'] = mint_count
                    summary['burn_count'] = burn_count

        except Exception as e:
            logger.debug(f"Could not extract flows summary: {e}")

        return summary

    def _extract_dex_summary(self, dex_data):
        """Extract key DEX metrics for executive summary"""
        summary = {
            'total_volume_usd': None,
            'total_trades': None,
            'top_token_by_volume': None,
            'top_chain_by_volume': None
        }

        try:
            # Try to get from KPI2 (weekly volume)
            if 'dex_kpi2_weekly_volume' in dex_data:
                kpi2_data = dex_data['dex_kpi2_weekly_volume']['data']
                if kpi2_data:
                    total_volume = sum(row.get('volume_usd_sum', 0) for row in kpi2_data)
                    total_trades = sum(row.get('trade_count_sum', 0) for row in kpi2_data)

                    summary['total_volume_usd'] = total_volume
                    summary['total_trades'] = total_trades

            # Try to get top token from KPI3 (token breakdown)
            if 'dex_kpi3_token_breakdown' in dex_data:
                kpi3_data = dex_data['dex_kpi3_token_breakdown']['data']
                if kpi3_data:
                    # First entry should be largest by volume
                    summary['top_token_by_volume'] = kpi3_data[0].get('stablecoin')

            # Try to get top chain from KPI5 (chain distribution)
            if 'dex_kpi5_chain_distribution' in dex_data:
                kpi5_data = dex_data['dex_kpi5_chain_distribution']['data']
                if kpi5_data:
                    # First entry should be largest
                    summary['top_chain_by_volume'] = kpi5_data[0].get('blockchain')

        except Exception as e:
            logger.debug(f"Could not extract DEX summary: {e}")

        return summary


# ============================================================================
# Backward Compatibility Function Wrapper
# ============================================================================

def generate_reports(supply_kpis, flows_kpis, dex_kpis, timestamp, output_dir='./data/reports'):
    """
    Function wrapper for backward compatibility

    Args:
        supply_kpis (dict): Supply KPI file paths
        flows_kpis (dict): Flows KPI file paths
        dex_kpis (dict): DEX KPI file paths
        timestamp (str): Timestamp string
        output_dir (str): Output directory

    Returns:
        Path: Path to generated report
    """
    generator = ReportGenerator(output_dir=output_dir)
    return generator.generate_consolidated_report(supply_kpis, flows_kpis, dex_kpis, timestamp)


# ============================================================================
# CLI for Testing
# ============================================================================

if __name__ == "__main__":
    """Test report generator with sample data"""
    from utils.logger import setup_logging

    setup_logging(log_level='DEBUG')
    logger = get_logger(__name__)

    logger.info("Testing ReportGenerator...")

    # Sample KPI paths (adjust to your actual file structure)
    sample_supply_kpis = {
        'supply_kpi1_weekly_supply': './data/kpi/supply_kpi1_weekly_supply_2026W01_20260106.csv',
        'supply_kpi2_supply_by_chain': './data/kpi/supply_kpi2_supply_by_chain_2026W01_20260106.csv',
    }

    sample_flows_kpis = {
        'flows_kpi1_daily_activity': './data/kpi/flows_kpi1_daily_activity_2026W01_20260106.csv',
    }

    sample_dex_kpis = {
        'dex_kpi1_daily_volume': './data/kpi/dex_kpi1_daily_volume_2026W01_20260106.csv',
    }

    # Generate report
    generator = ReportGenerator()
    report_path = generator.generate_consolidated_report(
        supply_kpis=sample_supply_kpis,
        flows_kpis=sample_flows_kpis,
        dex_kpis=sample_dex_kpis,
        timestamp='20260106130000'
    )

    logger.info(f"Test complete. Report: {report_path}")
