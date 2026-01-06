"""
Main pipeline orchestrator for LATAM Stablecoin Weekly Report

Coordinates:
1. Data extraction from Dune Analytics
2. KPI processing across all domains
3. Report generation (console + JSON)

Author: LATAM Stablecoin Team
Date: 2025-12-29
Version: 2.1.0 - DEPEG DISABLED
"""

import sys
from pathlib import Path
from datetime import datetime
import yaml

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from utils.logger import setup_logger
from extractors.data_extractor import DuneDataExtractor
from processors.supply_processor import SupplyKPIProcessor
from processors.mintburn_processor import MintBurnKPIProcessor
from processors.dex_processor import DexVolumeKPIProcessor
from generators.report_generator import (
    MinimalSupplyReporter,
    MinimalMintBurnReporter,
    MinimalDexReporter,
    consolidate_reports_to_json
)

logger = setup_logger(__name__)

# =====================================================================
# CONFIGURATION FLAG
# =====================================================================
ENABLE_DEPEG = False  # Set to True to re-enable depeg processing


def load_config(config_path: str = 'config.yaml') -> dict:
    """Load configuration from YAML file"""
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)


def main():
    """Main pipeline execution"""
    try:
        logger.info("\n" + "=" * 80)
        logger.info("LATAM STABLECOIN WEEKLY REPORT PIPELINE - STARTING")
        if not ENABLE_DEPEG:
            logger.info("⚠️  DEPEG MONITORING DISABLED")
        logger.info("=" * 80)

        start_time = datetime.now()
        timestamp = start_time.strftime('%Y%m%d_%H%M%S')

        # Load configuration
        config = load_config()
        logger.info("✓ Configuration loaded")

        # =====================================================================
        # STEP 1: DATA EXTRACTION
        # =====================================================================
        logger.info("\n" + "=" * 80)
        logger.info("STEP 1: EXTRACTING DATA FROM DUNE ANALYTICS")
        logger.info("=" * 80)

        extractor = DuneDataExtractor()

        # Extract all datasets at once
        all_data = extractor.extract_all()

        # Map to individual variables
        supply_data = all_data['circulating_supply']
        mintburn_data = all_data['mintburn']
        dex_data = all_data['dex_volume']

        if ENABLE_DEPEG:
            depeg_data = all_data['depeg_monitoring']
        else:
            logger.info("\n⊘ Depeg monitoring data extraction skipped (DISABLED)")
            depeg_data = None

        logger.info("\n✓ Data extraction completed")
        logger.info(f"  - Supply records: {len(supply_data):,}")
        logger.info(f"  - Mint/Burn records: {len(mintburn_data):,}")
        logger.info(f"  - DEX volume records: {len(dex_data):,}")
        if ENABLE_DEPEG and depeg_data is not None:
            logger.info(f"  - Depeg monitoring records: {len(depeg_data):,}")
        else:
            logger.info(f"  - Depeg monitoring: DISABLED")

        # =====================================================================
        # STEP 2: KPI PROCESSING
        # =====================================================================
        logger.info("\n" + "=" * 80)
        logger.info("STEP 2: PROCESSING KPIs")
        logger.info("=" * 80)

        # Process Supply KPIs
        logger.info("\n→ Processing Supply KPIs...")
        supply_processor = SupplyKPIProcessor()
        supply_kpis = supply_processor.process_all(supply_data)
        supply_files = supply_processor.export_kpis(timestamp)
        logger.info(f"✓ Supply: {len(supply_files)} KPI files exported")

        # Process Mint/Burn KPIs
        logger.info("\n→ Processing Mint/Burn KPIs...")
        mintburn_processor = MintBurnKPIProcessor()
        mintburn_kpis = mintburn_processor.process_all(mintburn_data)
        mintburn_files = mintburn_processor.export_kpis(timestamp)
        logger.info(f"✓ Mint/Burn: {len(mintburn_files)} KPI files exported")

        # Process DEX Volume KPIs
        logger.info("\n→ Processing DEX Volume KPIs...")
        dex_processor = DexVolumeKPIProcessor()
        dex_kpis = dex_processor.process_all(dex_data)
        dex_files = dex_processor.export_kpis(timestamp)
        logger.info(f"✓ DEX Volume: {len(dex_files)} KPI files exported")

        # DEPEG PROCESSING (CONDITIONAL)
        if ENABLE_DEPEG and depeg_data is not None:
            logger.info("\n→ Processing Depeg Monitoring KPIs...")
            from processors.depeg_processor import DepegKPIProcessor
            depeg_processor = DepegKPIProcessor()
            depeg_kpis = depeg_processor.process_all(depeg_data)
            depeg_files = depeg_processor.export_kpis(timestamp)
            logger.info(f"✓ Depeg: {len(depeg_files)} KPI files exported")
        else:
            logger.info("\n⊘ Skipping depeg KPI processing (DISABLED)")
            depeg_kpis = None
            depeg_files = {}

        logger.info("\n✓ All KPI processing completed")

        # =====================================================================
        # STEP 3: REPORT GENERATION
        # =====================================================================
        logger.info("\n" + "=" * 80)
        logger.info("STEP 3: GENERATING REPORTS")
        logger.info("=" * 80)

        # Generate minimal reports (console output)
        logger.info("\n→ Generating LinkedIn-style minimal reports...")

        supply_reporter = MinimalSupplyReporter()
        supply_report = supply_reporter.generate_minimal_report(supply_kpis)

        mintburn_reporter = MinimalMintBurnReporter()
        mintburn_report = mintburn_reporter.generate_minimal_report(mintburn_kpis)

        dex_reporter = MinimalDexReporter()
        dex_report = dex_reporter.generate_minimal_report(dex_kpis)

        # DEPEG REPORTING (CONDITIONAL)
        if ENABLE_DEPEG and depeg_kpis is not None:
            from generators.report_generator import MinimalDepegReporter
            depeg_reporter = MinimalDepegReporter()
            depeg_report = depeg_reporter.generate_minimal_report(depeg_kpis)
        else:
            logger.info("\n⊘ Skipping depeg report generation (DISABLED)")
            depeg_report = None

        # Consolidate all reports into single JSON
        logger.info("\n" + "=" * 80)
        logger.info("CONSOLIDATING REPORTS TO JSON")
        logger.info("=" * 80)

        json_path = consolidate_reports_to_json(
            supply_report=supply_report,
            mintburn_report=mintburn_report,
            dex_report=dex_report,
            depeg_report=depeg_report  # Will be None if disabled
        )

        logger.info(f"\n✓ Consolidated report available at: {json_path}")

        # =====================================================================
        # PIPELINE COMPLETION
        # =====================================================================
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        logger.info("\n" + "=" * 80)
        logger.info("PIPELINE EXECUTION COMPLETED SUCCESSFULLY")
        logger.info("=" * 80)
        logger.info(f"✓ Total execution time: {duration:.2f} seconds")
        logger.info(f"✓ Timestamp: {timestamp}")

        total_kpi_files = len(supply_files) + len(mintburn_files) + len(dex_files) + len(depeg_files)
        logger.info(f"✓ KPI files: {total_kpi_files}")

        if not ENABLE_DEPEG:
            logger.info("⚠️  Depeg monitoring was DISABLED for this run")

        logger.info(f"✓ Consolidated JSON: {json_path}")
        logger.info("=" * 80 + "\n")

        return 0

    except Exception as e:
        logger.error("\n" + "=" * 80)
        logger.error("PIPELINE EXECUTION FAILED")
        logger.error("=" * 80)
        logger.error(f"Error: {str(e)}", exc_info=True)
        logger.error("=" * 80 + "\n")
        return 1


if __name__ == "__main__":
    sys.exit(main())
