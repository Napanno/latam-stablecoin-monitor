"""
Main pipeline orchestrator for LATAM Stablecoin Monitor v3.0
Executes weekly analytics pipeline with 3 domains and 2 data sources
"""
from dotenv import load_dotenv # review
load_dotenv() # review
import os
import yaml
from datetime import datetime
from pathlib import Path

# Import utilities
from utils.logger import setup_logging, get_logger

# Import extractors
from extractors.data_extractor import DuneDataExtractor

# Import processors
from processors.supply_processor import SupplyProcessor
from processors.flows_processor import FlowsProcessor
from processors.dex_processor import DexProcessor

# Import generators
from generators.report_generator import ReportGenerator


def load_config(config_path='config/config.yaml'):
    """Load configuration from YAML file"""
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    return config


def validate_environment():
    """Validate required environment variables"""
    logger = get_logger(__name__)

    if not os.getenv('DUNE_API_KEY'):
        logger.error("DUNE_API_KEY environment variable not set")
        raise EnvironmentError("Missing DUNE_API_KEY. Set it as: export DUNE_API_KEY='your_key'")

    logger.debug("Environment validation passed")


def create_output_directories(config):
    """Create output directories if they don't exist"""
    logger = get_logger(__name__)

    directories = [
        config['output']['raw_data_dir'],
        config['output']['kpi_dir'],
        config['output']['reports_dir'],
        config['output']['logs_dir']
    ]

    for directory in directories:
        Path(directory).mkdir(parents=True, exist_ok=True)
        logger.debug(f"Created/verified directory: {directory}")


def main():
    """Main pipeline execution"""

    # Load configuration
    config = load_config('config/config.yaml')

    # Setup logging
    setup_logging(
        log_dir=config['output']['logs_dir'],
        log_level=config['logging']['level']
    )

    logger = get_logger(__name__)

    logger.info("=" * 80)
    logger.info("LATAM STABLECOIN WEEKLY REPORT PIPELINE v3.0 - STARTING")
    logger.info("=" * 80)

    start_time = datetime.now()
    timestamp = start_time.strftime('%Y%m%d%H%M%S')

    try:
        # Validate environment
        logger.info("Validating environment...")
        validate_environment()

        # Create output directories
        logger.info("Creating output directories...")
        create_output_directories(config)

        # ====================================================================
        # STEP 1: EXTRACT DATA FROM DUNE ANALYTICS
        # ====================================================================
        logger.info("\n" + "=" * 80)
        logger.info("STEP 1: EXTRACTING DATA FROM DUNE ANALYTICS")
        logger.info("=" * 80)

        extractor = DuneDataExtractor(config_path='config/config.yaml')
        raw_data = extractor.extract_all()

        # Validate extraction results
        if not raw_data:
            logger.error("Data extraction failed - no data returned")
            return 1

        if 'flows' not in raw_data or raw_data['flows'] is None:
            logger.error("Flows data extraction failed")
            return 1

        if 'dex' not in raw_data or raw_data['dex'] is None:
            logger.error("DEX data extraction failed")
            return 1

        # Log extraction results
        logger.info(f"\n✓ Data extraction completed:")
        logger.info(f"  - Flows (tokens.transfers): {len(raw_data['flows']):,} rows")
        logger.info(f"  - DEX (dex.trades): {len(raw_data['dex']):,} rows")

        # ====================================================================
        # STEP 2: PROCESS KPIs ACROSS 3 DOMAINS
        # ====================================================================
        logger.info("\n" + "=" * 80)
        logger.info("STEP 2: PROCESSING KPIs")
        logger.info("=" * 80)

        # Domain 1: Supply (uses flows data)
        logger.info("\nProcessing Domain 1: Supply...")
        supply_processor = SupplyProcessor()
        supply_processor.process_all(raw_data['flows'])
        supply_kpis = supply_processor.export_kpis(timestamp)
        logger.info(f"✓ Supply KPIs exported: {len(supply_kpis)} files")

        # Domain 2: Flows (uses flows data)
        logger.info("\nProcessing Domain 2: Mint/Burn Flows...")
        flows_processor = FlowsProcessor()
        flows_processor.process_all(raw_data['flows'])
        flows_kpis = flows_processor.export_kpis(timestamp)
        logger.info(f"✓ Flows KPIs exported: {len(flows_kpis)} files")

        # Domain 3: DEX (uses dex data)
        logger.info("\nProcessing Domain 3: DEX Volume...")
        dex_processor = DexProcessor()
        dex_processor.process_all(raw_data['dex'])
        dex_kpis = dex_processor.export_kpis(timestamp)
        logger.info(f"✓ DEX KPIs exported: {len(dex_kpis)} files")

        # Total KPI files
        total_kpis = len(supply_kpis) + len(flows_kpis) + len(dex_kpis)
        logger.info(f"\n✓ Total KPI files: {total_kpis}")

        # ====================================================================
        # STEP 3: GENERATE REPORTS
        # ====================================================================
        logger.info("\n" + "=" * 80)
        logger.info("STEP 3: GENERATING REPORTS")
        logger.info("=" * 80)

        report_generator = ReportGenerator()

        # Generate consolidated JSON report
        logger.info("\nGenerating consolidated report...")
        report_path = report_generator.generate_consolidated_report(
            supply_kpis=supply_kpis,
            flows_kpis=flows_kpis,
            dex_kpis=dex_kpis,
            timestamp=timestamp
        )

        logger.info(f"✓ Consolidated report saved: {report_path}")

        # ====================================================================
        # PIPELINE COMPLETION
        # ====================================================================
        end_time = datetime.now()
        execution_time = (end_time - start_time).total_seconds()

        logger.info("\n" + "=" * 80)
        logger.info("PIPELINE EXECUTION COMPLETED SUCCESSFULLY ✓")
        logger.info("=" * 80)
        logger.info(f"Start time:       {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"End time:         {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"Execution time:   {execution_time:.2f} seconds ({execution_time / 60:.1f} minutes)")
        logger.info(f"Timestamp:        {timestamp}")
        logger.info("")
        logger.info("Output files:")
        logger.info(f"  - Raw data:     {config['output']['raw_data_dir']}")
        logger.info(f"  - KPI files:    {config['output']['kpi_dir']} ({total_kpis} files)")
        logger.info(f"  - Report:       {report_path}")
        logger.info(
            f"  - Log:          {config['output']['logs_dir']}/pipeline_{datetime.now().strftime('%Y%m%d')}.log")
        logger.info("=" * 80)

        return 0

    except EnvironmentError as e:
        logger.error("\n" + "=" * 80)
        logger.error("ENVIRONMENT ERROR")
        logger.error("=" * 80)
        logger.error(f"Error: {str(e)}")
        logger.error("=" * 80)
        return 1

    except Exception as e:
        logger.error("\n" + "=" * 80)
        logger.error("PIPELINE EXECUTION FAILED ✗")
        logger.error("=" * 80)
        logger.error(f"Error: {str(e)}")
        logger.error("", exc_info=True)  # Full stack trace
        logger.error("=" * 80)
        return 1


if __name__ == "__main__":
    exit_code = main()
    exit(exit_code)
