# LATAM Stablecoin Analytics Pipeline

Automated weekly analytics pipeline for tracking stablecoin activity across Latin American markets. The system processes on-chain data from multiple blockchains to generate comprehensive KPI reports across three analytical domains: Token Supply, On-Chain Flows, and DEX Trading Activity.

## Overview

### Data Source
- **Platform**: Dune Analytics (DuneSQL + API v1)
- **Primary Tables**: `tokens.transfers`, `dex.trades`
- **Update Frequency**: Weekly (Monday 00:00 UTC)

### Analytical Domains
1. **Supply Domain**: Mint/burn tracking, supply changes, issuance rates
2. **Flows Domain**: Transfer activity, network health, wallet distribution
3. **DEX Domain**: Trading volume, liquidity analysis, market sentiment

### Output Formats
- CSV files (15 KPIs across 3 domains)
- JSON consolidated reports
- Markdown reports (LinkedIn-ready format)
- Console summaries with health scoring

## Architecture

```
latam-stablecoin-pipeline/
├── extractors/
│   ├── dune_extractor.py          # Dune API integration
│   └── sql/
│       ├── flows/                 # Mint/burn queries
│       └── dex/                   # DEX trading queries
├── processors/
│   ├── base_processor.py          # Abstract processor interface
│   ├── supply_processor.py        # Supply KPIs (4 KPIs)
│   ├── flows_processor.py         # Flow KPIs (5 KPIs)
│   └── dex_processor.py           # DEX KPIs (5 KPIs)
├── generators/
│   ├── report_generator.py        # JSON report consolidation
│   ├── markdown_exporter.py       # Markdown report generation
│   └── console_reporter.py        # Terminal output formatting
├── utils/
│   ├── logger.py                  # Structured logging
│   ├── date_utils.py              # ISO week calculations
│   ├── config_validator.py        # YAML validation
│   ├── retry_policy.py            # API retry logic
│   └── math_utils.py              # Safe division utilities
├── config.yaml                    # Token/blockchain configuration
├── run_pipeline.py                # Main orchestrator
└── data/                          # Output directory (gitignored)
    ├── raw/                       # Raw Dune exports
    ├── kpi/                       # Processed KPI CSVs
    │   ├── supply/
    │   ├── flows/
    │   └── dex/
    └── reports/                   # JSON/MD reports
```

## Key Performance Indicators

### Supply Domain (4 KPIs)
- **KPI 1**: Supply Change - Daily mint/burn volumes by token and blockchain
- **KPI 2**: Issuance Rate - Net issuance metrics with mint/burn event statistics
- **KPI 3**: Token Metrics - Aggregated supply statistics per token
- **KPI 4**: Week-over-Week Supply Change - Weekly growth rates and net issuance

### Flows Domain (5 KPIs)
- **KPI 1**: Daily Activity - Transfer counts and unique wallet participation
- **KPI 2**: Weekly Aggregates - Weekly transfer volumes and wallet metrics
- **KPI 3**: Net Issuance - Consolidated mint/burn volumes with event counts
- **KPI 4**: Week-over-Week Change - WoW transfer growth and issuance trends
- **KPI 5**: Network Health - Whale concentration and sender/receiver distribution

### DEX Domain (5 KPIs)
- **KPI 1**: Daily Volume - Trading volume and transaction counts per token/blockchain
- **KPI 2**: Weekly Aggregates - Buy/sell pressure with market share calculations
- **KPI 3**: Token Trading - Per-token trading statistics across all blockchains
- **KPI 4**: Week-over-Week Change - WoW volume growth and trade count trends
- **KPI 5**: Liquidity Analysis - DEX distribution, whale activity, buy frequency

## Report Features

### Token Rankings
- Top 5 tokens by trading volume (with market share)
- Top 5 tokens by supply growth (positive growth only)
- Top 5 tokens by network activity (transfer counts)
- Top 5 tokens by liquidity (DEX distribution)
- **Top 5 blockchains by trading volume** (with token counts)

### Market Intelligence
- **Market Health Score**: 0-100 composite score based on:
  - Buy pressure (0-30 points)
  - Network decentralization (0-25 points)
  - Liquidity distribution (0-25 points)
  - Network growth (0-20 points)
- **Automated Alerts**: HIGH/MEDIUM/INFO severity flags for:
  - Whale dump risk
  - Low liquidity warnings
  - High burn rate signals
  - Network growth indicators

### Cross-Domain Insights
- Supply vs Trading correlation analysis
- Network activity classification (expanding/contracting)
- Liquidity health ratings

## Installation

### Prerequisites
- Python 3.10 or higher
- Dune Analytics API key (free tier supported)

### Setup

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd latam-stablecoin-pipeline
   ```

2. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

3. **Set environment variable**
   ```bash
   export DUNE_API_KEY="your_dune_api_key_here"
   ```

   Or create a `.env` file:
   ```
   DUNE_API_KEY=your_dune_api_key_here
   ```

4. **Configure tokens and blockchains**

   Edit `config.yaml` to specify:
   - Stablecoin contracts (symbol, address, decimals)
   - Target blockchains
   - Date ranges and output paths

## Usage

### Run Full Pipeline

```bash
python run_pipeline.py
```

**Pipeline Steps:**
1. Extract raw data from Dune Analytics (2 queries)
2. Process 14 KPIs across 3 domains
3. Generate consolidated JSON report
4. Export Markdown report (LinkedIn format)
5. Display console summary with health metrics

### Generate Reports Only

```bash
# Skip extraction, use existing raw data
python run_pipeline.py --skip-extraction
```

### Test with Mock Data

```bash
# Generate synthetic data for testing
python tests/generate_mock_data.py

# Run pipeline tests
python tests/test_pipeline.py
```

## Configuration

### Token Configuration (`config.yaml`)

```yaml
tokens:
  - symbol: USDC
    contract_address: "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
    decimals: 6
    blockchains:
      - ethereum
      - polygon
      - base

blockchains:
  - ethereum
  - polygon
  - base
  - bnb
  - arbitrum
  - optimism
  - celo
```

### Date Range Configuration

```yaml
date_config:
  start_date: "2024-01-01"
  end_date: "2024-12-31"
  week_format: "%Y-W%V"
```

## Output Examples

### Console Output

```
=== LATAM STABLECOIN WEEKLY REPORT ===
Week: 2026-W07
Generated: 2026-02-10 14:07:21

Market Health: 73.5/100 (B - Good)
- Buy Pressure: 52.3% (23.0/30)
- Decentralization: 18.5/25
- Liquidity: 22.0/25
- Growth: 10.0/20

Alerts: 2 (1 HIGH, 1 MEDIUM)

Top Token by Volume: USDC ($45.2M)
Top Blockchain: Polygon (50.2% market share)
```

### JSON Report Structure

```json
{
  "report_metadata": {
    "report_type": "LATAM Stablecoin Weekly Report - Consolidated",
    "report_version": "4.0",
    "week": "2026-W07",
    "generated_at": "2026-02-10T19:07:21.123456"
  },
  "executive_summary": { ... },
  "market_health": {
    "overall_score": 73.5,
    "rating": "B - Good",
    "alerts": [ ... ]
  },
  "token_rankings": {
    "by_trading_volume": [ ... ],
    "by_blockchain_volume": [ ... ]
  },
  "domains": {
    "supply": { ... },
    "flows": { ... },
    "dex": { ... }
  }
}
```

## Development

### Adding New KPIs

1. Add SQL query to `extractors/sql/{domain}/`
2. Update processor in `processors/{domain}_processor.py`
3. Register KPI in processor's `_calculate_kpis()` method
4. Update report generator to consume new KPI

### Extending Report Formats

1. Create new exporter in `generators/`
2. Implement export interface from consolidated JSON
3. Register in `run_pipeline.py` orchestrator

## Logging

Logs are written to `logs/pipeline_YYYYMMDD_HHMMSS.log` with the following levels:
- **DEBUG**: Detailed processing steps
- **INFO**: Pipeline progress and milestones
- **WARNING**: Recoverable issues (missing data, retries)
- **ERROR**: Critical failures

## Error Handling

- **Dune API failures**: Automatic retry with exponential backoff (3 attempts)
- **Missing data**: Graceful degradation with warnings
- **Invalid configuration**: Validation fails at startup with detailed error messages

## Performance

- **Typical runtime**: 2-5 minutes (depends on Dune query execution)
- **Memory usage**: <500MB for weekly data
- **Output size**: ~2MB per week (all formats combined)

## Roadmap

- [ ] Support for additional DEX protocols
- [ ] Historical trend visualization (charts)
- [ ] Email/Slack notification integration
- [ ] Real-time monitoring mode
- [ ] Dashboard web interface

## Version History

**v4.1** (Current)
- Added blockchain-level trading volume rankings
- Enhanced aggregation logic for multi-blockchain tokens
- Improved markdown report formatting

**v4.0**
- Complete KPI refactor (14 → 15 KPIs)
- Market health scoring system
- Automated alert generation
- Cross-domain insights

**v3.0**
- Markdown export for LinkedIn
- Token rankings system
- Network health metrics

## License

[Specify License]

## Contact

[Your Contact Information]

---

**Note**: This pipeline requires a valid Dune Analytics API key. Free tier keys are subject to rate limits (300 requests/day).
