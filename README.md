# LATAM Stablecoin Monitor

Weekly analytics pipeline for LATAM-focused stablecoins using:
- Dune (DuneSQL + API) for on-chain data
- Python/pandas for KPI processing
- 3 domains: Supply, Flows (Mint/Burn), DEX Usage

## Project Structure

- `extractors/` – Dune API queries (tokens.transfers, dex.trades)
- `processors/` – Domain processors (supply, flows, dex)
- `generators/` – Report generation (console, JSON)
- `utils/` – Logging, date helpers, data helpers
- `data/` – Local CSV outputs (ignored by Git)
- `run_pipeline.py` – Main orchestrator

## Requirements

- Python 3.10+ (recommended)
- `pip install -r requirements.txt`
- `DUNE_API_KEY` set as an environment variable

## Run the pipeline

python run_pipeline.py