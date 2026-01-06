"""
Minimal reporters for LinkedIn-style output across all domains.

Generates compact metrics ready for weekly carousel posts.

Domains covered:
1. Supply (on-chain circulating supply)
2. Mint/Burn (issuance/redemption activity)
3. DEX Volume (trading activity)
4. Depeg Monitoring (price stability)

Author: LATAM Stablecoin Team
Date: 2025-12-29
Version: 2.1.0 - DEPEG OPTIONAL
"""

import pandas as pd
from typing import Dict, Optional
from datetime import datetime
from pathlib import Path
import yaml
import json
from utils.logger import setup_logger

logger = setup_logger(__name__)


# ============================================================================
# 1. SUPPLY REPORTER
# ============================================================================

class MinimalSupplyReporter:
    """Generate compact supply metrics for weekly LinkedIn content."""

    def __init__(self, config_path: str = 'config.yaml'):
        """
        Initialize reporter with config

        Args:
            config_path: Path to config.yaml
        """
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)

        self.week_format = self.config['reporting']['week_format']
        self.date_format = self.config['reporting']['date_format']
        self.top_n = self.config['reporting']['top_n_tokens']
        self.timestamp = datetime.now()

    def load_kpis(self, kpi_data: Dict[str, pd.DataFrame]) -> None:
        """
        Load KPI data from processing step

        Expected keys from SupplyKPIProcessor:
        - 'kpi1_weekly_supply'
        - 'kpi3_total_supply'
        - 'kpi4_growth_rate'

        Args:
            kpi_data: Dictionary of KPI DataFrames
        """
        self.kpis = kpi_data
        logger.info(f"Loaded {len(kpi_data)} Supply KPI datasets for minimal reporting")

    def build_metric_dict(self) -> Dict:
        """
        Build a small dictionary of metrics for LinkedIn post

        Returns:
            Dictionary with simple numeric/text values ready for carousel
        """
        kpi1 = self.kpis['kpi1_weekly_supply']
        kpi3 = self.kpis['kpi3_total_supply']

        latest_week = kpi1['week'].max()
        latest_kpi1 = kpi1[kpi1['week'] == latest_week]
        latest_kpi3 = kpi3[kpi3['week'] == latest_week]

        # Total LATAM supply (all tokens, all chains)
        total_latam_supply = float(latest_kpi3['total_supply_all_chains'].sum())

        # Week-over-week growth rate
        previous_week = sorted(kpi3['week'].unique())
        previous_week = previous_week[-2] if len(previous_week) >= 2 else None

        if previous_week is not None:
            previous_total = float(kpi3[kpi3['week'] == previous_week]['total_supply_all_chains'].sum())
            if previous_total > 0:
                supply_growth_wow = ((total_latam_supply - previous_total) / previous_total) * 100
            else:
                supply_growth_wow = 0.0
        else:
            supply_growth_wow = 0.0

        # Top tokens by total supply
        top_tokens = (
            latest_kpi3
            .sort_values('total_supply_all_chains', ascending=False)
            .head(self.top_n)
            [['stablecoin', 'total_supply_all_chains', 'market_share_pct']]
        )

        top_tokens_list = [
            {
                'stablecoin': row['stablecoin'],
                'supply': float(row['total_supply_all_chains']),
                'market_share_pct': float(row['market_share_pct'])
            }
            for _, row in top_tokens.iterrows()
        ]

        # Top chains by supply (using kpi1)
        chain_supply = (
            latest_kpi1.groupby('blockchain')['circulating_supply_tokens']
            .sum()
            .reset_index()
            .sort_values('circulating_supply_tokens', ascending=False)
        )

        top_chains_list = [
            {
                'blockchain': row['blockchain'],
                'supply': float(row['circulating_supply_tokens'])
            }
            for _, row in chain_supply.head(5).iterrows()
        ]

        metrics = {
            'week_label': latest_week.strftime(self.week_format),
            'week_date': latest_week.strftime(self.date_format),
            'total_latam_supply_tokens': total_latam_supply,
            'supply_growth_wow_pct': supply_growth_wow,
            'top_tokens_by_supply': top_tokens_list,
            'top_chains_by_supply': top_chains_list,
        }

        return metrics

    def print_for_linkedin(self, metrics: Dict) -> None:
        """
        Print ultra-compact lines for LinkedIn carousel template

        Args:
            metrics: Dictionary of calculated metrics
        """
        print("\n" + "=" * 65)
        print("LINKEDIN CAROUSEL METRICS - SUPPLY")
        print("=" * 65 + "\n")

        print(f"Week: {metrics['week_label']} ({metrics['week_date']})")
        print(f"Total LATAM stablecoins on-chain supply = {metrics['total_latam_supply_tokens']:,.0f} tokens")
        print(f"Week-over-week supply growth = {metrics['supply_growth_wow_pct']:+.1f}%")
        print("")

        print("Top tokens by on-chain supply:")
        for item in metrics['top_tokens_by_supply']:
            print(f"  - {item['stablecoin']}: {item['supply']:,.0f} tokens "
                  f"({item['market_share_pct']:.1f}% of LATAM supply)")

        print("")
        print("Top chains by stablecoin supply:")
        for item in metrics['top_chains_by_supply']:
            print(f"  - {item['blockchain']}: {item['supply']:,.0f} tokens")

        print("\n" + "=" * 65 + "\n")

    def generate_minimal_report(self, kpi_data: Dict[str, pd.DataFrame]) -> Dict:
        """
        Main entrypoint: build metrics + print minimal text

        Args:
            kpi_data: Dictionary of KPI DataFrames from processor

        Returns:
            Dictionary with domain and metrics for consolidation
        """
        self.load_kpis(kpi_data)
        metrics = self.build_metric_dict()
        self.print_for_linkedin(metrics)

        return {'domain': 'supply', 'metrics': metrics}


# ============================================================================
# 2. MINT/BURN REPORTER
# ============================================================================

class MinimalMintBurnReporter:
    """Generate compact mint/burn metrics for weekly content."""

    def __init__(self, config_path: str = 'config.yaml'):
        """
        Initialize reporter with config

        Args:
            config_path: Path to config.yaml
        """
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)

        self.week_format = self.config['reporting']['week_format']
        self.date_format = self.config['reporting']['date_format']
        self.top_n = self.config['reporting']['top_n_tokens']
        self.timestamp = datetime.now()

    def load_kpis(self, kpi_data: Dict[str, pd.DataFrame]) -> None:
        """
        Load KPI data from MintBurnKPIProcessor

        Expected keys:
        - 'mintburn_kpi2_weekly_aggregates'
        - 'mintburn_kpi3_net_issuance'

        Args:
            kpi_data: Dictionary of KPI DataFrames
        """
        self.kpis = kpi_data
        logger.info(f"Loaded {len(kpi_data)} Mint/Burn KPI datasets for minimal reporting")

    def build_metric_dict(self) -> Dict:
        """
        Build mint/burn metrics for LinkedIn

        Returns:
            Dictionary with mint/burn metrics
        """
        kpi2 = self.kpis['mintburn_kpi2_weekly_aggregates']
        kpi3 = self.kpis['mintburn_kpi3_net_issuance']

        latest_week = kpi2['week'].max()
        latest_kpi2 = kpi2[kpi2['week'] == latest_week]
        latest_kpi3 = kpi3[kpi3['week'] == latest_week]

        # Total net issuance across all tokens
        total_net_issuance = float(latest_kpi3['net_issuance'].sum())

        # Top expansion / contraction tokens
        top_expansion = (
            latest_kpi3
            .sort_values('net_issuance', ascending=False)
            .head(self.top_n)
        )

        top_contraction = (
            latest_kpi3
            .sort_values('net_issuance', ascending=True)
            .head(self.top_n)
        )

        expansion_list = [
            {
                'stablecoin': row['stablecoin'],
                'net_issuance': float(row['net_issuance'])
            }
            for _, row in top_expansion.iterrows() if row['net_issuance'] > 0
        ]

        contraction_list = [
            {
                'stablecoin': row['stablecoin'],
                'net_issuance': float(row['net_issuance'])
            }
            for _, row in top_contraction.iterrows() if row['net_issuance'] < 0
        ]

        metrics = {
            'week_label': latest_week.strftime(self.week_format),
            'week_date': latest_week.strftime(self.date_format),
            'total_net_issuance_tokens': total_net_issuance,
            'top_expansion_tokens': expansion_list,
            'top_contraction_tokens': contraction_list
        }

        return metrics

    def print_for_linkedin(self, metrics: Dict) -> None:
        """
        Print mint/burn metrics for LinkedIn

        Args:
            metrics: Dictionary of calculated metrics
        """
        print("\n" + "=" * 65)
        print("LINKEDIN CAROUSEL METRICS - MINT/BURN")
        print("=" * 65 + "\n")

        print(f"Week: {metrics['week_label']} ({metrics['week_date']})")
        print(f"Net on-chain issuance (mints - burns) = {metrics['total_net_issuance_tokens']:+,.0f} tokens")
        print("")

        if metrics['top_expansion_tokens']:
            print("Top expansion (net mints):")
            for item in metrics['top_expansion_tokens']:
                print(f"  - {item['stablecoin']}: +{item['net_issuance']:,.0f} tokens")

        if metrics['top_contraction_tokens']:
            print("\nTop contraction (net burns):")
            for item in metrics['top_contraction_tokens']:
                print(f"  - {item['stablecoin']}: {item['net_issuance']:,.0f} tokens")

        print("\n" + "=" * 65 + "\n")

    def generate_minimal_report(self, kpi_data: Dict[str, pd.DataFrame]) -> Dict:
        """
        Main entrypoint: build metrics + print minimal text

        Args:
            kpi_data: Dictionary of KPI DataFrames from processor

        Returns:
            Dictionary with domain and metrics for consolidation
        """
        self.load_kpis(kpi_data)
        metrics = self.build_metric_dict()
        self.print_for_linkedin(metrics)

        return {'domain': 'mint_burn', 'metrics': metrics}


# ============================================================================
# 3. DEX VOLUME REPORTER
# ============================================================================

class MinimalDexReporter:
    """Generate compact DEX volume metrics for weekly content."""

    def __init__(self, config_path: str = 'config.yaml'):
        """
        Initialize reporter with config

        Args:
            config_path: Path to config.yaml
        """
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)

        self.week_format = self.config['reporting']['week_format']
        self.date_format = self.config['reporting']['date_format']
        self.top_n = self.config['reporting']['top_n_tokens']
        self.timestamp = datetime.now()

    def load_kpis(self, kpi_data: Dict[str, pd.DataFrame]) -> None:
        """
        Load KPI data from DexVolumeKPIProcessor

        Expected keys:
        - 'dex_kpi2_weekly_volume'
        - 'dex_kpi3_token_breakdown'
        - 'dex_kpi4_wow_volume_change'

        Args:
            kpi_data: Dictionary of KPI DataFrames
        """
        self.kpis = kpi_data
        logger.info(f"Loaded {len(kpi_data)} DEX KPI datasets for minimal reporting")

    def build_metric_dict(self) -> Dict:
        """
        Build DEX volume metrics for LinkedIn

        Returns:
            Dictionary with DEX metrics
        """
        kpi2 = self.kpis['dex_kpi2_weekly_volume']
        kpi3 = self.kpis['dex_kpi3_token_breakdown']
        kpi4 = self.kpis['dex_kpi4_wow_volume_change']

        latest_week = kpi2['week'].max()
        latest_kpi2 = kpi2[kpi2['week'] == latest_week]
        latest_kpi3 = kpi3[kpi3['week'] == latest_week]
        latest_kpi4 = kpi4[kpi4['week'] == latest_week]

        # Total DEX volume across all tokens
        total_volume = float(latest_kpi2['volume_usd'].sum())

        # Top tokens by DEX volume
        top_tokens = (
            latest_kpi3
            .sort_values('total_volume_usd', ascending=False)
            .head(self.top_n)
        )

        top_tokens_list = [
            {
                'stablecoin': row['stablecoin'],
                'volume_usd': float(row['total_volume_usd']),
                'market_share_pct': float(row['market_share_pct'])
            }
            for _, row in top_tokens.iterrows()
        ]

        # Biggest WoW movers by volume (absolute %)
        movers = latest_kpi4.copy()
        movers['abs_volume_wow_pct'] = movers['volume_wow_pct'].abs()
        top_movers = (
            movers
            .dropna(subset=['volume_wow_pct'])
            .sort_values('abs_volume_wow_pct', ascending=False)
            .head(self.top_n)
        )

        movers_list = [
            {
                'stablecoin': row['stablecoin'],
                'blockchain': row['blockchain'],
                'volume_wow_pct': float(row['volume_wow_pct'])
            }
            for _, row in top_movers.iterrows()
        ]

        metrics = {
            'week_label': latest_week.strftime(self.week_format),
            'week_date': latest_week.strftime(self.date_format),
            'total_dex_volume_usd': total_volume,
            'top_tokens_by_dex_volume': top_tokens_list,
            'top_volume_movers': movers_list
        }

        return metrics

    def print_for_linkedin(self, metrics: Dict) -> None:
        """
        Print DEX volume metrics for LinkedIn

        Args:
            metrics: Dictionary of calculated metrics
        """
        print("\n" + "=" * 65)
        print("LINKEDIN CAROUSEL METRICS - DEX VOLUME")
        print("=" * 65 + "\n")

        print(f"Week: {metrics['week_label']} ({metrics['week_date']})")
        print(f"Total LATAM stablecoin DEX volume = ${metrics['total_dex_volume_usd']:,.0f}")
        print("")

        print("Top tokens by DEX volume:")
        for item in metrics['top_tokens_by_dex_volume']:
            print(f"  - {item['stablecoin']}: ${item['volume_usd']:,.0f} "
                  f"({item['market_share_pct']:.1f}% of LATAM DEX volume)")

        if metrics['top_volume_movers']:
            print("\nBiggest WoW movers by DEX volume:")
            for item in metrics['top_volume_movers']:
                sign = "+" if item['volume_wow_pct'] > 0 else ""
                print(f"  - {item['stablecoin']} on {item['blockchain']}: "
                      f"{sign}{item['volume_wow_pct']:.1f}% vs last week")

        print("\n" + "=" * 65 + "\n")

    def generate_minimal_report(self, kpi_data: Dict[str, pd.DataFrame]) -> Dict:
        """
        Main entrypoint: build metrics + print minimal text

        Args:
            kpi_data: Dictionary of KPI DataFrames from processor

        Returns:
            Dictionary with domain and metrics for consolidation
        """
        self.load_kpis(kpi_data)
        metrics = self.build_metric_dict()
        self.print_for_linkedin(metrics)

        return {'domain': 'dex_volume', 'metrics': metrics}


# ============================================================================
# 4. DEPEG MONITORING REPORTER
# ============================================================================

class MinimalDepegReporter:
    """Generate compact depeg/stability metrics for weekly content."""

    def __init__(self, config_path: str = 'config.yaml'):
        """
        Initialize reporter with config

        Args:
            config_path: Path to config.yaml
        """
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)

        self.date_format = self.config['reporting']['date_format']
        self.top_n = self.config['reporting']['top_n_tokens']
        self.timestamp = datetime.now()

    def load_kpis(self, kpi_data: Dict[str, pd.DataFrame]) -> None:
        """
        Load KPI data from DepegKPIProcessor

        Expected keys:
        - 'depeg_kpi2_volatility'
        - 'depeg_kpi3_depeg_events'
        - 'depeg_kpi4_stability_summary'

        Args:
            kpi_data: Dictionary of KPI DataFrames
        """
        self.kpis = kpi_data
        logger.info(f"Loaded {len(kpi_data)} Depeg KPI datasets for minimal reporting")

    def build_metric_dict(self) -> Dict:
        """
        Build depeg/stability metrics for LinkedIn

        Returns:
            Dictionary with depeg metrics
        """
        kpi2 = self.kpis['depeg_kpi2_volatility']
        kpi3 = self.kpis['depeg_kpi3_depeg_events']
        kpi4 = self.kpis['depeg_kpi4_stability_summary']

        latest_date = kpi2['price_date'].max()
        latest_vol = kpi2[kpi2['price_date'] == latest_date]
        latest_depeg = kpi3[kpi3['price_date'] == latest_date]
        latest_stab = kpi4[kpi4['week'] == kpi4['week'].max()]

        # Worst depeg today
        worst_depeg = latest_depeg.sort_values('depeg_pct', ascending=False).head(self.top_n)
        worst_list = [
            {
                'stablecoin': row['stablecoin'],
                'blockchain': row['blockchain'],
                'depeg_pct': float(row['depeg_pct']),
                'severity': str(row['depeg_severity'])
            }
            for _, row in worst_depeg.iterrows() if row['depeg_pct'] > 0
        ]

        # Most volatile over 30d
        most_volatile = latest_vol.sort_values('volatility_30d', ascending=False).head(self.top_n)
        volatile_list = [
            {
                'stablecoin': row['stablecoin'],
                'blockchain': row['blockchain'],
                'volatility_30d': float(row['volatility_30d']),
                'classification': row['volatility_classification']
            }
            for _, row in most_volatile.iterrows()
        ]

        # Overall stability snapshot (avg vol)
        avg_vol = float(latest_stab['avg_volatility'].mean()) if len(latest_stab) > 0 else 0.0

        metrics = {
            'date_label': latest_date.strftime(self.date_format),
            'avg_30d_volatility': avg_vol,
            'worst_depegs': worst_list,
            'most_volatile_tokens': volatile_list
        }

        return metrics

    def print_for_linkedin(self, metrics: Dict) -> None:
        """
        Print depeg/stability metrics for LinkedIn

        Args:
            metrics: Dictionary of calculated metrics
        """
        print("\n" + "=" * 65)
        print("LINKEDIN CAROUSEL METRICS - DEPEG/STABILITY")
        print("=" * 65 + "\n")

        print(f"Date: {metrics['date_label']}")
        print(f"Average 30-day volatility across LATAM stablecoins = {metrics['avg_30d_volatility']:.4f}")
        print("")

        if metrics['worst_depegs']:
            print("Worst depegs today (vs 1.0 peg):")
            for item in metrics['worst_depegs']:
                print(f"  - {item['stablecoin']} on {item['blockchain']}: "
                      f"{item['depeg_pct']:.2f}% ({item['severity']})")

        if metrics['most_volatile_tokens']:
            print("\nMost volatile over the last 30 days:")
            for item in metrics['most_volatile_tokens']:
                print(f"  - {item['stablecoin']} on {item['blockchain']}: "
                      f"vol={item['volatility_30d']:.4f} ({item['classification']})")

        print("\n" + "=" * 65 + "\n")

    def generate_minimal_report(self, kpi_data: Dict[str, pd.DataFrame]) -> Dict:
        """
        Main entrypoint: build metrics + print minimal text

        Args:
            kpi_data: Dictionary of KPI DataFrames from processor

        Returns:
            Dictionary with domain and metrics for consolidation
        """
        self.load_kpis(kpi_data)
        metrics = self.build_metric_dict()
        self.print_for_linkedin(metrics)

        return {'domain': 'depeg', 'metrics': metrics}


# ============================================================================
# JSON CONSOLIDATOR - Merge all domains into one visualization-ready file
# ============================================================================

def consolidate_reports_to_json(
        supply_report: Dict,
        mintburn_report: Dict,
        dex_report: Dict,
        depeg_report: Optional[Dict] = None,  # Made optional with default None
        output_dir: str = './data/reports'
) -> Path:
    """
    Consolidate all domain reports into single JSON for AI visualization

    Args:
        supply_report: Dict from MinimalSupplyReporter
        mintburn_report: Dict from MinimalMintBurnReporter
        dex_report: Dict from MinimalDexReporter
        depeg_report: Dict from MinimalDepegReporter (optional, can be None)
        output_dir: Directory to save consolidated JSON

    Returns:
        Path to consolidated JSON file
    """
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    # Extract week from supply report (all should have same week)
    week_label = supply_report['metrics']['week_label'].replace(' ', '_')

    # Determine which domains are included
    domains_included = ["supply", "mint_burn", "dex_volume"]
    if depeg_report is not None:
        domains_included.append("depeg")

    # Build consolidated structure optimized for visualization
    consolidated = {
        "report_metadata": {
            "report_type": "LATAM Stablecoin Weekly Report - Consolidated",
            "week": supply_report['metrics']['week_label'],
            "date_range": supply_report['metrics']['week_date'],
            "generated_at": datetime.now().isoformat(),
            "domains_included": domains_included
        },
        "executive_summary": {
            "total_supply_tokens": supply_report['metrics']['total_latam_supply_tokens'],
            "supply_growth_wow_pct": supply_report['metrics']['supply_growth_wow_pct'],
            "net_issuance_tokens": mintburn_report['metrics']['total_net_issuance_tokens'],
            "total_dex_volume_usd": dex_report['metrics']['total_dex_volume_usd'],
            "avg_volatility_30d": depeg_report['metrics']['avg_30d_volatility'] if depeg_report else None
        },
        "supply_metrics": {
            "total_supply": supply_report['metrics']['total_latam_supply_tokens'],
            "growth_wow_pct": supply_report['metrics']['supply_growth_wow_pct'],
            "top_tokens": supply_report['metrics']['top_tokens_by_supply'],
            "top_chains": supply_report['metrics']['top_chains_by_supply']
        },
        "mint_burn_metrics": {
            "net_issuance": mintburn_report['metrics']['total_net_issuance_tokens'],
            "expansion_tokens": mintburn_report['metrics']['top_expansion_tokens'],
            "contraction_tokens": mintburn_report['metrics']['top_contraction_tokens']
        },
        "dex_metrics": {
            "total_volume_usd": dex_report['metrics']['total_dex_volume_usd'],
            "top_tokens": dex_report['metrics']['top_tokens_by_dex_volume'],
            "volume_movers": dex_report['metrics']['top_volume_movers']
        },
        "narrative": {
            "overview": f"LATAM stablecoin ecosystem for week {supply_report['metrics']['week_label']} "
                        f"({supply_report['metrics']['week_date']}) shows "
                        f"{supply_report['metrics']['total_latam_supply_tokens']:,.0f} tokens in circulation "
                        f"with a {'positive' if supply_report['metrics']['supply_growth_wow_pct'] > 0 else 'negative'} "
                        f"growth of {supply_report['metrics']['supply_growth_wow_pct']:+.1f}% week-over-week.",
            "supply_trend": f"Supply {'expanded' if supply_report['metrics']['supply_growth_wow_pct'] > 0 else 'contracted'} "
                            f"by {abs(supply_report['metrics']['supply_growth_wow_pct']):.1f}% this week.",
            "issuance_trend": f"Net issuance was {mintburn_report['metrics']['total_net_issuance_tokens']:+,.0f} tokens, "
                              f"indicating {'expansion' if mintburn_report['metrics']['total_net_issuance_tokens'] > 0 else 'contraction'} "
                              f"in the market.",
            "dex_activity": f"DEX trading volume reached ${dex_report['metrics']['total_dex_volume_usd']:,.0f} "
                            f"across all LATAM stablecoins."
        }
    }

    # Add depeg metrics and narrative only if available
    if depeg_report is not None:
        consolidated["depeg_metrics"] = {
            "avg_volatility": depeg_report['metrics']['avg_30d_volatility'],
            "worst_depegs": depeg_report['metrics']['worst_depegs'],
            "most_volatile": depeg_report['metrics']['most_volatile_tokens']
        }
        consolidated["narrative"]["stability"] = (
            f"Average 30-day volatility stands at {depeg_report['metrics']['avg_30d_volatility']:.4f}, "
            f"with {len(depeg_report['metrics']['worst_depegs'])} tokens showing notable depeg events."
        )
    else:
        consolidated["depeg_metrics"] = None
        consolidated["narrative"]["stability"] = "Depeg monitoring data not available for this report."

    # Write consolidated JSON
    filename = f"latam_stablecoin_weekly_consolidated_{week_label}.json"
    filepath = output_path / filename

    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(consolidated, f, indent=2, ensure_ascii=False)

    logger.info(f"✓ Consolidated JSON saved: {filepath}")

    if depeg_report is None:
        logger.info("⚠️  Depeg metrics excluded from consolidated report (data not available)")

    return filepath
