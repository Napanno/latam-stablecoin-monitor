"""
Report Generator for LATAM Stablecoin Weekly Report v4.0

Generates consolidated JSON reports from processed KPI data across 3 domains

Version 4.0 Changes:
- Fixed all KPI file name mappings
- Fixed all column name references
- Added KPI 5 support (network health + liquidity analysis)
- Added cross-domain insights
- Added market health scoring
- Added token rankings WITH AGGREGATION FIX
- Added alert system
- Added time series trend analysis
"""

import json
import pandas as pd
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional
from utils.logger import get_logger

logger = get_logger(__name__)


class ReportGenerator:
    """
    Generates consolidated reports from KPI data across multiple domains

    Features:
    - Loads KPI CSVs from all 3 domains (Supply, Flows, DEX)
    - Creates consolidated JSON report with executive summary
    - Market health scoring and alerting
    - Token rankings and trend analysis WITH PROPER AGGREGATION
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
        logger.info(f"ReportGenerator v4.0 initialized: {self.output_dir}")

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

        # Extract week from data
        week = self._extract_week(supply_data, flows_data, dex_data)

        # Build executive summary
        executive_summary = self._build_executive_summary(supply_data, flows_data, dex_data)

        # Build cross-domain insights (NEW)
        cross_domain = self._build_cross_domain_insights(supply_data, flows_data, dex_data)

        # Build token rankings (FIXED WITH AGGREGATION)
        rankings = self._build_token_rankings(supply_data, flows_data, dex_data)

        # Build alerts (NEW)
        alerts = self._build_market_alerts(supply_data, flows_data, dex_data)

        # Calculate market health score (NEW)
        health_score = self._calculate_market_health_score(supply_data, flows_data, dex_data)

        # Build complete report structure
        report_data = {
            'report_metadata': {
                'report_type': 'LATAM Stablecoin Weekly Report - Consolidated',
                'report_version': '4.0',
                'week': week,
                'generated_at': datetime.now().isoformat(),
                'timestamp': timestamp,
                'domains_included': ['supply', 'flows', 'dex'],
                'total_kpis': len(supply_kpis) + len(flows_kpis) + len(dex_kpis),
                'kpi_count_by_domain': {
                    'supply': len(supply_kpis),
                    'flows': len(flows_kpis),
                    'dex': len(dex_kpis)
                }
            },
            'executive_summary': executive_summary,
            'market_health': {
                'overall_score': health_score,
                'rating': self._score_to_rating(health_score),
                'alerts': alerts
            },
            'token_rankings': rankings,
            'cross_domain_insights': cross_domain,
            'domains': {
                'supply': {
                    'description': 'Token supply metrics - mint/burn tracking',
                    'kpis': supply_data
                },
                'flows': {
                    'description': 'On-chain flow metrics - transfer activity and network health',
                    'kpis': flows_data
                },
                'dex': {
                    'description': 'DEX trading metrics - volume, liquidity, and market sentiment',
                    'kpis': dex_data
                }
            }
        }

        # Backward compatibility aliases
        report_data['metadata'] = report_data['report_metadata']
        report_data['weekly_summary'] = report_data['executive_summary']
        report_data['insights'] = report_data['cross_domain_insights']

        # Generate filename
        report_filename = self.output_dir / f"consolidated_report_{week}_{timestamp}.json"

        # Save to JSON with pretty formatting
        with open(report_filename, 'w', encoding='utf-8') as f:
            json.dump(report_data, f, indent=2, ensure_ascii=False, default=str)

        logger.info(f"✓ Consolidated report saved: {report_filename}")
        logger.info(f"  - Week: {week}")
        logger.info(f"  - Market Health Score: {health_score}/100 ({self._score_to_rating(health_score)})")
        logger.info(f"  - Alerts: {len(alerts)}")
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
        Build executive summary with key metrics (FIXED)

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
        """Extract key supply metrics for executive summary (FIXED)"""
        summary = {
            'total_mints_usd': None,
            'total_burns_usd': None,
            'net_supply_change_usd': None,
            'tokens_tracked': None,
            'top_token_by_supply_change': None,
            'avg_mint_event_size': None
        }

        try:
            # FIXED: Use actual KPI name - supply_kpi2_issuance_rate
            if 'issuance_rate' in supply_data:
                kpi_data = supply_data['issuance_rate']['data']
                if kpi_data:
                    # Aggregate across all tokens for latest week
                    summary['total_mints_usd'] = sum(row.get('total_mints_usd', 0) for row in kpi_data)
                    summary['total_burns_usd'] = sum(row.get('total_burns_usd', 0) for row in kpi_data)
                    summary['net_supply_change_usd'] = sum(row.get('net_issuance_usd', 0) for row in kpi_data)
                    summary['tokens_tracked'] = len(set(row.get('symbol') for row in kpi_data if row.get('symbol')))

                    # Find top token by net issuance
                    if kpi_data:
                        top_token = max(kpi_data, key=lambda x: abs(x.get('net_issuance_usd', 0)))
                        summary['top_token_by_supply_change'] = {
                            'symbol': top_token.get('symbol'),
                            'net_change_usd': top_token.get('net_issuance_usd')
                        }

            # FIXED: Use actual KPI name - supply_kpi3_token_metrics
            if 'token_metrics' in supply_data:
                kpi_data = supply_data['token_metrics']['data']
                if kpi_data:
                    # Calculate average mint event size
                    avg_sizes = [row.get('mint_event_avg_size', 0) for row in kpi_data if
                                 row.get('mint_event_avg_size', 0) > 0]
                    if avg_sizes:
                        summary['avg_mint_event_size'] = sum(avg_sizes) / len(avg_sizes)

        except Exception as e:
            logger.debug(f"Could not extract supply summary: {e}")

        return summary

    def _extract_flows_summary(self, flows_data):
        """Extract key flows metrics for executive summary (FIXED)"""
        summary = {
            'total_mints_usd': None,
            'total_burns_usd': None,
            'net_issuance_usd': None,
            'mint_count': None,
            'burn_count': None,
            'total_transfers': None,
            'unique_senders': None,
            'unique_receivers': None,
            'avg_whale_concentration': None
        }

        try:
            # FIXED: Use correct column names - flows_kpi3_net_issuance
            if 'net_issuance' in flows_data:
                kpi_data = flows_data['net_issuance']['data']
                if kpi_data:
                    summary['total_mints_usd'] = sum(row.get('mint_volume_usd', 0) for row in kpi_data)
                    summary['total_burns_usd'] = sum(row.get('burn_volume_usd', 0) for row in kpi_data)
                    summary['net_issuance_usd'] = sum(row.get('net_issuance_usd', 0) for row in kpi_data)
                    summary['mint_count'] = sum(row.get('mint_count', 0) for row in kpi_data)
                    summary['burn_count'] = sum(row.get('burn_count', 0) for row in kpi_data)

            # NEW: Network health metrics from flows_kpi5_network_health
            if 'network_health' in flows_data:
                kpi_data = flows_data['network_health']['data']
                if kpi_data:
                    summary['total_transfers'] = sum(row.get('transfer_count', 0) for row in kpi_data)
                    summary['unique_senders'] = sum(row.get('unique_senders', 0) for row in kpi_data)
                    summary['unique_receivers'] = sum(row.get('unique_receivers', 0) for row in kpi_data)

                    # Calculate average whale concentration
                    concentrations = [row.get('whale_concentration_ratio', 0) for row in kpi_data if
                                      row.get('whale_concentration_ratio', 0) > 0]
                    if concentrations:
                        summary['avg_whale_concentration'] = sum(concentrations) / len(concentrations)

        except Exception as e:
            logger.debug(f"Could not extract flows summary: {e}")

        return summary

    def _extract_dex_summary(self, dex_data):
        """Extract key DEX metrics for executive summary (FIXED)"""
        summary = {
            'total_volume_usd': None,
            'total_trades': None,
            'avg_buy_pressure_pct': None,
            'top_token_by_volume': None,
            'top_blockchain_by_volume': None,
            'unique_dexs_used': None,
            'max_whale_trade_usd': None
        }

        try:
            # FIXED: Use correct KPI name - dex_kpi2_weekly_aggregates
            if 'weekly_aggregates' in dex_data:
                kpi_data = dex_data['weekly_aggregates']['data']
                if kpi_data:
                    summary['total_volume_usd'] = sum(row.get('volume_usd', 0) for row in kpi_data)
                    summary['total_trades'] = sum(row.get('trade_count', 0) for row in kpi_data)

                    # Calculate average buy pressure
                    pressures = [row.get('buy_pressure_pct', 0) for row in kpi_data if
                                 row.get('buy_pressure_pct', 0) > 0]
                    if pressures:
                        summary['avg_buy_pressure_pct'] = sum(pressures) / len(pressures)

                    # Find top token by volume
                    if kpi_data:
                        top_token = max(kpi_data, key=lambda x: x.get('volume_usd', 0))
                        summary['top_token_by_volume'] = {
                            'symbol': top_token.get('token_symbol'),
                            'volume_usd': top_token.get('volume_usd')
                        }

                        # Find top blockchain
                        top_chain = max(kpi_data, key=lambda x: x.get('volume_usd', 0))
                        summary['top_blockchain_by_volume'] = top_chain.get('blockchain')

            # NEW: Liquidity metrics from dex_kpi5_liquidity_analysis
            if 'liquidity_analysis' in dex_data:
                kpi_data = dex_data['liquidity_analysis']['data']
                if kpi_data:
                    summary['unique_dexs_used'] = sum(row.get('unique_dex_count', 0) for row in kpi_data)
                    summary['max_whale_trade_usd'] = max((row.get('max_trade_usd', 0) for row in kpi_data), default=0)

        except Exception as e:
            logger.debug(f"Could not extract DEX summary: {e}")

        return summary

    def _build_token_rankings(self, supply_data, flows_data, dex_data):
        """
        Build token rankings WITH BLOCKCHAIN RANKINGS (v4.1)
        Top tokens and blockchains by various metrics
        """
        rankings = {
            'by_trading_volume': [],
            'by_supply_growth': [],
            'by_network_activity': [],
            'by_liquidity': [],
            'by_blockchain_volume': []  # ⭐ NEW
        }

        try:
            # ================================================================
            # TOP BY TRADING VOLUME (Aggregate by token symbol)
            # ================================================================
            if 'token_trading' in dex_data:
                kpi_data = dex_data['token_trading']['data']
                if kpi_data:
                    # Aggregate by symbol (sum volumes across blockchains/days)
                    token_agg = {}
                    for row in kpi_data:
                        symbol = row.get('token_symbol') or row.get('symbol', 'Unknown')
                        if symbol not in token_agg:
                            token_agg[symbol] = {'volume_usd': 0, 'trade_count': 0}
                        token_agg[symbol]['volume_usd'] += row.get('volume_usd', 0)
                        token_agg[symbol]['trade_count'] += row.get('trade_count', 0)

                    # Calculate market share
                    total_vol = sum(t['volume_usd'] for t in token_agg.values())

                    # Sort and format
                    sorted_tokens = sorted(token_agg.items(),
                                           key=lambda x: x[1]['volume_usd'],
                                           reverse=True)[:5]

                    rankings['by_trading_volume'] = [
                        {
                            'rank': idx + 1,
                            'symbol': symbol,
                            'volume_usd': data['volume_usd'],
                            'market_share_pct': round((data['volume_usd'] / total_vol * 100), 1) if total_vol > 0 else 0
                        }
                        for idx, (symbol, data) in enumerate(sorted_tokens)
                    ]

            # ================================================================
            # TOP BY SUPPLY GROWTH (Aggregate by token symbol)
            # ================================================================
            if 'wow_supply_change' in supply_data:
                kpi_data = supply_data['wow_supply_change']['data']
                if kpi_data:
                    # Aggregate by symbol
                    token_agg = {}
                    for row in kpi_data:
                        symbol = row.get('symbol', 'Unknown')
                        wow_pct = row.get('net_wow_pct', 0)
                        net_issuance = row.get('net_issuance_usd', 0)

                        if symbol not in token_agg:
                            token_agg[symbol] = {
                                'net_wow_pct': wow_pct,
                                'net_issuance_usd': net_issuance,
                                'count': 1
                            }
                        else:
                            # Average the WoW % and sum issuance
                            token_agg[symbol]['net_wow_pct'] = (
                                    (token_agg[symbol]['net_wow_pct'] * token_agg[symbol]['count'] + wow_pct) /
                                    (token_agg[symbol]['count'] + 1)
                            )
                            token_agg[symbol]['net_issuance_usd'] += net_issuance
                            token_agg[symbol]['count'] += 1

                    # Filter positive growth only
                    growth_tokens = [(s, d) for s, d in token_agg.items() if d['net_wow_pct'] > 0]

                    # Sort by growth %
                    sorted_growth = sorted(growth_tokens,
                                           key=lambda x: x[1]['net_wow_pct'],
                                           reverse=True)[:5]

                    rankings['by_supply_growth'] = [
                        {
                            'rank': idx + 1,
                            'symbol': symbol,
                            'growth_wow_pct': round(data['net_wow_pct'], 1),
                            'net_issuance_usd': round(data['net_issuance_usd'], 0)
                        }
                        for idx, (symbol, data) in enumerate(sorted_growth)
                    ]

            # ================================================================
            # TOP BY NETWORK ACTIVITY (Aggregate by token symbol)
            # ================================================================
            if 'network_health' in flows_data:
                kpi_data = flows_data['network_health']['data']
                if kpi_data:
                    # Aggregate by symbol
                    token_agg = {}
                    for row in kpi_data:
                        symbol = row.get('symbol', 'Unknown')
                        if symbol not in token_agg:
                            token_agg[symbol] = {
                                'transfer_count': 0,
                                'unique_senders': 0,
                                'unique_receivers': 0
                            }
                        token_agg[symbol]['transfer_count'] += row.get('transfer_count', 0)
                        token_agg[symbol]['unique_senders'] += row.get('unique_senders', 0)
                        token_agg[symbol]['unique_receivers'] += row.get('unique_receivers', 0)

                    # Sort by transfer count
                    sorted_activity = sorted(token_agg.items(),
                                             key=lambda x: x[1]['transfer_count'],
                                             reverse=True)[:5]

                    rankings['by_network_activity'] = [
                        {
                            'rank': idx + 1,
                            'symbol': symbol,
                            'transfer_count': data['transfer_count'],
                            'unique_wallets': data['unique_senders'] + data['unique_receivers']
                        }
                        for idx, (symbol, data) in enumerate(sorted_activity)
                    ]

            # ================================================================
            # TOP BY LIQUIDITY (Aggregate by token symbol)
            # ================================================================
            if 'liquidity_analysis' in dex_data:
                kpi_data = dex_data['liquidity_analysis']['data']
                if kpi_data:
                    # Aggregate by symbol
                    token_agg = {}
                    for row in kpi_data:
                        symbol = row.get('token_symbol') or row.get('symbol', 'Unknown')
                        if symbol not in token_agg:
                            token_agg[symbol] = {
                                'unique_dex_count': 0,
                                'total_volume_usd': 0
                            }
                        token_agg[symbol]['unique_dex_count'] += row.get('unique_dex_count', 0)
                        token_agg[symbol]['total_volume_usd'] += row.get('total_volume_usd', 0)

                    # Calculate avg volume per DEX
                    for symbol, data in token_agg.items():
                        if data['unique_dex_count'] > 0:
                            data['avg_volume_per_dex'] = data['total_volume_usd'] / data['unique_dex_count']
                        else:
                            data['avg_volume_per_dex'] = 0

                    # Sort by DEX count (higher = better distribution)
                    sorted_liquidity = sorted(token_agg.items(),
                                              key=lambda x: (x[1]['unique_dex_count'], x[1]['total_volume_usd']),
                                              reverse=True)[:5]

                    rankings['by_liquidity'] = [
                        {
                            'rank': idx + 1,
                            'symbol': symbol,
                            'unique_dex_count': data['unique_dex_count'],
                            'avg_volume_per_dex': round(data['avg_volume_per_dex'], 0)
                        }
                        for idx, (symbol, data) in enumerate(sorted_liquidity)
                    ]

            # ================================================================
            # ⭐ NEW: TOP BY BLOCKCHAIN VOLUME
            # ================================================================
            if 'weekly_aggregates' in dex_data:
                kpi_data = dex_data['weekly_aggregates']['data']
                if kpi_data:
                    # Aggregate by blockchain (sum volumes across tokens)
                    blockchain_agg = {}
                    for row in kpi_data:
                        blockchain = row.get('blockchain', 'Unknown')
                        if blockchain not in blockchain_agg:
                            blockchain_agg[blockchain] = {
                                'volume_usd': 0,
                                'trade_count': 0,
                                'unique_tokens': set()
                            }
                        blockchain_agg[blockchain]['volume_usd'] += row.get('volume_usd', 0)
                        blockchain_agg[blockchain]['trade_count'] += row.get('trade_count', 0)

                        # Track unique tokens per blockchain
                        token_symbol = row.get('token_symbol') or row.get('symbol', 'Unknown')
                        blockchain_agg[blockchain]['unique_tokens'].add(token_symbol)

                    # Convert set to count
                    for blockchain, data in blockchain_agg.items():
                        data['unique_tokens'] = len(data['unique_tokens'])

                    # Calculate market share
                    total_vol = sum(b['volume_usd'] for b in blockchain_agg.values())

                    # Sort and format top 5
                    sorted_blockchains = sorted(blockchain_agg.items(),
                                                key=lambda x: x[1]['volume_usd'],
                                                reverse=True)[:5]

                    rankings['by_blockchain_volume'] = [
                        {
                            'rank': idx + 1,
                            'blockchain': blockchain,
                            'volume_usd': data['volume_usd'],
                            'market_share_pct': round((data['volume_usd'] / total_vol * 100),
                                                      1) if total_vol > 0 else 0,
                            'trade_count': data['trade_count'],
                            'unique_tokens': data['unique_tokens']
                        }
                        for idx, (blockchain, data) in enumerate(sorted_blockchains)
                    ]

        except Exception as e:
            logger.debug(f"Could not build token rankings: {e}")

        return rankings

    def _build_cross_domain_insights(self, supply_data, flows_data, dex_data):
        """
        Build cross-domain insights (NEW)
        Combines metrics across domains for deeper analysis
        """
        insights = {
            'supply_vs_trading': {},
            'network_activity': {},
            'liquidity_health': {}
        }

        try:
            supply_summary = self._extract_supply_summary(supply_data)
            flows_summary = self._extract_flows_summary(flows_data)
            dex_summary = self._extract_dex_summary(dex_data)

            # Supply vs Trading correlation
            net_supply = supply_summary.get('net_supply_change_usd', 0) or 0
            trading_volume = dex_summary.get('total_volume_usd', 0) or 0

            insights['supply_vs_trading'] = {
                'net_supply_change_usd': net_supply,
                'trading_volume_usd': trading_volume,
                'supply_to_volume_ratio': round(net_supply / trading_volume * 100, 2) if trading_volume > 0 else 0,
                'interpretation': self._interpret_supply_vs_volume(net_supply, trading_volume)
            }

            # Network activity health
            unique_senders = flows_summary.get('unique_senders', 0) or 0
            unique_receivers = flows_summary.get('unique_receivers', 0) or 0
            total_transfers = flows_summary.get('total_transfers', 0) or 0

            insights['network_activity'] = {
                'total_unique_wallets': unique_senders + unique_receivers,
                'receiver_sender_ratio': round(unique_receivers / unique_senders, 2) if unique_senders > 0 else 0,
                'avg_transfers_per_wallet': round(total_transfers / (unique_senders + unique_receivers), 2) if (
                                                                                                                       unique_senders + unique_receivers) > 0 else 0,
                'network_state': self._classify_network_state(unique_receivers, unique_senders)
            }

            # Liquidity health
            buy_pressure = dex_summary.get('avg_buy_pressure_pct', 0) or 0
            whale_concentration = flows_summary.get('avg_whale_concentration', 0) or 0
            unique_dexs = dex_summary.get('unique_dexs_used', 0) or 0

            insights['liquidity_health'] = {
                'buy_pressure_pct': buy_pressure,
                'whale_concentration_ratio': whale_concentration,
                'dex_fragmentation_score': unique_dexs,
                'liquidity_rating': self._rate_liquidity(buy_pressure, whale_concentration, unique_dexs)
            }

        except Exception as e:
            logger.debug(f"Could not build cross-domain insights: {e}")

        return insights

    def _build_market_alerts(self, supply_data, flows_data, dex_data):
        """
        Build market alerts (NEW)
        Flag concerning patterns
        """
        alerts = []

        try:
            # Whale dump alert
            if 'liquidity_analysis' in dex_data:
                kpi_data = dex_data['liquidity_analysis']['data']
                for token in kpi_data:
                    whale_ratio = token.get('whale_concentration_ratio', 0)
                    buy_freq = token.get('buy_frequency_pct', 50)
                    if whale_ratio > 200 and buy_freq < 40:
                        alerts.append({
                            'severity': 'HIGH',
                            'type': 'WHALE_DUMP_RISK',
                            'symbol': token.get('token_symbol'),
                            'blockchain': token.get('blockchain'),
                            'details': f"High whale concentration ({whale_ratio:.1f}x) + sell pressure ({100 - buy_freq:.1f}%)",
                            'recommendation': 'Monitor large holder activity'
                        })

            # Illiquid token alert
            if 'liquidity_analysis' in dex_data:
                kpi_data = dex_data['liquidity_analysis']['data']
                for token in kpi_data:
                    dex_count = token.get('unique_dex_count', 0)
                    volume = token.get('total_volume_usd', 0)
                    if dex_count <= 1 and volume > 0:
                        alerts.append({
                            'severity': 'MEDIUM',
                            'type': 'LOW_LIQUIDITY',
                            'symbol': token.get('token_symbol'),
                            'blockchain': token.get('blockchain'),
                            'details': f"Trading on only {dex_count} DEX with ${volume:,.0f} volume",
                            'recommendation': 'Expect high slippage'
                        })

            # High burn alert
            if 'net_issuance' in flows_data:
                kpi_data = flows_data['net_issuance']['data']
                for token in kpi_data:
                    mints = token.get('mint_volume_usd', 0)
                    burns = token.get('burn_volume_usd', 0)
                    if burns > mints * 2 and burns > 10000:  # Burns > 2x mints and > $10K
                        alerts.append({
                            'severity': 'MEDIUM',
                            'type': 'HIGH_BURN_RATE',
                            'symbol': token.get('symbol'),
                            'details': f"Burns (${burns:,.0f}) > 2x mints (${mints:,.0f})",
                            'recommendation': 'Supply contracting - check redemption demand'
                        })

            # Growth signal (positive alert)
            if 'network_health' in flows_data:
                kpi_data = flows_data['network_health']['data']
                for token in kpi_data:
                    receiver_sender = token.get('receiver_sender_ratio', 0)
                    if receiver_sender > 1.5:
                        alerts.append({
                            'severity': 'INFO',
                            'type': 'GROWTH_SIGNAL',
                            'symbol': token.get('symbol'),
                            'blockchain': token.get('blockchain'),
                            'details': f"Receivers ({receiver_sender:.2f}x senders) - network expanding",
                            'recommendation': 'Positive adoption indicator'
                        })

        except Exception as e:
            logger.debug(f"Could not build market alerts: {e}")

        # Sort by severity
        severity_order = {'HIGH': 0, 'MEDIUM': 1, 'INFO': 2}
        alerts.sort(key=lambda x: severity_order.get(x['severity'], 999))

        return alerts

    def _calculate_market_health_score(self, supply_data, flows_data, dex_data):
        """
        Calculate overall market health score (NEW)
        Score 0-100 based on multiple factors
        """
        try:
            score_components = []

            flows_summary = self._extract_flows_summary(flows_data)
            dex_summary = self._extract_dex_summary(dex_data)

            # Component 1: Buy pressure (0-30 points)
            buy_pressure = dex_summary.get('avg_buy_pressure_pct', 50) or 50
            buy_score = min(30, (buy_pressure / 50) * 30)  # 50% = full points
            score_components.append(buy_score)

            # Component 2: Network decentralization (0-25 points)
            whale_concentration = flows_summary.get('avg_whale_concentration', 100) or 100
            # Lower concentration = better (inverse scoring)
            decentral_score = max(0, 25 - (whale_concentration / 10))
            score_components.append(decentral_score)

            # Component 3: Liquidity distribution (0-25 points)
            unique_dexs = dex_summary.get('unique_dexs_used', 0) or 0
            liquidity_score = min(25, unique_dexs * 5)  # 5 points per DEX, max 25
            score_components.append(liquidity_score)

            # Component 4: Network growth (0-20 points)
            unique_receivers = flows_summary.get('unique_receivers', 0) or 0
            unique_senders = flows_summary.get('unique_senders', 0) or 0
            if unique_senders > 0:
                receiver_ratio = unique_receivers / unique_senders
                growth_score = min(20, receiver_ratio * 10)  # 2.0 ratio = full points
            else:
                growth_score = 10  # Neutral if no data
            score_components.append(growth_score)

            # Total score
            total_score = sum(score_components)
            logger.debug(
                f"Health score components: buy={buy_score:.1f}, decentral={decentral_score:.1f}, "
                f"liquidity={liquidity_score:.1f}, growth={growth_score:.1f}"
            )

            return round(total_score, 1)

        except Exception as e:
            logger.debug(f"Could not calculate market health score: {e}")
            return 50.0  # Neutral fallback

    # =========================================================================
    # HELPER METHODS
    # =========================================================================

    def _score_to_rating(self, score):
        """Convert numeric score to letter rating"""
        if score >= 80:
            return 'A - Excellent'
        elif score >= 65:
            return 'B - Good'
        elif score >= 50:
            return 'C - Fair'
        elif score >= 35:
            return 'D - Poor'
        else:
            return 'F - Critical'

    def _interpret_supply_vs_volume(self, net_supply, trading_volume):
        """Interpret relationship between supply and trading volume"""
        if trading_volume == 0:
            return "No trading activity"

        ratio = abs(net_supply) / trading_volume

        if ratio < 0.05:
            return "Stable supply, high trading activity (healthy)"
        elif ratio < 0.15:
            return "Moderate supply changes relative to volume"
        else:
            return "High supply volatility relative to trading (watch for instability)"

    def _classify_network_state(self, receivers, senders):
        """Classify network expansion/contraction state"""
        if senders == 0:
            return "Insufficient data"

        ratio = receivers / senders

        if ratio > 1.3:
            return "Expanding (more receivers than senders)"
        elif ratio > 0.7:
            return "Balanced (equal distribution activity)"
        else:
            return "Contracting (more senders than receivers)"

    def _rate_liquidity(self, buy_pressure, whale_concentration, dex_count):
        """Rate overall liquidity health"""
        score = 0

        # Buy pressure (0-40 points)
        if buy_pressure >= 55:
            score += 40
        elif buy_pressure >= 45:
            score += 30
        elif buy_pressure >= 40:
            score += 20
        else:
            score += 10

        # Whale concentration (0-30 points) - lower is better
        if whale_concentration < 10:
            score += 30
        elif whale_concentration < 50:
            score += 20
        elif whale_concentration < 100:
            score += 10

        # DEX fragmentation (0-30 points)
        score += min(30, dex_count * 6)

        if score >= 80:
            return "Excellent"
        elif score >= 60:
            return "Good"
        elif score >= 40:
            return "Fair"
        else:
            return "Poor"


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
    import sys
    from utils.logger import get_logger

    logger = get_logger(__name__)
    logger.info("Testing ReportGenerator v4.0...")

    # Sample KPI paths (adjust to your actual file structure)
    sample_supply_kpis = {
        'supply_change': './data/kpi/supply_kpi1_supply_change_2026-W01_20260106.csv',
        'issuance_rate': './data/kpi/supply_kpi2_issuance_rate_2026-W01_20260106.csv',
        'token_metrics': './data/kpi/supply_kpi3_token_metrics_2026-W01_20260106.csv',
        'wow_supply_change': './data/kpi/supply_kpi4_wow_supply_change_2026-W01_20260106.csv',
    }

    sample_flows_kpis = {
        'daily_activity': './data/kpi/flows_kpi1_daily_activity_2026-W01_20260106.csv',
        'weekly_aggregates': './data/kpi/flows_kpi2_weekly_aggregates_2026-W01_20260106.csv',
        'net_issuance': './data/kpi/flows_kpi3_net_issuance_2026-W01_20260106.csv',
        'wow_change': './data/kpi/flows_kpi4_wow_change_2026-W01_20260106.csv',
        'network_health': './data/kpi/flows_kpi5_network_health_2026-W01_20260106.csv',
    }

    sample_dex_kpis = {
        'daily_volume': './data/kpi/dex_kpi1_daily_volume_2026-W01_20260106.csv',
        'weekly_aggregates': './data/kpi/dex_kpi2_weekly_aggregates_2026-W01_20260106.csv',
        'token_trading': './data/kpi/dex_kpi3_token_trading_2026-W01_20260106.csv',
        'wow_change': './data/kpi/dex_kpi4_wow_change_2026-W01_20260106.csv',
        'liquidity_analysis': './data/kpi/dex_kpi5_liquidity_analysis_2026-W01_20260106.csv',
    }

    # Generate report
    generator = ReportGenerator()
    report_path = generator.generate_consolidated_report(
        supply_kpis=sample_supply_kpis,
        flows_kpis=sample_flows_kpis,
        dex_kpis=sample_dex_kpis,
        timestamp='20260106130000'
    )

    logger.info(f"✅ Test complete. Report: {report_path}")
