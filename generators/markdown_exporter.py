"""
Markdown Report Exporter for LATAM Stablecoin Weekly Report

Generates LinkedIn-ready Markdown reports from JSON data
Optimized for screenshot tools like Carbon.now.sh or Canva
All content in table format for consistent visual presentation
"""

import json
from pathlib import Path
from typing import Dict, List
from datetime import datetime


class MarkdownExporter:
    """Export JSON reports to LinkedIn-ready Markdown format (table-only)"""

    def __init__(self, output_dir='./data/reports'):
        """
        Initialize Markdown exporter

        Args:
            output_dir: Directory for markdown output
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def export_report(self, json_report_path: Path) -> Path:
        """
        Export JSON report to Markdown

        Args:
            json_report_path: Path to JSON report file

        Returns:
            Path to generated markdown file
        """
        # Load JSON report
        with open(json_report_path, 'r', encoding='utf-8') as f:
            report_data = json.load(f)

        # Generate markdown content
        md_content = self._build_markdown(report_data)

        # Save markdown file
        week = report_data.get('metadata', {}).get('week', 'unknown')
        timestamp = report_data.get('metadata', {}).get('timestamp', 'unknown')
        md_filename = self.output_dir / f"linkedin_report_{week}_{timestamp}.md"

        with open(md_filename, 'w', encoding='utf-8') as f:
            f.write(md_content)

        print(f"✅ Markdown report exported: {md_filename}")
        print(f"   📄 File size: {md_filename.stat().st_size / 1024:.1f} KB")

        return md_filename

    def _build_markdown(self, data: Dict) -> str:
        """Build complete markdown content"""
        sections = []

        # Slide 1: Cover
        sections.append(self._build_cover(data))

        # Slide 2: Executive Summary
        sections.append(self._build_executive_summary(data))

        # Slide 3: Top Tokens by Volume
        sections.append(self._build_top_tokens_volume(data))

        # ⭐ NEW: Slide 4: Top Blockchains by Volume
        sections.append(self._build_top_blockchains(data))

        # Slide 5: Top Tokens by Growth
        sections.append(self._build_top_tokens_growth(data))

        # Slide 6: Market Health Breakdown
        sections.append(self._build_market_health(data))

        # Slide 7: Network Activity
        sections.append(self._build_network_activity(data))

        # Slide 8: Supply vs Trading
        sections.append(self._build_supply_vs_trading(data))

        # Slide 9: Market Alerts
        sections.append(self._build_alerts(data))

        # Slide 10: Cross-Domain Insights
        sections.append(self._build_cross_domain_insights(data))

        # Slide 11: Methodology & Data
        sections.append(self._build_methodology(data))

        return "\n\n---\n\n".join(sections)

    def _build_cover(self, data: Dict) -> str:
        """Slide 1: Cover page - Table format"""
        metadata = data.get('metadata', {})
        health = data.get('market_health', {})

        week = metadata.get('week', 'N/A')
        score = health.get('overall_score', 'N/A')
        rating = health.get('rating', 'N/A')

        return f"""# 📊 LATAM Stablecoin Weekly Report

| **Report Details** | |
|:-------------------|:------|
| **Week** | {week} |
| **Market Health Score** | {score}/100 |
| **Rating** | {rating} |
| **Generated** | {datetime.now().strftime('%B %d, %Y')} |

*Comprehensive on-chain analysis of LATAM stablecoin markets*"""

    def _build_executive_summary(self, data: Dict) -> str:
        """Slide 2: Executive Summary - Table format"""
        summary = data.get('weekly_summary', {})
        dex = summary.get('dex_metrics', {})
        flows = summary.get('flows_metrics', {})
        supply = summary.get('supply_metrics', {})

        total_volume = dex.get('total_volume_usd', 0) or 0
        total_trades = dex.get('total_trades', 0) or 0
        net_issuance = flows.get('net_issuance_usd', 0) or 0
        buy_pressure = dex.get('avg_buy_pressure_pct', 0) or 0
        tokens_tracked = supply.get('tokens_tracked', 0) or 0

        return f"""## 💰 Executive Summary

| **Metric** | **Value** |
|:-----------|----------:|
| **Total Trading Volume** | ${total_volume:,.0f} |
| **Total Trades** | {total_trades:,} |
| **Net Issuance (Mints - Burns)** | ${net_issuance:,.0f} |
| **Buy Pressure** | {buy_pressure:.1f}% |
| **Tokens Tracked** | {tokens_tracked} |"""

    def _build_top_tokens_volume(self, data: Dict) -> str:
        """Slide 3: Top Tokens by Trading Volume - Table format"""
        rankings = data.get('token_rankings', {})
        top_tokens = rankings.get('by_trading_volume', [])[:5]

        if not top_tokens:
            return "## 🏆 Top Tokens by Trading Volume\n\n*No data available*"

        lines = ["## 🏆 Top Tokens by Trading Volume\n"]
        lines.append("| **Rank** | **Token** | **Volume (USD)** | **Market Share** |")
        lines.append("|:--------:|:----------|:----------------:|:----------------:|")

        for idx, token in enumerate(top_tokens, 1):
            emoji = {1: "🥇", 2: "🥈", 3: "🥉", 4: "4️⃣", 5: "5️⃣"}.get(idx, "•")
            symbol = token.get('symbol', 'N/A')
            volume = token.get('volume_usd', 0) or 0
            share = token.get('market_share_pct', 0) or 0

            lines.append(f"| {emoji} | **{symbol}** | ${volume:,.0f} | {share:.1f}% |")

        return "\n".join(lines)

    def _build_top_tokens_growth(self, data: Dict) -> str:
        """Slide 4: Top Tokens by Supply Growth - Table format"""
        rankings = data.get('token_rankings', {})
        top_growth = rankings.get('by_supply_growth', [])[:5]

        if not top_growth:
            return "## 📈 Fastest Growing Tokens\n\n*No positive growth this week*"

        lines = ["## 📈 Fastest Growing Tokens (Week-over-Week)\n"]
        lines.append("| **Rank** | **Token** | **Growth %** | **Net Issuance (USD)** |")
        lines.append("|:--------:|:----------|:------------:|:----------------------:|")

        for idx, token in enumerate(top_growth, 1):
            emoji = {1: "🚀", 2: "⬆️", 3: "📊", 4: "📈", 5: "🔼"}.get(idx, "•")
            symbol = token.get('symbol', 'N/A')
            growth = token.get('growth_wow_pct', 0) or 0
            issuance = token.get('net_issuance_usd', 0) or 0

            lines.append(f"| {emoji} | **{symbol}** | +{growth:.1f}% | ${issuance:,.0f} |")

        return "\n".join(lines)

    def _build_top_blockchains(self, data: Dict) -> str:
        """
        Slide X: Top Blockchains by Trading Volume
        Shows which blockchains dominate LATAM stablecoin activity
        """
        rankings = data.get('token_rankings', {})
        top_blockchains = rankings.get('by_blockchain_volume', [])

        if not top_blockchains:
            return "## ⛓️ Top Blockchains by Trading Volume\n\n*No blockchain data available*"

        lines = ["## ⛓️ Top Blockchains by Trading Volume\n"]
        lines.append("| **Rank** | **Blockchain** | **Volume (USD)** | **Market Share** | **Tokens** |")
        lines.append("|:--------:|:---------------|:----------------:|:----------------:|:----------:|")

        for blockchain_data in top_blockchains:
            rank = blockchain_data.get('rank', 0)
            blockchain = blockchain_data.get('blockchain', 'Unknown').upper()
            volume = blockchain_data.get('volume_usd', 0)
            share = blockchain_data.get('market_share_pct', 0)
            tokens = blockchain_data.get('unique_tokens', 0)

            # Emoji for top 3
            emoji = {1: "🥇", 2: "🥈", 3: "🥉"}.get(rank, f"{rank}️⃣")

            lines.append(f"| {emoji} | **{blockchain}** | ${volume:,.0f} | {share}% | {tokens} |")

        return "\n".join(lines)

    def _build_market_health(self, data: Dict) -> str:
        """Slide 5: Market Health Score Breakdown - Table format"""
        health = data.get('market_health', {})
        score = health.get('overall_score', 0) or 0
        rating = health.get('rating', 'N/A')

        # Calculate component scores
        buy_score = min(30, (score / 100) * 30)
        decentral_score = min(25, (score / 100) * 25)
        liquidity_score = min(25, (score / 100) * 25)
        growth_score = min(20, (score / 100) * 20)

        return f"""## 🏥 Market Health Score

| **Overall Score** | **Rating** |
|:-----------------:|:----------:|
| **{score}/100** | {rating} |

### Component Breakdown

| **Component** | **Score** | **Max** | **Status** |
|:--------------|:---------:|:-------:|:-----------|
| **Buy Pressure** | {buy_score:.0f} | 30 | {self._get_status_emoji(buy_score, 30)} |
| **Decentralization** | {decentral_score:.0f} | 25 | {self._get_status_emoji(decentral_score, 25)} |
| **Liquidity Distribution** | {liquidity_score:.0f} | 25 | {self._get_status_emoji(liquidity_score, 25)} |
| **Network Growth** | {growth_score:.0f} | 20 | {self._get_status_emoji(growth_score, 20)} |"""

    def _build_network_activity(self, data: Dict) -> str:
        """Slide 6: Network Activity - Table format"""
        insights = data.get('insights', {})
        network = insights.get('network_activity', {})

        wallets = network.get('total_unique_wallets', 0) or 0
        ratio = network.get('receiver_sender_ratio', 0) or 0
        avg_transfers = network.get('avg_transfers_per_wallet', 0) or 0
        state = network.get('network_state', 'Unknown')

        return f"""## 🌐 Network Activity

| **Metric** | **Value** | **Indicator** |
|:-----------|:---------:|:--------------|
| **Unique Wallets** | {wallets:,} | {self._get_wallet_emoji(wallets)} |
| **Receiver/Sender Ratio** | {ratio:.2f}x | {self._get_ratio_emoji(ratio)} |
| **Avg Transfers per Wallet** | {avg_transfers:.1f} | {self._get_transfer_emoji(avg_transfers)} |
| **Network State** | {state} | {self._get_state_emoji(state)} |"""

    def _build_supply_vs_trading(self, data: Dict) -> str:
        """Slide 7: Supply vs Trading Analysis - Table format"""
        insights = data.get('insights', {})
        supply_trading = insights.get('supply_vs_trading', {})

        net_supply = supply_trading.get('net_supply_change_usd', 0) or 0
        trading_vol = supply_trading.get('trading_volume_usd', 0) or 0
        ratio = supply_trading.get('supply_to_volume_ratio', 0) or 0
        interpretation = supply_trading.get('interpretation', 'No analysis available')

        return f"""## ⚖️ Supply vs Trading Volume

| **Metric** | **Value** |
|:-----------|:---------:|
| **Net Supply Change** | ${net_supply:,.0f} |
| **Trading Volume** | ${trading_vol:,.0f} |
| **Supply/Volume Ratio** | {ratio:.2f}% |

| **Analysis** |
|:-------------|
| {interpretation} |"""

    def _build_alerts(self, data: Dict) -> str:
        """Slide 8: Market Alerts - Table format"""
        health = data.get('market_health', {})
        alerts = health.get('alerts', [])

        if not alerts:
            return """## ⚠️ Market Alerts

| **Status** |
|:-----------|
| ✅ **No critical alerts this week** |
| *All monitored metrics within normal ranges* |"""

        lines = ["## ⚠️ Market Alerts\n"]

        # Group by severity
        high_alerts = [a for a in alerts if a.get('severity') == 'HIGH'][:3]
        medium_alerts = [a for a in alerts if a.get('severity') == 'MEDIUM'][:3]
        info_alerts = [a for a in alerts if a.get('severity') == 'INFO'][:2]

        if high_alerts:
            lines.append("### 🔴 High Priority\n")
            lines.append("| **Token** | **Alert** | **Details** |")
            lines.append("|:----------|:----------|:------------|")
            for alert in high_alerts:
                lines.append(f"| **{alert.get('symbol')}** | {alert.get('type')} | {alert.get('details')} |")
            lines.append("")

        if medium_alerts:
            lines.append("### 🟡 Medium Priority\n")
            lines.append("| **Token** | **Alert** | **Details** |")
            lines.append("|:----------|:----------|:------------|")
            for alert in medium_alerts:
                lines.append(f"| **{alert.get('symbol')}** | {alert.get('type')} | {alert.get('details')} |")
            lines.append("")

        if info_alerts:
            lines.append("### 🟢 Positive Signals\n")
            lines.append("| **Token** | **Alert** | **Details** |")
            lines.append("|:----------|:----------|:------------|")
            for alert in info_alerts:
                lines.append(f"| **{alert.get('symbol')}** | {alert.get('type')} | {alert.get('details')} |")

        return "\n".join(lines)

    def _build_cross_domain_insights(self, data: Dict) -> str:
        """Slide 9: Cross-Domain Insights - Table format"""
        insights = data.get('insights', {})
        liquidity = insights.get('liquidity_health', {})

        buy_pressure = liquidity.get('buy_pressure_pct', 0) or 0
        whale_conc = liquidity.get('whale_concentration_ratio', 0) or 0
        dex_count = liquidity.get('dex_fragmentation_score', 0) or 0
        rating = liquidity.get('liquidity_rating', 'N/A')

        return f"""## 🔍 Liquidity Health Analysis

| **Metric** | **Value** | **Status** |
|:-----------|:---------:|:-----------|
| **Buy Pressure** | {buy_pressure:.1f}% | {self._get_pressure_emoji(buy_pressure)} |
| **Whale Concentration** | {whale_conc:.1f}% | {self._get_whale_emoji(whale_conc)} |
| **DEX Distribution** | {dex_count} platforms | {self._get_dex_emoji(dex_count)} |
| **Overall Liquidity** | {rating} | {self._get_liquidity_emoji(rating)} |"""

    def _build_methodology(self, data: Dict) -> str:
        """Slide 10: Methodology & Data Sources - Table format"""
        metadata = data.get('metadata', {})

        supply_kpis = metadata.get('kpi_count_by_domain', {}).get('supply', 0)
        flows_kpis = metadata.get('kpi_count_by_domain', {}).get('flows', 0)
        dex_kpis = metadata.get('kpi_count_by_domain', {}).get('dex', 0)
        total_kpis = metadata.get('total_kpis', 0)

        return f"""## 📚 Methodology & Data Sources

| **Data Domain** | **KPIs Tracked** | **Focus Area** |
|:----------------|:----------------:|:---------------|
| **Supply Metrics** | {supply_kpis} | Mint/burn tracking |
| **Flow Metrics** | {flows_kpis} | On-chain transfers |
| **DEX Metrics** | {dex_kpis} | Trading activity |
| **Total KPIs** | **{total_kpis}** | **All domains** |

| **Information** | **Details** |
|:----------------|:------------|
| **Data Provider** | Dune Analytics |
| **Report Version** | {metadata.get('report_version', 'N/A')} |
| **Next Report** | {self._get_next_week(metadata.get('week', 'N/A'))} |

---

*For questions or collaboration: [Your Contact Info Here]*"""

    # =========================================================================
    # HELPER METHODS
    # =========================================================================

    def _get_status_emoji(self, score: float, max_score: float) -> str:
        """Get emoji based on score percentage"""
        pct = (score / max_score) * 100 if max_score > 0 else 0
        if pct >= 80:
            return "🟢 Excellent"
        elif pct >= 60:
            return "🟡 Good"
        elif pct >= 40:
            return "🟠 Fair"
        else:
            return "🔴 Poor"

    def _get_pressure_emoji(self, pressure: float) -> str:
        """Get emoji for buy pressure"""
        if pressure >= 55:
            return "🟢 Strong"
        elif pressure >= 45:
            return "🟡 Balanced"
        else:
            return "🔴 Weak"

    def _get_whale_emoji(self, concentration: float) -> str:
        """Get emoji for whale concentration"""
        if concentration < 50:
            return "🟢 Low"
        elif concentration < 100:
            return "🟡 Moderate"
        else:
            return "🔴 High"

    def _get_dex_emoji(self, count: int) -> str:
        """Get emoji for DEX distribution"""
        if count >= 5:
            return "🟢 Distributed"
        elif count >= 3:
            return "🟡 Moderate"
        else:
            return "🔴 Concentrated"

    def _get_wallet_emoji(self, count: int) -> str:
        """Get emoji for wallet activity"""
        if count >= 30000:
            return "🟢 High Activity"
        elif count >= 10000:
            return "🟡 Moderate"
        return "🔴 Low Activity"

    def _get_ratio_emoji(self, ratio: float) -> str:
        """Get emoji for receiver/sender ratio"""
        if ratio > 1.3:
            return "🟢 Expanding"
        elif ratio > 0.7:
            return "🟡 Balanced"
        return "🔴 Contracting"

    def _get_transfer_emoji(self, avg: float) -> str:
        """Get emoji for transfer activity"""
        if avg >= 2.0:
            return "🟢 Active"
        elif avg >= 0.5:
            return "🟡 Moderate"
        return "🔴 Low"

    def _get_state_emoji(self, state: str) -> str:
        """Get emoji for network state"""
        if "Expanding" in state:
            return "🚀"
        elif "Balanced" in state:
            return "⚖️"
        return "📉"

    def _get_liquidity_emoji(self, rating: str) -> str:
        """Get emoji for liquidity rating"""
        if "Excellent" in rating or "Good" in rating:
            return "🟢"
        elif "Fair" in rating:
            return "🟡"
        return "🔴"

    def _get_next_week(self, current_week: str) -> str:
        """Calculate next week"""
        try:
            # Parse week format like "2026-W07"
            year, week = current_week.split('-W')
            next_week_num = int(week) + 1
            return f"{year}-W{next_week_num:02d}"
        except:
            return "TBD"


# ============================================================================
# Standalone function for easy import
# ============================================================================

def export_markdown_report(json_report_path: str, output_dir: str = './data/reports') -> Path:
    """
    Export JSON report to Markdown

    Args:
        json_report_path: Path to JSON report file
        output_dir: Output directory for markdown

    Returns:
        Path to generated markdown file
    """
    exporter = MarkdownExporter(output_dir=output_dir)
    return exporter.export_report(Path(json_report_path))


if __name__ == "__main__":
    """Test markdown exporter"""
    import sys

    if len(sys.argv) < 2:
        print("Usage: python markdown_exporter.py <path_to_json_report>")
        sys.exit(1)

    json_path = sys.argv[1]
    md_path = export_markdown_report(json_path)
    print(f"✅ Markdown report generated: {md_path}")
