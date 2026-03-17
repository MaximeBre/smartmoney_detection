"""
Generate self-contained HTML dashboard for Hyperliquid Smart Money Tracker.
"""
import os
import pandas as pd
import numpy as np
from datetime import datetime
from config import DASHBOARD_HTML


# ── Formatting helpers ──────────────────────────────────────────────────────

def _fmt_usd(v):
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return "—"
    try:
        v = float(v)
        if abs(v) >= 1_000_000:
            return f"${v/1_000_000:.2f}M"
        if abs(v) >= 1_000:
            return f"${v:,.0f}"
        return f"${v:.2f}"
    except Exception:
        return "—"


def _fmt_pct(v, decimals=2):
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return "—"
    try:
        return f"{float(v)*100:.{decimals}f}%"
    except Exception:
        return "—"


def _fmt(v, decimals=2):
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return "—"
    try:
        return f"{float(v):.{decimals}f}"
    except Exception:
        return str(v)


def _color_pnl(v):
    """Return CSS class for positive/negative value."""
    try:
        return "pos" if float(v) >= 0 else "neg"
    except Exception:
        return ""


def _color_ic(v):
    """Return CSS class for IC values."""
    try:
        fv = float(v)
        if fv > 0.1:
            return "pos"
        if fv < -0.1:
            return "neg"
        return "muted"
    except Exception:
        return ""


def _grade_badge(grade: str) -> str:
    css_map = {
        "TIER_1":            "badge-tier1",
        "TIER_2":            "badge-tier2",
        "TIER_3":            "badge-tier3",
        "MARGINAL":          "badge-marginal",
        "NO_EDGE":           "badge-noedge",
        "INSUFFICIENT_DATA": "badge-noedge",
        "UNKNOWN":           "badge-noedge",
    }
    css = css_map.get(grade, "badge-noedge")
    label = grade.replace("_", " ")
    return f'<span class="badge {css}">{label}</span>'


def _strength_badge(strength: str) -> str:
    css_map = {
        "STRONG":   "badge-strong",
        "MODERATE": "badge-moderate",
        "WEAK":     "badge-weak",
    }
    css = css_map.get(strength, "badge-weak")
    return f'<span class="badge {css}">{strength}</span>'


def _consensus_badge(strength: str) -> str:
    css_map = {
        "STRONG":   "badge-strong",
        "MODERATE": "badge-consensus-mod",
        "WEAK":     "badge-weak",
    }
    css = css_map.get(strength, "badge-weak")
    return f'<span class="badge {css}">{strength}</span>'


# ── Section builders ────────────────────────────────────────────────────────

def build_signals_section(signals: list,
                            consensus: list,
                            current_state: dict) -> str:
    """Build the LIVE SIGNALS section — most prominent."""
    n_monitored = len(current_state)

    # New signals panel
    if signals:
        sig_rows = ""
        for s in signals:
            direction_css = "pos" if s["direction"] == "LONG" else "neg"
            sig_rows += f"""
        <tr>
          <td>{_strength_badge(s['signal_strength'])}</td>
          <td><strong class="{direction_css}">{s['direction']}</strong></td>
          <td><strong>{s['coin']}</strong></td>
          <td>{_fmt_usd(s.get('entry_price', 0))}</td>
          <td>{s.get('primary_wallet_name', '')[:12]}</td>
          <td>{_fmt(s.get('primary_score'))}</td>
          <td>{s.get('consensus_wallets', 1)}</td>
          <td>{_fmt(s.get('consensus_confidence'))}</td>
          <td>{_grade_badge(s.get('grade',''))}</td>
          <td class="muted" style="font-size:11px">{s.get('leverage','1')}x lev</td>
        </tr>"""
        signals_html = f"""
    <table class="data-table">
      <thead>
        <tr>
          <th>Strength</th><th>Dir</th><th>Coin</th><th>Entry Price</th>
          <th>Wallet</th><th>SM Score</th><th>Wallets</th>
          <th>Confidence</th><th>Grade</th><th>Note</th>
        </tr>
      </thead>
      <tbody>{sig_rows}</tbody>
    </table>"""
    else:
        signals_html = f'<div class="no-signal">No new signals this run — monitoring {n_monitored} smart money wallets</div>'

    # Consensus table
    if consensus:
        con_rows = ""
        for c in consensus:
            direction_css = "pos" if c["direction"] == "LONG" else "neg"
            wallets_str   = ", ".join(c["wallets"][:4])
            con_rows += f"""
        <tr>
          <td><strong>{c['coin']}</strong></td>
          <td><strong class="{direction_css}">{c['direction']}</strong></td>
          <td>{c['n_wallets']}</td>
          <td>{_fmt(c.get('avg_score'))}</td>
          <td>{_fmt(c.get('max_score'))}</td>
          <td>{_consensus_badge(c.get('consensus_strength','WEAK'))}</td>
          <td class="muted" style="font-size:11px;max-width:200px;overflow:hidden;text-overflow:ellipsis">{wallets_str}</td>
        </tr>"""
        consensus_html = f"""
    <h3 style="margin:20px 0 10px;font-size:13px;color:var(--muted);text-transform:uppercase;letter-spacing:0.5px">
      Current Smart Money Positions ({len(consensus)} coins tracked)
    </h3>
    <table class="data-table">
      <thead>
        <tr>
          <th>Coin</th><th>Direction</th><th># Wallets</th>
          <th>Avg Score</th><th>Max Score</th><th>Strength</th><th>Wallets</th>
        </tr>
      </thead>
      <tbody>{con_rows}</tbody>
    </table>"""
    else:
        consensus_html = '<div class="no-signal muted">No smart money consensus positions found</div>'

    return signals_html + consensus_html


def build_smart_money_table(smart_money_df: pd.DataFrame,
                             current_state: dict) -> str:
    """Smart Money Leaderboard ranked by IC score."""
    if smart_money_df is None or smart_money_df.empty:
        return "<p class='muted'>No smart money data yet</p>"

    # Build position lookup from current_state
    pos_lookup = {}
    for addr, state in current_state.items():
        positions = state.get("positions", {})
        if positions:
            coins = list(positions.keys())
            sides = [positions[c]["side"][0] for c in coins]  # L or S
            pos_lookup[addr] = " ".join(f"{c}({s})" for c, s in zip(coins[:4], sides[:4]))
        else:
            pos_lookup[addr] = "—"

    rows = ""
    for _, r in smart_money_df.head(50).iterrows():
        addr        = r.get("address", "")
        addr_short  = addr[:6] + "…" + addr[-4:] if len(addr) > 10 else addr
        display     = r.get("display_name", "") or addr_short
        hl_link     = f"https://app.hyperliquid.xyz/explorer/address/{addr}"
        grade       = r.get("grade", "UNKNOWN")
        score       = r.get("smart_money_score", 0)
        ic_8h       = r.get("ic_8h", 0)
        ic_rec      = r.get("ic_recent_8h", 0)
        icir        = r.get("icir_8h", 0)
        trend_dir   = r.get("ic_trend_direction", "")
        pnl_all     = r.get("pnl_alltime", 0)
        sm_rank     = int(r.get("smart_money_rank", 0))
        open_pos    = pos_lookup.get(addr, "—")

        trend_html = (
            '<span class="pos" style="font-size:14px">&#9650;</span>'
            if trend_dir == "IMPROVING" else
            '<span class="neg" style="font-size:14px">&#9660;</span>'
        )

        rows += f"""
        <tr>
          <td class="muted">{sm_rank}</td>
          <td><a href="{hl_link}" target="_blank" class="addr">{display}</a></td>
          <td>{_grade_badge(grade)}</td>
          <td class="{_color_ic(score)}" style="font-weight:700">{_fmt(score, 3)}</td>
          <td class="{_color_ic(ic_8h)}">{_fmt(ic_8h, 3)}</td>
          <td class="{_color_ic(ic_rec)}">{_fmt(ic_rec, 3)}</td>
          <td class="{_color_ic(icir)}">{_fmt(icir, 3)}</td>
          <td style="text-align:center">{trend_html}</td>
          <td class="{_color_pnl(pnl_all)}">{_fmt_usd(pnl_all)}</td>
          <td class="muted" style="font-size:11px;max-width:180px;overflow:hidden;text-overflow:ellipsis">{open_pos}</td>
        </tr>"""

    return f"""
    <table class="data-table">
      <thead>
        <tr>
          <th>#</th><th>Wallet</th><th>Grade</th>
          <th>SM Score</th><th>IC 8h</th><th>IC Recent</th>
          <th>ICIR</th><th>Trend</th><th>PnL All-Time</th><th>Open Positions</th>
        </tr>
      </thead>
      <tbody>{rows}</tbody>
    </table>"""


def build_leaderboard_table(leaderboard_df: pd.DataFrame) -> str:
    if leaderboard_df is None or leaderboard_df.empty:
        return "<p>No data</p>"

    rows = ""
    for _, r in leaderboard_df.head(50).iterrows():
        addr       = r.get("address", "")
        addr_short = addr[:6] + "…" + addr[-4:] if len(addr) > 10 else addr
        display    = r.get("display_name", "") or addr_short
        hl_link    = f"https://app.hyperliquid.xyz/explorer/address/{addr}"

        pnl_all = r.get("pnl_alltime", 0)
        pnl_mo  = r.get("pnl_month",   0)
        pnl_wk  = r.get("pnl_week",    0)
        roi_all = r.get("roi_alltime",  0)
        n_pos   = r.get("n_open_positions", 0)
        open_ass = r.get("open_assets", "") or ""

        rows += f"""
        <tr>
          <td>{int(r.get('rank', 0))}</td>
          <td><a href="{hl_link}" target="_blank" class="addr">{display}</a></td>
          <td class="{_color_pnl(pnl_all)}">{_fmt_usd(pnl_all)}</td>
          <td class="{_color_pnl(pnl_mo)}">{_fmt_usd(pnl_mo)}</td>
          <td class="{_color_pnl(pnl_wk)}">{_fmt_usd(pnl_wk)}</td>
          <td class="{_color_pnl(roi_all)}">{_fmt_pct(roi_all)}</td>
          <td>{int(n_pos) if n_pos == n_pos else 0}</td>
          <td class="assets">{str(open_ass)[:40]}</td>
        </tr>"""

    return f"""
    <table class="data-table">
      <thead>
        <tr>
          <th>#</th><th>Address</th><th>PnL All-Time</th>
          <th>PnL Month</th><th>PnL Week</th><th>ROI All-Time</th>
          <th>Open Pos</th><th>Open Assets</th>
        </tr>
      </thead>
      <tbody>{rows}</tbody>
    </table>"""


def build_profiles_table(profiles_df: pd.DataFrame) -> str:
    if profiles_df is None or profiles_df.empty:
        return "<p>No profiles</p>"

    valid = profiles_df[
        profiles_df.get("sufficient_data", pd.Series([False]*len(profiles_df))) == True
    ].copy()
    if valid.empty:
        return "<p>Insufficient data for profiling</p>"

    if "pnl_alltime" in valid.columns:
        valid = valid.sort_values("pnl_alltime", ascending=False)

    rows = ""
    for _, r in valid.iterrows():
        addr       = r.get("address", "")
        addr_short = addr[:6] + "…" + addr[-4:] if len(addr) > 10 else addr
        hl_link    = f"https://app.hyperliquid.xyz/explorer/address/{addr}"

        hold_style = r.get("hold_style", "")
        bias       = r.get("bias", "")
        top_asset  = r.get("top_asset_1", "")

        style_badge = f'<span class="badge badge-{hold_style.lower()}">{hold_style}</span>' if hold_style else ""
        bias_badge  = f'<span class="badge badge-{bias.lower()}">{bias}</span>' if bias else ""

        rows += f"""
        <tr>
          <td><a href="{hl_link}" target="_blank" class="addr">{addr_short}</a></td>
          <td class="{_color_pnl(r.get('pnl_alltime',0))}">{_fmt_usd(r.get('pnl_alltime'))}</td>
          <td>{_fmt_pct(r.get('win_rate'))}</td>
          <td>{_fmt(r.get('sharpe'))}</td>
          <td>{_fmt_pct(abs(r.get('max_drawdown', 0)))}</td>
          <td>{_fmt(r.get('avg_hold_hours'))}h</td>
          <td>{_fmt(r.get('trades_per_day'))}/d</td>
          <td>{style_badge}</td>
          <td>{bias_badge}</td>
          <td>{top_asset}</td>
          <td>{_fmt(r.get('profit_factor'))}</td>
          <td class="neg">{int(r.get('n_liquidations', 0))}</td>
        </tr>"""

    return f"""
    <table class="data-table">
      <thead>
        <tr>
          <th>Address</th><th>PnL</th><th>Win Rate</th><th>Sharpe</th>
          <th>Max DD</th><th>Avg Hold</th><th>Freq</th>
          <th>Style</th><th>Bias</th><th>Top Asset</th>
          <th>Profit Factor</th><th>Liq.</th>
        </tr>
      </thead>
      <tbody>{rows}</tbody>
    </table>"""


def build_market_table(market: dict) -> str:
    if not market:
        return "<p>No market data</p>"
    snapshot = market.get("snapshot", [])
    if not snapshot:
        return "<p>No market data</p>"

    rows = ""
    for a in snapshot[:30]:
        coin = a.get("coin", "")
        fr   = a.get("funding_rate_8h", 0)
        fa   = a.get("funding_annual", 0)
        oi   = a.get("oi_usd", 0)
        px   = a.get("mid_price", 0)
        sent = a.get("funding_sentiment", "")

        sent_class = {
            "VERY_BULLISH": "very-bullish",
            "BULLISH":      "bullish",
            "NEUTRAL":      "neutral",
            "BEARISH":      "bearish",
            "VERY_BEARISH": "very-bearish",
        }.get(sent, "neutral")

        rows += f"""
        <tr>
          <td><strong>{coin}</strong></td>
          <td>{_fmt_usd(px)}</td>
          <td class="{_color_pnl(fr)}">{fr*100:.4f}%</td>
          <td class="{_color_pnl(fa)}">{fa*100:.1f}%</td>
          <td>{_fmt_usd(oi)}</td>
          <td><span class="badge badge-{sent_class}">{sent.replace('_',' ')}</span></td>
        </tr>"""

    return f"""
    <table class="data-table">
      <thead>
        <tr>
          <th>Asset</th><th>Price</th><th>Funding 8h</th>
          <th>Funding Ann.</th><th>OI (USD)</th><th>Sentiment</th>
        </tr>
      </thead>
      <tbody>{rows}</tbody>
    </table>"""


def build_patterns_section(patterns: dict) -> str:
    if not patterns:
        return "<p class='muted'>No pattern data</p>"

    findings     = patterns.get("key_findings", [])
    tier_cmp     = patterns.get("tier_comparison", pd.DataFrame())

    findings_html = ""
    if findings:
        for f in findings:
            findings_html += f'<div class="finding">&#8594; {f}</div>'
    else:
        findings_html = '<div class="finding muted">Not enough data for statistical significance yet.</div>'

    table_html = ""
    if isinstance(tier_cmp, pd.DataFrame) and not tier_cmp.empty:
        rows = ""
        for _, r in tier_cmp.head(15).iterrows():
            sig = "&#10003;" if r.get("significant") else ""
            rows += f"""
            <tr class="{'sig-row' if r.get('significant') else ''}">
              <td>{r.get('metric','')}</td>
              <td>{_fmt(r.get('top_tier_mean'))}</td>
              <td>{_fmt(r.get('bottom_tier_mean'))}</td>
              <td class="{_color_pnl(r.get('difference',0))}">{_fmt(r.get('pct_difference'))}%</td>
              <td>{_fmt(r.get('p_value'), 3)}</td>
              <td>{sig}</td>
            </tr>"""
        table_html = f"""
        <table class="data-table">
          <thead><tr>
            <th>Metric</th><th>Top Tier</th><th>Bottom Tier</th>
            <th>&#916; %</th><th>p-value</th><th>Sig.</th>
          </tr></thead>
          <tbody>{rows}</tbody>
        </table>"""

    return f"""
    <div class="findings-box">{findings_html}</div>
    {table_html}"""


# ── Main generate() ─────────────────────────────────────────────────────────

def generate(leaderboard_df: pd.DataFrame,
             profiles_df: pd.DataFrame,
             smart_money_df: pd.DataFrame,
             signals: dict,
             current_state: dict,
             patterns: dict,
             market: dict) -> None:
    """
    Generate the complete self-contained HTML dashboard.
    """
    # Defensive defaults
    if smart_money_df is None:
        smart_money_df = pd.DataFrame()
    if leaderboard_df is None:
        leaderboard_df = pd.DataFrame()
    if profiles_df is None:
        profiles_df = pd.DataFrame()
    if signals is None:
        signals = {"signals": [], "consensus": []}
    if current_state is None:
        current_state = {}
    if patterns is None:
        patterns = {"key_findings": [], "tier_comparison": pd.DataFrame()}
    if market is None:
        market = {}

    sentiment    = market.get("sentiment", {})
    regime       = sentiment.get("market_regime", "N/A")
    regime_color = {
        "RISK_ON":  "#00c853",
        "BULLISH":  "#69f0ae",
        "NEUTRAL":  "#ffd740",
        "BEARISH":  "#ff6d00",
        "RISK_OFF": "#ff1744",
    }.get(regime, "#888888")

    now         = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    n_wallets   = len(leaderboard_df)
    total_oi    = _fmt_usd(sentiment.get("total_oi_usd", 0))
    oi_funding  = f"{sentiment.get('oi_weighted_funding', 0)*100:.4f}%"
    annual_f    = f"{sentiment.get('funding_annual_pct', 0):.1f}%"
    pct_bull    = f"{sentiment.get('pct_positive_funding', 0)*100:.0f}%"

    sig_list  = signals.get("signals", [])
    con_list  = signals.get("consensus", [])
    n_signals = len(sig_list)
    n_sm      = int(
        (smart_money_df["smart_money_score"] >= 0.15).sum()
        if not smart_money_df.empty and "smart_money_score" in smart_money_df.columns
        else 0
    )

    oi_funding_class = "pos" if sentiment.get("oi_weighted_funding", 0) >= 0 else "neg"

    # Build section HTML
    signals_html      = build_signals_section(sig_list, con_list, current_state)
    smart_money_html  = build_smart_money_table(smart_money_df, current_state)
    leaderboard_html  = build_leaderboard_table(leaderboard_df)
    profiles_html     = build_profiles_table(profiles_df)
    market_html       = build_market_table(market)
    patterns_html     = build_patterns_section(patterns)

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Hyperliquid Smart Money Tracker</title>
<style>
  :root {{
    --bg:     #0d0f14;
    --card:   #151820;
    --border: #252830;
    --text:   #e2e8f0;
    --muted:  #64748b;
    --accent: #6366f1;
    --pos:    #10b981;
    --neg:    #ef4444;
    --warn:   #f59e0b;
    --gold:   #f59e0b;
    --silver: #94a3b8;
    --bronze: #b45309;
  }}
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{ background: var(--bg); color: var(--text); font-family: 'Inter', system-ui, sans-serif; font-size: 13px; }}

  /* Header */
  .header {{ background: var(--card); border-bottom: 1px solid var(--border); padding: 16px 24px; display: flex; align-items: center; justify-content: space-between; }}
  .header h1 {{ font-size: 18px; font-weight: 700; letter-spacing: -0.5px; }}
  .header .subtitle {{ color: var(--muted); font-size: 12px; margin-top: 2px; }}

  /* Stats bar */
  .stats-bar {{ display: flex; gap: 1px; background: var(--border); border-bottom: 1px solid var(--border); }}
  .stat-block {{ background: var(--card); padding: 14px 24px; flex: 1; min-width: 120px; }}
  .stat-block .label {{ color: var(--muted); font-size: 11px; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 4px; }}
  .stat-block .value {{ font-size: 20px; font-weight: 700; }}

  /* Layout */
  .main {{ padding: 20px 24px; display: flex; flex-direction: column; gap: 20px; }}

  /* Cards */
  .card {{ background: var(--card); border: 1px solid var(--border); border-radius: 8px; overflow: hidden; }}
  .card-header {{ padding: 14px 18px; border-bottom: 1px solid var(--border); display: flex; align-items: center; gap: 10px; }}
  .card-header h2 {{ font-size: 14px; font-weight: 600; }}
  .card-body {{ padding: 16px 18px; overflow-x: auto; }}

  /* Signals highlight */
  .card-signals {{ background: #0f1420; border: 1px solid #1e3a5f; border-radius: 8px; overflow: hidden; }}
  .card-signals .card-header {{ background: #0d1f35; border-bottom: 1px solid #1e3a5f; }}
  .card-signals .card-header h2 {{ color: #60a5fa; }}

  /* Tables */
  .data-table {{ width: 100%; border-collapse: collapse; }}
  .data-table th {{ color: var(--muted); font-size: 11px; text-transform: uppercase; letter-spacing: 0.4px; padding: 8px 10px; text-align: left; border-bottom: 1px solid var(--border); white-space: nowrap; }}
  .data-table td {{ padding: 8px 10px; border-bottom: 1px solid #1a1d24; white-space: nowrap; }}
  .data-table tr:hover td {{ background: #1a1d24; }}
  .data-table tr:last-child td {{ border-bottom: none; }}

  /* Colors */
  .pos   {{ color: var(--pos); }}
  .neg   {{ color: var(--neg); }}
  .muted {{ color: var(--muted); }}

  a.addr {{ color: var(--accent); text-decoration: none; font-family: monospace; font-size: 12px; }}
  a.addr:hover {{ text-decoration: underline; }}

  /* Badges */
  .badge {{ display: inline-block; padding: 2px 7px; border-radius: 4px; font-size: 10px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.4px; white-space: nowrap; }}

  /* Grade badges */
  .badge-tier1    {{ background: #2d1f00; color: var(--gold); border: 1px solid #78500a; }}
  .badge-tier2    {{ background: #1e2028; color: var(--silver); border: 1px solid #475569; }}
  .badge-tier3    {{ background: #1c1208; color: var(--bronze); border: 1px solid #78350f; }}
  .badge-marginal {{ background: #1a1a1a; color: #94a3b8; border: 1px solid #334155; }}
  .badge-noedge   {{ background: #0f0f0f; color: #475569; border: 1px solid #1e293b; }}

  /* Signal strength badges */
  .badge-strong   {{ background: #052e16; color: #4ade80; border: 1px solid #166534; }}
  .badge-moderate {{ background: #2d2000; color: #fbbf24; border: 1px solid #92400e; }}
  .badge-weak     {{ background: #1a1a1a; color: #94a3b8; border: 1px solid #334155; }}
  .badge-consensus-mod {{ background: #0c1a3a; color: #60a5fa; border: 1px solid #1e40af; }}

  /* Trading style badges */
  .badge-scalper   {{ background: #1e1b4b; color: #818cf8; }}
  .badge-intraday  {{ background: #1c3461; color: #60a5fa; }}
  .badge-swing     {{ background: #1a3320; color: #4ade80; }}
  .badge-position  {{ background: #2d1b10; color: #fb923c; }}
  .badge-long      {{ background: #0f2a1e; color: #10b981; }}
  .badge-short     {{ background: #2a0f0f; color: #ef4444; }}
  .badge-neutral   {{ background: #1e1e1e; color: #94a3b8; }}

  /* Market sentiment badges */
  .badge-bullish      {{ background: #0f2a1e; color: #10b981; }}
  .badge-very-bullish {{ background: #064e3b; color: #6ee7b7; }}
  .badge-bearish      {{ background: #2a0f0f; color: #ef4444; }}
  .badge-very-bearish {{ background: #4c0519; color: #fca5a5; }}
  .badge-neutral-sent {{ background: #1e1e2e; color: #94a3b8; }}

  /* Live pulse dot */
  .live-dot {{
    display: inline-block;
    width: 8px; height: 8px;
    border-radius: 50%;
    background: #4ade80;
    margin-right: 8px;
    animation: pulse 1.5s infinite;
    vertical-align: middle;
  }}
  @keyframes pulse {{
    0%,100% {{ opacity: 1; transform: scale(1); box-shadow: 0 0 0 0 rgba(74,222,128,0.4); }}
    50%      {{ opacity: 0.8; transform: scale(1.15); box-shadow: 0 0 0 6px rgba(74,222,128,0); }}
  }}

  /* No-signal message */
  .no-signal {{ padding: 16px; color: var(--muted); font-style: italic; background: #0f1218; border-radius: 4px; border: 1px solid var(--border); }}

  /* Patterns */
  .finding {{ padding: 8px 12px; margin-bottom: 6px; background: #0f172a; border-left: 3px solid var(--accent); border-radius: 0 4px 4px 0; line-height: 1.5; }}
  .finding.muted {{ border-left-color: var(--muted); color: var(--muted); }}
  .findings-box {{ margin-bottom: 16px; }}
  .sig-row td {{ font-weight: 600; }}

  /* Regime indicator */
  .regime-dot {{ width: 10px; height: 10px; border-radius: 50%; background: {regime_color}; display: inline-block; margin-right: 6px; vertical-align: middle; }}

  .assets {{ color: var(--muted); font-size: 11px; max-width: 200px; overflow: hidden; text-overflow: ellipsis; }}

  .two-col {{ display: grid; grid-template-columns: 1fr 1fr; gap: 20px; }}
  @media (max-width: 900px) {{ .two-col {{ grid-template-columns: 1fr; }} }}
</style>
</head>
<body>

<!-- Header -->
<div class="header">
  <div>
    <h1>&#x2B21; Hyperliquid Smart Money Tracker</h1>
    <div class="subtitle">Top {n_wallets} traders &middot; {n_sm} smart money wallets &middot; Updated {now}</div>
  </div>
  <div style="text-align:right">
    <span class="regime-dot"></span>
    <span style="font-weight:700">{regime}</span>
  </div>
</div>

<!-- Stats bar -->
<div class="stats-bar">
  <div class="stat-block">
    <div class="label">Wallets Tracked</div>
    <div class="value">{n_wallets}</div>
  </div>
  <div class="stat-block">
    <div class="label">Smart Money</div>
    <div class="value pos">{n_sm}</div>
  </div>
  <div class="stat-block">
    <div class="label">New Signals</div>
    <div class="value {'pos' if n_signals > 0 else ''}">{n_signals}</div>
  </div>
  <div class="stat-block">
    <div class="label">Total HL OI</div>
    <div class="value">{total_oi}</div>
  </div>
  <div class="stat-block">
    <div class="label">OI-Weighted Funding (8h)</div>
    <div class="value {oi_funding_class}">{oi_funding}</div>
  </div>
  <div class="stat-block">
    <div class="label">Annualised Funding</div>
    <div class="value">{annual_f}</div>
  </div>
  <div class="stat-block">
    <div class="label">% Bullish Funding</div>
    <div class="value">{pct_bull}</div>
  </div>
</div>

<div class="main">

  <!-- LIVE SIGNALS (most prominent) -->
  <div class="card-signals">
    <div class="card-header">
      <span class="live-dot"></span>
      <h2>LIVE SIGNALS &amp; SMART MONEY CONSENSUS</h2>
    </div>
    <div class="card-body">{signals_html}</div>
  </div>

  <!-- Smart Money Leaderboard -->
  <div class="card">
    <div class="card-header">
      <h2>Smart Money Leaderboard &mdash; Ranked by IC Score</h2>
    </div>
    <div class="card-body">{smart_money_html}</div>
  </div>

  <!-- Market Overview -->
  <div class="card">
    <div class="card-header">
      <h2>Market Overview &mdash; Funding Rates &amp; Open Interest</h2>
    </div>
    <div class="card-body">{market_html}</div>
  </div>

  <!-- Trader Profiles -->
  <div class="card">
    <div class="card-header">
      <h2>Trader Profiles &mdash; Deep Statistics</h2>
    </div>
    <div class="card-body">{profiles_html}</div>
  </div>

  <!-- Pattern Analysis -->
  <div class="card">
    <div class="card-header">
      <h2>Pattern Analysis &mdash; What do top traders do differently?</h2>
    </div>
    <div class="card-body">{patterns_html}</div>
  </div>

  <!-- Leaderboard (by PnL) -->
  <div class="card">
    <div class="card-header">
      <h2>Leaderboard &mdash; Top {n_wallets} Traders by PnL</h2>
    </div>
    <div class="card-body">{leaderboard_html}</div>
  </div>

</div>
</body>
</html>"""

    os.makedirs(os.path.dirname(DASHBOARD_HTML), exist_ok=True)
    with open(DASHBOARD_HTML, "w", encoding="utf-8") as f:
        f.write(html)
