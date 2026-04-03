"""
Email Notification Layer — Resend + MJML.

Pipeline:
  1. Load .mjml template (Jinja2 syntax embedded)
  2. Render Jinja2 → valid MJML string
  3. Compile MJML → bulletproof responsive HTML (table-based, works in Gmail/mobile)
  4. Send via Resend API

Strategy:
  - Morning digest (8:30 AM ET daily): top 3 ETFs + top 3 stocks + watchlist table
  - Intraday alert: only confidence >= 8/10, max once per day
"""
import logging
import os
from pathlib import Path

import mjml
import resend
from jinja2 import Template

from src.alerts.digest import MorningDigest, compute_rr, get_action_color, get_trend_color
from src.analysis.signals import Alert

logger = logging.getLogger(__name__)

TEMPLATES_DIR = Path(__file__).parent.parent.parent / "templates"


def _render(template_file: str, **context) -> str:
    """Jinja2 render → MJML compile → HTML."""
    raw = (TEMPLATES_DIR / template_file).read_text()
    mjml_str = Template(raw).render(**context)
    result = mjml.mjml_to_html(mjml_str)
    if result.errors:
        for err in result.errors:
            logger.warning(f"MJML warning: {err}")
    return result.html

FROM_EMAIL = "Stock Alerts <alerts@theknightcodes.dev>"


# ---------------------------------------------------------------------------
# Legacy inline templates — kept only as fallback reference, not used
# ---------------------------------------------------------------------------

_MORNING_DIGEST_HTML_LEGACY = """
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Arial, sans-serif;
         background: #f0f2f5; color: #1a1a2e; font-size: 14px; }
  .wrapper { max-width: 680px; margin: 0 auto; padding: 20px 10px; }

  /* Header */
  .header { background: #1a1a2e; border-radius: 10px 10px 0 0; padding: 20px 24px; }
  .header-title { color: #fff; font-size: 20px; font-weight: 700; letter-spacing: 0.5px; }
  .header-sub { color: #a0a8c0; font-size: 12px; margin-top: 4px; }

  /* Market Pulse */
  .pulse { background: #fff; padding: 16px 24px; border-bottom: 1px solid #eee; }
  .pulse-row { display: flex; align-items: center; gap: 12px; flex-wrap: wrap; }
  .pulse-badge { padding: 4px 12px; border-radius: 20px; font-size: 12px;
                  font-weight: 700; color: #fff; letter-spacing: 0.5px; }
  .pulse-counts { display: flex; gap: 16px; margin-top: 10px; }
  .pulse-count { text-align: center; }
  .pulse-count-num { font-size: 22px; font-weight: 700; }
  .pulse-count-lbl { font-size: 10px; color: #888; text-transform: uppercase; }
  .verdict { margin-top: 12px; font-size: 13px; color: #444;
             background: #f8f9fa; border-left: 3px solid #1a1a2e;
             padding: 10px 14px; border-radius: 0 6px 6px 0; line-height: 1.6; }

  /* Section headers */
  .section { background: #fff; margin-top: 2px; padding: 0 24px 16px; }
  .section-header { padding: 14px 0 10px; font-size: 11px; font-weight: 700;
                     text-transform: uppercase; letter-spacing: 1px; color: #888;
                     border-bottom: 1px solid #f0f0f0; margin-bottom: 12px; }

  /* Signal Card */
  .card { border: 1px solid #e8eaf0; border-radius: 8px; margin-bottom: 12px; overflow: hidden; }
  .card-header { padding: 12px 16px; display: flex;
                  justify-content: space-between; align-items: center; }
  .card-symbol { font-size: 18px; font-weight: 700; }
  .card-name { font-size: 11px; color: #888; margin-top: 2px; }
  .card-price { text-align: right; }
  .card-price-val { font-size: 18px; font-weight: 700; }
  .card-price-sub { font-size: 11px; color: #888; }
  .card-action { padding: 8px 16px; color: #fff; font-size: 13px; font-weight: 700;
                  letter-spacing: 0.5px; display: flex; justify-content: space-between;
                  align-items: center; }
  .card-confidence { font-size: 11px; font-weight: 400; opacity: 0.9; }
  .card-verdict { padding: 10px 16px; font-size: 13px; color: #333;
                   background: #fafafa; border-top: 1px solid #f0f0f0;
                   line-height: 1.5; font-style: italic; }
  .card-levels { display: flex; border-top: 1px solid #f0f0f0; }
  .card-level { flex: 1; padding: 10px 12px; text-align: center;
                 border-right: 1px solid #f0f0f0; }
  .card-level:last-child { border-right: none; }
  .card-level-lbl { font-size: 10px; color: #888; text-transform: uppercase;
                     letter-spacing: 0.5px; margin-bottom: 3px; }
  .card-level-val { font-size: 13px; font-weight: 600; }
  .card-footer { display: flex; justify-content: space-between; padding: 8px 16px;
                  background: #f8f9fa; border-top: 1px solid #f0f0f0; font-size: 11px; color: #666; }
  .conf-bar-bg { background: #e0e0e0; border-radius: 4px; height: 5px;
                  width: 80px; display: inline-block; vertical-align: middle; margin: 0 6px; }
  .conf-bar-fill { height: 5px; border-radius: 4px; background: #2ecc71; }
  .horizon-row { display: flex; border-top: 1px solid #f0f0f0; }
  .horizon { flex: 1; padding: 8px 10px; border-right: 1px solid #f0f0f0; font-size: 11px; }
  .horizon:last-child { border-right: none; }
  .horizon-lbl { color: #888; font-size: 10px; text-transform: uppercase;
                  letter-spacing: 0.5px; margin-bottom: 3px; }
  .horizon-val { font-weight: 600; color: #1a1a2e; }

  /* Watchlist Table */
  .table-wrap { overflow-x: auto; }
  table { width: 100%; border-collapse: collapse; font-size: 12px; }
  th { background: #f0f2f5; padding: 8px 10px; text-align: left;
       font-size: 10px; text-transform: uppercase; letter-spacing: 0.5px; color: #666; }
  td { padding: 8px 10px; border-bottom: 1px solid #f5f5f5; }
  tr:hover td { background: #fafafa; }
  .trend-bull { color: #2ecc71; font-weight: 600; }
  .trend-bear { color: #e74c3c; font-weight: 600; }
  .trend-side { color: #f39c12; font-weight: 600; }
  .stance-pill { padding: 2px 8px; border-radius: 10px; font-size: 10px;
                  font-weight: 700; display: inline-block; }
  .stance-ACCUMULATE { background: #d4edda; color: #1a7a4a; }
  .stance-HOLD       { background: #e2e3e5; color: #383d41; }
  .stance-AVOID      { background: #f8d7da; color: #721c24; }

  /* Footer */
  .footer { background: #fff; border-top: 1px solid #eee; padding: 14px 24px;
             border-radius: 0 0 10px 10px; }
  .footer-text { font-size: 11px; color: #aaa; line-height: 1.6; }
</style>
</head>
<body>
<div class="wrapper">

  <!-- HEADER -->
  <div class="header">
    <div class="header-title">Stock Alert System</div>
    <div class="header-sub">Morning Briefing &nbsp;|&nbsp; {{ pulse.date }}</div>
  </div>

  <!-- MARKET PULSE -->
  <div class="pulse">
    <div class="pulse-row">
      <span style="font-weight:700;font-size:13px;">Market Pulse</span>
      <span class="pulse-badge" style="background:{{ pulse.risk_color }};">
        {{ pulse.risk_environment }}
      </span>
    </div>
    <div class="pulse-counts">
      <div class="pulse-count">
        <div class="pulse-count-num" style="color:#2ecc71;">{{ pulse.bullish_count }}</div>
        <div class="pulse-count-lbl">Bullish</div>
      </div>
      <div class="pulse-count">
        <div class="pulse-count-num" style="color:#f39c12;">{{ pulse.neutral_count }}</div>
        <div class="pulse-count-lbl">Neutral</div>
      </div>
      <div class="pulse-count">
        <div class="pulse-count-num" style="color:#e74c3c;">{{ pulse.bearish_count }}</div>
        <div class="pulse-count-lbl">Bearish</div>
      </div>
      <div class="pulse-count">
        <div class="pulse-count-num" style="color:#1a1a2e;">{{ pulse.total }}</div>
        <div class="pulse-count-lbl">Watched</div>
      </div>
    </div>
    <div class="verdict">{{ pulse.verdict }}</div>
  </div>

  <!-- TOP ETF SIGNALS -->
  <div class="section">
    <div class="section-header">Top ETF Signals</div>
    {% for a in top_etfs %}
    {% set top_alert = a.alerts | sort(attribute='confidence_score', reverse=True) | first if a.alerts else none %}
    {% set action = top_alert.action if top_alert else 'HOLD' %}
    {% set action_color = get_action_color(action) %}
    {% set rr = compute_rr(top_alert.entry_zone, top_alert.exit_target, top_alert.stop_loss) if top_alert else none %}
    <div class="card">
      <div class="card-header" style="background:#fafafa;">
        <div>
          <div class="card-symbol">{{ a.symbol }}</div>
          <div class="card-name">{{ a.fundamentals.get('name', a.symbol) if a.fundamentals else a.symbol }}</div>
        </div>
        <div class="card-price">
          <div class="card-price-val">${{ "%.2f"|format(a.price) }}</div>
          <div class="card-price-sub">{{ a.asset_type }}</div>
        </div>
      </div>
      <div class="card-action" style="background:{{ action_color }};">
        <span>{{ action }}</span>
        <span class="card-confidence">
          Confidence &nbsp;
          <span class="conf-bar-bg"><span class="conf-bar-fill" style="width:{{ a.confidence_score * 10 }}%;"></span></span>
          {{ a.confidence_score }}/10
        </span>
      </div>
      {% if top_alert %}
      <div class="card-verdict">{{ top_alert.condition }}</div>
      <div class="card-levels">
        <div class="card-level">
          <div class="card-level-lbl">Entry</div>
          <div class="card-level-val">{{ top_alert.entry_zone or '—' }}</div>
        </div>
        <div class="card-level">
          <div class="card-level-lbl">Target</div>
          <div class="card-level-val">{{ top_alert.exit_target or '—' }}</div>
        </div>
        <div class="card-level">
          <div class="card-level-lbl">Stop Loss</div>
          <div class="card-level-val">{{ top_alert.stop_loss or '—' }}</div>
        </div>
        <div class="card-level">
          <div class="card-level-lbl">Risk/Reward</div>
          <div class="card-level-val">{{ rr or '—' }}</div>
        </div>
      </div>
      {% endif %}
      <div class="horizon-row">
        <div class="horizon">
          <div class="horizon-lbl">Short-Term</div>
          <div class="horizon-val">{{ a.st_direction }}</div>
        </div>
        <div class="horizon">
          <div class="horizon-lbl">Mid-Term</div>
          <div class="horizon-val">{{ a.mt_expected_outcome[:40] ~ '…' if a.mt_expected_outcome|length > 40 else a.mt_expected_outcome }}</div>
        </div>
        <div class="horizon">
          <div class="horizon-lbl">Long-Term</div>
          <div class="horizon-val">{{ a.lt_stance }}</div>
        </div>
      </div>
    </div>
    {% else %}
    <p style="color:#999;font-size:13px;padding:10px 0;">No ETF signals today.</p>
    {% endfor %}
  </div>

  <!-- TOP STOCK SIGNALS -->
  <div class="section">
    <div class="section-header">Top Stock Signals</div>
    {% for a in top_stocks %}
    {% set top_alert = a.alerts | sort(attribute='confidence_score', reverse=True) | first if a.alerts else none %}
    {% set action = top_alert.action if top_alert else 'HOLD' %}
    {% set action_color = get_action_color(action) %}
    {% set rr = compute_rr(top_alert.entry_zone, top_alert.exit_target, top_alert.stop_loss) if top_alert else none %}
    <div class="card">
      <div class="card-header" style="background:#fafafa;">
        <div>
          <div class="card-symbol">{{ a.symbol }}</div>
          <div class="card-name">{{ a.fundamentals.get('name', a.symbol) if a.fundamentals else a.symbol }}</div>
        </div>
        <div class="card-price">
          <div class="card-price-val">${{ "%.2f"|format(a.price) }}</div>
          <div class="card-price-sub">{{ a.fundamentals.get('sector', 'Stock') if a.fundamentals else 'Stock' }}</div>
        </div>
      </div>
      <div class="card-action" style="background:{{ action_color }};">
        <span>{{ action }}</span>
        <span class="card-confidence">
          Confidence &nbsp;
          <span class="conf-bar-bg"><span class="conf-bar-fill" style="width:{{ a.confidence_score * 10 }}%;"></span></span>
          {{ a.confidence_score }}/10
        </span>
      </div>
      {% if top_alert %}
      <div class="card-verdict">{{ top_alert.condition }}</div>
      <div class="card-levels">
        <div class="card-level">
          <div class="card-level-lbl">Entry</div>
          <div class="card-level-val">{{ top_alert.entry_zone or '—' }}</div>
        </div>
        <div class="card-level">
          <div class="card-level-lbl">Target</div>
          <div class="card-level-val">{{ top_alert.exit_target or '—' }}</div>
        </div>
        <div class="card-level">
          <div class="card-level-lbl">Stop Loss</div>
          <div class="card-level-val">{{ top_alert.stop_loss or '—' }}</div>
        </div>
        <div class="card-level">
          <div class="card-level-lbl">Risk/Reward</div>
          <div class="card-level-val">{{ rr or '—' }}</div>
        </div>
      </div>
      {% endif %}
      <div class="horizon-row">
        <div class="horizon">
          <div class="horizon-lbl">Short-Term</div>
          <div class="horizon-val">{{ a.st_direction }}</div>
        </div>
        <div class="horizon">
          <div class="horizon-lbl">Mid-Term</div>
          <div class="horizon-val">{{ a.mt_expected_outcome[:40] ~ '…' if a.mt_expected_outcome|length > 40 else a.mt_expected_outcome }}</div>
        </div>
        <div class="horizon">
          <div class="horizon-lbl">Long-Term</div>
          <div class="horizon-val">{{ a.lt_stance }}</div>
        </div>
      </div>
    </div>
    {% else %}
    <p style="color:#999;font-size:13px;padding:10px 0;">No stock signals today.</p>
    {% endfor %}
  </div>

  <!-- FULL WATCHLIST TABLE -->
  <div class="section">
    <div class="section-header">Full Watchlist Snapshot</div>
    <div class="table-wrap">
      <table>
        <thead>
          <tr>
            <th>Symbol</th>
            <th>Price</th>
            <th>Trend</th>
            <th>RSI</th>
            <th>Support</th>
            <th>Resistance</th>
            <th>LT Stance</th>
            <th>Confidence</th>
          </tr>
        </thead>
        <tbody>
          {% for a in all_analyses %}
          <tr>
            <td><strong>{{ a.symbol }}</strong></td>
            <td>${{ "%.2f"|format(a.price) }}</td>
            <td>
              <span class="trend-{{ 'bull' if a.trend == 'BULLISH' else 'bear' if a.trend == 'BEARISH' else 'side' }}">
                {{ a.trend }}
              </span>
            </td>
            <td>{{ "%.0f"|format(a.technical.rsi) if a.technical and a.technical.rsi else '—' }}</td>
            <td>{{ '$' ~ "%.2f"|format(a.technical.support) if a.technical and a.technical.support else '—' }}</td>
            <td>{{ '$' ~ "%.2f"|format(a.technical.resistance) if a.technical and a.technical.resistance else '—' }}</td>
            <td>
              <span class="stance-pill stance-{{ a.lt_stance }}">{{ a.lt_stance }}</span>
            </td>
            <td>{{ a.confidence_score }}/10</td>
          </tr>
          {% endfor %}
        </tbody>
      </table>
    </div>
  </div>

  <!-- FOOTER -->
  <div class="footer">
    <div class="footer-text">
      Stock Alert System &nbsp;|&nbsp; Data: Yahoo Finance &nbsp;|&nbsp;
      {{ all_analyses | length }} symbols monitored &nbsp;|&nbsp;
      Generated {{ pulse.date }}<br>
      <strong>Disclaimer:</strong> This is not financial advice.
      All signals are algorithmic and for informational purposes only.
      Always do your own research before making investment decisions.
    </div>
  </div>

</div>
</body>
</html>
"""


# ---------------------------------------------------------------------------
# Intraday Alert Template (urgent, minimal)
# ---------------------------------------------------------------------------

INTRADAY_HTML = """
<!DOCTYPE html>
<html>
<head>
<style>
  body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Arial, sans-serif;
         background: #f0f2f5; font-size: 14px; color: #1a1a2e; }
  .wrapper { max-width: 680px; margin: 0 auto; padding: 20px 10px; }
  .header { background: #1a1a2e; border-radius: 10px 10px 0 0; padding: 16px 24px; }
  .header-title { color: #fff; font-size: 18px; font-weight: 700; }
  .header-sub { color: #f39c12; font-size: 12px; margin-top: 4px; }
  .body { background: #fff; padding: 16px 24px; border-radius: 0 0 10px 10px; }
  .card { border-radius: 8px; margin-bottom: 12px; overflow: hidden;
           border: 1px solid #e8eaf0; }
  .card-top { padding: 12px 16px; color: #fff; display: flex;
               justify-content: space-between; align-items: center; }
  .card-symbol { font-size: 18px; font-weight: 700; }
  .card-action { font-size: 13px; font-weight: 700; background: rgba(255,255,255,0.2);
                  padding: 3px 10px; border-radius: 12px; }
  .card-body { padding: 12px 16px; font-size: 13px; line-height: 1.6; }
  .levels { display: flex; gap: 16px; margin-top: 10px; flex-wrap: wrap; }
  .level { font-size: 12px; }
  .level-lbl { color: #888; }
  .level-val { font-weight: 600; }
  .conf { font-size: 11px; color: #888; margin-top: 8px; }
  .footer { font-size: 11px; color: #aaa; margin-top: 16px; padding-top: 12px;
             border-top: 1px solid #eee; }
</style>
</head>
<body>
<div class="wrapper">
  <div class="header">
    <div class="header-title">Intraday Alert</div>
    <div class="header-sub">{{ alert_count }} high-confidence signal(s) — {{ now }}</div>
  </div>
  <div class="body">
    {% for alert in alerts %}
    <div class="card">
      <div class="card-top" style="background:{{ get_action_color(alert.action) }};">
        <span class="card-symbol">{{ alert.symbol }}</span>
        <span class="card-action">{{ alert.action }}</span>
      </div>
      <div class="card-body">
        <strong>{{ alert.signal_type }}</strong>
        {% if alert.is_pre_signal %}<span style="color:#888;font-size:11px;"> — Pre-Signal (next session)</span>{% endif %}
        <br>{{ alert.condition }}<br>
        <em>{{ alert.action_detail }}</em>
        <div class="levels">
          {% if alert.entry_zone %}
          <div class="level"><div class="level-lbl">Entry</div><div class="level-val">{{ alert.entry_zone }}</div></div>
          {% endif %}
          {% if alert.exit_target %}
          <div class="level"><div class="level-lbl">Target</div><div class="level-val">{{ alert.exit_target }}</div></div>
          {% endif %}
          {% if alert.stop_loss %}
          <div class="level"><div class="level-lbl">Stop</div><div class="level-val">{{ alert.stop_loss }}</div></div>
          {% endif %}
          {% if alert.risk_pct %}
          <div class="level"><div class="level-lbl">Risk</div><div class="level-val">{{ alert.risk_pct }}%</div></div>
          {% endif %}
        </div>
        <div class="conf">Confidence: {{ alert.confidence }} ({{ alert.confidence_score }}/10)</div>
      </div>
    </div>
    {% endfor %}
    <div class="footer">Not financial advice. Stock Alert System.</div>
  </div>
</div>
</body>
</html>
"""


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def send_morning_digest(digest: MorningDigest, report_date: str = None) -> bool:
    api_key = os.getenv("RESEND_API_KEY")
    recipients_raw = os.getenv("EMAIL_RECIPIENTS", "")

    if not api_key:
        logger.warning("RESEND_API_KEY not set — skipping email")
        return False
    if not recipients_raw:
        logger.warning("EMAIL_RECIPIENTS not set — skipping email")
        return False

    resend.api_key = api_key
    recipients = [r.strip() for r in recipients_raw.split(",") if r.strip()]

    bull = digest.pulse.bullish_count
    bear = digest.pulse.bearish_count
    subject = (
        f"[StockAlert] {digest.pulse.date} — "
        f"{bull} Bullish / {bear} Bearish | {digest.pulse.risk_environment}"
    )

    html = _render(
        "morning_digest.mjml",
        pulse=digest.pulse,
        top_etfs=digest.top_etfs,
        top_stocks=digest.top_stocks,
        all_analyses=digest.all_analyses,
        get_action_color=get_action_color,
        compute_rr=compute_rr,
    )

    return _send(recipients, subject, html)


def send_intraday_alerts(alerts: list[Alert]) -> bool:
    """Only sends alerts with confidence >= 8."""
    high_conf = [a for a in alerts if a.confidence_score >= 8]
    if not high_conf:
        return True

    api_key = os.getenv("RESEND_API_KEY")
    recipients_raw = os.getenv("EMAIL_RECIPIENTS", "")
    if not api_key or not recipients_raw:
        return False

    resend.api_key = api_key
    recipients = [r.strip() for r in recipients_raw.split(",") if r.strip()]

    from datetime import datetime
    now = datetime.now().strftime("%H:%M ET")
    subject = f"[StockAlert] INTRADAY — {len(high_conf)} high-confidence alert(s)"

    html = _render(
        "intraday_alert.mjml",
        alerts=high_conf,
        alert_count=len(high_conf),
        now=now,
        get_action_color=get_action_color,
    )

    return _send(recipients, subject, html)


# keep backward compat for any callers using old send_daily_report name
def send_daily_report(analyses, report_date=None) -> bool:
    from src.alerts.digest import build_digest
    digest = build_digest(analyses)
    return send_morning_digest(digest, report_date)


def _send(recipients: list[str], subject: str, html: str) -> bool:
    try:
        response = resend.Emails.send({
            "from": FROM_EMAIL,
            "to": recipients,
            "subject": subject,
            "html": html,
        })
        logger.info(f"Email sent: '{subject}' → {recipients} | id={response.get('id')}")
        return True
    except Exception as e:
        logger.error(f"Resend failed: {e}")
        return False
