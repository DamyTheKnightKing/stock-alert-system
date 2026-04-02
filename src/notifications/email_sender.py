"""
Email Notification Layer — Gmail SMTP (free).
Sends formatted daily analysis reports and intraday alerts.
Uses Jinja2 for clean HTML emails.
"""
import logging
import os
import smtplib
from dataclasses import dataclass
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Optional

from jinja2 import Template

from src.analysis.signals import Alert, FullAnalysis

logger = logging.getLogger(__name__)

# Gmail SMTP config
SMTP_HOST = "smtp.gmail.com"
SMTP_PORT = 587

DAILY_REPORT_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
<style>
  body { font-family: Arial, sans-serif; font-size: 13px; color: #222; background: #f5f5f5; }
  .container { max-width: 900px; margin: 20px auto; background: #fff; padding: 20px; border-radius: 8px; }
  h1 { color: #1a1a2e; border-bottom: 2px solid #e63946; padding-bottom: 8px; }
  h2 { color: #1a1a2e; margin-top: 30px; }
  .symbol-block { border: 1px solid #ddd; border-radius: 6px; margin: 15px 0; padding: 15px; }
  .bullish { border-left: 4px solid #2ecc71; }
  .bearish { border-left: 4px solid #e74c3c; }
  .sideways { border-left: 4px solid #f39c12; }
  .unknown { border-left: 4px solid #95a5a6; }
  .badge { display: inline-block; padding: 2px 8px; border-radius: 10px; font-size: 11px; font-weight: bold; }
  .badge-bull { background: #2ecc71; color: #fff; }
  .badge-bear { background: #e74c3c; color: #fff; }
  .badge-neutral { background: #95a5a6; color: #fff; }
  .badge-etf { background: #3498db; color: #fff; }
  .badge-stock { background: #9b59b6; color: #fff; }
  .metric { display: inline-block; margin: 4px 10px 4px 0; }
  .metric-label { color: #666; font-size: 11px; }
  .metric-value { font-weight: bold; }
  .alert-box { background: #fff3cd; border: 1px solid #ffc107; border-radius: 4px; padding: 10px; margin: 8px 0; }
  .alert-strong-buy { background: #d4edda; border-color: #28a745; }
  .alert-buy { background: #cce5ff; border-color: #004085; }
  .alert-sell { background: #f8d7da; border-color: #721c24; }
  .alert-strong-sell { background: #f8d7da; border-color: #721c24; font-weight: bold; }
  .alert-watch { background: #fff3cd; border-color: #856404; }
  .pre-signal { font-style: italic; color: #666; }
  .confidence-bar { height: 6px; background: #eee; border-radius: 3px; margin-top: 4px; }
  .confidence-fill { height: 6px; background: #2ecc71; border-radius: 3px; }
  table { width: 100%; border-collapse: collapse; margin: 10px 0; }
  td, th { padding: 6px 10px; border-bottom: 1px solid #eee; text-align: left; }
  th { background: #f8f9fa; font-weight: bold; }
  .footer { font-size: 11px; color: #999; margin-top: 30px; border-top: 1px solid #eee; padding-top: 10px; }
</style>
</head>
<body>
<div class="container">
  <h1>📊 Stock Alert System — Daily Report</h1>
  <p><strong>Date:</strong> {{ report_date }} | <strong>Symbols analyzed:</strong> {{ analyses|length }}</p>

  {% for a in analyses %}
  <div class="symbol-block {{ a.trend|lower }}">
    <h2>
      {{ a.symbol }}
      <span class="badge badge-{{ 'etf' if a.asset_type == 'ETF' else 'stock' }}">{{ a.asset_type }}</span>
      <span class="badge badge-{{ 'bull' if a.trend == 'BULLISH' else 'bear' if a.trend == 'BEARISH' else 'neutral' }}">
        {{ a.trend }}
      </span>
    </h2>

    <div>
      <span class="metric"><span class="metric-label">Price</span> <span class="metric-value">${{ "%.2f"|format(a.price) }}</span></span>
      <span class="metric"><span class="metric-label">Momentum</span> <span class="metric-value">{{ a.momentum }}</span></span>
      {% if a.technical %}
      <span class="metric"><span class="metric-label">RSI</span> <span class="metric-value">{{ "%.1f"|format(a.technical.rsi) if a.technical.rsi else 'N/A' }}</span></span>
      <span class="metric"><span class="metric-label">50 SMA</span> <span class="metric-value">${{ "%.2f"|format(a.technical.sma_50) if a.technical.sma_50 else 'N/A' }}</span></span>
      <span class="metric"><span class="metric-label">200 SMA</span> <span class="metric-value">${{ "%.2f"|format(a.technical.sma_200) if a.technical.sma_200 else 'N/A' }}</span></span>
      <span class="metric"><span class="metric-label">Support</span> <span class="metric-value">${{ "%.2f"|format(a.technical.support) if a.technical.support else 'N/A' }}</span></span>
      <span class="metric"><span class="metric-label">Resistance</span> <span class="metric-value">${{ "%.2f"|format(a.technical.resistance) if a.technical.resistance else 'N/A' }}</span></span>
      {% endif %}
    </div>

    <table>
      <tr>
        <th colspan="2">SHORT-TERM (1–14 days)</th>
        <th colspan="2">MID-TERM (1–3 months)</th>
        <th colspan="2">LONG-TERM (6–24 months)</th>
      </tr>
      <tr>
        <td><strong>Direction:</strong> {{ a.st_direction }}</td>
        <td><strong>Entry:</strong> {{ a.st_entry_zone }}</td>
        <td colspan="2">{{ a.mt_strategy }}</td>
        <td><strong>{{ a.lt_stance }}</strong></td>
        <td>{{ a.lt_reason }}</td>
      </tr>
      <tr>
        <td><strong>Target:</strong> {{ a.st_exit_target }}</td>
        <td><strong>Risk:</strong> {{ a.st_risk }}</td>
        <td colspan="2"><em>{{ a.mt_expected_outcome }}</em></td>
        <td colspan="2"></td>
      </tr>
    </table>

    <div><strong>Confidence Score:</strong> {{ a.confidence_score }}/10
      <div class="confidence-bar"><div class="confidence-fill" style="width: {{ a.confidence_score * 10 }}%"></div></div>
    </div>

    {% if a.alerts %}
    <h3 style="margin-top: 15px;">⚡ Alerts ({{ a.alerts|length }})</h3>
    {% for alert in a.alerts %}
    <div class="alert-box alert-{{ alert.action|lower|replace(' ', '-') }}">
      <strong>{{ alert.signal_type }}</strong>
      {% if alert.is_pre_signal %}<span class="pre-signal">[PRE-SIGNAL — tomorrow's setup]</span>{% endif %}
      <br>
      <strong>Signal:</strong> {{ alert.signal_type }}<br>
      <strong>Condition:</strong> {{ alert.condition }}<br>
      <strong>Action:</strong> <strong>{{ alert.action }}</strong> — {{ alert.action_detail }}<br>
      {% if alert.entry_zone %}<strong>Entry:</strong> {{ alert.entry_zone }} | {% endif %}
      {% if alert.exit_target %}<strong>Target:</strong> {{ alert.exit_target }} | {% endif %}
      {% if alert.stop_loss %}<strong>Stop:</strong> {{ alert.stop_loss }}<br>{% endif %}
      <strong>Confidence:</strong> {{ alert.confidence }} ({{ alert.confidence_score }}/10)
    </div>
    {% endfor %}
    {% else %}
    <p style="color: #999; font-style: italic;">No alerts triggered for {{ a.symbol }} today.</p>
    {% endif %}
  </div>
  {% endfor %}

  <div class="footer">
    Generated by Stock Alert System | Data source: Yahoo Finance | Not financial advice.
    Past performance is not indicative of future results.
  </div>
</div>
</body>
</html>
"""

INTRADAY_ALERT_TEMPLATE = """
<!DOCTYPE html>
<html>
<body style="font-family: Arial, sans-serif; font-size: 13px;">
<h2>⚡ Intraday Alert — {{ alert_count }} signal(s) detected</h2>
{% for alert in alerts %}
<div style="border: 1px solid #ddd; padding: 12px; margin: 10px 0; border-radius: 6px;
     border-left: 4px solid {{ '#2ecc71' if 'BUY' in alert.action else '#e74c3c' if 'SELL' in alert.action else '#f39c12' }};">
  <strong>{{ alert.symbol }} — {{ alert.signal_type }}</strong><br>
  {{ alert.condition }}<br>
  <strong>Action: {{ alert.action }}</strong> — {{ alert.action_detail }}<br>
  Confidence: {{ alert.confidence }} | Risk: {{ alert.risk_pct }}%
</div>
{% endfor %}
<p style="color: #999; font-size: 11px;">Not financial advice. Stock Alert System.</p>
</body>
</html>
"""


def send_daily_report(analyses: list[FullAnalysis], report_date: str = None) -> bool:
    """Send the full daily analysis email."""
    sender = os.getenv("EMAIL_SENDER")
    password = os.getenv("EMAIL_PASSWORD")
    recipients_raw = os.getenv("EMAIL_RECIPIENTS", "")

    if not all([sender, password, recipients_raw]):
        logger.warning("Email not configured — set EMAIL_SENDER, EMAIL_PASSWORD, EMAIL_RECIPIENTS")
        return False

    recipients = [r.strip() for r in recipients_raw.split(",") if r.strip()]
    subject_prefix = "[StockAlert]"
    subject = f"{subject_prefix} Daily Report — {report_date or 'Today'} | {len(analyses)} symbols"

    html = Template(DAILY_REPORT_TEMPLATE).render(
        analyses=analyses,
        report_date=report_date or "Today",
    )

    return _send_email(sender, password, recipients, subject, html)


def send_intraday_alerts(alerts: list[Alert]) -> bool:
    """Send intraday alert email (only called when alerts exist)."""
    if not alerts:
        return True

    sender = os.getenv("EMAIL_SENDER")
    password = os.getenv("EMAIL_PASSWORD")
    recipients_raw = os.getenv("EMAIL_RECIPIENTS", "")

    if not all([sender, password, recipients_raw]):
        logger.warning("Email not configured — skipping intraday notification")
        return False

    recipients = [r.strip() for r in recipients_raw.split(",") if r.strip()]
    subject = f"[StockAlert] ⚡ INTRADAY — {len(alerts)} alert(s) triggered"

    html = Template(INTRADAY_ALERT_TEMPLATE).render(
        alerts=alerts,
        alert_count=len(alerts),
    )

    return _send_email(sender, password, recipients, subject, html)


def _send_email(sender: str, password: str, recipients: list[str], subject: str, html: str) -> bool:
    try:
        msg = MIMEMultipart("alternative")
        msg["From"] = sender
        msg["To"] = ", ".join(recipients)
        msg["Subject"] = subject
        msg.attach(MIMEText(html, "html"))

        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
            server.ehlo()
            server.starttls()
            server.login(sender, password)
            server.sendmail(sender, recipients, msg.as_string())

        logger.info(f"Email sent: '{subject}' → {recipients}")
        return True

    except smtplib.SMTPAuthenticationError:
        logger.error("Gmail SMTP authentication failed. Check EMAIL_PASSWORD is a Gmail App Password.")
        return False
    except Exception as e:
        logger.error(f"Email send failed: {e}")
        return False
