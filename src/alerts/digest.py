"""
Morning Digest Builder.
Ranks analyses by confidence, picks top 3 ETFs + top 3 stocks,
computes market pulse, and assembles the full digest payload.
"""
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

from src.analysis.signals import FullAnalysis


@dataclass
class MarketPulse:
    date: str
    risk_environment: str      # RISK-ON / RISK-OFF / NEUTRAL
    risk_color: str            # for email styling
    bullish_count: int
    neutral_count: int
    bearish_count: int
    total: int
    verdict: str               # plain English 2-line summary


@dataclass
class MorningDigest:
    pulse: MarketPulse
    top_etfs: list[FullAnalysis]        # top 3 ETFs by confidence
    top_stocks: list[FullAnalysis]      # top 3 stocks by confidence
    all_analyses: list[FullAnalysis]    # full watchlist for table
    generated_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())


def build_digest(analyses: list[FullAnalysis]) -> MorningDigest:
    """
    Build the morning digest from a list of FullAnalysis objects.
    Separates ETFs from stocks, ranks by confidence, picks top 3 each.
    """
    etfs    = [a for a in analyses if a.asset_type == "ETF"]
    stocks  = [a for a in analyses if a.asset_type != "ETF"]

    top_etfs   = _rank(etfs)[:3]
    top_stocks = _rank(stocks)[:3]

    pulse = _build_pulse(analyses)

    return MorningDigest(
        pulse=pulse,
        top_etfs=top_etfs,
        top_stocks=top_stocks,
        all_analyses=sorted(analyses, key=lambda a: a.confidence_score, reverse=True),
    )


def _rank(analyses: list[FullAnalysis]) -> list[FullAnalysis]:
    """
    Rank by: highest alert confidence first, then analysis confidence score.
    Symbols with active alerts bubble to the top.
    """
    def score(a: FullAnalysis) -> int:
        alert_score = max((al.confidence_score for al in a.alerts), default=0)
        return max(alert_score, a.confidence_score)

    return sorted(analyses, key=score, reverse=True)


def _build_pulse(analyses: list[FullAnalysis]) -> MarketPulse:
    bullish = sum(1 for a in analyses if a.trend == "BULLISH")
    bearish = sum(1 for a in analyses if a.trend == "BEARISH")
    neutral = len(analyses) - bullish - bearish

    # Use SPY as primary risk barometer
    spy = next((a for a in analyses if a.symbol == "SPY"), None)
    tlt = next((a for a in analyses if a.symbol == "TLT"), None)

    if spy and spy.trend == "BULLISH":
        risk_env = "RISK-ON"
        risk_color = "#2ecc71"
    elif spy and spy.trend == "BEARISH":
        risk_env = "RISK-OFF"
        risk_color = "#e74c3c"
    else:
        risk_env = "NEUTRAL"
        risk_color = "#f39c12"

    # Plain English verdict
    verdict = _generate_verdict(analyses, spy, tlt, bullish, bearish, neutral)

    return MarketPulse(
        date=datetime.now().strftime("%A, %b %d %Y"),
        risk_environment=risk_env,
        risk_color=risk_color,
        bullish_count=bullish,
        neutral_count=neutral,
        bearish_count=bearish,
        total=len(analyses),
        verdict=verdict,
    )


def _generate_verdict(analyses, spy, tlt, bullish, bearish, neutral) -> str:
    total = len(analyses)
    if total == 0:
        return "No data available."

    bull_pct = (bullish / total) * 100
    bear_pct = (bearish / total) * 100

    lines = []

    # Market breadth read
    if bull_pct >= 60:
        lines.append(f"{bullish}/{total} symbols in uptrend — broad market strength. Favor long setups.")
    elif bear_pct >= 60:
        lines.append(f"{bearish}/{total} symbols in downtrend — market under distribution. Protect capital.")
    else:
        lines.append(f"Market mixed: {bullish} bullish, {bearish} bearish, {neutral} neutral. Be selective.")

    # SPY specific read
    if spy:
        if spy.trend == "BULLISH":
            lines.append(f"SPY at ${spy.price:.2f} — above key moving averages. Trend intact, dips are opportunities.")
        elif spy.trend == "BEARISH":
            lines.append(f"SPY at ${spy.price:.2f} — below key moving averages. Rallies are sell opportunities until trend reverses.")
        else:
            lines.append(f"SPY at ${spy.price:.2f} consolidating. Wait for directional break before adding exposure.")

    # Bond signal
    if tlt:
        if tlt.trend == "BULLISH":
            lines.append("TLT rising — flight to safety. Defensive posture warranted.")
        elif tlt.trend == "BEARISH":
            lines.append("TLT falling — yields rising. Pressure on growth and rate-sensitive names.")

    return " ".join(lines[:2])  # max 2 sentences


def get_action_color(action: str) -> str:
    mapping = {
        "STRONG BUY":  "#1a7a4a",
        "BUY":         "#2ecc71",
        "WATCH":       "#f39c12",
        "HOLD":        "#95a5a6",
        "SELL":        "#e74c3c",
        "STRONG SELL": "#922b21",
    }
    return mapping.get(action.upper(), "#95a5a6")


def get_trend_color(trend: str) -> str:
    return {"BULLISH": "#2ecc71", "BEARISH": "#e74c3c"}.get(trend, "#f39c12")


def compute_rr(entry_zone: str, exit_target: str, stop_loss: str) -> Optional[str]:
    """Parse price strings and compute Risk/Reward ratio."""
    try:
        entry = _extract_price(entry_zone)
        target = _extract_price(exit_target)
        stop = _extract_price(stop_loss)
        if None in (entry, target, stop) or entry == stop:
            return None
        reward = abs(target - entry)
        risk = abs(entry - stop)
        if risk == 0:
            return None
        ratio = reward / risk
        return f"1 : {ratio:.1f}"
    except Exception:
        return None


def _extract_price(s: str) -> Optional[float]:
    """Extract first dollar amount from a string like '$177.39' or '$177–179'."""
    if not s:
        return None
    import re
    match = re.search(r"\$?([\d,]+\.?\d*)", s.replace(",", ""))
    if match:
        return float(match.group(1))
    return None
