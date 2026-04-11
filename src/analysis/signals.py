"""
Signal Generation Engine.
Takes TechnicalSnapshot + Fundamentals → produces structured Alert objects.
Follows the mandatory analysis framework: macro → technical → fundamental → risk.
"""
import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

from src.analysis.technical import TechnicalSnapshot

logger = logging.getLogger(__name__)

# Signal types
SIGNAL_BREAKOUT = "BREAKOUT"
SIGNAL_PRE_BREAKOUT = "PRE_BREAKOUT"
SIGNAL_BREAKDOWN = "BREAKDOWN"
SIGNAL_PRE_BREAKDOWN = "PRE_BREAKDOWN"
SIGNAL_RSI_EXTREME = "RSI_EXTREME"
SIGNAL_RSI_PREWARNING = "RSI_PREWARNING"
SIGNAL_VOLUME_SPIKE = "VOLUME_SPIKE"
SIGNAL_MACD_CROSS = "MACD_CROSS"
SIGNAL_BULLISH_CONFLUENCE = "BULLISH_CONFLUENCE"
SIGNAL_BEARISH_CONFLUENCE = "BEARISH_CONFLUENCE"

ACTION_STRONG_BUY = "STRONG BUY"
ACTION_BUY = "BUY"
ACTION_HOLD = "HOLD"
ACTION_SELL = "SELL"
ACTION_STRONG_SELL = "STRONG SELL"
ACTION_WATCH = "WATCH"


@dataclass
class Alert:
    symbol: str
    signal_type: str
    condition: str
    action: str
    action_detail: str
    confidence: str          # Low / Medium / High
    confidence_score: int    # 1-10
    entry_zone: Optional[str] = None
    exit_target: Optional[str] = None
    stop_loss: Optional[str] = None
    risk_pct: Optional[float] = None
    triggered_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())
    is_pre_signal: bool = False   # True = early warning for NEXT day


@dataclass
class FullAnalysis:
    symbol: str
    asset_type: str           # ETF / Stock
    trend: str
    momentum: str
    price: float
    # Short-term
    st_direction: str
    st_entry_zone: str
    st_exit_target: str
    st_risk: str
    # Mid-term
    mt_strategy: str
    mt_expected_outcome: str
    # Long-term
    lt_stance: str            # Hold / Accumulate / Avoid
    lt_reason: str
    # Meta
    confidence_score: int
    alerts: list[Alert] = field(default_factory=list)
    technical: Optional[TechnicalSnapshot] = None
    fundamentals: Optional[dict] = None
    news: list = field(default_factory=list)           # list[NewsItem] — v2
    reddit: Optional[object] = None                    # RedditSentiment — v2
    ai_commentary: Optional[str] = None                # AI narrative — v3 OpenRouter
    generated_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())


def generate_alerts(snap: TechnicalSnapshot, fundamentals: dict = None, asset_type: str = "stock") -> list[Alert]:
    """
    Evaluate TechnicalSnapshot signals → produce Alert list.
    Each alert maps to the output format defined in the system spec.
    """
    alerts = []
    sigs = set(snap.signals)

    # --- Breakout alert ---
    if "NEAR_RESISTANCE" in sigs and snap.momentum in ("STRONG_BULL", "BULL") and "VOLUME_ELEVATED" in sigs or "VOLUME_SPIKE" in sigs:
        confidence = _score_confidence(snap, bullish=True)
        alerts.append(Alert(
            symbol=snap.symbol,
            signal_type=SIGNAL_PRE_BREAKOUT,
            condition=(
                f"Price within {snap.pct_to_resistance:.1f}% of resistance at ${snap.resistance:.2f}. "
                f"Volume {snap.volume_ratio:.1f}x avg. RSI {snap.rsi:.0f}. Momentum: {snap.momentum}."
            ),
            action=ACTION_BUY if confidence >= 7 else ACTION_WATCH,
            action_detail=(
                f"Monitor for confirmed break above ${snap.resistance:.2f} with volume > 1.5x avg. "
                f"Enter on breakout candle close."
            ),
            confidence="High" if confidence >= 7 else "Medium",
            confidence_score=confidence,
            entry_zone=f"${snap.resistance:.2f} breakout confirmation",
            exit_target=f"${snap.price * 1.04:.2f} (+4%)",
            stop_loss=f"${snap.resistance * 0.99:.2f} (below breakout level)",
            risk_pct=1.5,
            is_pre_signal=True,
        ))

    # --- Breakdown alert ---
    if "NEAR_SUPPORT" in sigs and snap.momentum in ("STRONG_BEAR", "BEAR"):
        confidence = _score_confidence(snap, bullish=False)
        alerts.append(Alert(
            symbol=snap.symbol,
            signal_type=SIGNAL_PRE_BREAKDOWN,
            condition=(
                f"Price within {snap.pct_to_support:.1f}% of support at ${snap.support:.2f}. "
                f"Momentum: {snap.momentum}. Trend: {snap.trend}."
            ),
            action=ACTION_SELL if confidence >= 7 else ACTION_WATCH,
            action_detail=(
                f"Watch for breakdown below ${snap.support:.2f} with volume confirmation. "
                f"Reduce exposure if closes below support."
            ),
            confidence="High" if confidence >= 7 else "Medium",
            confidence_score=confidence,
            entry_zone=f"Breakdown below ${snap.support:.2f}",
            exit_target=f"${snap.price * 0.96:.2f} (-4%)",
            stop_loss=f"${snap.support * 1.01:.2f} (above breakdown level)",
            risk_pct=2.0,
            is_pre_signal=True,
        ))

    # --- RSI Oversold (buy signal) ---
    if "RSI_OVERSOLD" in sigs and snap.trend != "BEARISH":
        confidence = min(8, _score_confidence(snap, bullish=True) + 1)
        alerts.append(Alert(
            symbol=snap.symbol,
            signal_type=SIGNAL_RSI_EXTREME,
            condition=(
                f"RSI at {snap.rsi:.0f} — deeply oversold. "
                f"Mean-reversion opportunity if trend holds at {snap.trend}."
            ),
            action=ACTION_BUY,
            action_detail="Scale in on oversold bounce. Use support as stop reference.",
            confidence="Medium",
            confidence_score=confidence,
            entry_zone=f"${snap.price:.2f} ±1%",
            exit_target=f"${snap.price * 1.05:.2f} (+5%)",
            stop_loss=f"${snap.support:.2f}" if snap.support else "2% below entry",
            risk_pct=2.0,
        ))

    # --- RSI Overbought (sell/avoid signal) ---
    if "RSI_OVERBOUGHT" in sigs and snap.trend != "BULLISH":
        alerts.append(Alert(
            symbol=snap.symbol,
            signal_type=SIGNAL_RSI_EXTREME,
            condition=(
                f"RSI at {snap.rsi:.0f} — overbought in a {snap.trend} trend. "
                f"Risk of mean-reversion pullback elevated."
            ),
            action=ACTION_SELL,
            action_detail="Trim positions or tighten stops. Avoid new longs at this level.",
            confidence="Medium",
            confidence_score=6,
            risk_pct=3.0,
        ))

    # --- RSI Pre-warnings (next-day alerts) ---
    if "RSI_PRE_OVERBOUGHT" in sigs:
        alerts.append(Alert(
            symbol=snap.symbol,
            signal_type=SIGNAL_RSI_PREWARNING,
            condition=f"RSI at {snap.rsi:.0f} — approaching overbought (70). Momentum could stall.",
            action=ACTION_WATCH,
            action_detail="Prepare to take partial profits if RSI crosses 70.",
            confidence="Low",
            confidence_score=5,
            is_pre_signal=True,
        ))

    if "RSI_PRE_OVERSOLD" in sigs:
        alerts.append(Alert(
            symbol=snap.symbol,
            signal_type=SIGNAL_RSI_PREWARNING,
            condition=f"RSI at {snap.rsi:.0f} — approaching oversold (30). Accumulation zone forming.",
            action=ACTION_WATCH,
            action_detail="Watch for RSI to reach 30 and bounce — potential buy setup forming.",
            confidence="Low",
            confidence_score=5,
            is_pre_signal=True,
        ))

    # --- MACD Crossover ---
    if "MACD_BULLISH_CROSS" in sigs:
        confidence = _score_confidence(snap, bullish=True)
        alerts.append(Alert(
            symbol=snap.symbol,
            signal_type=SIGNAL_MACD_CROSS,
            condition=f"MACD bullish crossover confirmed. Histogram: {snap.macd_histogram:.3f}.",
            action=ACTION_BUY if confidence >= 6 else ACTION_WATCH,
            action_detail="MACD crossover on daily — early momentum shift. Confirm with volume.",
            confidence="Medium",
            confidence_score=confidence,
            entry_zone=f"${snap.price:.2f}",
            exit_target=f"${snap.price * 1.06:.2f} (+6%)",
            risk_pct=2.5,
        ))

    if "MACD_BEARISH_CROSS" in sigs:
        alerts.append(Alert(
            symbol=snap.symbol,
            signal_type=SIGNAL_MACD_CROSS,
            condition=f"MACD bearish crossover confirmed. Histogram: {snap.macd_histogram:.3f}.",
            action=ACTION_SELL,
            action_detail="MACD crossover to downside — reduce or exit long exposure.",
            confidence="Medium",
            confidence_score=6,
            risk_pct=3.0,
        ))

    # --- Volume Spike (attention signal) ---
    if "VOLUME_SPIKE" in sigs and snap.volume_ratio:
        alerts.append(Alert(
            symbol=snap.symbol,
            signal_type=SIGNAL_VOLUME_SPIKE,
            condition=(
                f"Volume {snap.volume_ratio:.1f}x 20-day average. "
                f"Institutional activity likely. Price: ${snap.price:.2f}."
            ),
            action=ACTION_BUY if snap.trend == "BULLISH" else ACTION_SELL if snap.trend == "BEARISH" else ACTION_WATCH,
            action_detail="High volume amplifies any directional move. Follow price action for direction.",
            confidence="Medium",
            confidence_score=6,
            risk_pct=2.0,
        ))

    # --- Bullish/Bearish Confluence ---
    if "BULLISH_CONFLUENCE" in sigs:
        confidence = _score_confidence(snap, bullish=True)
        alerts.append(Alert(
            symbol=snap.symbol,
            signal_type=SIGNAL_BULLISH_CONFLUENCE,
            condition=(
                f"Price above 50/200 SMA. RSI {snap.rsi:.0f}. MACD positive. "
                f"Multiple bullish signals aligned."
            ),
            action=ACTION_STRONG_BUY if confidence >= 8 else ACTION_BUY,
            action_detail="Multiple indicators aligned bullish — high-probability setup.",
            confidence="High" if confidence >= 7 else "Medium",
            confidence_score=confidence,
            entry_zone=f"${snap.price:.2f} or on pullback to ${snap.sma_20:.2f}" if snap.sma_20 else f"${snap.price:.2f}",
            exit_target=f"${snap.resistance:.2f}" if snap.resistance else f"${snap.price * 1.08:.2f}",
            stop_loss=f"${snap.sma_50:.2f}" if snap.sma_50 else "3% below entry",
            risk_pct=3.0,
        ))

    if "BEARISH_CONFLUENCE" in sigs:
        confidence = _score_confidence(snap, bullish=False)
        alerts.append(Alert(
            symbol=snap.symbol,
            signal_type=SIGNAL_BEARISH_CONFLUENCE,
            condition=(
                f"Price below 50/200 SMA. RSI {snap.rsi:.0f}. MACD negative. "
                f"Multiple bearish signals aligned."
            ),
            action=ACTION_STRONG_SELL if confidence >= 8 else ACTION_SELL,
            action_detail="Distribution pattern confirmed — avoid longs, protect capital.",
            confidence="High" if confidence >= 7 else "Medium",
            confidence_score=confidence,
            risk_pct=4.0,
        ))

    return alerts


def build_full_analysis(snap: TechnicalSnapshot, fundamentals: dict = None, asset_type: str = "stock") -> FullAnalysis:
    """
    Build the complete structured analysis output per the mandatory framework.
    """
    alerts = generate_alerts(snap, fundamentals, asset_type)
    confidence = _score_confidence(snap, bullish=(snap.trend == "BULLISH"))

    # Short-term direction
    if snap.momentum in ("STRONG_BULL", "BULL") and snap.trend == "BULLISH":
        st_direction = "LONG"
        st_entry = f"${snap.price:.2f} or pullback to ${snap.sma_20:.2f}" if snap.sma_20 else f"${snap.price:.2f}"
        st_exit = f"${snap.resistance:.2f}" if snap.resistance else f"${snap.price * 1.04:.2f}"
        st_risk = f"Stop at ${snap.sma_50:.2f} (~{abs((snap.sma_50 - snap.price)/snap.price*100):.1f}% risk)" if snap.sma_50 else "2-3% below entry"
    elif snap.momentum in ("STRONG_BEAR", "BEAR") and snap.trend == "BEARISH":
        st_direction = "SHORT / AVOID LONGS"
        st_entry = f"Sell rallies to ${snap.sma_20:.2f}" if snap.sma_20 else "Avoid long entries"
        st_exit = f"${snap.support:.2f}" if snap.support else f"${snap.price * 0.96:.2f}"
        st_risk = f"Stop above ${snap.sma_50:.2f}" if snap.sma_50 else "2-3% above entry"
    else:
        st_direction = "NEUTRAL / WAIT"
        st_entry = "Wait for clearer setup"
        st_exit = "N/A"
        st_risk = "No active position"

    # Mid-term strategy
    mt_strategy, mt_outcome = _mid_term_strategy(snap, fundamentals)

    # Long-term
    lt_stance, lt_reason = _long_term_stance(snap, fundamentals, asset_type)

    return FullAnalysis(
        symbol=snap.symbol,
        asset_type=asset_type.upper(),
        trend=snap.trend,
        momentum=snap.momentum,
        price=snap.price,
        st_direction=st_direction,
        st_entry_zone=st_entry,
        st_exit_target=st_exit,
        st_risk=st_risk,
        mt_strategy=mt_strategy,
        mt_expected_outcome=mt_outcome,
        lt_stance=lt_stance,
        lt_reason=lt_reason,
        confidence_score=confidence,
        alerts=alerts,
        technical=snap,
        fundamentals=fundamentals,
    )


def _score_confidence(snap: TechnicalSnapshot, bullish: bool) -> int:
    """Score confidence 1-10 based on signal alignment."""
    score = 5  # baseline

    # Trend alignment
    if bullish and snap.trend == "BULLISH":
        score += 1
    elif not bullish and snap.trend == "BEARISH":
        score += 1
    elif snap.trend == "SIDEWAYS":
        score -= 1

    # Momentum
    if bullish and snap.momentum in ("STRONG_BULL", "BULL"):
        score += 1
    elif not bullish and snap.momentum in ("STRONG_BEAR", "BEAR"):
        score += 1

    # Volume confirmation
    if snap.volume_signal in ("SPIKE", "ELEVATED"):
        score += 1

    # MACD confirmation
    if bullish and snap.macd_histogram and snap.macd_histogram > 0:
        score += 1
    elif not bullish and snap.macd_histogram and snap.macd_histogram < 0:
        score += 1

    # RSI not fighting the trade
    if bullish and snap.rsi and snap.rsi < 70:
        score += 0
    elif bullish and snap.rsi and snap.rsi >= 70:
        score -= 1  # buying overbought is risky

    return max(1, min(10, score))


def _mid_term_strategy(snap: TechnicalSnapshot, fundamentals: dict) -> tuple[str, str]:
    if snap.trend == "BULLISH" and snap.momentum in ("STRONG_BULL", "BULL"):
        strategy = "Hold and add on dips to 20/50 SMA. Use trailing stop at 7% below entry."
        outcome = f"Target ${snap.resistance:.2f} resistance. Potential +5–10% over 1–3 months." if snap.resistance else "Uptrend continuation likely."
    elif snap.trend == "BEARISH":
        strategy = "Reduce exposure on bounces. Do not add to losing positions."
        outcome = f"Risk of testing ${snap.support:.2f} support. Potential -5–10% if breakdown occurs." if snap.support else "Continued downside pressure expected."
    else:
        strategy = "Range-bound — buy near support, sell near resistance. Keep position size small."
        outcome = "Sideways consolidation expected. Breakout direction will set next trend."
    return strategy, outcome


def _long_term_stance(snap: TechnicalSnapshot, fundamentals: dict, asset_type: str) -> tuple[str, str]:
    is_etf = asset_type.lower() == "etf"

    if is_etf and snap.trend == "BULLISH":
        return "ACCUMULATE", "Broad-market ETFs in uptrend are core holdings. Add on 5–10% pullbacks."
    if is_etf and snap.trend == "BEARISH":
        return "HOLD", "ETF drawdown — do not sell in panic. DCA on weakness. Review macro first."
    if not is_etf:
        if fundamentals and fundamentals.get("revenue_growth") and fundamentals["revenue_growth"] > 0.15:
            if snap.trend == "BULLISH":
                return "ACCUMULATE", f"Strong revenue growth ({fundamentals['revenue_growth']*100:.0f}% YoY) + bullish trend = accumulate on dips."
        if snap.trend == "BEARISH":
            return "AVOID", "Individual stock in downtrend with no catalyst — capital at risk. Avoid until trend reversal confirmed."
    if snap.trend == "BULLISH":
        return "HOLD", "Uptrend intact. No strong reason to add aggressively — hold current position."
    return "HOLD", "No clear long-term edge identified. Maintain current allocation."


# ── AI Commentary (OpenRouter — free) ─────────────────────────────────────────

_OPENROUTER_BASE = "https://openrouter.ai/api/v1"
_OPENROUTER_HEADERS = {
    "HTTP-Referer": "https://github.com/theknightcodes/stock-alert-system",
    "X-Title": "Stock Alert System",
}


_STOCK_MODEL_FALLBACKS = [
    "google/gemma-3-27b-it:free",
    "meta-llama/llama-3.3-70b-instruct:free",
    "google/gemma-4-31b-it:free",
    "nousresearch/hermes-3-llama-3.1-405b:free",
    "nvidia/nemotron-3-super-120b-a12b:free",
]


def generate_ai_commentary(analysis: "FullAnalysis") -> Optional[str]:
    """
    Generate a structured AI market commentary for a symbol via OpenRouter free models.
    Tries multiple free models in order on rate-limit (429). Returns None gracefully on failure.
    """
    import os
    api_key = os.environ.get("OPENROUTER_API_KEY", "")
    if not api_key:
        return None

    try:
        from openai import OpenAI, RateLimitError

        primary_model = os.environ.get("OPENROUTER_STOCK_MODEL", "google/gemma-3-27b-it:free")
        model_queue = [primary_model] + [m for m in _STOCK_MODEL_FALLBACKS if m != primary_model]

        client = OpenAI(
            base_url=_OPENROUTER_BASE,
            api_key=api_key,
            default_headers=_OPENROUTER_HEADERS,
        )

        snap = analysis.technical
        signals_str = ", ".join(snap.signals) if snap and snap.signals else "none"
        rsi_val = f"{snap.rsi:.1f}" if snap and snap.rsi else "N/A"
        sma50 = f"${snap.sma_50:.2f}" if snap and snap.sma_50 else "N/A"
        sma200 = f"${snap.sma_200:.2f}" if snap and snap.sma_200 else "N/A"

        # Get stop loss from top alert if available
        top_alert = max(analysis.alerts, key=lambda a: a.confidence_score) if analysis.alerts else None
        stop_str = f" | Stop: {top_alert.stop_loss}" if top_alert and top_alert.stop_loss else ""

        news_str = ""
        if analysis.news:
            headlines = [getattr(n, "headline", str(n)) for n in analysis.news[:3]]
            news_str = f"\nRecent headlines: {'; '.join(headlines)}"

        prompt = f"""You are a senior equity analyst writing a morning briefing. Analyze {analysis.symbol} and write a structured commentary.

Data:
- Price: ${analysis.price:.2f} | Trend: {analysis.trend} | Momentum: {analysis.momentum}
- RSI: {rsi_val} | 50 SMA: {sma50} | 200 SMA: {sma200}
- Active signals: {signals_str}
- Short-term call: {analysis.st_direction} | Entry: {analysis.st_entry_zone} | Target: {analysis.st_exit_target}{stop_str}
- Confidence: {analysis.confidence_score}/10{news_str}

Write EXACTLY 3 lines (no headers, no bullets, no markdown):
Line 1 — Current setup: Describe price action and what the technicals show right now (1 sentence, ~20 words).
Line 2 — Key level to watch: Identify the single most important price level and why it matters (1 sentence, ~20 words).
Line 3 — Predicted move: Give a specific near-term directional call with a price target and timeframe (1 sentence, ~20 words).

Be direct, specific, data-driven. Use actual price numbers."""

        last_exc = None
        for model in model_queue:
            try:
                response = client.chat.completions.create(
                    model=model,
                    messages=[{"role": "user", "content": prompt}],
                    max_tokens=180,
                    temperature=0.35,
                )
                commentary = response.choices[0].message.content.strip()
                logger.info("AI commentary for %s via %s (%d chars)", analysis.symbol, model, len(commentary))
                return commentary
            except RateLimitError as exc:
                logger.warning("Model %s rate-limited for %s, trying next...", model, analysis.symbol)
                last_exc = exc
            except Exception as exc:
                logger.warning("Model %s failed for %s: %s", model, analysis.symbol, exc)
                last_exc = exc
                break  # non-rate-limit errors: don't retry with other models

        logger.warning("All models exhausted for %s: %s", analysis.symbol, last_exc)
        return None

    except Exception as exc:
        logger.warning("AI commentary failed for %s: %s", analysis.symbol, exc)
        return None
