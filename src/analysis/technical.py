"""
Technical Analysis Engine.
Computes RSI, MACD, moving averages, Bollinger Bands, support/resistance.
Pure pandas/numpy — no external TA library deps.
"""
import logging
from dataclasses import dataclass, field
from typing import Optional

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


@dataclass
class TechnicalSnapshot:
    symbol: str
    price: float
    # Trend
    sma_20: Optional[float] = None
    sma_50: Optional[float] = None
    sma_200: Optional[float] = None
    trend: str = "UNKNOWN"           # BULLISH / BEARISH / SIDEWAYS
    # Momentum
    rsi: Optional[float] = None
    rsi_signal: str = "NEUTRAL"      # OVERBOUGHT / OVERSOLD / NEUTRAL / PRE_HIGH / PRE_LOW
    macd: Optional[float] = None
    macd_signal_line: Optional[float] = None
    macd_histogram: Optional[float] = None
    macd_crossover: str = "NONE"     # BULLISH_CROSS / BEARISH_CROSS / NONE
    momentum: str = "NEUTRAL"        # STRONG_BULL / BULL / NEUTRAL / BEAR / STRONG_BEAR
    # Volume
    avg_volume_20: Optional[float] = None
    current_volume: Optional[float] = None
    volume_ratio: Optional[float] = None   # current / avg
    volume_signal: str = "NORMAL"         # SPIKE / ELEVATED / NORMAL / LOW
    # Support / Resistance
    resistance: Optional[float] = None
    support: Optional[float] = None
    pct_to_resistance: Optional[float] = None
    pct_to_support: Optional[float] = None
    # Bollinger Bands
    bb_upper: Optional[float] = None
    bb_lower: Optional[float] = None
    bb_position: str = "MIDDLE"      # ABOVE_UPPER / UPPER / MIDDLE / LOWER / BELOW_LOWER
    # 52-week context
    pct_from_52w_high: Optional[float] = None
    # Raw signals list for alert engine
    signals: list = field(default_factory=list)


def compute(df: pd.DataFrame, symbol: str, settings: dict = None) -> TechnicalSnapshot:
    """
    Full technical analysis on a OHLCV DataFrame.
    Returns a TechnicalSnapshot with all indicators computed.
    """
    if settings is None:
        settings = {}

    cfg = {
        "rsi_period": settings.get("rsi_period", 14),
        "rsi_overbought": settings.get("rsi_overbought", 70),
        "rsi_oversold": settings.get("rsi_oversold", 30),
        "rsi_pre_high": settings.get("rsi_prewarning_high", 65),
        "rsi_pre_low": settings.get("rsi_prewarning_low", 35),
        "sma_short": settings.get("sma_short", 20),
        "sma_medium": settings.get("sma_medium", 50),
        "sma_long": settings.get("sma_long", 200),
        "volume_spike": settings.get("volume_spike_multiplier", 1.5),
        "sr_window": settings.get("sr_window", 20),
        "resistance_prox": settings.get("resistance_proximity_pct", 1.5),
        "support_prox": settings.get("support_proximity_pct", 1.5),
    }

    price = float(df["close"].iloc[-1])
    snap = TechnicalSnapshot(symbol=symbol, price=price)

    # --- Moving Averages ---
    if len(df) >= cfg["sma_long"]:
        snap.sma_200 = float(df["close"].rolling(cfg["sma_long"]).mean().iloc[-1])
    if len(df) >= cfg["sma_medium"]:
        snap.sma_50 = float(df["close"].rolling(cfg["sma_medium"]).mean().iloc[-1])
    if len(df) >= cfg["sma_short"]:
        snap.sma_20 = float(df["close"].rolling(cfg["sma_short"]).mean().iloc[-1])

    # --- Trend Classification ---
    snap.trend = _classify_trend(price, snap.sma_50, snap.sma_200)

    # --- RSI ---
    rsi_series = _compute_rsi(df["close"], cfg["rsi_period"])
    if rsi_series is not None and not rsi_series.empty:
        snap.rsi = float(rsi_series.iloc[-1])
        snap.rsi_signal = _classify_rsi(snap.rsi, cfg)

    # --- MACD ---
    macd_df = _compute_macd(df["close"], fast=12, slow=26, signal=9)
    if macd_df is not None and not macd_df.empty:
        snap.macd = float(macd_df["macd"].iloc[-1])
        snap.macd_histogram = float(macd_df["histogram"].iloc[-1])
        snap.macd_signal_line = float(macd_df["signal"].iloc[-1])
        snap.macd_crossover = _detect_macd_crossover(macd_df)

    # --- Momentum Score ---
    snap.momentum = _classify_momentum(snap.rsi, snap.macd_histogram, snap.trend)

    # --- Volume ---
    if len(df) >= cfg["sma_short"]:
        snap.avg_volume_20 = float(df["volume"].rolling(cfg["sma_short"]).mean().iloc[-1])
        snap.current_volume = float(df["volume"].iloc[-1])
        if snap.avg_volume_20 > 0:
            snap.volume_ratio = snap.current_volume / snap.avg_volume_20
            snap.volume_signal = _classify_volume(snap.volume_ratio, cfg["volume_spike"])

    # --- Support & Resistance ---
    snap.resistance, snap.support = _compute_support_resistance(df, cfg["sr_window"])
    if snap.resistance and snap.resistance > price:
        snap.pct_to_resistance = ((snap.resistance - price) / price) * 100
    if snap.support and snap.support < price:
        snap.pct_to_support = ((price - snap.support) / price) * 100

    # --- Bollinger Bands ---
    bb = _compute_bbands(df["close"], length=20, std=2)
    if bb is not None and not bb.empty:
        snap.bb_upper = float(bb["upper"].iloc[-1])
        snap.bb_lower = float(bb["lower"].iloc[-1])
        snap.bb_position = _classify_bb_position(price, snap.bb_upper, snap.bb_lower)

    # --- 52-week context ---
    if len(df) >= 252:
        high_52w = float(df["high"].tail(252).max())
        snap.pct_from_52w_high = ((price - high_52w) / high_52w) * 100
    elif len(df) > 0:
        high_range = float(df["high"].max())
        snap.pct_from_52w_high = ((price - high_range) / high_range) * 100

    # --- Compile signals for alert engine ---
    snap.signals = _compile_signals(snap, cfg)

    logger.info(
        f"{symbol}: price={price:.2f} trend={snap.trend} rsi={snap.rsi:.1f} "
        f"momentum={snap.momentum} volume={snap.volume_signal}"
    )
    return snap


def _classify_trend(price: float, sma_50: Optional[float], sma_200: Optional[float]) -> str:
    if sma_50 is None and sma_200 is None:
        return "UNKNOWN"
    if sma_50 and sma_200:
        if price > sma_50 > sma_200:
            return "BULLISH"
        if price < sma_50 < sma_200:
            return "BEARISH"
        if sma_50 > sma_200 and price < sma_50:
            return "SIDEWAYS"
        if sma_50 < sma_200 and price > sma_50:
            return "SIDEWAYS"
    if sma_50:
        return "BULLISH" if price > sma_50 else "BEARISH"
    return "UNKNOWN"


def _classify_rsi(rsi: float, cfg: dict) -> str:
    if rsi >= cfg["rsi_overbought"]:
        return "OVERBOUGHT"
    if rsi <= cfg["rsi_oversold"]:
        return "OVERSOLD"
    if rsi >= cfg["rsi_pre_high"]:
        return "PRE_OVERBOUGHT"
    if rsi <= cfg["rsi_pre_low"]:
        return "PRE_OVERSOLD"
    return "NEUTRAL"


def _classify_momentum(rsi: Optional[float], macd_hist: Optional[float], trend: str) -> str:
    score = 0
    if rsi:
        if rsi > 60:
            score += 2
        elif rsi > 50:
            score += 1
        elif rsi < 40:
            score -= 2
        elif rsi < 50:
            score -= 1
    if macd_hist:
        if macd_hist > 0:
            score += 1
        else:
            score -= 1
    if trend == "BULLISH":
        score += 1
    elif trend == "BEARISH":
        score -= 1

    if score >= 3:
        return "STRONG_BULL"
    if score >= 1:
        return "BULL"
    if score <= -3:
        return "STRONG_BEAR"
    if score <= -1:
        return "BEAR"
    return "NEUTRAL"


def _compute_rsi(close: pd.Series, length: int = 14) -> pd.Series:
    delta = close.diff()
    gain = delta.where(delta > 0, 0.0).ewm(com=length - 1, adjust=False).mean()
    loss = (-delta.where(delta < 0, 0.0)).ewm(com=length - 1, adjust=False).mean()
    rs = gain / loss.replace(0, np.nan)
    return 100 - (100 / (1 + rs))


def _compute_macd(close: pd.Series, fast: int = 12, slow: int = 26, signal: int = 9) -> pd.DataFrame:
    ema_fast = close.ewm(span=fast, adjust=False).mean()
    ema_slow = close.ewm(span=slow, adjust=False).mean()
    macd = ema_fast - ema_slow
    signal_line = macd.ewm(span=signal, adjust=False).mean()
    histogram = macd - signal_line
    return pd.DataFrame({"macd": macd, "signal": signal_line, "histogram": histogram})


def _compute_bbands(close: pd.Series, length: int = 20, std: float = 2) -> pd.DataFrame:
    sma = close.rolling(length).mean()
    stddev = close.rolling(length).std()
    return pd.DataFrame({"upper": sma + std * stddev, "mid": sma, "lower": sma - std * stddev})


def _detect_macd_crossover(macd_df: pd.DataFrame) -> str:
    if len(macd_df) < 2:
        return "NONE"
    macd_prev = macd_df["macd"].iloc[-2]
    macd_curr = macd_df["macd"].iloc[-1]
    sig_prev = macd_df["signal"].iloc[-2]
    sig_curr = macd_df["signal"].iloc[-1]
    if macd_prev < sig_prev and macd_curr > sig_curr:
        return "BULLISH_CROSS"
    if macd_prev > sig_prev and macd_curr < sig_curr:
        return "BEARISH_CROSS"
    return "NONE"


def _classify_volume(ratio: float, spike_threshold: float) -> str:
    if ratio >= spike_threshold * 1.5:
        return "SPIKE"
    if ratio >= spike_threshold:
        return "ELEVATED"
    if ratio < 0.5:
        return "LOW"
    return "NORMAL"


def _compute_support_resistance(df: pd.DataFrame, window: int) -> tuple[Optional[float], Optional[float]]:
    """
    Resistance = highest high of last N candles (excluding current).
    Support = lowest low of last N candles (excluding current).
    """
    if len(df) < window + 1:
        return None, None
    lookback = df.iloc[-(window + 1):-1]
    resistance = float(lookback["high"].max())
    support = float(lookback["low"].min())
    return resistance, support


def _classify_bb_position(price: float, bb_upper: float, bb_lower: float) -> str:
    band_range = bb_upper - bb_lower
    if band_range == 0:
        return "MIDDLE"
    pct = (price - bb_lower) / band_range
    if price > bb_upper:
        return "ABOVE_UPPER"
    if pct > 0.8:
        return "UPPER"
    if price < bb_lower:
        return "BELOW_LOWER"
    if pct < 0.2:
        return "LOWER"
    return "MIDDLE"


def _compile_signals(snap: TechnicalSnapshot, cfg: dict) -> list[str]:
    signals = []
    # RSI signals
    if snap.rsi_signal == "OVERBOUGHT":
        signals.append("RSI_OVERBOUGHT")
    elif snap.rsi_signal == "OVERSOLD":
        signals.append("RSI_OVERSOLD")
    elif snap.rsi_signal == "PRE_OVERBOUGHT":
        signals.append("RSI_PRE_OVERBOUGHT")
    elif snap.rsi_signal == "PRE_OVERSOLD":
        signals.append("RSI_PRE_OVERSOLD")
    # MACD crossover
    if snap.macd_crossover != "NONE":
        signals.append(f"MACD_{snap.macd_crossover}")
    # Volume
    if snap.volume_signal == "SPIKE":
        signals.append("VOLUME_SPIKE")
    elif snap.volume_signal == "ELEVATED":
        signals.append("VOLUME_ELEVATED")
    # Proximity to resistance/support
    if snap.pct_to_resistance is not None and snap.pct_to_resistance <= cfg["resistance_prox"]:
        signals.append("NEAR_RESISTANCE")
    if snap.pct_to_support is not None and snap.pct_to_support <= cfg["support_prox"]:
        signals.append("NEAR_SUPPORT")
    # Bollinger
    if snap.bb_position in ("ABOVE_UPPER", "BELOW_LOWER"):
        signals.append(f"BB_{snap.bb_position}")
    # Trend
    if snap.trend == "BULLISH" and snap.momentum in ("STRONG_BULL", "BULL"):
        signals.append("BULLISH_CONFLUENCE")
    if snap.trend == "BEARISH" and snap.momentum in ("STRONG_BEAR", "BEAR"):
        signals.append("BEARISH_CONFLUENCE")
    return signals
