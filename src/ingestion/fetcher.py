"""
Data Ingestion Layer — yfinance primary source.
No hallucination: yfinance pulls directly from Yahoo Finance servers.
Validates all data before returning.
"""
import logging
from datetime import datetime, timedelta
from typing import Optional

import pandas as pd
import yfinance as yf

logger = logging.getLogger(__name__)


def fetch_ohlcv(symbol: str, period: str = "1y", interval: str = "1d") -> Optional[pd.DataFrame]:
    """
    Fetch OHLCV (Open, High, Low, Close, Volume) from Yahoo Finance.
    Returns None if data is unavailable or insufficient.
    """
    try:
        ticker = yf.Ticker(symbol)
        df = ticker.history(period=period, interval=interval, auto_adjust=True)

        if df.empty:
            logger.warning(f"{symbol}: No data returned from Yahoo Finance")
            return None

        df.index = pd.to_datetime(df.index)
        df = df[["Open", "High", "Low", "Close", "Volume"]].copy()
        df.columns = ["open", "high", "low", "close", "volume"]
        df = df.dropna()

        if len(df) < 30:
            logger.warning(f"{symbol}: Insufficient data ({len(df)} rows) — need at least 30")
            return None

        logger.info(f"{symbol}: Fetched {len(df)} rows ({interval}) up to {df.index[-1].date()}")
        return df

    except Exception as e:
        logger.error(f"{symbol}: Fetch failed — {e}")
        return None


def fetch_intraday(symbol: str, period: str = "5d", interval: str = "5m") -> Optional[pd.DataFrame]:
    """Fetch intraday OHLCV for real-time signal checking."""
    return fetch_ohlcv(symbol, period=period, interval=interval)


def fetch_fundamentals(symbol: str) -> dict:
    """
    Fetch fundamental data via yfinance Ticker.info.
    Returns only the fields we actually use — no hallucinated metrics.
    """
    try:
        ticker = yf.Ticker(symbol)
        info = ticker.fast_info  # faster than .info for price fields

        # Build fundamental snapshot with safe .get() access
        full_info = ticker.info

        fundamentals = {
            "symbol": symbol,
            "name": full_info.get("longName", symbol),
            "sector": full_info.get("sector", "Unknown"),
            "industry": full_info.get("industry", "Unknown"),
            "market_cap": full_info.get("marketCap"),
            "pe_ratio": full_info.get("trailingPE"),
            "forward_pe": full_info.get("forwardPE"),
            "revenue_growth": full_info.get("revenueGrowth"),       # YoY
            "earnings_growth": full_info.get("earningsGrowth"),
            "profit_margin": full_info.get("profitMargins"),
            "debt_to_equity": full_info.get("debtToEquity"),
            "free_cash_flow": full_info.get("freeCashflow"),
            "dividend_yield": full_info.get("dividendYield"),
            "52w_high": full_info.get("fiftyTwoWeekHigh"),
            "52w_low": full_info.get("fiftyTwoWeekLow"),
            "avg_volume": full_info.get("averageVolume"),
            "fetched_at": datetime.utcnow().isoformat(),
        }

        logger.info(f"{symbol}: Fundamentals fetched — sector={fundamentals['sector']}")
        return fundamentals

    except Exception as e:
        logger.error(f"{symbol}: Fundamentals fetch failed — {e}")
        return {"symbol": symbol, "error": str(e)}


def fetch_batch(symbols: list[str], period: str = "1y") -> dict[str, Optional[pd.DataFrame]]:
    """Fetch OHLCV for multiple symbols. Returns dict of symbol -> DataFrame."""
    results = {}
    for symbol in symbols:
        results[symbol] = fetch_ohlcv(symbol, period=period)
    return results


def is_market_open() -> bool:
    """Check if US market is currently open (9:30 AM - 4:00 PM ET, Mon-Fri)."""
    from zoneinfo import ZoneInfo
    now = datetime.now(ZoneInfo("America/New_York"))
    if now.weekday() >= 5:  # Saturday or Sunday
        return False
    market_open = now.replace(hour=9, minute=30, second=0, microsecond=0)
    market_close = now.replace(hour=16, minute=0, second=0, microsecond=0)
    return market_open <= now <= market_close
