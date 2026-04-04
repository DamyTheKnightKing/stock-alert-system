"""
Alert Engine — orchestrates the full daily analysis pipeline.
For each symbol:
  1. Fetch data
  2. Compute technicals
  3. Fetch fundamentals
  4. Generate signals
  5. Persist to DB
  6. Return alerts for notification
"""
import json
import logging
from datetime import datetime

import yaml

from src.analysis import signals as sig_engine
from src.analysis import technical as tech
from src.ingestion import fetcher
from src.ingestion.news import fetch_news
from src.ingestion.reddit import fetch_reddit_sentiment
from src.storage import db

logger = logging.getLogger(__name__)


def load_watchlist(path: str = "config/watchlist.yml") -> list[dict]:
    with open(path) as f:
        data = yaml.safe_load(f)
    return data.get("watchlist", [])


def load_settings(path: str = "config/settings.yml") -> dict:
    with open(path) as f:
        return yaml.safe_load(f)


def run_daily_analysis(watchlist_path: str = "config/watchlist.yml",
                       settings_path: str = "config/settings.yml") -> list[sig_engine.FullAnalysis]:
    """
    Main daily pipeline. Returns list of FullAnalysis objects (one per symbol).
    Called by main.py and GitHub Actions workflow.
    """
    settings = load_settings(settings_path)
    watchlist = load_watchlist(watchlist_path)
    tech_cfg = settings.get("technical", {})
    alert_cfg = settings.get("alerts", {})

    results = []

    for item in watchlist:
        symbol = item["symbol"]
        asset_type = item.get("type", "stock")
        priority = item.get("priority", "medium")

        logger.info(f"--- Analyzing {symbol} ({asset_type.upper()}, priority={priority}) ---")

        # 1. Fetch OHLCV
        df = fetcher.fetch_ohlcv(symbol, period=settings["data"]["history_period"])
        if df is None:
            logger.warning(f"{symbol}: Skipping — no data")
            continue

        # 2. Technical analysis
        snap = tech.compute(df, symbol, {**tech_cfg, **alert_cfg})

        # 3. Fundamentals (best-effort — don't block on failure)
        fundamentals = fetcher.fetch_fundamentals(symbol)

        # 4. Generate full analysis
        analysis = sig_engine.build_full_analysis(snap, fundamentals, asset_type)

        # 4a. Enrich with news + Reddit sentiment (best-effort — don't block on failure)
        try:
            analysis.news = fetch_news(symbol, max_items=3)
        except Exception as e:
            logger.warning(f"{symbol}: News fetch failed — {e}")

        try:
            analysis.reddit = fetch_reddit_sentiment(symbol)
        except Exception as e:
            logger.warning(f"{symbol}: Reddit fetch failed — {e}")

        # 5. Persist price snapshot
        _persist_snapshot(snap, df)

        # 6. Persist alerts
        for alert in analysis.alerts:
            _persist_alert(alert)

        # 7. Persist analysis report
        _persist_report(analysis)

        results.append(analysis)
        logger.info(
            f"{symbol}: trend={analysis.trend} momentum={analysis.momentum} "
            f"confidence={analysis.confidence_score} alerts={len(analysis.alerts)}"
        )

    logger.info(f"Daily analysis complete — {len(results)} symbols processed")
    return results


def run_for_all_users() -> int:
    """
    Multi-user pipeline.
    1. Load all active users from DB
    2. Collect all unique symbols across ALL users (fetch once, serve many)
    3. Run analysis per unique symbol
    4. For each user, build their personalised digest and send email
    Returns number of users processed.
    """
    import json as _json
    import os as _os
    import time as _time
    import resend as _resend
    from src.storage.db import get_active_users
    from src.notifications.email_sender import send_morning_digest
    from src.alerts.digest import build_digest

    settings = load_settings()
    tech_cfg = settings.get("technical", {})
    alert_cfg = settings.get("alerts", {})

    users = get_active_users()
    if not users:
        logger.info("No active users found")
        return 0

    logger.info(f"Running analysis for {len(users)} users")

    # --- Step 1: collect all unique symbols across all users ---
    all_symbols: set[str] = set()
    for user in users:
        try:
            symbols = _json.loads(user["watchlist"])
            all_symbols.update(symbols)
        except Exception:
            pass

    logger.info(f"Unique symbols across all users: {len(all_symbols)}")

    # --- Step 2: fetch + analyse each unique symbol ONCE ---
    symbol_analysis: dict[str, sig_engine.FullAnalysis] = {}
    for symbol in all_symbols:
        try:
            df = fetcher.fetch_ohlcv(symbol)
            if df is None:
                continue
            snap = tech.compute(df, symbol, {**tech_cfg, **alert_cfg})
            fundamentals = fetcher.fetch_fundamentals(symbol)
            sector = fundamentals.get("sector", "")
            asset_type = "etf" if not sector or sector == "Unknown" else "stock"
            analysis = sig_engine.build_full_analysis(snap, fundamentals, asset_type)
            try:
                analysis.news = fetch_news(symbol, max_items=3)
                logger.info(f"{symbol}: news={len(analysis.news)} items")
            except Exception as e:
                logger.warning(f"{symbol}: news fetch failed — {e}")
            try:
                analysis.reddit = fetch_reddit_sentiment(symbol)
                if analysis.reddit:
                    logger.info(f"{symbol}: reddit={analysis.reddit.overall} ({analysis.reddit.mention_count} mentions)")
            except Exception as e:
                logger.warning(f"{symbol}: reddit fetch failed — {e}")
            symbol_analysis[symbol] = analysis
        except Exception as e:
            logger.error(f"Analysis failed for {symbol}: {e}")

    # --- Step 3: build personalised digest per user and send ---
    _resend.api_key = _os.getenv("RESEND_API_KEY", "")
    processed = 0

    for user in users:
        try:
            symbols = _json.loads(user["watchlist"])
            user_analyses = [
                symbol_analysis[s] for s in symbols if s in symbol_analysis
            ]
            if not user_analyses:
                logger.warning(f"No valid analyses for {user['email']}")
                continue

            digest = build_digest(user_analyses)

            # Override recipient to this specific user
            original_recipients = _os.getenv("EMAIL_RECIPIENTS", "")
            try:
                _os.environ["EMAIL_RECIPIENTS"] = user["email"]
                send_morning_digest(digest)
            finally:
                _os.environ["EMAIL_RECIPIENTS"] = original_recipients

            processed += 1
            logger.info(f"Sent digest to {user['email']} ({len(user_analyses)} symbols)")
            _time.sleep(1)  # Resend rate limit: max 2 requests/second

        except Exception as e:
            logger.error(f"Failed to process user {user.get('email')}: {e}")

    logger.info(f"Multi-user run complete: {processed}/{len(users)} users processed")
    return processed


def run_intraday_check(watchlist_path: str = "config/watchlist.yml",
                       settings_path: str = "config/settings.yml") -> list[sig_engine.Alert]:
    """
    Lightweight intraday check — fetches 5m candles, checks for volume spikes
    and RSI extremes only. Fast and cheap on API calls.
    """
    settings = load_settings(settings_path)
    watchlist = load_watchlist(watchlist_path)
    tech_cfg = settings.get("technical", {})
    alert_cfg = settings.get("alerts", {})

    # Only check high-priority symbols intraday
    high_priority = [s for s in watchlist if s.get("priority") == "high"]

    alerts = []
    for item in high_priority:
        symbol = item["symbol"]
        asset_type = item.get("type", "stock")

        df = fetcher.fetch_intraday(symbol,
                                     period=settings["data"]["intraday_period"],
                                     interval=settings["data"]["intraday_interval"])
        if df is None:
            continue

        snap = tech.compute(df, symbol, {**tech_cfg, **alert_cfg})
        symbol_alerts = sig_engine.generate_alerts(snap, asset_type=asset_type)

        # Filter: only volume spikes and RSI extremes for intraday
        intraday_alerts = [
            a for a in symbol_alerts
            if a.signal_type in (
                sig_engine.SIGNAL_VOLUME_SPIKE,
                sig_engine.SIGNAL_RSI_EXTREME,
                sig_engine.SIGNAL_BREAKOUT,
                sig_engine.SIGNAL_BREAKDOWN,
            )
        ]

        for alert in intraday_alerts:
            _persist_alert(alert)
            alerts.append(alert)

    logger.info(f"Intraday check complete — {len(alerts)} alerts generated")
    return alerts


def _persist_snapshot(snap: tech.TechnicalSnapshot, df):
    try:
        db.save_price_snapshot({
            "symbol": snap.symbol,
            "date": datetime.utcnow(),
            "open": float(df["open"].iloc[-1]),
            "high": float(df["high"].iloc[-1]),
            "low": float(df["low"].iloc[-1]),
            "close": snap.price,
            "volume": snap.current_volume,
            "sma_20": snap.sma_20,
            "sma_50": snap.sma_50,
            "sma_200": snap.sma_200,
            "rsi": snap.rsi,
            "macd": snap.macd,
            "macd_signal": snap.macd_signal_line,
            "macd_histogram": snap.macd_histogram,
            "trend": snap.trend,
            "momentum": snap.momentum,
            "volume_signal": snap.volume_signal,
            "support": snap.support,
            "resistance": snap.resistance,
        })
    except Exception as e:
        logger.error(f"Failed to persist snapshot for {snap.symbol}: {e}")


def _persist_alert(alert: sig_engine.Alert):
    try:
        db.save_alert({
            "symbol": alert.symbol,
            "signal_type": alert.signal_type,
            "condition": alert.condition,
            "action": alert.action,
            "action_detail": alert.action_detail,
            "confidence": alert.confidence,
            "confidence_score": alert.confidence_score,
            "entry_zone": alert.entry_zone,
            "exit_target": alert.exit_target,
            "stop_loss": alert.stop_loss,
            "risk_pct": alert.risk_pct,
            "is_pre_signal": int(alert.is_pre_signal),
            "triggered_at": datetime.fromisoformat(alert.triggered_at),
        })
    except Exception as e:
        logger.error(f"Failed to persist alert for {alert.symbol}: {e}")


def _persist_report(analysis: sig_engine.FullAnalysis):
    try:
        payload = {
            "symbol": analysis.symbol,
            "asset_type": analysis.asset_type,
            "trend": analysis.trend,
            "momentum": analysis.momentum,
            "price": analysis.price,
            "confidence_score": analysis.confidence_score,
            "st_direction": analysis.st_direction,
            "st_entry_zone": analysis.st_entry_zone,
            "st_exit_target": analysis.st_exit_target,
            "st_risk": analysis.st_risk,
            "mt_strategy": analysis.mt_strategy,
            "mt_expected_outcome": analysis.mt_expected_outcome,
            "lt_stance": analysis.lt_stance,
            "lt_reason": analysis.lt_reason,
            "alerts_count": len(analysis.alerts),
        }
        db.save_analysis_report({
            "symbol": analysis.symbol,
            "asset_type": analysis.asset_type,
            "report_date": datetime.utcnow(),
            "trend": analysis.trend,
            "momentum": analysis.momentum,
            "confidence_score": analysis.confidence_score,
            "lt_stance": analysis.lt_stance,
            "payload": json.dumps(payload),
        })
    except Exception as e:
        logger.error(f"Failed to persist report for {analysis.symbol}: {e}")
