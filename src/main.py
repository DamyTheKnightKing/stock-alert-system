"""
Stock Alert System — Main Entry Point
Usage:
  python -m src.main daily          # Full daily analysis + email report
  python -m src.main intraday       # Intraday signal check (high-priority symbols only)
  python -m src.main analyze AAPL NVDA SPY   # Analyze specific symbols + print report
"""
import argparse
import logging
import os
import sys
from datetime import datetime

from dotenv import load_dotenv

load_dotenv()

# Configure logging
log_level = os.getenv("LOG_LEVEL", "INFO")
logging.basicConfig(
    level=getattr(logging, log_level),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("logs/autoposter.log", mode="a"),
    ],
)
logger = logging.getLogger(__name__)

import os
os.makedirs("logs", exist_ok=True)
os.makedirs("data", exist_ok=True)


def cmd_daily(all_users: bool = False):
    """Full daily pipeline."""
    from src.storage.db import init_db, purge_old_records

    logger.info("=== DAILY ANALYSIS START ===")
    init_db()

    if all_users:
        # Multi-user mode: read from DB, send personalised email per user
        from src.alerts.engine import run_for_all_users
        count = run_for_all_users()
        logger.info(f"Multi-user run complete: {count} users processed")
    else:
        # Single-user mode: use watchlist.yml (personal/admin use)
        from src.alerts.engine import run_daily_analysis
        from src.notifications.email_sender import send_daily_report
        from src.storage.db import get_pending_alerts, mark_alerts_notified

        analyses = run_daily_analysis()
        if not analyses:
            logger.warning("No analyses generated — check watchlist and data connectivity")
            return

        _print_console_report(analyses)
        today = datetime.now().strftime("%Y-%m-%d")
        success = send_daily_report(analyses, report_date=today)

        if success:
            pending = get_pending_alerts()
            if pending:
                mark_alerts_notified([a["id"] for a in pending])

    purge_old_records(retention_days=90)
    logger.info("=== DAILY ANALYSIS COMPLETE ===")


def cmd_intraday():
    """Intraday check: fast scan of high-priority symbols."""
    from src.alerts.engine import run_intraday_check
    from src.ingestion.fetcher import is_market_open
    from src.notifications.email_sender import send_intraday_alerts
    from src.storage.db import get_pending_alerts, init_db, mark_alerts_notified

    if not is_market_open():
        logger.info("Market is closed — skipping intraday check")
        return

    logger.info("=== INTRADAY CHECK START ===")
    init_db()

    alerts = run_intraday_check()

    if alerts:
        logger.info(f"{len(alerts)} intraday alerts generated")
        send_intraday_alerts(alerts)
        pending = get_pending_alerts()
        if pending:
            mark_alerts_notified([a["id"] for a in pending])
    else:
        logger.info("No intraday alerts triggered")

    logger.info("=== INTRADAY CHECK COMPLETE ===")


def cmd_analyze(symbols: list[str]):
    """Ad-hoc analysis for specific symbols — prints formatted report."""
    from src.analysis import signals as sig_engine
    from src.analysis import technical as tech
    from src.ingestion import fetcher
    from src.storage.db import init_db

    init_db()

    for symbol in symbols:
        symbol = symbol.upper()
        print(f"\n{'='*60}")
        print(f"  ANALYZING: {symbol}")
        print(f"{'='*60}")

        df = fetcher.fetch_ohlcv(symbol)
        if df is None:
            print(f"  ERROR: No data for {symbol}")
            continue

        snap = tech.compute(df, symbol)
        fundamentals = fetcher.fetch_fundamentals(symbol)

        # Determine type
        info = fundamentals
        sector = info.get("sector", "")
        asset_type = "etf" if not sector or sector == "Unknown" else "stock"

        analysis = sig_engine.build_full_analysis(snap, fundamentals, asset_type)
        _print_single_analysis(analysis)


def _print_console_report(analyses):
    from tabulate import tabulate
    rows = []
    for a in analyses:
        rows.append([
            a.symbol,
            a.asset_type,
            f"${a.price:.2f}",
            a.trend,
            a.momentum,
            f"{a.technical.rsi:.0f}" if a.technical and a.technical.rsi else "N/A",
            a.lt_stance,
            f"{a.confidence_score}/10",
            len(a.alerts),
        ])
    print("\n" + "=" * 80)
    print("DAILY ANALYSIS SUMMARY")
    print("=" * 80)
    print(tabulate(rows, headers=[
        "Symbol", "Type", "Price", "Trend", "Momentum", "RSI", "LT Stance", "Confidence", "Alerts"
    ], tablefmt="grid"))


def _print_single_analysis(a):
    t = a.technical
    print(f"\nSYMBOL: {a.symbol}")
    print(f"Type: {a.asset_type}")
    print(f"Trend: {a.trend}")
    print(f"Momentum: {a.momentum}")
    print()
    if t:
        print(f"Price: ${a.price:.2f}")
        print(f"RSI: {t.rsi:.1f} ({t.rsi_signal})" if t.rsi else "RSI: N/A")
        print(f"50 SMA: ${t.sma_50:.2f}" if t.sma_50 else "50 SMA: N/A")
        print(f"200 SMA: ${t.sma_200:.2f}" if t.sma_200 else "200 SMA: N/A")
        print(f"Support: ${t.support:.2f}" if t.support else "Support: N/A")
        print(f"Resistance: ${t.resistance:.2f}" if t.resistance else "Resistance: N/A")
        print(f"Volume: {t.volume_signal} ({t.volume_ratio:.1f}x avg)" if t.volume_ratio else "Volume: N/A")
        print()

    print("Short-Term (1–14 days):")
    print(f"  Direction: {a.st_direction}")
    print(f"  Entry Zone: {a.st_entry_zone}")
    print(f"  Exit Target: {a.st_exit_target}")
    print(f"  Risk: {a.st_risk}")
    print()
    print("Mid-Term (1–3 months):")
    print(f"  Strategy: {a.mt_strategy}")
    print(f"  Expected Outcome: {a.mt_expected_outcome}")
    print()
    print("Long-Term (6–24 months):")
    print(f"  {a.lt_stance}")
    print(f"  Reason: {a.lt_reason}")
    print()
    print(f"Confidence Score: {a.confidence_score}/10")

    if a.alerts:
        print(f"\nALERTS ({len(a.alerts)}):")
        for alert in a.alerts:
            pre = "[PRE-SIGNAL] " if alert.is_pre_signal else ""
            print(f"\n  ALERT: {pre}{alert.signal_type}")
            print(f"  Signal: {'Breakout' if 'BREAK' in alert.signal_type else alert.signal_type}")
            print(f"  Condition: {alert.condition}")
            print(f"  Action: {alert.action}")
            print(f"  Confidence: {alert.confidence} ({alert.confidence_score}/10)")


def main():
    parser = argparse.ArgumentParser(description="Stock Alert System")
    subparsers = parser.add_subparsers(dest="command")

    daily_parser = subparsers.add_parser("daily", help="Run full daily analysis + email report")
    daily_parser.add_argument("--all-users", action="store_true",
                              help="Run for all DB users (multi-user mode)")
    subparsers.add_parser("intraday", help="Run intraday signal check")

    analyze_parser = subparsers.add_parser("analyze", help="Analyze specific symbols")
    analyze_parser.add_argument("symbols", nargs="+", help="Stock symbols to analyze")

    args = parser.parse_args()

    if args.command == "daily":
        cmd_daily(all_users=getattr(args, "all_users", False))
    elif args.command == "intraday":
        cmd_intraday()
    elif args.command == "analyze":
        cmd_analyze(args.symbols)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
