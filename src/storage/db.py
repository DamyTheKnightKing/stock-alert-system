"""
Storage Layer.
- Turso (libsql): cloud persistent, free tier, never pauses  [DEFAULT when DATABASE_URL set]
- SQLite: local fallback, zero setup                         [fallback]

libsql_experimental is used directly (not via SQLAlchemy) because the SQLAlchemy
pysqlite dialect calls create_function() which libsql doesn't support.
Raw SQL is used instead — explicit, portable, no ORM magic.
"""
import json
import logging
import os
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path

logger = logging.getLogger(__name__)

DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///data/stock_alerts.db")

# --- Connection factory ---

def _parse_turso_url(url: str) -> tuple[str, str]:
    """Extract sync_url and auth_token from libsql+https://host?authToken=token"""
    url_part = url.replace("libsql+https://", "").replace("libsql://", "")
    if "?authToken=" in url_part:
        host, token = url_part.split("?authToken=", 1)
    else:
        host, token = url_part, ""
    return f"https://{host}", token


def _get_connection():
    """Return a DB connection — Turso or SQLite depending on DATABASE_URL."""
    if DATABASE_URL.startswith("libsql"):
        import libsql_experimental as libsql
        sync_url, token = _parse_turso_url(DATABASE_URL)
        conn = libsql.connect("turso_local.db", sync_url=sync_url, auth_token=token)
        conn.sync()
        return conn, True   # (connection, is_turso)
    else:
        db_path = DATABASE_URL.replace("sqlite:///", "")
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        return conn, False


@contextmanager
def _db():
    """Context manager: get connection, commit + sync on exit, close on done."""
    conn, is_turso = _get_connection()
    try:
        yield conn
        conn.commit()
        if is_turso:
            conn.sync()
    except Exception:
        conn.rollback() if hasattr(conn, 'rollback') else None
        raise
    finally:
        conn.close()


# --- Schema ---

SCHEMA = """
CREATE TABLE IF NOT EXISTS price_snapshots (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol       TEXT NOT NULL,
    date         TEXT NOT NULL,
    open         REAL,
    high         REAL,
    low          REAL,
    close        REAL,
    volume       REAL,
    sma_20       REAL,
    sma_50       REAL,
    sma_200      REAL,
    rsi          REAL,
    macd         REAL,
    macd_signal  REAL,
    macd_histogram REAL,
    trend        TEXT,
    momentum     TEXT,
    volume_signal TEXT,
    support      REAL,
    resistance   REAL,
    created_at   TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS alerts (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol           TEXT NOT NULL,
    signal_type      TEXT NOT NULL,
    condition        TEXT,
    action           TEXT,
    action_detail    TEXT,
    confidence       TEXT,
    confidence_score INTEGER,
    entry_zone       TEXT,
    exit_target      TEXT,
    stop_loss        TEXT,
    risk_pct         REAL,
    is_pre_signal    INTEGER DEFAULT 0,
    triggered_at     TEXT NOT NULL,
    notified         INTEGER DEFAULT 0,
    created_at       TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS analysis_reports (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol           TEXT NOT NULL,
    asset_type       TEXT,
    report_date      TEXT NOT NULL,
    trend            TEXT,
    momentum         TEXT,
    confidence_score INTEGER,
    lt_stance        TEXT,
    payload          TEXT,
    created_at       TEXT DEFAULT (datetime('now'))
);
"""


def init_db():
    """Create all tables if they don't exist."""
    with _db() as conn:
        for statement in SCHEMA.strip().split(";"):
            stmt = statement.strip()
            if stmt:
                conn.execute(stmt)
    logger.info(f"Database initialized: {DATABASE_URL.split('?')[0]}")  # hide token in logs


def save_price_snapshot(data: dict):
    sql = """
        INSERT INTO price_snapshots
            (symbol, date, open, high, low, close, volume,
             sma_20, sma_50, sma_200, rsi, macd, macd_signal, macd_histogram,
             trend, momentum, volume_signal, support, resistance)
        VALUES
            (:symbol, :date, :open, :high, :low, :close, :volume,
             :sma_20, :sma_50, :sma_200, :rsi, :macd, :macd_signal, :macd_histogram,
             :trend, :momentum, :volume_signal, :support, :resistance)
    """
    with _db() as conn:
        conn.execute(sql, _coerce(data))
    logger.debug(f"Saved snapshot: {data.get('symbol')}")


def save_alert(data: dict) -> int:
    sql = """
        INSERT INTO alerts
            (symbol, signal_type, condition, action, action_detail,
             confidence, confidence_score, entry_zone, exit_target,
             stop_loss, risk_pct, is_pre_signal, triggered_at)
        VALUES
            (:symbol, :signal_type, :condition, :action, :action_detail,
             :confidence, :confidence_score, :entry_zone, :exit_target,
             :stop_loss, :risk_pct, :is_pre_signal, :triggered_at)
    """
    with _db() as conn:
        cur = conn.execute(sql, _coerce(data))
        row_id = cur.lastrowid
    logger.info(f"Saved alert: {data.get('symbol')} — {data.get('signal_type')}")
    return row_id


def save_analysis_report(data: dict):
    sql = """
        INSERT INTO analysis_reports
            (symbol, asset_type, report_date, trend, momentum,
             confidence_score, lt_stance, payload)
        VALUES
            (:symbol, :asset_type, :report_date, :trend, :momentum,
             :confidence_score, :lt_stance, :payload)
    """
    with _db() as conn:
        conn.execute(sql, _coerce(data))
    logger.debug(f"Saved report: {data.get('symbol')}")


def get_pending_alerts() -> list[dict]:
    with _db() as conn:
        cur = conn.execute("SELECT * FROM alerts WHERE notified = 0")
        return [dict(row) for row in cur.fetchall()]


def mark_alerts_notified(alert_ids: list[int]):
    if not alert_ids:
        return
    placeholders = ",".join("?" * len(alert_ids))
    with _db() as conn:
        conn.execute(
            f"UPDATE alerts SET notified = 1 WHERE id IN ({placeholders})",
            alert_ids
        )


def get_recent_snapshots(symbol: str, days: int = 7) -> list[dict]:
    cutoff = (datetime.utcnow() - timedelta(days=days)).isoformat()
    with _db() as conn:
        cur = conn.execute(
            "SELECT * FROM price_snapshots WHERE symbol = ? AND date >= ? ORDER BY date DESC",
            (symbol, cutoff)
        )
        return [dict(row) for row in cur.fetchall()]


def purge_old_records(retention_days: int = 90):
    cutoff = (datetime.utcnow() - timedelta(days=retention_days)).isoformat()
    with _db() as conn:
        r1 = conn.execute("DELETE FROM alerts WHERE created_at < ?", (cutoff,)).rowcount
        r2 = conn.execute("DELETE FROM price_snapshots WHERE created_at < ?", (cutoff,)).rowcount
        r3 = conn.execute("DELETE FROM analysis_reports WHERE created_at < ?", (cutoff,)).rowcount
    logger.info(f"Purged: {r1} alerts, {r2} snapshots, {r3} reports")


def _coerce(data: dict) -> dict:
    """Convert datetime objects to ISO strings for storage."""
    out = {}
    for k, v in data.items():
        if isinstance(v, datetime):
            out[k] = v.isoformat()
        else:
            out[k] = v
    return out
