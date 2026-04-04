"""
Storage Layer.
- Turso HTTP API: cloud persistent, free, zero native deps  [when DATABASE_URL=libsql+https://]
- SQLite: local fallback, zero setup                        [when DATABASE_URL=sqlite:///]

Uses Turso's HTTP API (/v2/pipeline) instead of libsql-experimental,
so no Rust compilation needed — works on any Python version, any platform.
"""
import json
import logging
import os
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///data/stock_alerts.db")

# ---------------------------------------------------------------------------
# Turso HTTP API client
# ---------------------------------------------------------------------------

def _parse_turso_url(url: str) -> tuple[str, str]:
    url_part = url.replace("libsql+https://", "").replace("libsql://", "")
    if "?authToken=" in url_part:
        host, token = url_part.split("?authToken=", 1)
    else:
        host, token = url_part, ""
    return f"https://{host}", token


class TursoClient:
    """Minimal Turso HTTP API client — no native deps, pure httpx."""

    def __init__(self, url: str, token: str):
        self.url = f"{url}/v2/pipeline"
        self.headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }
        self._pending: list[dict] = []
        self._last_rowid: Optional[int] = None

    def execute(self, sql: str, params: tuple = ()):
        import httpx
        args = [self._to_value(p) for p in params]
        stmt = {"sql": sql, "args": args}
        payload = {"requests": [
            {"type": "execute", "stmt": stmt},
            {"type": "close"},
        ]}
        resp = httpx.post(self.url, headers=self.headers,
                          content=json.dumps(payload), timeout=15.0)
        resp.raise_for_status()
        data = resp.json()
        result = data["results"][0]["response"]["result"]
        self._last_result = result
        self._last_rowid = int(result.get("last_insert_rowid") or 0) or None
        return _TursoCursor(result)

    def executescript(self, script: str):
        """Execute multiple statements separated by semicolons."""
        for stmt in script.strip().split(";"):
            s = stmt.strip()
            if s:
                self.execute(s)

    def commit(self):
        pass   # Turso auto-commits each HTTP request

    def rollback(self):
        pass

    def close(self):
        pass

    @property
    def lastrowid(self):
        return self._last_rowid

    @staticmethod
    def _to_value(v) -> dict:
        if v is None:
            return {"type": "null", "value": None}
        if isinstance(v, bool):
            return {"type": "integer", "value": "1" if v else "0"}
        if isinstance(v, int):
            return {"type": "integer", "value": str(v)}
        if isinstance(v, float):
            return {"type": "real", "value": str(v)}
        return {"type": "text", "value": str(v)}


class _TursoCursor:
    """Mimics sqlite3.Cursor for row fetching."""

    def __init__(self, result: dict):
        self._cols = [c["name"] for c in result.get("cols", [])]
        self._rows = result.get("rows", [])
        self._idx = 0
        self.lastrowid = int(result.get("last_insert_rowid") or 0) or None
        self.rowcount = result.get("affected_row_count", 0)

    @property
    def description(self):
        return [(c,) for c in self._cols]

    def fetchone(self):
        if self._idx >= len(self._rows):
            return None
        row = self._rows[self._idx]
        self._idx += 1
        return [v["value"] if isinstance(v, dict) else v for v in row]

    def fetchall(self):
        rows = []
        for row in self._rows[self._idx:]:
            rows.append([v["value"] if isinstance(v, dict) else v for v in row])
        self._idx = len(self._rows)
        return rows


# ---------------------------------------------------------------------------
# Connection factory
# ---------------------------------------------------------------------------

def _get_connection():
    if DATABASE_URL.startswith("libsql"):
        sync_url, token = _parse_turso_url(DATABASE_URL)
        return TursoClient(sync_url, token), True
    else:
        db_path = DATABASE_URL.replace("sqlite:///", "")
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        return conn, False


@contextmanager
def _db():
    conn, is_turso = _get_connection()
    try:
        yield conn
        conn.commit()
    except Exception:
        if hasattr(conn, "rollback"):
            conn.rollback()
        raise
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

SCHEMA = """
CREATE TABLE IF NOT EXISTS users (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    email               TEXT NOT NULL UNIQUE,
    watchlist           TEXT NOT NULL,
    schedule            TEXT DEFAULT 'morning',
    market              TEXT DEFAULT 'us',
    active              INTEGER DEFAULT 1,
    unsubscribe_token   TEXT NOT NULL,
    created_at          TEXT DEFAULT (datetime('now')),
    updated_at          TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS price_snapshots (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol       TEXT NOT NULL,
    date         TEXT NOT NULL,
    open         REAL, high REAL, low REAL, close REAL, volume REAL,
    sma_20       REAL, sma_50 REAL, sma_200 REAL,
    rsi          REAL, macd REAL, macd_signal REAL, macd_histogram REAL,
    trend        TEXT, momentum TEXT, volume_signal TEXT,
    support      REAL, resistance REAL,
    created_at   TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS alerts (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol           TEXT NOT NULL,
    signal_type      TEXT NOT NULL,
    condition        TEXT, action TEXT, action_detail TEXT,
    confidence       TEXT, confidence_score INTEGER,
    entry_zone       TEXT, exit_target TEXT, stop_loss TEXT,
    risk_pct         REAL, is_pre_signal INTEGER DEFAULT 0,
    triggered_at     TEXT NOT NULL,
    notified         INTEGER DEFAULT 0,
    created_at       TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS analysis_reports (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol           TEXT NOT NULL,
    asset_type       TEXT, report_date TEXT NOT NULL,
    trend            TEXT, momentum TEXT,
    confidence_score INTEGER, lt_stance TEXT, payload TEXT,
    created_at       TEXT DEFAULT (datetime('now'))
)
"""


def init_db():
    with _db() as conn:
        for stmt in SCHEMA.strip().split(";"):
            s = stmt.strip()
            if s:
                conn.execute(s)
        # Migration: add market column to existing users tables
        try:
            conn.execute("ALTER TABLE users ADD COLUMN market TEXT DEFAULT 'us'")
            logger.info("Migration: added market column to users table")
        except Exception:
            pass  # Column already exists
    logger.info(f"Database initialized: {DATABASE_URL.split('?')[0]}")


# ---------------------------------------------------------------------------
# Row helpers
# ---------------------------------------------------------------------------

def _rows_to_dicts(cursor) -> list[dict]:
    cols = [d[0] for d in cursor.description]
    return [dict(zip(cols, row)) for row in cursor.fetchall()]


def _row_to_dict(cursor, row) -> Optional[dict]:
    if row is None:
        return None
    cols = [d[0] for d in cursor.description]
    return dict(zip(cols, row))


def _coerce(data: dict) -> dict:
    out = {}
    for k, v in data.items():
        if isinstance(v, datetime):
            out[k] = v.isoformat()
        else:
            out[k] = v
    return out


# ---------------------------------------------------------------------------
# Price snapshots
# ---------------------------------------------------------------------------

def save_price_snapshot(data: dict):
    d = _coerce(data)
    sql = """
        INSERT INTO price_snapshots
            (symbol, date, open, high, low, close, volume,
             sma_20, sma_50, sma_200, rsi, macd, macd_signal, macd_histogram,
             trend, momentum, volume_signal, support, resistance)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """
    with _db() as conn:
        conn.execute(sql, (
            d.get("symbol"), d.get("date"), d.get("open"), d.get("high"),
            d.get("low"), d.get("close"), d.get("volume"), d.get("sma_20"),
            d.get("sma_50"), d.get("sma_200"), d.get("rsi"), d.get("macd"),
            d.get("macd_signal"), d.get("macd_histogram"), d.get("trend"),
            d.get("momentum"), d.get("volume_signal"), d.get("support"), d.get("resistance"),
        ))
    logger.debug(f"Saved snapshot: {data.get('symbol')}")


def save_alert(data: dict) -> int:
    d = _coerce(data)
    sql = """
        INSERT INTO alerts
            (symbol, signal_type, condition, action, action_detail,
             confidence, confidence_score, entry_zone, exit_target,
             stop_loss, risk_pct, is_pre_signal, triggered_at)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
    """
    with _db() as conn:
        cur = conn.execute(sql, (
            d.get("symbol"), d.get("signal_type"), d.get("condition"),
            d.get("action"), d.get("action_detail"), d.get("confidence"),
            d.get("confidence_score"), d.get("entry_zone"), d.get("exit_target"),
            d.get("stop_loss"), d.get("risk_pct"), d.get("is_pre_signal"),
            d.get("triggered_at"),
        ))
        row_id = cur.lastrowid
    logger.info(f"Saved alert: {data.get('symbol')} — {data.get('signal_type')}")
    return row_id or 0


def save_analysis_report(data: dict):
    d = _coerce(data)
    sql = """
        INSERT INTO analysis_reports
            (symbol, asset_type, report_date, trend, momentum,
             confidence_score, lt_stance, payload)
        VALUES (?,?,?,?,?,?,?,?)
    """
    with _db() as conn:
        conn.execute(sql, (
            d.get("symbol"), d.get("asset_type"), d.get("report_date"),
            d.get("trend"), d.get("momentum"), d.get("confidence_score"),
            d.get("lt_stance"), d.get("payload"),
        ))
    logger.debug(f"Saved report: {data.get('symbol')}")


def get_pending_alerts() -> list[dict]:
    with _db() as conn:
        cur = conn.execute("SELECT * FROM alerts WHERE notified = 0")
        return _rows_to_dicts(cur)


def mark_alerts_notified(alert_ids: list[int]):
    if not alert_ids:
        return
    placeholders = ",".join("?" * len(alert_ids))
    with _db() as conn:
        conn.execute(
            f"UPDATE alerts SET notified = 1 WHERE id IN ({placeholders})",
            tuple(alert_ids),
        )


def get_recent_snapshots(symbol: str, days: int = 7) -> list[dict]:
    cutoff = (datetime.utcnow() - timedelta(days=days)).isoformat()
    with _db() as conn:
        cur = conn.execute(
            "SELECT * FROM price_snapshots WHERE symbol = ? AND date >= ? ORDER BY date DESC",
            (symbol, cutoff),
        )
        return _rows_to_dicts(cur)


def purge_old_records(retention_days: int = 90):
    cutoff = (datetime.utcnow() - timedelta(days=retention_days)).isoformat()
    with _db() as conn:
        r1 = conn.execute("DELETE FROM alerts WHERE created_at < ?", (cutoff,)).rowcount
        r2 = conn.execute("DELETE FROM price_snapshots WHERE created_at < ?", (cutoff,)).rowcount
        r3 = conn.execute("DELETE FROM analysis_reports WHERE created_at < ?", (cutoff,)).rowcount
    logger.info(f"Purged: {r1} alerts, {r2} snapshots, {r3} reports")


# ---------------------------------------------------------------------------
# User management
# ---------------------------------------------------------------------------

def save_user(data: dict):
    with _db() as conn:
        conn.execute("""
            INSERT INTO users (email, watchlist, schedule, market, active, unsubscribe_token)
            VALUES (?,?,?,?,?,?)
            ON CONFLICT(email) DO UPDATE SET
                watchlist         = excluded.watchlist,
                schedule          = excluded.schedule,
                market            = excluded.market,
                active            = 1,
                updated_at        = datetime('now')
        """, (
            data["email"], data["watchlist"],
            data.get("schedule", "morning"),
            data.get("market", "us"),
            data.get("active", 1),
            data["unsubscribe_token"],
        ))
    logger.info(f"User saved: {data['email']}")


def get_active_users() -> list[dict]:
    with _db() as conn:
        cur = conn.execute("SELECT * FROM users WHERE active = 1 ORDER BY created_at ASC")
        return _rows_to_dicts(cur)


def get_user_by_token(token: str) -> Optional[dict]:
    with _db() as conn:
        cur = conn.execute("SELECT * FROM users WHERE unsubscribe_token = ?", (token,))
        return _row_to_dict(cur, cur.fetchone())


def unsubscribe_user(token: str):
    with _db() as conn:
        conn.execute(
            "UPDATE users SET active = 0, updated_at = datetime('now') WHERE unsubscribe_token = ?",
            (token,),
        )
    logger.info(f"Unsubscribed token: {token[:8]}...")
