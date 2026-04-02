"""
Storage Layer — SQLite by default.
Zero setup, zero cost. Swap to PostgreSQL/Supabase by setting DATABASE_URL env var.
SQLAlchemy abstracts the database — same code works for both.
"""
import json
import logging
import os
from datetime import datetime, timedelta
from pathlib import Path

from sqlalchemy import (Column, DateTime, Float, Integer, String, Text,
                        create_engine, text)
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker

logger = logging.getLogger(__name__)

DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///data/stock_alerts.db")

# SQLite: ensure the data/ directory exists
if DATABASE_URL.startswith("sqlite"):
    db_path = DATABASE_URL.replace("sqlite:///", "")
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)

engine = create_engine(DATABASE_URL, echo=False)
SessionLocal = sessionmaker(bind=engine)


class Base(DeclarativeBase):
    pass


class PriceSnapshot(Base):
    """Daily OHLCV + key indicator snapshot per symbol."""
    __tablename__ = "price_snapshots"

    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String(10), nullable=False, index=True)
    date = Column(DateTime, nullable=False, index=True)
    open = Column(Float)
    high = Column(Float)
    low = Column(Float)
    close = Column(Float)
    volume = Column(Float)
    sma_20 = Column(Float)
    sma_50 = Column(Float)
    sma_200 = Column(Float)
    rsi = Column(Float)
    macd = Column(Float)
    macd_signal = Column(Float)
    macd_histogram = Column(Float)
    trend = Column(String(20))
    momentum = Column(String(20))
    volume_signal = Column(String(20))
    support = Column(Float)
    resistance = Column(Float)
    created_at = Column(DateTime, default=datetime.utcnow)


class AlertRecord(Base):
    """Persisted alert with full context."""
    __tablename__ = "alerts"

    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String(10), nullable=False, index=True)
    signal_type = Column(String(50), nullable=False)
    condition = Column(Text)
    action = Column(String(30))
    action_detail = Column(Text)
    confidence = Column(String(10))
    confidence_score = Column(Integer)
    entry_zone = Column(String(50))
    exit_target = Column(String(50))
    stop_loss = Column(String(50))
    risk_pct = Column(Float)
    is_pre_signal = Column(Integer, default=0)  # SQLite bool
    triggered_at = Column(DateTime, nullable=False)
    notified = Column(Integer, default=0)   # 0 = pending, 1 = sent
    created_at = Column(DateTime, default=datetime.utcnow)


class AnalysisReport(Base):
    """Full analysis report stored as JSON blob (queryable by symbol/date)."""
    __tablename__ = "analysis_reports"

    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String(10), nullable=False, index=True)
    asset_type = Column(String(10))
    report_date = Column(DateTime, nullable=False, index=True)
    trend = Column(String(20))
    momentum = Column(String(20))
    confidence_score = Column(Integer)
    lt_stance = Column(String(20))
    payload = Column(Text)   # Full JSON for email rendering
    created_at = Column(DateTime, default=datetime.utcnow)


def init_db():
    """Create all tables if they don't exist."""
    Base.metadata.create_all(engine)
    logger.info(f"Database initialized: {DATABASE_URL}")


def save_price_snapshot(snap_data: dict):
    with SessionLocal() as session:
        record = PriceSnapshot(**snap_data)
        session.add(record)
        session.commit()
        logger.debug(f"Saved price snapshot: {snap_data.get('symbol')}")


def save_alert(alert_data: dict) -> int:
    with SessionLocal() as session:
        record = AlertRecord(**alert_data)
        session.add(record)
        session.commit()
        session.refresh(record)
        logger.info(f"Saved alert: {alert_data.get('symbol')} — {alert_data.get('signal_type')}")
        return record.id


def save_analysis_report(report_data: dict):
    with SessionLocal() as session:
        record = AnalysisReport(**report_data)
        session.add(record)
        session.commit()
        logger.debug(f"Saved analysis report: {report_data.get('symbol')}")


def get_pending_alerts() -> list[dict]:
    """Fetch all unsent alerts for notification dispatch."""
    with SessionLocal() as session:
        rows = session.query(AlertRecord).filter(AlertRecord.notified == 0).all()
        return [_alert_to_dict(r) for r in rows]


def mark_alerts_notified(alert_ids: list[int]):
    with SessionLocal() as session:
        session.query(AlertRecord).filter(
            AlertRecord.id.in_(alert_ids)
        ).update({"notified": 1}, synchronize_session=False)
        session.commit()


def get_recent_snapshots(symbol: str, days: int = 7) -> list[dict]:
    cutoff = datetime.utcnow() - timedelta(days=days)
    with SessionLocal() as session:
        rows = session.query(PriceSnapshot).filter(
            PriceSnapshot.symbol == symbol,
            PriceSnapshot.date >= cutoff
        ).order_by(PriceSnapshot.date.desc()).all()
        return [_snapshot_to_dict(r) for r in rows]


def purge_old_records(retention_days: int = 90):
    """Remove records older than retention period."""
    cutoff = datetime.utcnow() - timedelta(days=retention_days)
    with SessionLocal() as session:
        deleted_alerts = session.query(AlertRecord).filter(
            AlertRecord.created_at < cutoff
        ).delete()
        deleted_snaps = session.query(PriceSnapshot).filter(
            PriceSnapshot.created_at < cutoff
        ).delete()
        deleted_reports = session.query(AnalysisReport).filter(
            AnalysisReport.created_at < cutoff
        ).delete()
        session.commit()
        logger.info(f"Purged: {deleted_alerts} alerts, {deleted_snaps} snapshots, {deleted_reports} reports")


def _alert_to_dict(r: AlertRecord) -> dict:
    return {
        "id": r.id, "symbol": r.symbol, "signal_type": r.signal_type,
        "condition": r.condition, "action": r.action, "action_detail": r.action_detail,
        "confidence": r.confidence, "confidence_score": r.confidence_score,
        "entry_zone": r.entry_zone, "exit_target": r.exit_target,
        "stop_loss": r.stop_loss, "risk_pct": r.risk_pct,
        "is_pre_signal": bool(r.is_pre_signal), "triggered_at": r.triggered_at,
    }


def _snapshot_to_dict(r: PriceSnapshot) -> dict:
    return {
        "symbol": r.symbol, "date": r.date, "close": r.close,
        "rsi": r.rsi, "trend": r.trend, "momentum": r.momentum,
    }
