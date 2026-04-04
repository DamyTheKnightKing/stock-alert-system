"""
Stock Alert System — FastAPI
Endpoints:
  GET  /                        health check
  GET  /api/search?q=AAPL       symbol autocomplete (proxies Yahoo Finance)
  POST /api/subscribe           register a new user
  GET  /api/unsubscribe?token=  unsubscribe a user
  POST /api/run                 trigger analysis run (internal, API-key protected)
"""
import json
import logging
import os
import re
import secrets

import httpx
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, EmailStr, field_validator
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.util import get_remote_address

from src.storage.db import (get_user_by_token, init_db, save_user,
                             unsubscribe_user)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Rate limiter
# ---------------------------------------------------------------------------

limiter = Limiter(key_func=get_remote_address)
app = FastAPI(title="Stock Alert System API", version="1.0.0")
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

ALLOWED_ORIGIN = os.getenv("ALLOWED_ORIGIN", "https://alerts.theknightcodes.com")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[ALLOWED_ORIGIN, "http://localhost:8000", "http://127.0.0.1:5500"],
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

# Valid stock symbol: 1-6 uppercase letters/numbers only
SYMBOL_RE = re.compile(r'^[A-Z0-9]{1,6}$')


@app.on_event("startup")
async def startup():
    init_db()
    logger.info("Database initialised")


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------

class SubscribeRequest(BaseModel):
    email: EmailStr
    symbols: list[str]
    schedule: str = "morning"

    @field_validator("symbols")
    @classmethod
    def validate_symbols(cls, v):
        if not v:
            raise ValueError("Select at least 1 symbol")
        if len(v) > 10:
            raise ValueError("Maximum 10 symbols allowed")
        cleaned = [s.upper().strip() for s in v]
        for s in cleaned:
            if not SYMBOL_RE.match(s):
                raise ValueError(f"Invalid symbol: {s}")
        return cleaned

    @field_validator("schedule")
    @classmethod
    def validate_schedule(cls, v):
        if v not in ("morning", "evening", "both"):
            raise ValueError("Schedule must be morning, evening, or both")
        return v


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.get("/")
async def health():
    return {"status": "ok", "service": "Stock Alert System API"}


@app.get("/api/search")
@limiter.limit("30/minute")
async def search_symbols(request: Request, q: str = Query(..., min_length=1, max_length=10)):
    """Proxy Yahoo Finance symbol search — avoids CORS from browser."""
    # Only allow alphanumeric search queries
    if not re.match(r'^[A-Za-z0-9\s]+$', q):
        return []
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            resp = await client.get(
                "https://query1.finance.yahoo.com/v1/finance/search",
                params={"q": q, "quotesCount": 8, "newsCount": 0, "enableFuzzyQuery": False},
                headers={"User-Agent": "Mozilla/5.0 (compatible; StockAlertBot/1.0)"},
            )
            data = resp.json()

        results = []
        for quote in data.get("quotes", []):
            if quote.get("quoteType") not in ("EQUITY", "ETF"):
                continue
            sym = quote.get("symbol", "")
            if "." in sym or len(sym) > 6:
                continue
            results.append({
                "symbol": sym,
                "name": quote.get("longname") or quote.get("shortname", sym),
                "type": quote.get("quoteType", ""),
                "exchange": quote.get("exchange", ""),
            })
        return results[:6]

    except Exception as e:
        logger.error(f"Symbol search failed: {e}")
        raise HTTPException(status_code=502, detail="Symbol search unavailable")


@app.post("/api/subscribe")
@limiter.limit("5/minute")
async def subscribe(request: Request, req: SubscribeRequest):
    """Register or update a user subscription."""
    token = secrets.token_urlsafe(32)

    try:
        save_user({
            "email": req.email,
            "watchlist": json.dumps(req.symbols),
            "schedule": req.schedule,
            "active": 1,
            "unsubscribe_token": token,
        })
    except Exception as e:
        logger.error(f"Subscribe failed for {req.email}: {e}")
        raise HTTPException(status_code=500, detail="Subscription failed. Please try again.")

    try:
        _send_welcome_email(req.email, req.symbols, req.schedule)
    except Exception as e:
        logger.warning(f"Welcome email failed: {e}")

    logger.info(f"New subscriber: {req.email} | symbols={req.symbols} | schedule={req.schedule}")
    return {
        "message": "Subscribed successfully! Check your email for confirmation.",
        "symbols": req.symbols,
        "schedule": req.schedule,
    }


@app.get("/api/unsubscribe", response_class=HTMLResponse)
async def unsubscribe(token: str = Query(..., min_length=10, max_length=100)):
    """Unsubscribe via token link from email footer."""
    # Only allow URL-safe base64 characters in token
    if not re.match(r'^[A-Za-z0-9_\-]+$', token):
        return HTMLResponse(_unsubscribe_html("Invalid unsubscribe link."), status_code=400)

    user = get_user_by_token(token)
    if not user:
        return HTMLResponse(_unsubscribe_html("Invalid or expired unsubscribe link."), status_code=404)

    unsubscribe_user(token)
    logger.info(f"Unsubscribed: {user.get('email')}")
    return HTMLResponse(_unsubscribe_html(
        f"You have been unsubscribed successfully.<br>"
        f"<small style='color:#888'>{user.get('email')}</small>",
        success=True
    ))


@app.post("/api/run")
async def trigger_run(request: Request):
    """Internal endpoint — trigger analysis run for all users (GitHub Actions calls this)."""
    api_key = request.headers.get("X-API-Key")
    expected = os.getenv("API_SECRET_KEY", "")
    if not expected or api_key != expected:
        raise HTTPException(status_code=401, detail="Unauthorized")

    from src.alerts.engine import run_for_all_users
    try:
        count = run_for_all_users()
        return {"status": "ok", "users_processed": count}
    except Exception as e:
        logger.error(f"Run failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _send_welcome_email(email: str, symbols: list[str], schedule: str):
    import resend
    resend.api_key = os.getenv("RESEND_API_KEY", "")
    if not resend.api_key:
        return

    schedule_label = {
        "morning": "8:30 AM ET (before market open)",
        "evening": "4:30 PM ET (after market close)",
        "both": "8:30 AM ET and 4:30 PM ET",
    }.get(schedule, schedule)

    html = f"""
    <div style="font-family:-apple-system,Arial,sans-serif;max-width:520px;margin:0 auto;padding:20px;">
      <div style="background:#1a1a2e;border-radius:10px 10px 0 0;padding:20px 24px;">
        <h2 style="color:#fff;margin:0;font-size:18px;">Welcome to Stock Alert System</h2>
      </div>
      <div style="background:#fff;border:1px solid #eee;border-top:none;border-radius:0 0 10px 10px;padding:24px;">
        <p style="color:#333;">You're subscribed! Here's what you'll receive:</p>
        <table style="width:100%;border-collapse:collapse;margin:16px 0;">
          <tr><td style="padding:8px;color:#888;font-size:12px;">SYMBOLS</td>
              <td style="padding:8px;font-weight:600;">{', '.join(symbols)}</td></tr>
          <tr style="background:#f8f9fa;"><td style="padding:8px;color:#888;font-size:12px;">SCHEDULE</td>
              <td style="padding:8px;font-weight:600;">{schedule_label}</td></tr>
        </table>
        <p style="color:#555;font-size:13px;">
          Your first alert will arrive at the next scheduled run.
        </p>
        <p style="color:#aaa;font-size:11px;margin-top:24px;border-top:1px solid #eee;padding-top:12px;">
          Not financial advice. Stock Alert System.
        </p>
      </div>
    </div>
    """
    resend.Emails.send({
        "from": "Stock Alerts <alerts@theknightcodes.com>",
        "to": [email],
        "subject": "[StockAlert] You're subscribed!",
        "html": html,
    })


def _unsubscribe_html(message: str, success: bool = False) -> str:
    color = "#2ecc71" if success else "#e74c3c"
    return f"""
    <!DOCTYPE html><html><body style="font-family:Arial,sans-serif;display:flex;
    justify-content:center;align-items:center;min-height:100vh;background:#f0f2f5;margin:0;">
    <div style="background:#fff;border-radius:10px;padding:40px;text-align:center;
                max-width:400px;box-shadow:0 2px 20px rgba(0,0,0,0.08);">
      <div style="font-size:40px;">{'✓' if success else '✗'}</div>
      <h2 style="color:{color};margin:12px 0;">{'Unsubscribed' if success else 'Error'}</h2>
      <p style="color:#555;">{message}</p>
      <a href="https://alerts.theknightcodes.com"
         style="display:inline-block;margin-top:16px;padding:10px 24px;
                background:#1a1a2e;color:#fff;border-radius:6px;text-decoration:none;">
        Subscribe Again
      </a>
    </div></body></html>
    """
