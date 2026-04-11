"""
News Ingestion — two sources:
  1. Yahoo Finance news (via yfinance — free, no key)
  2. NewsAPI (free tier: 100 req/day — needs NEWS_API_KEY env var)

Returns a unified list of NewsItem dicts per symbol.
"""
import logging
import os
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Optional

import yfinance as yf

logger = logging.getLogger(__name__)

NEWS_API_KEY = os.getenv("NEWS_API_KEY", "")
_NEWSAPI_BASE = "https://newsapi.org/v2/everything"


@dataclass
class NewsItem:
    symbol: str
    title: str
    summary: str
    source: str
    url: str
    published_at: str          # ISO 8601
    sentiment_hint: str = ""   # "positive" | "negative" | "neutral" — keyword-based pre-screen


def fetch_news(symbol: str, max_items: int = 5) -> list[NewsItem]:
    """
    Fetch recent news for a symbol.
    Tries Yahoo Finance first (always available), then NewsAPI if key is set.
    Returns deduplicated list sorted by recency.
    """
    items: list[NewsItem] = []

    items.extend(_fetch_yahoo_news(symbol, max_items))

    if NEWS_API_KEY:
        items.extend(_fetch_newsapi(symbol, max_items))

    # Deduplicate by title, keep most recent
    seen: set[str] = set()
    unique: list[NewsItem] = []
    for item in sorted(items, key=lambda x: x.published_at, reverse=True):
        key = item.title[:60].lower()
        if key not in seen:
            seen.add(key)
            unique.append(item)

    result = unique[:max_items]
    result = score_news_sentiment(result)
    logger.info(f"{symbol}: {len(result)} news items fetched")
    return result


def _fetch_yahoo_news(symbol: str, max_items: int) -> list[NewsItem]:
    try:
        ticker = yf.Ticker(symbol)
        raw = ticker.news or []
        items = []
        for article in raw[:max_items]:
            content = article.get("content", {})
            title = (content.get("title") or article.get("title") or "").strip()
            if not title:
                continue

            summary = (content.get("summary") or content.get("description") or "").strip()

            # published_at — handle both int (Unix) and string
            pub = content.get("pubDate") or article.get("providerPublishTime")
            if isinstance(pub, int):
                published_at = datetime.fromtimestamp(pub, tz=timezone.utc).isoformat()
            elif isinstance(pub, str):
                published_at = pub
            else:
                published_at = datetime.now(timezone.utc).isoformat()

            url = ""
            click_through = content.get("clickThroughUrl") or content.get("canonicalUrl") or {}
            if isinstance(click_through, dict):
                url = click_through.get("url", "")

            source = content.get("provider", {}).get("displayName", "Yahoo Finance") \
                if isinstance(content.get("provider"), dict) else "Yahoo Finance"

            items.append(NewsItem(
                symbol=symbol,
                title=title,
                summary=summary[:300],
                source=source,
                url=url,
                published_at=published_at,
                sentiment_hint=_keyword_sentiment(title + " " + summary),
            ))
        return items
    except Exception as e:
        logger.warning(f"{symbol}: Yahoo news fetch failed — {e}")
        return []


def _fetch_newsapi(symbol: str, max_items: int) -> list[NewsItem]:
    try:
        import httpx
        # Strip exchange suffix for cleaner search (.NS / .BO)
        query = symbol.split(".")[0]
        from_date = (datetime.now(timezone.utc) - timedelta(days=3)).strftime("%Y-%m-%d")

        resp = httpx.get(
            _NEWSAPI_BASE,
            params={
                "q": f'"{query}" stock',
                "language": "en",
                "sortBy": "publishedAt",
                "pageSize": max_items,
                "from": from_date,
                "apiKey": NEWS_API_KEY,
            },
            timeout=8.0,
        )
        resp.raise_for_status()
        data = resp.json()

        items = []
        for article in data.get("articles", [])[:max_items]:
            title = (article.get("title") or "").strip()
            if not title or title == "[Removed]":
                continue
            summary = (article.get("description") or "").strip()
            items.append(NewsItem(
                symbol=symbol,
                title=title,
                summary=summary[:300],
                source=article.get("source", {}).get("name", "NewsAPI"),
                url=article.get("url", ""),
                published_at=article.get("publishedAt", ""),
                sentiment_hint=_keyword_sentiment(title + " " + summary),
            ))
        return items
    except Exception as e:
        logger.warning(f"{symbol}: NewsAPI fetch failed — {e}")
        return []


# ---------------------------------------------------------------------------
# Sentiment scoring — AI-powered via OpenRouter, keyword fallback
# ---------------------------------------------------------------------------

_OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY", "")
_OPENROUTER_SENTIMENT_MODEL = os.getenv(
    "OPENROUTER_SENTIMENT_MODEL", "mistralai/mistral-small-3.1-24b-instruct:free"
)
_OPENROUTER_BASE = "https://openrouter.ai/api/v1"


def score_news_sentiment(items: list) -> list:
    """
    Score a list of NewsItems with sentiment_hint.
    Uses OpenRouter (batch) when OPENROUTER_API_KEY is set, else keyword fallback.
    Mutates items in-place and returns the list.
    """
    if _OPENROUTER_API_KEY and items:
        try:
            return _ai_sentiment_batch(items)
        except Exception as e:
            logger.warning("AI sentiment failed (%s) — falling back to keywords", e)
    for item in items:
        item.sentiment_hint = _keyword_sentiment(item.title + " " + item.summary)
    return items


def _ai_sentiment_batch(items: list) -> list:
    """Single OpenRouter call to score all headlines at once."""
    from openai import OpenAI

    client = OpenAI(
        base_url=_OPENROUTER_BASE,
        api_key=_OPENROUTER_API_KEY,
        default_headers={"HTTP-Referer": "https://github.com/DamyTheKnightKing/stock-alert-system"},
    )

    headlines = "\n".join(
        f"{i+1}. {item.title}" for i, item in enumerate(items)
    )
    prompt = (
        "You are a financial news sentiment classifier. "
        "Classify each headline as exactly one of: positive, negative, neutral. "
        "Consider nuance: 'better than feared' = positive, 'in-line with estimates' = neutral. "
        "Reply ONLY with a JSON array of strings in the same order as the input. "
        "Example: [\"positive\", \"neutral\", \"negative\"]\n\n"
        f"Headlines:\n{headlines}"
    )

    resp = client.chat.completions.create(
        model=_OPENROUTER_SENTIMENT_MODEL,
        messages=[{"role": "user", "content": prompt}],
        max_tokens=60,
        temperature=0.0,
    )

    import json as _json
    raw = resp.choices[0].message.content.strip()
    # Strip markdown fences if present
    raw = raw.strip("`").replace("json", "").strip()
    labels = _json.loads(raw)

    valid = {"positive", "negative", "neutral"}
    for i, item in enumerate(items):
        if i < len(labels) and labels[i] in valid:
            item.sentiment_hint = labels[i]
        else:
            item.sentiment_hint = _keyword_sentiment(item.title + " " + item.summary)

    return items


# ---------------------------------------------------------------------------
# Keyword fallback (used when OpenRouter is not configured)
# ---------------------------------------------------------------------------

_POSITIVE_WORDS = {
    "beat", "beats", "surge", "surges", "record", "upgrade", "upgraded",
    "raises", "raised", "bullish", "growth", "profit", "gains", "rally",
    "strong", "positive", "outperform", "buy", "breakout", "deal", "acquisition",
}
_NEGATIVE_WORDS = {
    "miss", "misses", "decline", "declines", "downgrade", "downgraded",
    "cut", "loss", "losses", "bearish", "warning", "weak", "sell", "layoff",
    "layoffs", "recall", "lawsuit", "investigation", "fraud", "crash", "plunge",
}


def _keyword_sentiment(text: str) -> str:
    words = set(text.lower().split())
    pos = len(words & _POSITIVE_WORDS)
    neg = len(words & _NEGATIVE_WORDS)
    if pos > neg:
        return "positive"
    if neg > pos:
        return "negative"
    return "neutral"
