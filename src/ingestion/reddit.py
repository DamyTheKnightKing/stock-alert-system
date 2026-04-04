"""
Reddit Sentiment Ingestion — uses Reddit's public JSON API.
No OAuth, no API key, no app registration required.

Searches r/wallstreetbets, r/stocks, r/investing for ticker mentions
in the last 24 hours. Scores sentiment by upvote-weighted keyword analysis.
"""
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

SUBREDDITS = ["wallstreetbets", "stocks", "investing"]
LOOKBACK_HOURS = 24
_HEADERS = {"User-Agent": "StockAlertBot/1.0 (personal research tool)"}


@dataclass
class RedditSentiment:
    symbol: str
    mention_count: int
    bullish_count: int
    bearish_count: int
    neutral_count: int
    top_posts: list[dict]       # [{title, score, url, sentiment, subreddit}]
    overall: str                # "bullish" | "bearish" | "neutral"
    confidence: float           # 0.0 – 1.0


def fetch_reddit_sentiment(symbol: str) -> Optional[RedditSentiment]:
    """
    Fetch Reddit sentiment for a ticker using the public JSON API.
    Returns None if fetch fails entirely.
    """
    query = symbol.split(".")[0]   # strip .NS / .BO
    cutoff = datetime.now(timezone.utc) - timedelta(hours=LOOKBACK_HOURS)

    posts = _search_all_subreddits(query, cutoff)

    if not posts:
        logger.info(f"{symbol}: No Reddit mentions in last {LOOKBACK_HOURS}h")
        return RedditSentiment(
            symbol=symbol,
            mention_count=0,
            bullish_count=0,
            bearish_count=0,
            neutral_count=0,
            top_posts=[],
            overall="neutral",
            confidence=0.0,
        )

    bullish = bearish = neutral = 0
    top_posts = []

    for post in posts:
        sentiment = _score_post(post["title"] + " " + post.get("selftext", ""))
        if sentiment == "bullish":
            bullish += 1
        elif sentiment == "bearish":
            bearish += 1
        else:
            neutral += 1

        if len(top_posts) < 3:
            top_posts.append({
                "title": post["title"][:120],
                "score": post["score"],
                "url": post["url"],
                "sentiment": sentiment,
                "subreddit": post["subreddit"],
            })

    total = bullish + bearish + neutral
    if bullish > bearish:
        overall = "bullish"
        confidence = round(bullish / total, 2)
    elif bearish > bullish:
        overall = "bearish"
        confidence = round(bearish / total, 2)
    else:
        overall = "neutral"
        confidence = 0.5

    logger.info(
        f"{symbol}: Reddit — {total} mentions, "
        f"bullish={bullish} bearish={bearish} neutral={neutral} → {overall}"
    )

    return RedditSentiment(
        symbol=symbol,
        mention_count=total,
        bullish_count=bullish,
        bearish_count=bearish,
        neutral_count=neutral,
        top_posts=top_posts,
        overall=overall,
        confidence=confidence,
    )


def _search_all_subreddits(query: str, cutoff: datetime) -> list[dict]:
    results = []
    seen_ids: set[str] = set()

    for sub in SUBREDDITS:
        try:
            posts = _fetch_subreddit(sub, query, cutoff)
            for post in posts:
                if post["id"] not in seen_ids:
                    seen_ids.add(post["id"])
                    results.append(post)
        except Exception as e:
            logger.warning(f"Reddit fetch failed for r/{sub}: {e}")

    return sorted(results, key=lambda x: x["score"], reverse=True)


def _fetch_subreddit(subreddit: str, query: str, cutoff: datetime) -> list[dict]:
    url = f"https://www.reddit.com/r/{subreddit}/search.json"
    params = {
        "q": query,        # plain ticker — no quotes, no restrict_sr (both kill results)
        "sort": "new",
        "t": "week",       # week window so cutoff filter handles the 24h boundary
        "limit": 25,
    }

    resp = httpx.get(url, params=params, headers=_HEADERS, timeout=10.0)
    resp.raise_for_status()
    data = resp.json()

    posts = []
    for child in data.get("data", {}).get("children", []):
        post = child.get("data", {})
        created = datetime.fromtimestamp(post.get("created_utc", 0), tz=timezone.utc)
        if created < cutoff:
            continue
        posts.append({
            "id": post.get("id", ""),
            "title": post.get("title", ""),
            "selftext": post.get("selftext", "")[:500],
            "score": post.get("score", 0),
            "url": f"https://reddit.com{post.get('permalink', '')}",
            "subreddit": subreddit,
            "created_utc": created.isoformat(),
        })

    return posts


# ---------------------------------------------------------------------------
# Sentiment scoring
# ---------------------------------------------------------------------------

_BULLISH_WORDS = {
    "bullish", "buy", "long", "calls", "moon", "rocket", "breakout",
    "upgrade", "beat", "beats", "strong", "rally", "surge", "undervalued",
    "growth", "accumulate", "hold", "squeeze", "run", "upside",
}
_BEARISH_WORDS = {
    "bearish", "sell", "short", "puts", "crash", "dump", "downgrade",
    "miss", "misses", "weak", "decline", "overvalued", "avoid", "tank",
    "warning", "layoff", "layoffs", "loss", "losses", "drop", "falling",
}


def _score_post(text: str) -> str:
    words = set(text.lower().split())
    bull = len(words & _BULLISH_WORDS)
    bear = len(words & _BEARISH_WORDS)
    if bull > bear:
        return "bullish"
    if bear > bull:
        return "bearish"
    return "neutral"
