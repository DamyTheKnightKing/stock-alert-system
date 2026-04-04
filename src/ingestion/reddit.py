"""
Reddit Sentiment Ingestion — uses PRAW (Python Reddit API Wrapper).

Searches r/wallstreetbets, r/stocks, r/investing for ticker mentions
in the last 24 hours. Scores sentiment by upvote-weighted keyword analysis.

Requires env vars:
  REDDIT_CLIENT_ID
  REDDIT_CLIENT_SECRET
  REDDIT_USER_AGENT  (optional, defaults to StockAlertBot/1.0)

Reddit free API: 100 requests/minute — more than enough.
"""
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional

logger = logging.getLogger(__name__)

REDDIT_CLIENT_ID     = os.getenv("REDDIT_CLIENT_ID", "")
REDDIT_CLIENT_SECRET = os.getenv("REDDIT_CLIENT_SECRET", "")
REDDIT_USER_AGENT    = os.getenv("REDDIT_USER_AGENT", "StockAlertBot/1.0")

SUBREDDITS = ["wallstreetbets", "stocks", "investing"]
LOOKBACK_HOURS = 24


@dataclass
class RedditSentiment:
    symbol: str
    mention_count: int
    bullish_count: int
    bearish_count: int
    neutral_count: int
    top_posts: list[dict]          # [{title, score, url, sentiment}]
    overall: str                   # "bullish" | "bearish" | "neutral"
    confidence: float              # 0.0 – 1.0, how skewed the sentiment is


def fetch_reddit_sentiment(symbol: str) -> Optional[RedditSentiment]:
    """
    Fetch Reddit sentiment for a ticker symbol.
    Returns None if PRAW credentials are not configured or fetch fails.
    """
    if not REDDIT_CLIENT_ID or not REDDIT_CLIENT_SECRET:
        logger.debug("Reddit credentials not set — skipping sentiment")
        return None

    try:
        import praw
    except ImportError:
        logger.warning("praw not installed — run: pip install praw")
        return None

    # Strip exchange suffix for search
    query = symbol.split(".")[0]
    cutoff = datetime.now(timezone.utc) - timedelta(hours=LOOKBACK_HOURS)

    try:
        reddit = praw.Reddit(
            client_id=REDDIT_CLIENT_ID,
            client_secret=REDDIT_CLIENT_SECRET,
            user_agent=REDDIT_USER_AGENT,
            ratelimit_seconds=1,
        )

        posts = _search_reddit(reddit, query, cutoff)

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
            post["sentiment"] = sentiment
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

    except Exception as e:
        logger.error(f"{symbol}: Reddit fetch failed — {e}")
        return None


def _search_reddit(reddit, query: str, cutoff: datetime) -> list[dict]:
    """Search across configured subreddits for ticker mentions."""
    results = []
    seen_ids: set[str] = set()

    for sub_name in SUBREDDITS:
        try:
            sub = reddit.subreddit(sub_name)
            # Search by ticker symbol — exact match in title/text
            for post in sub.search(f'"{query}"', sort="new", time_filter="day", limit=20):
                if post.id in seen_ids:
                    continue
                post_time = datetime.fromtimestamp(post.created_utc, tz=timezone.utc)
                if post_time < cutoff:
                    continue
                seen_ids.add(post.id)
                results.append({
                    "id": post.id,
                    "title": post.title,
                    "selftext": post.selftext[:500] if post.selftext else "",
                    "score": post.score,
                    "url": f"https://reddit.com{post.permalink}",
                    "subreddit": sub_name,
                    "created_utc": post_time.isoformat(),
                })
        except Exception as e:
            logger.warning(f"Reddit search failed for r/{sub_name}: {e}")

    # Sort by score descending
    return sorted(results, key=lambda x: x["score"], reverse=True)


# ---------------------------------------------------------------------------
# Sentiment scoring — weighted keyword analysis
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
