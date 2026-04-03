# Stock Alert System — Product Brief

---

## The Problem

Retail investors miss opportunities — not because they lack knowledge, but because they can't watch the market all day.

- Markets move fast. By the time most people check, the signal has passed.
- Generic financial news is noisy and not relevant to your portfolio.
- Professional tools (Bloomberg, Refinitiv) cost thousands per month.

---

## The Solution

**Stock Alert System** is a fully automated, personalized stock alert platform that monitors your watchlist 24/7 and delivers actionable signals directly to your inbox — before and during market hours.

You set it once. It works every day.

---

## How It Works

```
1. Sign up → pick your stocks → choose your schedule
2. Every morning at 8:30 AM ET, the system analyses every stock on your watchlist
3. You receive a personalised digest with the top opportunities ranked by confidence
```

High-confidence signals (intraday breakouts, RSI extremes, MACD crossovers) are delivered in real time throughout the trading day.

---

## Key Features

| Feature | Details |
|---------|---------|
| Personalised watchlist | Up to 10 US stocks or ETFs per subscriber |
| Morning digest | Daily briefing before market open — top 3 ETFs + top 3 stocks |
| Intraday alerts | Real-time alerts for high-confidence signals during market hours |
| Technical signals | RSI, MACD, SMA 20/50/200, Bollinger Bands, Support & Resistance, Volume |
| Confidence scoring | Every signal ranked — only the best reach your inbox |
| Unsubscribe anytime | One-click unsubscribe link in every email |

---

## Signals We Detect

- **Momentum** — RSI overbought / oversold / pre-signal zones
- **Trend** — MACD bullish and bearish crossovers
- **Breakout** — Price near key resistance or support levels
- **Volume** — Unusual volume spikes signalling institutional interest
- **Bollinger Band** — Price outside bands (volatility squeeze / expansion)
- **Confluence** — Multiple signals aligning for higher conviction

---

## Who Is It For

- **Retail investors** who want professional-grade signals without the cost
- **Busy professionals** who cannot monitor the market during working hours
- **ETF-first investors** who want macro + sector signals alongside individual stocks
- **Long-term investors** who want early warning before major moves

---

## Technology

Built entirely on free tiers — zero infrastructure cost.

| Layer | Technology |
|-------|-----------|
| Signup | GitHub Pages |
| API | FastAPI on Render.com |
| Database | Turso (libSQL Cloud) |
| Market data | Yahoo Finance (free) |
| Email | Resend (3,000 emails/month free) |
| Automation | GitHub Actions |

---

## Live Links

- **Sign up:** https://theknightcodes.github.io/stock-alert-system/
- **Source code:** https://github.com/theknightcodes/stock-alert-system

---

*Built by Dhamotharan Palanisamy*
