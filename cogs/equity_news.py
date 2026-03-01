import discord
from discord.ext import commands, tasks
import aiohttp
from datetime import datetime, timedelta, timezone
import hashlib
from database import db_manager
import os
from dotenv import load_dotenv

load_dotenv()

FINNHUB_API_KEYS = [os.getenv(f"FINNHUB_API_KEY_{i}") for i in range(1, 11)]

TRACKED_SYMBOLS = [
    # looking at tech-related companies for now
    "AAPL", "MSFT", "GOOGL", "AMZN", "NVDA",
    "META", "TSLA", "AMD", "INTC", "CRM",
    "ORCL", "ADBE", "AVGO", "QCOM", "MU",
    "PLTR", "SNOW", "NET", "CRWD", "PANW",
    "SQ", "SHOP", "UBER", "COIN", "SMCI",
]

# should be 5 
FETCH_INTERVAL = 15
# should be 2 depends on when last ran
LOOKBACK_DAYS = 60

def make_event_identifier(symbol, headline, timestamp):
    """Generate unique identifier for deduplication."""
    raw = f"equity-{symbol}-{headline}-{timestamp}"
    return hashlib.sha256(raw.encode()).hexdigest()

class EquityNews(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.fetch_equity_news.start()

    def cog_unload(self):
        self.fetch_equity_news.cancel()

    async def fetch_company_news(self, symbol):
        today = datetime.now().strftime('%Y-%m-%d')
        from_date = (datetime.now() - timedelta(days=LOOKBACK_DAYS)).strftime('%Y-%m-%d')

        for i, api_key in enumerate(FINNHUB_API_KEYS):
            if not api_key:
                continue
            url = (
                f"https://finnhub.io/api/v1/company-news"
                f"?symbol={symbol}&from={from_date}&to={today}&token={api_key}"
            )
            try:
                 async with aiohttp.ClientSession() as session:
                     async with session.get(url) as resp:
                        if resp.status == 200:
                            articles = await resp.json()
                            return articles
                        elif resp.status == 429:
                            continue
                        else:
                            continue
            except Exception as e:
                print(f"--- [EquityNews] Error fetching {symbol} with key {i+1}: {e}")
                continue

        return None
    
    @tasks.loop(minutes=FETCH_INTERVAL)
    async def fetch_equity_news(self):
        """fetching news and storing in db"""
        await self.bot.wait_until_ready()

        total_new = 0
        total_skipped = 0

        for symbol in TRACKED_SYMBOLS:
            articles = await self.fetch_company_news(symbol)

            if articles is None:
                print(f'--- [equitynews] failed to fetch news for {symbol}')
                continue
            
            for article in articles:
                headline = article.get('headline', '')
                if not headline:
                    continue

                # creating specific timestamp for dedeupplication
                timestamp = article.get("datetime", 0)
                identifier = make_event_identifier(symbol, headline, timestamp)

                already_seen = await db_manager.is_event_seen(identifier)
                if already_seen:
                    total_skipped += 1
                    continue

                # convert unix timestamp to datetime
                try:
                    event_time = datetime.fromtimestamp(timestamp, tz=timezone.utc)
                except (ValueError, OSError):
                    event_time = datetime.now(timezone.utc)

                # store the event 
                await db_manager.store_event({
                    "identifier": identifier,
                    "symbol": symbol,
                    "headline": headline,
                    "summary": article.get("summary", ""),
                    "source": article.get("source", "finnhub"),
                    "url": article.get("url", ""),
                    "event_timestamp": event_time,
                    "created_at": datetime.now(timezone.utc),

                    #  filled in by novelty_engine.py later
                    "embedding": None,
                    "embedding_model": None,
                    "novelty_score": None,
                    "novelty_percentile": None,
                })

                total_new += 1

        if total_new > 0 or total_skipped > 0:
            print(f"*** [EquityNews] Stored {total_new} new events, skipped {total_skipped} duplicates")

    @fetch_equity_news.before_loop
    async def before_fetch(self):
        await self.bot.wait_until_ready()

        # delay startup so other cogs can initialize first
        import asyncio
        await asyncio.sleep(20)


async def setup(bot):
    await bot.add_cog(EquityNews(bot))



