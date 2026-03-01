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

FETCH_INTERVAL = 10
LOOKBACK_DAYS = 7

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
                print(f"[EquityNews] Error fetching {symbol} with key {i+1}: {e}")
                continue

        return None

