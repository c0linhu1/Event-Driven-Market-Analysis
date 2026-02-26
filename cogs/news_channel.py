import discord
from discord.ext import commands, tasks
import aiohttp
from datetime import datetime, timedelta, timezone
import hashlib
from database import db_manager
import os
from dotenv import load_dotenv
import tweepy
from tweepy.errors import TooManyRequests, Unauthorized


load_dotenv()

FINNHUB_API_KEYS = [os.getenv(f"FINNHUB_API_KEY_{i}") for i in range(1, 11)]
FINNHUB_URL = "https://finnhub.io/api/v1/news?category={category}&token={token}"
FINNHUB_CATEGORIES = ["general", "forex", "crypto", "merger"]
FINNHUB_FETCH_INTERVAL = 1   # minutes

MARKETAUX_API_KEYS = [os.getenv(f"MARKETAUX_API_KEY_{i}") for i in range(1, 13)]
MARKETAUX_FETCH_INTERVAL = 1.5    # minutes


TWITTER_API_KEYS = [os.getenv(f"TWITTER_X_API_KEY{i}") for i in range(1, 16)]

# twtiter limits to 100 post pulls a month per account - working with what i got
TWITTER_FETCH_INTERVAL = 90    # minutes
TWITTER_START_HOUR = 9
TWITTER_END_HOUR = 15 # 3pm
TWITTER_ACCOUNTS = ["FirstSquawk", "zerohedge"]
# minimum 5 posts per acc for some reason
POSTS_PER_ACC = 5


HEARTBEAT_COOLDOWN = timedelta(minutes=10)


def make_identifier(article, prefix=""):
    """Generate a unique identifier for each article."""
    headline = article.get("headline") or article.get("title") or ""
    timestamp = str(article.get("datetime") or article.get("published_at") or "")
    url = article.get("url", "")
    raw = f"{prefix}{headline}-{timestamp}-{url}"
    return hashlib.sha256(raw.encode()).hexdigest()
    

class NewsCog(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.fetch_finnhub.start()
        self.fetch_marketaux.start()
        self.fetch_twitter.start()


    def cog_unload(self):
        self.fetch_finnhub.cancel()
        self.fetch_marketaux.cancel()
        self.fetch_twitter.cancel()

    async def send_articles(self, guild, articles, source):
        """Send new articles or heartbeat if none."""
        channel = discord.utils.get(guild.text_channels, name="news")
        if channel is None:
            return

        sent_any = False

        for article in reversed(articles):
            identifier = make_identifier(article, prefix=f"{source}-")
            
            # using database to see if article has been posted already
            if not await db_manager.is_article_seen(guild.id, identifier):
                embed = self.build_embed(article, source)
                await channel.send(embed=embed)
                
                # Mark article as seen in database
                await db_manager.mark_article_seen(guild.id, identifier, source)
                sent_any = True

        # create heartbeat if no new news
        if not sent_any:
            now = datetime.now()
            last_sent = await db_manager.get_last_heartbeat(guild.id)

            if not last_sent or now - last_sent >= HEARTBEAT_COOLDOWN:
                await channel.send(
                    "🔄 No new news at this time.\n"
                    "⚠️ ATTENTION ⚠️ StockBot can miss news. News may be delayed from 1 minute up to 24 hours. "
                    "Do your own research for faster or equity-specific news.\n"
                )
                # updating heatbeat in db
                await db_manager.update_heartbeat(guild.id, now)


    @tasks.loop(minutes=TWITTER_FETCH_INTERVAL)
    async def fetch_twitter(self):
        """Fetch from Twitter ONLY from 9am to 3pm bc of limit"""

        # only run from 9am to 3pm due to api request limits
        if not (TWITTER_START_HOUR <= datetime.now().hour <= TWITTER_END_HOUR):
            return
        
        all_tweets = []
        accounts_fetched = set()

        for i, key in enumerate(TWITTER_API_KEYS):
            # skip if we got all accounts already
            if len(accounts_fetched) == len(TWITTER_ACCOUNTS):
                print("All accounts fetched successfully")
                break
                
            try:
                client = tweepy.Client(bearer_token=key)
                
                for username in TWITTER_ACCOUNTS:
                    if username in accounts_fetched:
                        continue
                        
                    try:
                        user = client.get_user(username=username)
                        if not user.data:
                            print(f"Couldn't find @{username}")
                            continue
                        
                        tweets = client.get_users_tweets(
                            id=user.data.id,
                            max_results=POSTS_PER_ACC,
                            tweet_fields=['created_at', 'text', 'id'],
                            exclude=['retweets', 'replies']
                        )

                        if tweets.data:
                            for tweet in tweets.data:
                                all_tweets.append({
                                    'username': username,
                                    'created_at': tweet.created_at,
                                    'text': tweet.text,
                                    'id': tweet.id,
                                    'url': f"https://twitter.com/{username}/status/{tweet.id}"
                                })
                            accounts_fetched.add(username)
                            print(f"*** Fetched {len(tweets.data)} tweets from @{username} with API key {i+1}")
                            
                    except TooManyRequests:
                        print(f"--Rate limited on @{username} with key {i+1}, trying next key")
                        continue
                        
            except TooManyRequests:
                print(f"---API key {i+1} rate limited, trying next key")
                continue
            except Unauthorized:
                print(f"---API key {i+1} unauthorized")
                continue
        
        if not all_tweets:
            print("---No tweets fetched - all keys rate limited or failed")
            return
        
        # Post tweets to Discord
        for guild in self.bot.guilds:
            channel = discord.utils.get(guild.text_channels, name="news")
            if not channel:
                continue
                
            for tweet in reversed(all_tweets):
                # identifier is different from articles
                identifier = f"twitter-{tweet['id']}"
                # posting recent articles - making sure no duplicates
                if not db_manager.is_article_seen(guild.id, identifier):
                    embed = self.build_embed(tweet, "twitter")
                    await channel.send(embed=embed)
                    # making sure tweet is seen in db after posting
                    db_manager.mark_article_seen(guild.id, identifier, "twitter")


    @tasks.loop(minutes=FINNHUB_FETCH_INTERVAL)
    async def fetch_finnhub(self):
        """Fetch from Finnhub (last 2 days) cycling through all API keys."""
        async def fetch_with_key(api_key, key_index):
            cutoff = int((datetime.utcnow() - timedelta(days=2)).timestamp())
            all_news_items = []
            
            async with aiohttp.ClientSession() as session:
                for category in FINNHUB_CATEGORIES:
                    url = FINNHUB_URL.format(category=category, token=api_key)
                    async with session.get(url) as resp:
                        if resp.status == 200:
                            news_items = await resp.json()
                            # only include from last 2 days
                            filtered = [n for n in news_items if n.get("datetime", 0) >= cutoff]
                            all_news_items.extend(filtered)
                        elif resp.status == 429:  # Rate limit exceeded
                            text = await resp.text()
                            print(f"--Finnhub API key {key_index + 1} rate limited for {category}: {text}")
                            return None
                        else:
                            text = await resp.text()
                            print(f"--Finnhub error {resp.status} for {category} with key {key_index + 1}: {text}")
                            return None
            
            return all_news_items

        try:
            all_news_items = None
            successful_key = None
            
            # Try each API key in order until one works
            for i, api_key in enumerate(FINNHUB_API_KEYS):
                print(f"Trying Finnhub API key {i + 1}...")
                all_news_items = await fetch_with_key(api_key, i)
                
                if all_news_items is not None:
                    successful_key = i + 1
                    print(f"*** Successfully fetched news with Finnhub API key {successful_key}")
                    break
                else:
                    print(f"--Finnhub API key {i + 1} failed, trying next key...")

            if all_news_items is None:
                print("---All Finnhub API keys failed or reached limits.")
                return

        except Exception as e:
            print(f"---Error fetching Finnhub: {e}")
            return

        for guild in self.bot.guilds:
            await self.send_articles(guild, all_news_items, "finnhub")

    @tasks.loop(minutes=MARKETAUX_FETCH_INTERVAL)
    async def fetch_marketaux(self):
        """Fetch from Marketaux (last 2 days) and filter locally."""
        async def fetch_with_key(api_key, key_index):
            url = f"https://api.marketaux.com/v1/news/all?api_token={api_key}&language=en&limit=50"
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as resp:
                    if resp.status == 429:  # Rate limit exceeded
                        print(f"--Marketaux API key {key_index + 1} rate limited")
                        return None
                    elif resp.status != 200:
                        print(f"--Marketaux usage limit with key {key_index + 1}")
                        return None
                    data = await resp.json()

            # Filter locally for last 2 days
            cutoff = datetime.now(timezone.utc) - timedelta(days=2)
            filtered = [
                article for article in data.get("data", [])
                if article.get("language") == "en" and
                datetime.fromisoformat(article["published_at"].replace("Z", "+00:00")) >= cutoff
            ]

            return {"data": filtered}

        try:
            data = None
            successful_key = None
            
            # Try each API key in order until one works
            for i, api_key in enumerate(MARKETAUX_API_KEYS):
                print(f"Trying Marketaux API key {i + 1}...")
                data = await fetch_with_key(api_key, i)
                
                if data is not None:
                    successful_key = i + 1
                    print(f"***Successfully fetched news with Marketaux API key {successful_key}")
                    break
                else:
                    print(f"--Marketaux API key {i + 1} failed, trying next key...")

            if data is None:
                print("---All Marketaux API keys failed or reached limits.")
                return

            news_items = data.get("data", [])

        except Exception as e:
            print(f"---Error fetching Marketaux: {e}")
            return

        for guild in self.bot.guilds:
            await self.send_articles(guild, news_items, "marketaux")

    # prioritizing getting news as soon as bot is run
    @fetch_twitter.before_loop
    @fetch_finnhub.before_loop
    @fetch_marketaux.before_loop
    async def before_loop(self):
        await self.bot.wait_until_ready()


    def build_embed(self, article, source):
        """Build a Discord embed for an article."""
        if source == "finnhub":
            headline = article.get("headline", "No title")[:256]
            summary = article.get("summary", "")
            timestamp = article.get("datetime")
            color = discord.Color.yellow()
        
        # twitter data structure is diff so cant use same embed
        elif source == "twitter":
            headline = f"🐦 @{article['username']}"
            summary = article.get("text", "")
            timestamp = article.get("created_at")
            url = article.get("url", "")
            embed = discord.Embed(
                title = headline,
                url = url,
                description = summary[:300] + ("..." if len(summary) > 300 else ""),
                color = discord.Color.blue(),
                timestamp = timestamp
            )
            embed.set_footer(text=f"Twitter: @{article['username']}")
            return embed


        else:
            headline = article.get("title", "No title")[:256]
            summary = article.get("description", "")
            timestamp = article.get("published_at")
            color = discord.Color.green()
            
        url = article.get("url", "")
        embed = discord.Embed(
            title=headline,
            url=url,
            description=(summary[:300] + "...") if summary else "No description",
            color=color
        )

        if timestamp:
            try:
                if source == "finnhub":
                    embed.timestamp = datetime.fromtimestamp(timestamp)
                else:
                    embed.timestamp = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
            except Exception:
                pass

        embed.set_footer(text=article.get("source"))
        return embed


async def setup(bot):
    await bot.add_cog(NewsCog(bot))