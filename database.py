"""
Dual-database backend: SQLite (local) + MongoDB Atlas (cloud/GCP)
"""

from datetime import datetime, timedelta
from sqlalchemy import create_engine, Column, Integer, String, DateTime, Float, Index
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import os
from dotenv import load_dotenv

load_dotenv()


Base = declarative_base()

class HelpMessage(Base):
    __tablename__ = 'help_messages'
    id = Column(Integer, primary_key=True)
    guild_id = Column(Integer, nullable=False, unique=True)
    message_id = Column(Integer, nullable=False)
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

class SeenArticle(Base):
    __tablename__ = 'seen_articles'
    id = Column(Integer, primary_key=True)
    guild_id = Column(Integer, nullable=False)
    article_identifier = Column(String(64), nullable=False)
    source = Column(String(20), nullable=False)
    created_at = Column(DateTime, default=datetime.now)
    __table_args__ = (
        Index('idx_guild_article', 'guild_id', 'article_identifier'),
        Index('idx_guild_created', 'guild_id', 'created_at'),
    )

class GuildHeartbeat(Base):
    __tablename__ = 'guild_heartbeats'
    id = Column(Integer, primary_key=True)
    guild_id = Column(Integer, nullable=False, unique=True)
    last_heartbeat = Column(DateTime, nullable=False)
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

class WatchlistItem(Base):
    __tablename__ = 'watchlist_items'
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, nullable=False)
    guild_id = Column(Integer, nullable=False)
    symbol = Column(String(20), nullable=False)
    company_name = Column(String(200))
    created_at = Column(DateTime, default=datetime.now)
    __table_args__ = (
        Index('idx_user_guild', 'user_id', 'guild_id'),
        Index('idx_user_guild_symbol', 'user_id', 'guild_id', 'symbol', unique=True),
    )

class PortfolioPosition(Base):
    __tablename__ = 'portfolio_positions'
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, nullable=False)
    guild_id = Column(Integer, nullable=False)
    symbol = Column(String(20), nullable=False)
    shares = Column(Float, nullable=False)
    total_cost = Column(Float, nullable=False)
    average_price = Column(Float, nullable=False)
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)
    __table_args__ = (
        Index('idx_portfolio_user_guild', 'user_id', 'guild_id'),
        Index('idx_portfolio_user_guild_symbol', 'user_id', 'guild_id', 'symbol', unique=True),
    )

class UserStats(Base):
    __tablename__ = 'user_stats'
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, nullable=False)
    guild_id = Column(Integer, nullable=False)
    total_realized_pnl = Column(Float, default=0.0)
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)
    __table_args__ = (
        Index('idx_user_stats', 'user_id', 'guild_id', unique=True),
    )

class Event(Base):
    __tablename__ = 'events'
    id = Column(Integer, primary_key=True)
    identifier = Column(String(64), nullable=False, unique=True)
    symbol = Column(String(5), nullable=False)
    headline = Column(String(500), nullable=False)
    summary = Column(String(2000))
    source = Column(String(50))
    url = Column(String(500))
    event_timestamp = Column(DateTime, nullable=False)
    created_at = Column(DateTime, default=datetime.now)
    embedding_model = Column(String(100))
    novelty_score = Column(Float)
    novelty_percentile = Column(Float)
    embedding_json = Column(String)
    __table_args__ = (
        Index('idx_event_identifier', 'identifier', unique=True),
        Index('idx_event_symbol', 'symbol'),
        Index('idx_event_symbol_timestamp', 'symbol', 'event_timestamp'),
        Index('idx_event_novelty_score', 'novelty_score'),
    )

class SQLiteManager:
    """
    SQLite backend using SQLAlchemy
    Methods are async so cogs can use the same await pattern
    """

    def __init__(self, database_url='sqlite:///bot_data.db'):
        self.engine = create_engine(database_url, echo=False)
        self.SessionLocal = sessionmaker(bind=self.engine)
        Base.metadata.create_all(bind=self.engine)
        self._cleanup_old_articles()

    def _get_session(self):
        return self.SessionLocal()

    async def initialize(self):
        print("[SQLite] Backend ready — using bot_data.db")

    def _cleanup_old_articles(self):
        with self._get_session() as session:
            try:
                guilds = session.query(SeenArticle.guild_id).distinct().all()
                for (guild_id,) in guilds:
                    articles = session.query(SeenArticle)\
                        .filter(SeenArticle.guild_id == guild_id)\
                        .order_by(SeenArticle.created_at.desc()).all()
                    if len(articles) > 500:
                        for article in articles[500:]:
                            session.delete(article)
                session.commit()
            except Exception as e:
                print(f"[SQLite] Cleanup error: {e}")
                session.rollback()

    def _cleanup_guild_articles(self, session, guild_id):
        try:
            articles = session.query(SeenArticle)\
                .filter(SeenArticle.guild_id == guild_id)\
                .order_by(SeenArticle.created_at.desc()).all()
            if len(articles) > 500:
                for article in articles[500:]:
                    session.delete(article)
                session.commit()
        except Exception as e:
            print(f"[SQLite] Guild cleanup error: {e}")
            session.rollback()

    async def get_help_message_id(self, guild_id):
        with self._get_session() as session:
            msg = session.query(HelpMessage).filter(HelpMessage.guild_id == guild_id).first()
            return msg.message_id if msg else None

    async def save_help_message_id(self, guild_id, message_id):
        with self._get_session() as session:
            try:
                msg = session.query(HelpMessage).filter(HelpMessage.guild_id == guild_id).first()
                if msg:
                    msg.message_id = message_id
                    msg.updated_at = datetime.now()
                else:
                    msg = HelpMessage(guild_id=guild_id, message_id=message_id)
                    session.add(msg)
                session.commit()
            except Exception as e:
                print(f"[SQLite] Error saving help message: {e}")
                session.rollback()

    async def is_article_seen(self, guild_id, article_identifier):
        with self._get_session() as session:
            return session.query(SeenArticle)\
                .filter_by(guild_id=guild_id, article_identifier=article_identifier)\
                .first() is not None

    async def mark_article_seen(self, guild_id, article_identifier, source):
        with self._get_session() as session:
            try:
                exists = session.query(SeenArticle)\
                    .filter_by(guild_id=guild_id, article_identifier=article_identifier)\
                    .first() is not None
                if not exists:
                    article = SeenArticle(guild_id=guild_id, article_identifier=article_identifier, source=source)
                    session.add(article)
                    session.commit()
                    self._cleanup_guild_articles(session, guild_id)
            except Exception as e:
                print(f"[SQLite] Error marking article: {e}")
                session.rollback()

    async def get_last_heartbeat(self, guild_id):
        with self._get_session() as session:
            hb = session.query(GuildHeartbeat).filter_by(guild_id=guild_id).first()
            return hb.last_heartbeat if hb else None

    async def update_heartbeat(self, guild_id, timestamp):
        with self._get_session() as session:
            try:
                hb = session.query(GuildHeartbeat).filter_by(guild_id=guild_id).first()
                if hb:
                    hb.last_heartbeat = timestamp
                    hb.updated_at = datetime.now()
                else:
                    hb = GuildHeartbeat(guild_id=guild_id, last_heartbeat=timestamp)
                    session.add(hb)
                session.commit()
            except Exception as e:
                print(f"[SQLite] Error updating heartbeat: {e}")
                session.rollback()

    async def add_to_watchlist(self, user_id, guild_id, symbol, company_name=None):
        with self._get_session() as session:
            try:
                existing = session.query(WatchlistItem)\
                    .filter_by(user_id=user_id, guild_id=guild_id, symbol=symbol.upper()).first()
                if existing:
                    return False
                item = WatchlistItem(user_id=user_id, guild_id=guild_id, symbol=symbol.upper(), company_name=company_name)
                session.add(item)
                session.commit()
                return True
            except Exception as e:
                print(f"[SQLite] Error adding to watchlist: {e}")
                session.rollback()
                return False

    async def remove_from_watchlist(self, user_id, guild_id, symbol):
        with self._get_session() as session:
            try:
                item = session.query(WatchlistItem)\
                    .filter_by(user_id=user_id, guild_id=guild_id, symbol=symbol.upper()).first()
                if item:
                    session.delete(item)
                    session.commit()
                    return True
                return False
            except Exception as e:
                print(f"[SQLite] Error removing from watchlist: {e}")
                session.rollback()
                return False

    async def get_user_watchlist(self, user_id, guild_id):
        with self._get_session() as session:
            try:
                items = session.query(WatchlistItem)\
                    .filter_by(user_id=user_id, guild_id=guild_id)\
                    .order_by(WatchlistItem.symbol).all()
                return [{'symbol': i.symbol, 'company_name': i.company_name, 'created_at': i.created_at} for i in items]
            except Exception as e:
                print(f"[SQLite] Error getting watchlist: {e}")
                return []

    async def get_watchlist_count(self, user_id, guild_id):
        with self._get_session() as session:
            return session.query(WatchlistItem).filter_by(user_id=user_id, guild_id=guild_id).count()

    async def add_portfolio_position(self, user_id, guild_id, symbol, quantity, price):
        with self._get_session() as session:
            try:
                pos = session.query(PortfolioPosition)\
                    .filter_by(user_id=user_id, guild_id=guild_id, symbol=symbol.upper()).first()
                if pos:
                    new_shares = pos.shares + quantity
                    new_total = pos.total_cost + (quantity * price)
                    pos.shares = new_shares
                    pos.total_cost = new_total
                    pos.average_price = new_total / new_shares
                    pos.updated_at = datetime.now()
                else:
                    pos = PortfolioPosition(
                        user_id=user_id, guild_id=guild_id, symbol=symbol.upper(),
                        shares=quantity, total_cost=quantity * price, average_price=price
                    )
                    session.add(pos)
                session.commit()
                return True
            except Exception as e:
                print(f"[SQLite] Error adding position: {e}")
                session.rollback()
                return False

    async def sell_portfolio_position(self, user_id, guild_id, symbol, quantity, price):
        with self._get_session() as session:
            try:
                pos = session.query(PortfolioPosition)\
                    .filter_by(user_id=user_id, guild_id=guild_id, symbol=symbol.upper()).first()

                if not pos:
                    return (False, f"❌ You don't own any **{symbol.upper()}** shares.")
                if pos.shares < quantity:
                    return (False, f"❌ You only have **{pos.shares}** shares.")

                profit_per_share = price - pos.average_price
                total_profit = profit_per_share * quantity
                sale_value = quantity * price

                await self.add_realized_pnl(user_id, guild_id, total_profit)

                new_shares = pos.shares - quantity
                if new_shares == 0:
                    session.delete(pos)
                    msg = (f"✅ Sold all **{quantity} shares** of **{symbol.upper()}** at **${price:.2f}**\n"
                           f"Sale: ${sale_value:.2f} | P&L: ${total_profit:,.2f} ({profit_per_share/pos.average_price*100:+.2f}%)")
                else:
                    pos.shares = new_shares
                    pos.total_cost -= quantity * pos.average_price
                    pos.updated_at = datetime.now()
                    msg = (f"✅ Sold **{quantity} shares** of **{symbol.upper()}** at **${price:.2f}**\n"
                           f"Sale: ${sale_value:.2f} | P&L: ${total_profit:,.2f} ({profit_per_share/pos.average_price*100:+.2f}%)\n"
                           f"Remaining: **{new_shares} shares**")

                session.commit()
                return (True, msg)
            except Exception as e:
                print(f"[SQLite] Error selling position: {e}")
                session.rollback()
                return (False, f"❌ Error: {str(e)}")

    async def get_user_portfolio(self, user_id, guild_id):
        with self._get_session() as session:
            try:
                positions = session.query(PortfolioPosition)\
                    .filter_by(user_id=user_id, guild_id=guild_id)\
                    .order_by(PortfolioPosition.symbol).all()
                return [{'symbol': p.symbol, 'shares': p.shares, 'average_price': p.average_price,
                         'total_cost': p.total_cost, 'created_at': p.created_at, 'updated_at': p.updated_at}
                        for p in positions]
            except Exception as e:
                print(f"[SQLite] Error getting portfolio: {e}")
                return []

    async def get_portfolio_count(self, user_id, guild_id):
        with self._get_session() as session:
            return session.query(PortfolioPosition).filter_by(user_id=user_id, guild_id=guild_id).count()

    async def remove_portfolio_position(self, user_id, guild_id, symbol):
        with self._get_session() as session:
            try:
                pos = session.query(PortfolioPosition)\
                    .filter_by(user_id=user_id, guild_id=guild_id, symbol=symbol.upper()).first()
                if pos:
                    session.delete(pos)
                    session.commit()
                    return True
                return False
            except Exception as e:
                print(f"[SQLite] Error removing position: {e}")
                session.rollback()
                return False

    async def add_realized_pnl(self, user_id, guild_id, amount):
        with self._get_session() as session:
            try:
                stats = session.query(UserStats).filter_by(user_id=user_id, guild_id=guild_id).first()
                if stats:
                    stats.total_realized_pnl += amount
                    stats.updated_at = datetime.now()
                else:
                    stats = UserStats(user_id=user_id, guild_id=guild_id, total_realized_pnl=amount)
                    session.add(stats)
                session.commit()
            except Exception as e:
                print(f"[SQLite] Error adding P&L: {e}")
                session.rollback()

    async def get_realized_pnl(self, user_id, guild_id):
        with self._get_session() as session:
            try:
                stats = session.query(UserStats).filter_by(user_id=user_id, guild_id=guild_id).first()
                return stats.total_realized_pnl if stats else 0.0
            except Exception as e:
                print(f"[SQLite] Error getting P&L: {e}")
                return 0.0

    async def reset_realized_pnl(self, user_id, guild_id):
        with self._get_session() as session:
            try:
                stats = session.query(UserStats).filter_by(user_id=user_id, guild_id=guild_id).first()
                if stats:
                    stats.total_realized_pnl = 0.0
                    stats.updated_at = datetime.now()
                    session.commit()
            except Exception as e:
                print(f"[SQLite] Error resetting P&L: {e}")
                session.rollback()

    async def is_event_seen(self, identifier):
        with self._get_session() as session:
            return session.query(Event)\
                .filter_by(identifier=identifier)\
                .first() is not None

    async def store_event(self, event_data):
        with self._get_session() as session:
            try:
                import json
                exists = session.query(Event)\
                    .filter_by(identifier=event_data['identifier'])\
                    .first() is not None
                if exists:
                    return False

                embedding_json = None
                if event_data.get('embedding') is not None:
                    embedding_json = json.dumps(event_data['embedding'])

                event = Event(
                    identifier=event_data['identifier'],
                    symbol=event_data['symbol'],
                    headline=event_data['headline'],
                    summary=event_data.get('summary', ''),
                    source=event_data.get('source', ''),
                    url=event_data.get('url', ''),
                    event_timestamp=event_data['event_timestamp'],
                    created_at=event_data.get('created_at', datetime.now()),
                    embedding_json=embedding_json,
                    embedding_model=event_data.get('embedding_model'),
                    novelty_score=event_data.get('novelty_score'),
                    novelty_percentile=event_data.get('novelty_percentile'),
                )
                session.add(event)
                session.commit()
                return True
            except Exception as e:
                print(f"[SQLite] Error storing event: {e}")
                session.rollback()
                return False

    async def get_events_by_symbol(self, symbol, days_back=30):
        with self._get_session() as session:
            try:
                import json
                cutoff = datetime.now() - timedelta(days=days_back)
                events = session.query(Event)\
                    .filter(Event.symbol == symbol.upper(),
                            Event.event_timestamp >= cutoff)\
                    .order_by(Event.event_timestamp.desc())\
                    .all()
                results = []
                for e in events:
                    embedding = None
                    if e.embedding_json:
                        embedding = json.loads(e.embedding_json)
                    results.append({
                        'identifier': e.identifier,
                        'symbol': e.symbol,
                        'headline': e.headline,
                        'event_timestamp': e.event_timestamp,
                        'embedding': embedding,
                        'novelty_score': e.novelty_score,
                        'novelty_percentile': e.novelty_percentile,
                    })
                return results
            except Exception as e:
                print(f"[SQLite] Error getting events: {e}")
                return []

    async def get_unprocessed_events(self, limit=100):
        with self._get_session() as session:
            try:
                events = session.query(Event)\
                    .filter(Event.embedding_json == None)\
                    .order_by(Event.created_at.asc())\
                    .limit(limit)\
                    .all()
                return [{
                    'identifier': e.identifier,
                    'symbol': e.symbol,
                    'headline': e.headline,
                    'event_timestamp': e.event_timestamp,
                } for e in events]
            except Exception as e:
                print(f"[SQLite] Error getting unprocessed events: {e}")
                return []

    async def update_event_novelty(self, identifier, embedding, model_name, score, percentile):
        with self._get_session() as session:
            try:
                import json
                event = session.query(Event).filter_by(identifier=identifier).first()
                if event:
                    event.embedding_json = json.dumps(embedding)
                    event.embedding_model = model_name
                    event.novelty_score = score
                    event.novelty_percentile = percentile
                    session.commit()
                    return True
                return False
            except Exception as e:
                print(f"[SQLite] Error updating event novelty: {e}")
                session.rollback()
                return False


class MongoManager:
    """
    MongoDB Atlas backend using motor (async driver).
    Collections mirror SQLite tables. 
    """

    def __init__(self, mongo_uri):
        from motor.motor_asyncio import AsyncIOMotorClient
        self.client = AsyncIOMotorClient(mongo_uri)
        self.db = self.client['stockbot']

        # collection references
        self.help_messages = self.db['help_messages']
        self.seen_articles = self.db['seen_articles']
        self.guild_heartbeats = self.db['guild_heartbeats']
        self.watchlist_items = self.db['watchlist_items']
        self.portfolio_positions = self.db['portfolio_positions']
        self.user_stats = self.db['user_stats']
        self.events = self.db['events']

    async def initialize(self):
        """Verifying connection and create indexes."""
        try:
            await self.client.admin.command('ping')
            print("[MongoDB] Connected to Atlas cluster on GCP")

            # TTL index — auto-delete seen articles after 2 days
            await self.seen_articles.create_index("created_at", expireAfterSeconds=172800)
            await self.seen_articles.create_index([("guild_id", 1), ("article_identifier", 1)], unique=True)

            await self.help_messages.create_index("guild_id", unique=True)
            await self.guild_heartbeats.create_index("guild_id", unique=True)

            await self.watchlist_items.create_index([("user_id", 1), ("guild_id", 1)])
            await self.watchlist_items.create_index([("user_id", 1), ("guild_id", 1), ("symbol", 1)], unique=True)

            await self.portfolio_positions.create_index([("user_id", 1), ("guild_id", 1)])
            await self.portfolio_positions.create_index([("user_id", 1), ("guild_id", 1), ("symbol", 1)], unique=True)

            await self.user_stats.create_index([("user_id", 1), ("guild_id", 1)], unique=True)

            await self.events.create_index("identifier", unique=True)
            await self.events.create_index("symbol")
            await self.events.create_index([("symbol", 1), ("event_timestamp", -1)])
            await self.events.create_index("novelty_score")

            print("[MongoDB] All indexes created")

        except Exception as e:
            print(f"[MongoDB] Connection failed: {e}")
            raise


    async def get_help_message_id(self, guild_id):
        doc = await self.help_messages.find_one({"guild_id": guild_id})
        return doc["message_id"] if doc else None

    async def save_help_message_id(self, guild_id, message_id):
        try:
            await self.help_messages.update_one(
                {"guild_id": guild_id},
                {
                    "$set": {"message_id": message_id, "updated_at": datetime.now()},
                    "$setOnInsert": {"created_at": datetime.now()}
                },
                upsert=True
            )
        except Exception as e:
            print(f"[MongoDB] Error saving help message: {e}")

    async def is_article_seen(self, guild_id, article_identifier):
        doc = await self.seen_articles.find_one(
            {"guild_id": guild_id, "article_identifier": article_identifier}
        )
        return doc is not None

    async def mark_article_seen(self, guild_id, article_identifier, source):
        try:
            await self.seen_articles.update_one(
                {"guild_id": guild_id, "article_identifier": article_identifier},
                {"$setOnInsert": {
                    "guild_id": guild_id,
                    "article_identifier": article_identifier,
                    "source": source,
                    "created_at": datetime.now()
                }},
                upsert=True
            )
        except Exception as e:
            print(f"[MongoDB] Error marking article: {e}")

    async def get_last_heartbeat(self, guild_id):
        doc = await self.guild_heartbeats.find_one({"guild_id": guild_id})
        return doc["last_heartbeat"] if doc else None

    async def update_heartbeat(self, guild_id, timestamp):
        try:
            await self.guild_heartbeats.update_one(
                {"guild_id": guild_id},
                {
                    "$set": {"last_heartbeat": timestamp, "updated_at": datetime.now()},
                    "$setOnInsert": {"created_at": datetime.now()}
                },
                upsert=True
            )
        except Exception as e:
            print(f"[MongoDB] Error updating heartbeat: {e}")


    async def add_to_watchlist(self, user_id, guild_id, symbol, company_name=None):
        try:
            result = await self.watchlist_items.update_one(
                {"user_id": user_id, "guild_id": guild_id, "symbol": symbol.upper()},
                {"$setOnInsert": {
                    "user_id": user_id, "guild_id": guild_id,
                    "symbol": symbol.upper(), "company_name": company_name,
                    "created_at": datetime.now()
                }},
                upsert=True
            )
            return result.upserted_id is not None
        except Exception as e:
            print(f"[MongoDB] Error adding to watchlist: {e}")
            return False

    async def remove_from_watchlist(self, user_id, guild_id, symbol):
        try:
            result = await self.watchlist_items.delete_one(
                {"user_id": user_id, "guild_id": guild_id, "symbol": symbol.upper()}
            )
            return result.deleted_count > 0
        except Exception as e:
            print(f"[MongoDB] Error removing from watchlist: {e}")
            return False

    async def get_user_watchlist(self, user_id, guild_id):
        try:
            cursor = self.watchlist_items.find(
                {"user_id": user_id, "guild_id": guild_id}
            ).sort("symbol", 1)
            items = await cursor.to_list(length=100)
            return [{'symbol': i['symbol'], 'company_name': i.get('company_name'),
                     'created_at': i['created_at']} for i in items]
        except Exception as e:
            print(f"[MongoDB] Error getting watchlist: {e}")
            return []

    async def get_watchlist_count(self, user_id, guild_id):
        try:
            return await self.watchlist_items.count_documents(
                {"user_id": user_id, "guild_id": guild_id}
            )
        except Exception as e:
            print(f"[MongoDB] Error counting watchlist: {e}")
            return 0


    async def add_portfolio_position(self, user_id, guild_id, symbol, quantity, price):
        try:
            existing = await self.portfolio_positions.find_one(
                {"user_id": user_id, "guild_id": guild_id, "symbol": symbol.upper()}
            )

            if existing:
                new_shares = existing['shares'] + quantity
                new_total = existing['total_cost'] + (quantity * price)
                await self.portfolio_positions.update_one(
                    {"_id": existing['_id']},
                    {"$set": {
                        "shares": new_shares,
                        "total_cost": new_total,
                        "average_price": new_total / new_shares,
                        "updated_at": datetime.now()
                    }}
                )
            else:
                await self.portfolio_positions.insert_one({
                    "user_id": user_id, "guild_id": guild_id, "symbol": symbol.upper(),
                    "shares": quantity, "total_cost": quantity * price,
                    "average_price": price, "created_at": datetime.now(), "updated_at": datetime.now()
                })
            return True
        except Exception as e:
            print(f"[MongoDB] Error adding position: {e}")
            return False

    async def sell_portfolio_position(self, user_id, guild_id, symbol, quantity, price):
        try:
            pos = await self.portfolio_positions.find_one(
                {"user_id": user_id, "guild_id": guild_id, "symbol": symbol.upper()}
            )

            if not pos:
                return (False, f"❌ You don't own any **{symbol.upper()}** shares.")
            if pos['shares'] < quantity:
                return (False, f"❌ You only have **{pos['shares']}** shares.")

            profit_per_share = price - pos['average_price']
            total_profit = profit_per_share * quantity
            sale_value = quantity * price

            await self.add_realized_pnl(user_id, guild_id, total_profit)

            new_shares = pos['shares'] - quantity
            if new_shares == 0:
                await self.portfolio_positions.delete_one({"_id": pos['_id']})
                msg = (f"✅ Sold all **{quantity} shares** of **{symbol.upper()}** at **${price:.2f}**\n"
                       f"Sale: ${sale_value:.2f} | P&L: ${total_profit:,.2f} ({profit_per_share/pos['average_price']*100:+.2f}%)")
            else:
                await self.portfolio_positions.update_one(
                    {"_id": pos['_id']},
                    {"$set": {
                        "shares": new_shares,
                        "total_cost": pos['total_cost'] - (quantity * pos['average_price']),
                        "updated_at": datetime.now()
                    }}
                )
                msg = (f"✅ Sold **{quantity} shares** of **{symbol.upper()}** at **${price:.2f}**\n"
                       f"Sale: ${sale_value:.2f} | P&L: ${total_profit:,.2f} ({profit_per_share/pos['average_price']*100:+.2f}%)\n"
                       f"Remaining: **{new_shares} shares**")

            return (True, msg)
        except Exception as e:
            print(f"[MongoDB] Error selling position: {e}")
            return (False, f"❌ Error: {str(e)}")

    async def get_user_portfolio(self, user_id, guild_id):
        try:
            cursor = self.portfolio_positions.find(
                {"user_id": user_id, "guild_id": guild_id}
            ).sort("symbol", 1)
            positions = await cursor.to_list(length=100)
            return [{'symbol': p['symbol'], 'shares': p['shares'], 'average_price': p['average_price'],
                     'total_cost': p['total_cost'], 'created_at': p['created_at'],
                     'updated_at': p.get('updated_at')} for p in positions]
        except Exception as e:
            print(f"[MongoDB] Error getting portfolio: {e}")
            return []

    async def get_portfolio_count(self, user_id, guild_id):
        try:
            return await self.portfolio_positions.count_documents(
                {"user_id": user_id, "guild_id": guild_id}
            )
        except Exception as e:
            print(f"[MongoDB] Error counting portfolio: {e}")
            return 0

    async def remove_portfolio_position(self, user_id, guild_id, symbol):
        try:
            result = await self.portfolio_positions.delete_one(
                {"user_id": user_id, "guild_id": guild_id, "symbol": symbol.upper()}
            )
            return result.deleted_count > 0
        except Exception as e:
            print(f"[MongoDB] Error removing position: {e}")
            return False

    async def add_realized_pnl(self, user_id, guild_id, amount):
        try:
            await self.user_stats.update_one(
                {"user_id": user_id, "guild_id": guild_id},
                {
                    "$inc": {"total_realized_pnl": amount},
                    "$set": {"updated_at": datetime.now()},
                    "$setOnInsert": {"created_at": datetime.now()}
                },
                upsert=True
            )
        except Exception as e:
            print(f"[MongoDB] Error adding P&L: {e}")

    async def get_realized_pnl(self, user_id, guild_id):
        try:
            doc = await self.user_stats.find_one({"user_id": user_id, "guild_id": guild_id})
            return doc['total_realized_pnl'] if doc else 0.0
        except Exception as e:
            print(f"[MongoDB] Error getting P&L: {e}")
            return 0.0

    async def reset_realized_pnl(self, user_id, guild_id):
        try:
            await self.user_stats.update_one(
                {"user_id": user_id, "guild_id": guild_id},
                {"$set": {"total_realized_pnl": 0.0, "updated_at": datetime.now()}}
            )
        except Exception as e:
            print(f"[MongoDB] Error resetting P&L: {e}")

    async def is_event_seen(self, identifier):
        doc = await self.events.find_one({"identifier": identifier})
        return doc is not None

    async def store_event(self, event_data):
        try:
            result = await self.events.update_one(
                {"identifier": event_data['identifier']},
                {"$setOnInsert": event_data},
                upsert=True
            )
            return result.upserted_id is not None
        except Exception as e:
            print(f"[MongoDB] Error storing event: {e}")
            return False

    async def get_events_by_symbol(self, symbol, days_back=30):
        try:
            from datetime import timezone
            cutoff = datetime.now(timezone.utc) - timedelta(days=days_back)
            cursor = self.events.find({
                "symbol": symbol.upper(),
                "event_timestamp": {"$gte": cutoff}
            }).sort("event_timestamp", -1)
            events = await cursor.to_list(length=5000)
            return [{
                'identifier': e['identifier'],
                'symbol': e['symbol'],
                'headline': e['headline'],
                'event_timestamp': e['event_timestamp'],
                'embedding': e.get('embedding'),
                'novelty_score': e.get('novelty_score'),
                'novelty_percentile': e.get('novelty_percentile'),
            } for e in events]
        except Exception as e:
            print(f"[MongoDB] Error getting events: {e}")
            return []

    async def get_unprocessed_events(self, limit=100):
        try:
            cursor = self.events.find(
                {"embedding": None}
            ).sort("created_at", 1).limit(limit)
            events = await cursor.to_list(length=limit)
            return [{
                'identifier': e['identifier'],
                'symbol': e['symbol'],
                'headline': e['headline'],
                'event_timestamp': e['event_timestamp'],
            } for e in events]
        except Exception as e:
            print(f"[MongoDB] Error getting unprocessed events: {e}")
            return []

    async def update_event_novelty(self, identifier, embedding, model_name, score, percentile):
        try:
            await self.events.update_one(
                {"identifier": identifier},
                {"$set": {
                    "embedding": embedding,
                    "embedding_model": model_name,
                    "novelty_score": score,
                    "novelty_percentile": percentile,
                }}
            )
            return True
        except Exception as e:
            print(f"[MongoDB] Error updating event novelty: {e}")
            return False

def create_db_manager():
    backend = os.getenv('DB_BACKEND', 'sqlite').lower()

    if backend == 'mongodb':
        mongo_uri = os.getenv('MONGO_DB_URI')
        if not mongo_uri:
            print("[DB] MONGO_URI not set — falling back to SQLite")
            return SQLiteManager()
        print(f"[DB] Using MongoDB backend")
        return MongoManager(mongo_uri)
    else:
        print(f"[DB] Using SQLite backend")
        return SQLiteManager()


# global instance 
db_manager = create_db_manager()