# creates connection to database - making sure we incorporate all datatypes used
from sqlalchemy import create_engine, Column, Integer, String, Text, DateTime, Boolean, Float, Index
# base class that models inherit from 
from sqlalchemy.ext.declarative import declarative_base
# incorporating database sessions 
from sqlalchemy.orm import sessionmaker
from datetime import datetime
import os

"""
Using SQLalchemy so we can still use python while designing database with flexibility of SQL
    - ORM - Object Relational mapper - technique that allows devs to interact with databases
"""

# base class - acts as registry for all the database models
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
    
    # composite indexes - so article lookups are faster
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
    

    # using index for instant results - speed up database queries - dont need to scan each row
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
    
    # again creating indexes to make lookups faster 
    # bot will lookup by user id and guild id rather than scanning entire table
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

# this class is where api interacts with database 
class DatabaseManager:
    # creating a sqlite database file 
    def __init__(self, database_url='sqlite:///bot_data.db'):
        # creating connection to database - basically translates python to SQL
        self.engine = create_engine(database_url, echo=False)
        # creating session - connecting to database
        self.SessionLocal = sessionmaker(bind=self.engine)
        # creating all tables if havent already - connecting to database
        Base.metadata.create_all(bind=self.engine) 

        # making sure old articles are cleaned on startup
        self.cleanup_old_articles()
        
    # creating new database session helper method 
    def _get_session(self):
        return self.SessionLocal()
    
    def _cleanup_old_articles(self):
        # closes session when done cleaning 
        with self.get_session() as session:
            try:
                guilds = session.query(SeenArticle.guild_id).distinct().all()
                for (guild_id,) in guilds:
                    # getting articles through each server - starting from the newest one
                    articles = session.query(SeenArticle)\
                        .filter(SeenArticle.guild_id == guild_id)\
                        .order_by(SeenArticle.created_at.desc())\
                        .all()
                    
                    # only keep most recent 500 articles
                    if len(articles) > 500:
                        for article in articles[500:]:
                            session.delete(article)
                
                # saving changes with this session
                session.commit()

            except Exception as e:
                print(f"Error during cleanup: {e}")
                # if error undo changes made in current session
                session.rollback()
    
    # Help Messages
    def get_help_message_id(self, guild_id):
        with self.get_session() as session:
            help_msg = session.query(HelpMessage).filter(HelpMessage.guild_id == guild_id).first()
            return help_msg.message_id if help_msg else None
    
    def save_help_message_id(self, guild_id, message_id):
        with self.get_session() as session:
            try:
                help_msg = session.query(HelpMessage).filter(HelpMessage.guild_id == guild_id).first()
                if help_msg:
                    help_msg.message_id = message_id
                    help_msg.updated_at = datetime.now()
                else:
                    help_msg = HelpMessage(guild_id=guild_id, message_id=message_id)
                    session.add(help_msg)
                session.commit()
            except Exception as e:
                print(f"Error saving help message: {e}")
                session.rollback()
    
    # Seen Articles
    def is_article_seen(self, guild_id, article_identifier):
        with self.get_session() as session:
            return session.query(SeenArticle)\
                .filter_by(guild_id=guild_id, article_identifier=article_identifier)\
                .first() is not None
    
    def mark_article_seen(self, guild_id, article_identifier, source):
        with self.get_session() as session:
            try:
                if not self.is_article_seen(guild_id, article_identifier):
                    article = SeenArticle(guild_id=guild_id, article_identifier=article_identifier, source=source)
                    session.add(article)
                    session.commit()
                    self.cleanup_guild_articles(session, guild_id)
            except Exception as e:
                print(f"Error marking article: {e}")
                session.rollback()
    
    def cleanup_guild_articles(self, session, guild_id):
        try:
            articles = session.query(SeenArticle)\
                .filter(SeenArticle.guild_id == guild_id)\
                .order_by(SeenArticle.created_at.desc())\
                .all()
            
            if len(articles) > 500:
                for article in articles[500:]:
                    session.delete(article)
                session.commit()
        except Exception as e:
            print(f"Error cleaning articles: {e}")
            session.rollback()
    
    # Heartbeat
    def get_last_heartbeat(self, guild_id):
        with self.get_session() as session:
            heartbeat = session.query(GuildHeartbeat).filter_by(guild_id=guild_id).first()
            return heartbeat.last_heartbeat if heartbeat else None
    
    def update_heartbeat(self, guild_id, timestamp):
        with self.get_session() as session:
            try:
                heartbeat = session.query(GuildHeartbeat).filter_by(guild_id=guild_id).first()
                if heartbeat:
                    heartbeat.last_heartbeat = timestamp
                    heartbeat.updated_at = datetime.now()
                else:
                    heartbeat = GuildHeartbeat(guild_id=guild_id, last_heartbeat=timestamp)
                    session.add(heartbeat)
                session.commit()
            except Exception as e:
                print(f"Error updating heartbeat: {e}")
                session.rollback()

    # Watchlist
    def add_to_watchlist(self, user_id, guild_id, symbol, company_name=None):
        with self.get_session() as session:
            try:
                existing = session.query(WatchlistItem)\
                    .filter_by(user_id=user_id, guild_id=guild_id, symbol=symbol.upper())\
                    .first()
                
                if existing:
                    return False
                
                item = WatchlistItem(user_id=user_id, guild_id=guild_id, symbol=symbol.upper(), company_name=company_name)
                session.add(item)
                session.commit()
                return True
            except Exception as e:
                print(f"Error adding to watchlist: {e}")
                session.rollback()
                return False
    
    def remove_from_watchlist(self, user_id, guild_id, symbol):
        with self.get_session() as session:
            try:
                item = session.query(WatchlistItem)\
                    .filter_by(user_id=user_id, guild_id=guild_id, symbol=symbol.upper())\
                    .first()
                
                if item:
                    session.delete(item)
                    session.commit()
                    return True
                return False
            except Exception as e:
                print(f"Error removing from watchlist: {e}")
                session.rollback()
                return False
    
    def get_user_watchlist(self, user_id, guild_id):
        with self.get_session() as session:
            try:
                # getting watchlist based on user id and guild id 
                items = session.query(WatchlistItem)\
                    .filter_by(user_id=user_id, guild_id=guild_id)\
                    .order_by(WatchlistItem.symbol)\
                    .all()
                
                return [{'symbol': i.symbol, 'company_name': i.company_name, 'created_at': i.created_at} for i in items]
            except Exception as e:
                print(f"Error getting watchlist: {e}")
                return []
    
    def get_watchlist_count(self, user_id, guild_id):
        with self.get_session() as session:
            return session.query(WatchlistItem).filter_by(user_id=user_id, guild_id=guild_id).count()

    # Portfolio
    def add_portfolio_position(self, user_id, guild_id, symbol, quantity, price):
        with self.get_session() as session:
            try:
                position = session.query(PortfolioPosition)\
                    .filter_by(user_id=user_id, guild_id=guild_id, symbol=symbol.upper())\
                    .first()
                
                if position:
                    # Update existing
                    new_shares = position.shares + quantity
                    new_total = position.total_cost + (quantity * price)
                    position.shares = new_shares
                    position.total_cost = new_total
                    position.average_price = new_total / new_shares
                    position.updated_at = datetime.now()
                else:
                    # Create new
                    position = PortfolioPosition(
                        user_id=user_id, guild_id=guild_id, symbol=symbol.upper(),
                        shares=quantity, total_cost=quantity * price, average_price=price
                    )
                    session.add(position)
                
                session.commit()
                return True
            except Exception as e:
                print(f"Error adding position: {e}")
                session.rollback()
                return False
    
    def sell_portfolio_position(self, user_id, guild_id, symbol, quantity, price):
        with self.get_session() as session:
            try:
                position = session.query(PortfolioPosition)\
                    .filter_by(user_id=user_id, guild_id=guild_id, symbol=symbol.upper())\
                    .first()
                
                if not position:
                    return (False, f"❌ You don't own any **{symbol.upper()}** shares.")
                
                if position.shares < quantity:
                    return (False, f"❌ You only have **{position.shares}** shares.")
                
                # Calculate P&L
                profit_per_share = price - position.average_price
                total_profit = profit_per_share * quantity
                sale_value = quantity * price
                
                # Track realized P&L
                self.add_realized_pnl(user_id, guild_id, total_profit)
                
                # Update position
                new_shares = position.shares - quantity
                
                if new_shares == 0:
                    session.delete(position)
                    msg = f"✅ Sold all **{quantity} shares** of **{symbol.upper()}** at **${price:.2f}**\n" \
                          f"Sale: ${sale_value:.2f} | P&L: ${total_profit:,.2f} ({profit_per_share/position.average_price*100:+.2f}%)"
                else:
                    position.shares = new_shares
                    position.total_cost -= quantity * position.average_price
                    position.updated_at = datetime.now()
                    msg = f"✅ Sold **{quantity} shares** of **{symbol.upper()}** at **${price:.2f}**\n" \
                          f"Sale: ${sale_value:.2f} | P&L: ${total_profit:,.2f} ({profit_per_share/position.average_price*100:+.2f}%)\n" \
                          f"Remaining: **{new_shares} shares**"
                
                session.commit()
                return (True, msg)
                
            except Exception as e:
                print(f"Error selling position: {e}")
                session.rollback()
                return (False, f"❌ Error: {str(e)}")
    
    def get_user_portfolio(self, user_id, guild_id):
        with self.get_session() as session:
            try:
                positions = session.query(PortfolioPosition)\
                    .filter_by(user_id=user_id, guild_id=guild_id)\
                    .order_by(PortfolioPosition.symbol)\
                    .all()
                
                return [{'symbol': p.symbol, 'shares': p.shares, 'average_price': p.average_price, 
                        'total_cost': p.total_cost, 'created_at': p.created_at, 'updated_at': p.updated_at} 
                        for p in positions]
            except Exception as e:
                print(f"Error getting portfolio: {e}")
                return []
    
    def get_portfolio_count(self, user_id, guild_id):
        with self.get_session() as session:
            return session.query(PortfolioPosition).filter_by(user_id=user_id, guild_id=guild_id).count()

    def remove_portfolio_position(self, user_id, guild_id, symbol):
        with self.get_session() as session:
            try:
                position = session.query(PortfolioPosition)\
                    .filter_by(user_id=user_id, guild_id=guild_id, symbol=symbol.upper())\
                    .first()
                
                if position:
                    session.delete(position)
                    session.commit()
                    return True
                return False
            except Exception as e:
                print(f"Error removing position: {e}")
                session.rollback()
                return False

    # User Stats (Realized P&L)
    def add_realized_pnl(self, user_id, guild_id, amount):
        with self.get_session() as session:
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
                print(f"Error adding P&L: {e}")
                session.rollback()

    def get_realized_pnl(self, user_id, guild_id):
        with self.get_session() as session:
            try:
                stats = session.query(UserStats).filter_by(user_id=user_id, guild_id=guild_id).first()
                return stats.total_realized_pnl if stats else 0.0
            except Exception as e:
                print(f"Error getting P&L: {e}")
                return 0.0

    def reset_realized_pnl(self, user_id, guild_id):
        with self.get_session() as session:
            try:
                stats = session.query(UserStats).filter_by(user_id=user_id, guild_id=guild_id).first()
                if stats:
                    stats.total_realized_pnl = 0.0
                    stats.updated_at = datetime.now()
                    session.commit()
            except Exception as e:
                print(f"Error resetting P&L: {e}")
                session.rollback()
        
# creating global instance of the database manager so i can import into other files
db_manager = DatabaseManager()