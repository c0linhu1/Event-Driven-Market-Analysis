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

