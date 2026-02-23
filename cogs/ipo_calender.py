import discord
from discord.ext import commands, tasks
from discord import app_commands
import aiohttp
from datetime import datetime, timedelta, timezone
import os
from dotenv import load_dotenv
import asyncio

load_dotenv()

FINNHUB_API_KEYS = [os.getenv(f"FINNHUB_API_KEY_{i}") for i in range(1, 11)]

class IPOCalender(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        