import discord 
from discord.ext import commands, tasks
import os
import asyncio
from datetime import datetime, timedelta, timezone
from database import db_manager
from dotenv import load_dotenv


load_dotenv()

intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix="/", intents=intents)

@bot.event
async def on_ready():
    # ensuring bot is ready to run
    print(f"{bot.user} ready")
    print(f"Database initialized with {len(bot.guilds)} guilds")
    
    # Sync slash commands
    try:
        synced = await bot.tree.sync()
        print(f"Synced {len(synced)} slash commands")
    except Exception as e:
        print(f"Failed to sync commands: {e}")
    
    # clean old messages 
    if not delete_old_messages.is_running():
        delete_old_messages.start()

@bot.event
async def on_command_completion(ctx):
    try:
        await ctx.message.delete()
    except discord.Forbidden:
        print("Missing permission to delete command messages.")
    except discord.HTTPException:
        print("Failed to delete command message.")

@tasks.loop(hours=1) 
# delete messages older than 48h - run every 1 hour
async def delete_old_messages():
    await bot.wait_until_ready()
    # creates a cutoff time of 2 days
    cutoff = datetime.now(timezone.utc) - timedelta(days=2) 
    # looping through public channels of servers
    for guild in bot.guilds:
        for channel in guild.text_channels:
            # only check public channels
            perms = channel.permissions_for(guild.default_role)
            if not perms.read_messages:
                # skip private/hidden channels
                continue  

            # skipping the commands channel
            if channel.name == "bot-descriptions-commands":
                continue

            try:
                deleted = await channel.purge(
                    limit=None,
                    check=lambda m: m.created_at < cutoff
                )
                if deleted:
                    print(f"Purged {len(deleted)} old messages in #{channel.name} of {guild.name}")
            except discord.Forbidden:
                print(f"Missing permission to delete in #{channel.name} ({guild.name})")
            except discord.HTTPException as e:
                print(f"Failed to purge #{channel.name} in {guild.name}: {e}")

async def load():
    """loading all .py files in the cogs folder"""
    for filename in os.listdir("./cogs"):
        if filename.endswith(".py"):
            await bot.load_extension(f"cogs.{filename[:-3]}")    

async def main():
    async with bot:
        # initialize database before loading cogs
        print("Initializing database...")
        await load()
        await db_manager.initialize()

        token = os.getenv("DISCORD_TOKEN")
        if not token:
            print("token not found in env file")
        await bot.start(token)
        
try:
    asyncio.run(main())
except KeyboardInterrupt:
    print("Bot stopped manually.")
        