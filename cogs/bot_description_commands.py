import discord
from discord.ext import commands
from discord import app_commands
import pathlib
import asyncio
from database import db_manager
from datetime import datetime, timedelta

class BotHelp(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.channel_name = "bot-descriptions-commands"
        self.description_file = pathlib.Path("bot_description.txt")
        self.posting_locks = {}  
        self.last_post_attempt = {} 
        self.cooldown_seconds = 5  

    def get_description_text(self):
        """Reads the description from a txt file."""
        try:
            if self.description_file.exists():
                return self.description_file.read_text(encoding="utf-8")
        except Exception as e:
            print(f"Error reading description file: {e}")
        return  

    async def post_help_message(self, guild: discord.Guild):
        """Posts or updates the help embed without duplicates - now fully atomic."""
        # Cooldown check to prevent spam
        now = datetime.now()
        last_attempt = self.last_post_attempt.get(guild.id)
        if last_attempt and (now - last_attempt).total_seconds() < self.cooldown_seconds:
            # print(f"Skipping description post for {guild.name} - cooldown active")
            return

        # Create lock if doesn't exist
        if guild.id not in self.posting_locks:
            self.posting_locks[guild.id] = asyncio.Lock()
        
        async with self.posting_locks[guild.id]:
            self.last_post_attempt[guild.id] = now
            
            try:
                channel = discord.utils.get(guild.text_channels, name=self.channel_name)
                if not channel:
                    print(f"Help channel not found in {guild.name}")
                    return

                embed = self._build_help_embed()
                
                # ATOMIC OPERATION: Try to update existing message first
                msg_id = await db_manager.get_help_message_id(guild.id)
                
                if msg_id:
                    try:
                        msg = await channel.fetch_message(msg_id)
                        await msg.edit(embed=embed)
                        print(f"Updated existing help message in {guild.name}")
                        return
                    except discord.NotFound:
                        # Message deleted, continue to create new one
                        print(f"Stored help message not found in {guild.name}, creating new one")
                    except discord.Forbidden:
                        print(f"No permission to edit help message in {guild.name}")
                        return
                    except discord.HTTPException as e:
                        print(f"HTTP error editing help message in {guild.name}: {e}")
                        # Continue to try creating new message

                # ATOMIC OPERATION: Clean up old messages then create new one
                await self._cleanup_old_help_messages(channel)
                
                # Create new message
                try:
                    msg = await channel.send(embed=embed)
                    await db_manager.save_help_message_id(guild.id, msg.id)
                except discord.Forbidden:
                    print(f"No permission to send help message in {guild.name}")
                except discord.HTTPException as e:
                    print(f"Failed to send help message in {guild.name}: {e}")
                    
            except Exception as e:
                print(f"Unexpected error posting help message in {guild.name}: {e}")

    async def _cleanup_old_help_messages(self, channel: discord.TextChannel):
        """Clean up any existing help messages in the channel."""
        try:
            messages_to_delete = []
            
            # fetch recent help message
            async for message in channel.history(limit=1):
                if (message.author == self.bot.user and 
                    message.embeds and 
                    len(message.embeds) > 0 and
                    message.embeds[0].title and
                    "Bot Description + Commands" in message.embeds[0].title):
                    messages_to_delete.append(message)
            
            # Delete old messages
            for msg in messages_to_delete:
                try:
                    await msg.delete()
                    await asyncio.sleep(0.5)  # Rate limit protection
                except (discord.Forbidden, discord.HTTPException) as e:
                    print(f"Could not delete old help message: {e}")
                    
        except (discord.Forbidden, discord.HTTPException) as e:
            print(f"Could not clean up old help messages: {e}")

    def _build_help_embed(self):
        """Build the help embed."""
        embed = discord.Embed(
            title="🤖 Bot Description + Commands",
            description=self.get_description_text(),
            color=discord.Color.blue()
        )
        
        # Add slash commands
        slash_commands = []
        for command in self.bot.tree.get_commands():
            slash_commands.append(f"**/{command.name}** - {command.description}")
        
        if slash_commands:
            embed.add_field(
                name="📋 Slash Commands (type / to see them)",
                value="\n".join(slash_commands),
                inline=False
            )

        
        return embed

    @commands.Cog.listener()
    async def on_ready(self):
        # Add delay to let other cogs initialize
        await asyncio.sleep(2)
        
        # Process guilds with staggered timing to avoid rate limits
        for i, guild in enumerate(self.bot.guilds):
            try:
                await self.post_help_message(guild)
                # Stagger requests to avoid rate limits
                if i < len(self.bot.guilds) - 1:
                    await asyncio.sleep(1)
            except Exception as e:
                print(f"Error posting help message for {guild.name}: {e}")

    @commands.Cog.listener()
    async def on_guild_join(self, guild):
        # Add delay to let BaseChannels create the channel first
        await asyncio.sleep(3)
        try:
            await self.post_help_message(guild)
        except Exception as e:
            print(f"Error posting help message for new guild {guild.name}: {e}")

    def cog_unload(self):
        """Cleanup when cog is unloaded."""
        self.posting_locks.clear()
        self.last_post_attempt.clear()

async def setup(bot):
    await bot.add_cog(BotHelp(bot))