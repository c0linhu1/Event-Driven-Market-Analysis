import discord
from discord.ext import commands
import asyncio

class BaseChannels(commands.Cog):  
    def __init__(self, bot):
        self.bot = bot
        self.setup_locks = {}  # Guild ID -> asyncio.Lock

    async def ensure_required_channels(self, guild: discord.Guild):
        """Ensure required channels exist with proper permissions."""
        # Prevent concurrent setup for same guild
        if guild.id not in self.setup_locks:
            self.setup_locks[guild.id] = asyncio.Lock()
        
        async with self.setup_locks[guild.id]:
            required_channels = [
                "news", 
                "earnings-calendar-dashboard", 
                "bot-descriptions-commands",
                'IPO-calendar-dashboard'
            ]

            for channel_name in required_channels:
                try:
                    await self._ensure_channel(guild, channel_name)
                except Exception as e:
                    print(f"Error ensuring channel '{channel_name}' in {guild.name}: {e}")
                    # Continue with other channels even if one fails

    async def _ensure_channel(self, guild: discord.Guild, channel_name: str):
        """Ensure a single channel exists with proper permissions."""
        try:
            existing_channel = discord.utils.get(
                guild.text_channels, name=channel_name.lower()
            )

            # Build permission overwrites
            overwrites = self._build_overwrites(guild)

            if not existing_channel:
                await self._create_channel(guild, channel_name, overwrites)
            else:
                await self._update_channel(guild, existing_channel, overwrites, channel_name)
                
        except discord.Forbidden:
            print(f"Missing permissions to manage '{channel_name}' in {guild.name}")
        except discord.HTTPException as e:
            print(f"Discord API error for '{channel_name}' in {guild.name}: {e}")
            if e.status == 429:  # Rate limit
                print(f"Hit rate limit - waiting before retry")
                await asyncio.sleep(5)
        except Exception as e:
            print(f"Unexpected error with '{channel_name}' in {guild.name}: {type(e).__name__}: {e}")

    def _build_overwrites(self, guild: discord.Guild):
        """Build permission overwrites for channels."""
        overwrites = {
            guild.default_role: discord.PermissionOverwrite(
                view_channel=True,
                send_messages=False
            )
        }
        
        # Give admins full access
        for role in guild.roles:
            if role.permissions.administrator:
                overwrites[role] = discord.PermissionOverwrite(
                    view_channel=True,
                    send_messages=True
                )
        
        return overwrites

    async def _create_channel(self, guild: discord.Guild, channel_name: str, overwrites: dict):
        """Create a new channel."""
        try:
            new_channel = await guild.create_text_channel(
                channel_name, 
                overwrites=overwrites
            )
            print(f"Created '{channel_name}' in {guild.name}")

            # Trigger help message for bot-descriptions-commands
            if channel_name == "bot-descriptions-commands":
                await self._trigger_help_post(guild)
                
        except discord.Forbidden:
            print(f"No permission to create '{channel_name}' in {guild.name}")
            print(f"Bot needs 'Manage Channels' permission")
        except discord.HTTPException as e:
            if e.status == 403:
                print(f"Forbidden: Cannot create '{channel_name}' in {guild.name}")
            elif e.status == 429:
                print(f"Rate limited creating '{channel_name}' in {guild.name}")
            else:
                print(f"HTTP {e.status} creating '{channel_name}' in {guild.name}: {e.text}")

    async def _update_channel(self, guild: discord.Guild, channel: discord.TextChannel, 
                             overwrites: dict, channel_name: str):
        """Update existing channel permissions."""
        try:
            await channel.edit(overwrites=overwrites)

            # Trigger help message for bot-descriptions-commands
            if channel_name == "bot-descriptions-commands":
                await self._trigger_help_post(guild)
                
        except discord.Forbidden:
            print(f"No permission to update '{channel_name}' in {guild.name}")
        except discord.HTTPException as e:
            print(f"Failed to update '{channel_name}' in {guild.name}: {e}")

    async def _trigger_help_post(self, guild: discord.Guild):
        """Trigger help message posting with error handling."""
        try:
            bothelp = self.bot.get_cog("BotHelp")
            if bothelp:
                await asyncio.sleep(1)  # Small delay to ensure channel is ready
                await bothelp.post_help_message(guild)
            else:
                print(f"BotHelp cog not loaded yet for {guild.name}")
        except Exception as e:
            print(f"Error triggering help post for {guild.name}: {e}")

    @commands.Cog.listener()
    async def on_ready(self):
        """Setup channels when bot is ready."""
        print("Setting up required channels for all guilds...")
        for guild in self.bot.guilds:
            try:
                await self.ensure_required_channels(guild)
            except Exception as e:
                print(f"Error setting up channels for {guild.name}: {e}")

    @commands.Cog.listener()
    async def on_guild_join(self, guild):
        """Setup channels when bot joins a new guild."""
        print(f"Joined new guild: {guild.name} (ID: {guild.id})")
        try:
            await self.ensure_required_channels(guild)
        except Exception as e:
            print(f"Error setting up channels for new guild {guild.name}: {e}")

    def cog_unload(self):
        """Cleanup when cog is unloaded."""
        self.setup_locks.clear()

async def setup(bot):
    await bot.add_cog(BaseChannels(bot))