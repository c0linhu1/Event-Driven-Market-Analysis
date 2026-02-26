import discord
from discord.ext import commands
from discord import app_commands
from database import db_manager
import asyncio

class StockInfoButton(discord.ui.View):
    """Button view for getting stock info directly from watchlist"""
    
    def __init__(self, symbols):
        super().__init__(timeout=300)  # 5 minute timeout
        
        # Add buttons for each symbol (max 25 buttons per view)
        for symbol in symbols[:25]:  
            button = discord.ui.Button(
                label=f"📊 {symbol}",
                style=discord.ButtonStyle.secondary,
                custom_id=f"stock_info_{symbol}"
            )
            button.callback = self.create_callback(symbol)
            self.add_item(button)
    
    def create_callback(self, symbol):
        """Create callback function for each button"""
        async def callback(interaction):
            # Get the StockInfo cog
            stock_cog = interaction.client.get_cog("StockInfo")
            if not stock_cog:
                await interaction.response.send_message("❌ Stock info service not available.", ephemeral=True)
                return
            
            await interaction.response.defer(ephemeral=True)
            
            # Fetch stock data
            stock_data = await stock_cog.get_stock_data_finnhub(symbol)
            
            if not stock_data:
                await interaction.followup.send(f"❌ Could not fetch data for **{symbol}**", ephemeral=True)
                return
            
            embed = stock_cog.create_stock_info_embed(stock_data)
            if embed:
                await interaction.followup.send(embed=embed, ephemeral=True)
            else:
                await interaction.followup.send(f"❌ Error processing data for **{symbol}**", ephemeral=True)
        
        return callback

class Watchlist(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    @app_commands.command(name="watchlist", description="Creates a private watchlist channel for you")
    async def watchlist_slash(self, interaction: discord.Interaction):
        """Creates a private watchlist channel for the user"""
        guild = interaction.guild
        user = interaction.user

        overwrites = {
            guild.default_role: discord.PermissionOverwrite(view_channel=False),
            user: discord.PermissionOverwrite(view_channel=True, send_messages=True),
            guild.me: discord.PermissionOverwrite(view_channel=False),
        }

        channel_name = f"private_watchlist-{user.name.lower()}"
        existing_channel = discord.utils.get(guild.text_channels, name=channel_name)

        response_handler = self.bot.get_cog("ResponseHandler")
        if not response_handler:
            await interaction.response.send_message("❌ Response handler not available.", ephemeral=True)
            return

        if existing_channel:
            # Channel already exists
            message = f"⚠️ You already have a private watchlist: {existing_channel.mention}"
            await response_handler.send_response(interaction, message)
        else:
            # Create new channel
            private_channel = await guild.create_text_channel(
                channel_name,
                overwrites=overwrites,
            )
            message = f"✅ I created your private watchlist: {private_channel.mention}"
            await response_handler.send_response(interaction, message)

    @app_commands.command(name="delete_watchlist", description="Deletes your private watchlist channel")
    async def delete_watchlist_slash(self, interaction: discord.Interaction):
        """Deletes the user's private watchlist channel"""
        guild = interaction.guild
        user = interaction.user

        channel_name = f"private_watchlist-{user.name.lower()}"
        existing_channel = discord.utils.get(guild.text_channels, name=channel_name)

        response_handler = self.bot.get_cog("ResponseHandler")
        if not response_handler:
            await interaction.response.send_message("❌ Response handler not available.", ephemeral=True)
            return

        if existing_channel:
            await existing_channel.delete()
            message = "🗑️ Your private watchlist has been deleted."
            await response_handler.send_response(interaction, message)
        else:
            message = "⚠️ You don't have a private watchlist channel to delete."
            await response_handler.send_response(interaction, message)

    @app_commands.command(name="add_company", description="Add a company to your watchlist")
    @app_commands.describe(
        symbol="Stock symbol (e.g., AAPL, TSLA, MSFT)",
        company_name="Optional company name for reference"
    )


    async def add_company_slash(self, interaction: discord.Interaction, symbol: str, company_name: str = None):
        """Add a company to the user's watchlist"""
        guild = interaction.guild
        user = interaction.user

        response_handler = self.bot.get_cog("ResponseHandler")
        if not response_handler:
            await interaction.response.send_message("❌ Response handler not available.", ephemeral=True)
            return

        # Validate symbol 
        symbol = symbol.upper().strip()
        if not symbol or len(symbol) > 10 or not symbol.isalpha():
            message = "❌ Please provide a valid stock symbol (letters only, max 10 characters)"
            await response_handler.send_response(interaction, message)
            return

        # Make a watchlist limit
        watchlist_count = await db_manager.get_watchlist_count(user.id, guild.id)
        if watchlist_count > 25:
            message = "You watchlist is full. Maximum 25 companies allowed. " \
            "Please use the '/remove_company' command to make space"
            await response_handler.send_response(interaction, message)
            
        # Add to database
        success = await db_manager.add_to_watchlist(user.id, guild.id, symbol, company_name)
        
        if success:
            if company_name:
                message = f"📈 Added **{symbol}** ({company_name}) to your watchlist!"
            else:
                message = f"📈 Added **{symbol}** to your watchlist!"
        else:
            message = f"⚠️ **{symbol}** is already in your watchlist."

        await response_handler.send_response(interaction, message)

    @app_commands.command(name="remove_company", description="Remove a company from your watchlist")
    @app_commands.describe(symbol="Stock symbol to remove (e.g., AAPL, TSLA, MSFT)")
    async def remove_company_slash(self, interaction: discord.Interaction, symbol: str):
        """Remove a company from the user's watchlist"""
        guild = interaction.guild
        user = interaction.user

        response_handler = self.bot.get_cog("ResponseHandler")
        if not response_handler:
            await interaction.response.send_message("❌ Response handler not available.", ephemeral=True)
            return

        # Validate symbol
        symbol = symbol.upper().strip()
        if not symbol:
            message = "❌ Please provide a stock symbol to remove."
            await response_handler.send_response(interaction, message)
            return

        # Remove from database
        success = await db_manager.remove_from_watchlist(user.id, guild.id, symbol)
        
        if success:
            message = f"📉 Removed **{symbol}** from your watchlist."
        else:
            message = f"⚠️ **{symbol}** was not found in your watchlist."

        await response_handler.send_response(interaction, message)

    @app_commands.command(name="show_watchlist", description="Show all companies in your watchlist")
    async def show_watchlist_slash(self, interaction: discord.Interaction):
        """Display the user's watchlist with interactive buttons"""
        guild = interaction.guild
        user = interaction.user

        response_handler = self.bot.get_cog("ResponseHandler")
        if not response_handler:
            await interaction.response.send_message("❌ Response handler not available.", ephemeral=True)
            return

        # Get watchlist from database
        watchlist_items = await db_manager.get_user_watchlist(user.id, guild.id)

        if not watchlist_items:
            message = "📭 Your watchlist is empty. Use `/add_company` to add companies!"
            await response_handler.send_response(interaction, message)
            return

        # Create embed for better formatting
        embed = discord.Embed(
            title=f"📊 {user.display_name}'s Watchlist",
            color=discord.Color.green(),
            description=f"You're watching {len(watchlist_items)} companies"
        )

        # Add companies to embed
        watchlist_text = ""
        symbols = []
        for item in watchlist_items:
            symbol = item['symbol']
            company_name = item['company_name']
            added_date = item['created_at'].strftime("%m/%d/%Y")
            symbols.append(symbol)
            
            if company_name:
                watchlist_text += f"**{symbol}** - {company_name} *(added {added_date})*\n"
            else:
                watchlist_text += f"**{symbol}** *(added {added_date})*\n"

        # Split into chunks if too long
        if len(watchlist_text) > 1024:
            chunks = [watchlist_text[i:i+1000] for i in range(0, len(watchlist_text), 1000)]
            for i, chunk in enumerate(chunks):
                field_name = "Companies" if i == 0 else f"Companies (cont. {i+1})"
                embed.add_field(name=field_name, value=chunk, inline=False)
        else:
            embed.add_field(name="Companies", value=watchlist_text, inline=False)

        embed.add_field(
            name="💡 Quick Actions",
            value="• Click buttons below for recent day stock info\n• Use `/stock_info [symbol]` " \
            "for detailed data\n• Use `/add_company` or `/remove_company` to manage \n• " \
            "IF INTERACTION FAILS USE COMMAND 'show_watchlist' AGAIN",
            inline=False
        )

        # Create interactive buttons for stock info
        view = StockInfoButton(symbols) if symbols else None

        # Send response with buttons
        if response_handler.is_user_private_channel(interaction.channel, user):
            await interaction.response.send_message(embed=embed, view=view, ephemeral=False)
        else:
            await interaction.response.send_message("✅ Watchlist sent - check your DMs!", ephemeral=True)
            try:
                await user.send(embed=embed, view=view)
            except discord.Forbidden:
                await interaction.edit_original_response(embed=embed, view=view)


async def setup(bot):
    await bot.add_cog(Watchlist(bot))