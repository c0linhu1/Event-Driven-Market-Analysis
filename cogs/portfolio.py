import discord
from discord.ext import commands
from discord import app_commands
from database import db_manager
import aiohttp
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv()

FINNHUB_API_KEYS = [os.getenv(f"FINNHUB_API_KEY_{i}") for i in range(1, 11)]

class Portfolio(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    async def get_current_price(self, ticker):
        """Getting current price for specific stock"""
        for api_key in FINNHUB_API_KEYS:
            url = f"https://finnhub.io/api/v1/quote?symbol={ticker}&token={api_key}"
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(url) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            current_price = data.get('c', 0)
                            if current_price > 0:
                                return current_price
                        elif resp.status == 429:
                            continue
            except Exception as e:
                print(f"Error fetching price for {ticker}: {e}")
                continue
        
        return None
    
    @app_commands.command(name = "portfolio", description = "Creates a private portfolio for the user")
    async def private_portfolio(self, interaction: discord.Interaction):
        """Creates a private portfolio channel"""
        guild = interaction.guild
        user = interaction.user
        
        # adjusting channel perms 
        overwrites = {
            guild.default_role: discord.PermissionOverwrite(view_channel = False),
            user: discord.PermissionOverwrite(view_channel = True, send_messages = True),
            guild.me: discord.PermissionOverwrite(view_channel = True, send_messages = False)
        }

        channel_name = f"{user.name.lower()}'s-private-portfolio"
        existing_channel = discord.utils.get(guild.text_channels, name = channel_name)

        # creating channel if its not already existing - preventing duplicates
        if existing_channel:
            await interaction.response.send_message(
                f"⚠️ You already have a private portfolio: {existing_channel.mention}",
                ephemeral=True
            )
        else:
            private_channel = await guild.create_text_channel(channel_name, overwrites=overwrites)
            await interaction.response.send_message(
                f"✅ Created your private portfolio: {private_channel.mention}",
                ephemeral=True
            )
    
    @app_commands.command(name = "add_position", description = "Buy stocks and add to your discord portfolio")
    @app_commands.describe(
        ticker = "Stock ticker (e.g., NVDA, OKLO)",
        price = "Price per share of purchase",
        lot_size = "Number of shares bought"
    )

    # need to specify to discord what values user need to put in
    async def add_position(self, interaction: discord.Interaction, ticker: str, price: float, lot_size: float):
        """Add stocks to portfolio"""

        user = interaction.user
        guild = interaction.guild

        # normalize ticker
        ticker = ticker.upper().strip()
    
        # making sure ticker, lotsize, price is valid inputs
        if not ticker or len(ticker) > 10 or not ticker.isalpha():
            await interaction.response.send_message("❌ Invalid ticker symbol", ephemeral=True)
            return
        
        if lot_size <= 0 or price <= 0:
            await interaction.response.send_message("❌ Invalid price/lot_size", ephemeral=True)
            return
        
        db_manager.add_portfolio_position(user.id, guild.id, ticker, lot_size, price)
        total_cost = price * lot_size

        await interaction.response.send_message(
            f"✅ Bought **{lot_size} shares** of **{ticker}** at **${price:.2f}** (Total: ${total_cost:.2f})",
            ephemeral=True
        )

    @app_commands.command(name = "sell_position", description = "Sell stocks from your discord portfolio")
    @app_commands.describe(
        ticker="Stock ticker to sell",
        price="Price per share at sale",
        lot_size="Number of shares to sell"
    )
    async def sell_position(self, interaction: discord.Interaction, ticker: str, price: float, lot_size: float):
        """Sell stocks from portfolio"""
        user = interaction.user
        guild = interaction.guild

        ticker = ticker.upper().strip()
        
        if not ticker or len(ticker) > 10 or not ticker.isalpha():
            await interaction.response.send_message("❌ Invalid ticker symbol", ephemeral=True)
            return
        
        if lot_size <= 0 or price <= 0:
            await interaction.response.send_message("❌ Invalid price/lot_size", ephemeral=True)
            return
        
        # selljing from database - expecting a tuple from function in database
        success, message = db_manager.sell_portfolio_position(user.id, guild.id, ticker, lot_size, price)
        await interaction.response.send_message(message, ephemeral=True)

    @app_commands.command(name = "show_portfolio", description = "View all your stock positions in discord portfolio")
    async def show_portfolio(self, interaction: discord.Interaction):
        """Display portfolio with current prices"""
        await interaction.response.defer(ephemeral=True)
        
        user = interaction.user
        guild = interaction.guild

        # access user specific portfolio in database
        portfolio = db_manager.get_user_portfolio(user.id, guild.id)

        if not portfolio:
            await interaction.followup.send("Your portfolio is empty. Use `/add_position` to add stocks!", ephemeral=True)
            return
        
        total_invested = 0
        total_current_value = 0
        positions_text = ""

        # iterate through user portfolio from database - initiating values 
        for position in portfolio:
            ticker = position["symbol"]
            shares = position["shares"]
            avg_price = position["average_price"]
            cost_basis = shares * avg_price
            total_invested += cost_basis

            # getting current price of chosen stock - pulling data from finnhub api
            current_price = await self.get_current_price(ticker)
            

            if current_price:
                current_value = shares * current_price
                gain_loss = current_value - cost_basis
                gain_loss_percent = ((current_price - avg_price) / avg_price) * 100
                total_current_value += current_value

                gain_indicator_emoji = "📈" if gain_loss >= 0 else "📉"
                positions_text += (
                    f"{gain_indicator_emoji} **{ticker}** - {shares} shares\n"
                    f"  Avg: ${avg_price:.2f} -> Now: ${current_price:.2f}\n"
                    f"  P&L: ${gain_loss:,.2f} ({gain_loss_percent:+.2f}%)\n\n"
                )
            else:
                positions_text += (
                    f"⚠️ **{ticker}** - {shares} shares @ ${avg_price:.2f}\n"
                    f"  (Price unavailable)\n\n"
                )

        total_pnl = total_current_value - total_invested
        total_pnl_percent = (total_pnl / total_invested * 100) if total_invested > 0 else 0

        # Create embed
        color = discord.Color.green() if total_pnl >= 0 else discord.Color.red()
        trend = "📈" if total_pnl >= 0 else "📉"

        embed = discord.Embed(
            title = f"{trend} {user.display_name}'s Portfolio",
            color = color,
            timestamp = datetime.now()
        )

        embed.add_field(name="📊 Positions", value = positions_text or "No positions", inline=False)
        embed.add_field(
            name="Summary",
            value=f"**Invested:** ${total_invested:,.2f}\n"
                  f"**Current:** ${total_current_value:,.2f}\n"
                  f"**P&L:** ${total_pnl:,.2f} ({total_pnl_percent:+.2f}%)",
            inline=False
        )

        embed.set_footer(text=f"{len(portfolio)} positions • Data from Finnhub")

        await interaction.followup.send(embed=embed, ephemeral=True)

    @app_commands.command(name = "pnl", description = "Quick view of profit and loss")
    async def profit_loss(self, interaction: discord.Interaction):
        """Display total P&L including past sales"""
        await interaction.response.defer(ephemeral=True)
        
        user = interaction.user
        guild = interaction.guild

        portfolio = db_manager.get_user_portfolio(user.id, guild.id)
        
        realized_pnl = db_manager.get_realized_pnl(user.id, guild.id)

        # Calculate unrealized P&L from current holdings
        unrealized_invested = 0
        unrealized_current = 0

        for position in portfolio:
            shares = position['shares']
            avg_price = position['average_price']
            unrealized_invested += shares * avg_price

            current_price = await self.get_current_price(position['symbol'])
            if current_price:
                unrealized_current += shares * current_price

        unrealized_pnl = unrealized_current - unrealized_invested
        total_pnl = unrealized_pnl + realized_pnl

        color = discord.Color.green() if total_pnl >= 0 else discord.Color.red()
        trend = "📈" if total_pnl >= 0 else "📉"

        embed = discord.Embed(
            title = f"{trend} Total Profit & Loss",
            color = color,
            timestamp = datetime.now()
        )
        
        embed.add_field(
            name="Performance",
            value=f"**Unrealized P&L:** ${unrealized_pnl:,.2f} (current holdings)\n"
                  f"**Realized P&L:** ${realized_pnl:,.2f} (past sales)\n"
                  f"**Total P&L:** ${total_pnl:,.2f}",
            inline=False
        )

        if portfolio:
            embed.set_footer(text=f"{len(portfolio)} active positions")
        else:
            embed.set_footer(text="No active positions")

        await interaction.followup.send(embed=embed, ephemeral=True)

    @app_commands.command(name = "reset_portfolio", description = "Delete all positions and reset your portfolio")
    async def reset_portfolio(self, interaction: discord.Interaction):
        """Deletes all positions"""

        user = interaction.user
        guild = interaction.guild

        portfolio = db_manager.get_user_portfolio(user.id, guild.id)

        if not portfolio:
            await interaction.response.send_message(
                "📭 Your portfolio is already empty.",
                ephemeral=True
            )
            return

        deleted_count = 0
        for position in portfolio:
            success = db_manager.remove_portfolio_position(user.id, guild.id, position['symbol'])
            if success:
                deleted_count += 1

        await interaction.response.send_message(
            f"✅ Portfolio reset complete! Deleted **{deleted_count} positions**.",
            ephemeral=True
        )

    @app_commands.command(name = "reset_pnl", description = "Reset your realized P&L from past sales")
    async def reset_pnl(self, interaction: discord.Interaction):
        """Reset realized P&L to zero"""
        user = interaction.user
        guild = interaction.guild

        # Get current realized P&L
        realized_pnl = db_manager.get_realized_pnl(user.id, guild.id)

        if realized_pnl == 0:
            await interaction.response.send_message(
                "Your realized P&L is already at $0.00",
                ephemeral=True
            )
            return

        db_manager.reset_realized_pnl(user.id, guild.id)

        await interaction.response.send_message(
            f"Realized P&L reset complete! Previous realized P&L was ${realized_pnl:,.2f}",
            ephemeral=True
        )


async def setup(bot):
    await bot.add_cog(Portfolio(bot))