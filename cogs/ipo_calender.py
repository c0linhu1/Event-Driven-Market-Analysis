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


class IPOCalendar(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.post_daily_ipos.start()

    def cog_unload(self):
        self.post_daily_ipos.cancel()

    async def fetch_ipo_calendar(self, days_ahead = 28):
        """Fetching IPO calendar from Finnhub API"""
        today = datetime.now().strftime('%Y-%m-%d')
        future_time = (datetime.now() + timedelta(days=days_ahead)).strftime('%Y-%m-%d')

        for i, api_key in enumerate(FINNHUB_API_KEYS):
            url = f"https://finnhub.io/api/v1/calendar/ipo?from={today}&to={future_time}&token={api_key}"
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(url) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            print(f"*** Successfully fetched IPO calendar with API key {i + 1}")
                            return data
                        elif resp.status == 429:
                            print(f"--Finnhub IPO API key {i + 1} rate limited")
                            continue
                        else:
                            text = await resp.text()
                            print(f"-- {resp.status}: IPO key {i + 1}: {text}")
                            continue
            except Exception as e:
                print(f"--Error fetching IPO with key {i + 1}: {e}")
                continue

        print("---All Finnhub API keys failed for IPO calendar")
        return None

    def build_ipo_day_embeds(self, date, ipo_list):
        """Building Discord embeds for a day's IPOs - splits if too large"""
        try:
            date_obj = datetime.strptime(date, '%Y-%m-%d')
            formatted_date = date_obj.strftime('%B %d, %Y (%A)')
        except:
            formatted_date = date

        entries = []
        for ipo in ipo_list:
            symbol = ipo.get('symbol', 'N/A')
            name = ipo.get('name', 'Unknown')
            price = ipo.get('price', '')
            exchange = ipo.get('exchange', '')
            shares = ipo.get('numberOfShares')
            status = ipo.get('status', '')

            entry = f"**{symbol}** — {name}"
            if price:
                entry += f" • ${price}"
            if exchange:
                entry += f" • {exchange}"
            if shares:
                entry += f" • {shares:,.0f} shares"
            if status:
                entry += f" • *{status}*"
            entries.append(entry)

        embeds = []
        chunk_size = 25

        for i in range(0, len(entries), chunk_size):
            chunk = entries[i:i + chunk_size]
            part_num = (i // chunk_size) + 1
            total_parts = (len(entries) + chunk_size - 1) // chunk_size

            if total_parts > 1:
                title = f"🆕 IPOs: {formatted_date} (Part {part_num}/{total_parts})"
            else:
                title = f"🆕 IPOs: {formatted_date}"

            embed = discord.Embed(
                title=title,
                description=f"**{len(ipo_list)} IPOs** scheduled" if part_num == 1 else None,
                color=discord.Color.purple(),
                timestamp=datetime.now(timezone.utc)
            )
            embed.add_field(name="Companies", value="\n".join(chunk) if chunk else "None", inline=False)
            embed.set_footer(text="Data from Finnhub • Prices may not have released yet")
            embeds.append(embed)

        return embeds

    @tasks.loop(hours=24)
    async def post_daily_ipos(self):
        """Post IPO calendar once daily"""
        await self.bot.wait_until_ready()

        try:
            ipo_data = await self.fetch_ipo_calendar()
            if not ipo_data or 'ipoCalendar' not in ipo_data:
                print("Failed to fetch IPO calendar")
                return

            ipo_list = ipo_data['ipoCalendar']

            if not ipo_list:
                for guild in self.bot.guilds:
                    channel = discord.utils.get(guild.text_channels, name="ipo-calendar-dashboard")
                    if not channel:
                        continue
                    try:
                        await channel.purge(limit=None)
                        embed = discord.Embed(
                            title="🆕 Upcoming IPOs (Next 28 Days)",
                            description="No IPOs scheduled in the next 28 days.",
                            color=discord.Color.purple(),
                            timestamp=datetime.now(timezone.utc)
                        )
                        embed.set_footer(text="Data from Finnhub")
                        await channel.send(embed=embed)
                    except (discord.Forbidden, discord.HTTPException):
                        pass
                return
            
            # Group by date
            by_date = {}
            for ipo in ipo_list:
                d = ipo.get('date', 'Unknown')
                if d not in by_date:
                    by_date[d] = []
                by_date[d].append(ipo)

            for guild in self.bot.guilds:
                channel = discord.utils.get(guild.text_channels, name="ipo-calendar-dashboard")
                if not channel:
                    continue

                try:
                    await channel.purge(limit=None)

                    summary = discord.Embed(
                        title="🆕 Upcoming IPOs (Next 28 Days)",
                        description=f"**Total: {len(ipo_list)} IPOs** scheduled",
                        color=discord.Color.purple(),
                        timestamp=datetime.now(timezone.utc)
                    )
                    lines = []
                    for date in sorted(by_date.keys()):
                        try:
                            day_name = datetime.strptime(date, '%Y-%m-%d').strftime('%a %m/%d')
                        except:
                            day_name = date
                        lines.append(f"• **{day_name}**: {len(by_date[date])} IPOs")

                    summary.add_field(
                        name="Daily Breakdown",
                        value="\n".join(lines) if lines else "No IPOs scheduled",
                        inline=False
                    )
                    await channel.send(embed=summary)
                    await asyncio.sleep(0.5)

                    # Post each day's IPOs
                    for date in sorted(by_date.keys()):
                        embeds = self.build_ipo_day_embeds(date, by_date[date])
                        for embed in embeds:
                            await channel.send(embed=embed)
                            await asyncio.sleep(0.5)

                    print(f"Posted {len(by_date)} day(s) of IPOs to {guild.name}")

                except discord.Forbidden:
                    print(f"No permission to post in ipo-calendar-dashboard in {guild.name}")
                except discord.HTTPException as e:
                    print(f"Failed to post IPO calendar in {guild.name}: {e}")

        except Exception as e:
            print(f"IPO calendar error: {e}")

    @post_daily_ipos.before_loop
    async def before_daily_ipos(self):
        await self.bot.wait_until_ready()
        await asyncio.sleep(10)


async def setup(bot):
    await bot.add_cog(IPOCalendar(bot))