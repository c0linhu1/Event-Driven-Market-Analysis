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

# only showing major SEC forms
MAJOR_FORMS = {"10-K", "10-Q", "8-K", "S-1"}


class SECFilings(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.post_daily_filings.start()

    def cog_unload(self):
        self.post_daily_filings.cancel()

    async def fetch_sec_filings(self, days_back = 28):
        """Fetching recent SEC filings from Finnhub"""
        from_date = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%d')

        for i, api_key in enumerate(FINNHUB_API_KEYS):
            url = f"https://finnhub.io/api/v1/stock/filings?from={from_date}&token={api_key}"
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(url) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            print(f"*** Successfully fetched SEC filings with API key {i + 1}")
                            return data
                        elif resp.status == 429:
                            print(f"--Finnhub SEC API key {i + 1} rate limited")
                            continue
                        else:
                            text = await resp.text()
                            print(f"-- {resp.status}: SEC key {i + 1}: {text}")
                            continue
            except Exception as e:
                print(f"--Error fetching SEC filings with key {i + 1}: {e}")
                continue

        print("---All Finnhub API keys failed for SEC filings")
        return None

    def filter_major_filings(self, filings):
        """Filter to only include major SEC forms"""
        return [f for f in filings if f.get('form', '') in MAJOR_FORMS]

    def build_filings_day_embeds(self, date, filings_list):
        """Building Discord embeds for a day's filings - splits if too large"""
        try:
            date_obj = datetime.strptime(date, '%Y-%m-%d')
            formatted_date = date_obj.strftime('%B %d, %Y (%A)')
        except:
            formatted_date = date

        entries = []
        for filing in filings_list:
            symbol = filing.get('symbol', 'N/A')
            form = filing.get('form', 'Unknown')
            report_url = filing.get('reportUrl', '')

            # color code by form type
            form_emoji = {
                '10-K': '📊',   # annual report
                '10-Q': '📋',   # quarterly report
                '8-K': '⚡',    # current report (material events)
                'S-1': '🆕',    # IPO registration
            }.get(form, '📄')

            entry = f"{form_emoji} **{symbol}** — {form}"
            if report_url:
                entry += f" • [View Filing]({report_url})"
            entries.append(entry)

        embeds = []
        chunk_size = 25

        for i in range(0, len(entries), chunk_size):
            chunk = entries[i:i + chunk_size]
            part_num = (i // chunk_size) + 1
            total_parts = (len(entries) + chunk_size - 1) // chunk_size

            if total_parts > 1:
                title = f"📑 SEC Filings: {formatted_date} (Part {part_num}/{total_parts})"
            else:
                title = f"📑 SEC Filings: {formatted_date}"

            embed = discord.Embed(
                title=title,
                description=f"**{len(filings_list)} filings** submitted" if part_num == 1 else None,
                color=discord.Color.dark_gold(),
                timestamp=datetime.now(timezone.utc)
            )
            embed.add_field(name="Filings", value="\n".join(chunk) if chunk else "None", inline=False)
            embed.set_footer(text="📊 10-K Annual • 📋 10-Q Quarterly • ⚡ 8-K Material Event • 🆕 S-1 IPO • Data from Finnhub")
            embeds.append(embed)

        return embeds

    @tasks.loop(hours=24)
    async def post_daily_filings(self):
        """Post SEC filings once daily"""
        await self.bot.wait_until_ready()

        try:
            filings_data = await self.fetch_sec_filings()
            if not filings_data:
                print("Failed to fetch SEC filings")
                return

            # filter for major forms only
            filings = self.filter_major_filings(filings_data)

            if not filings:
                for guild in self.bot.guilds:
                    channel = discord.utils.get(guild.text_channels, name="sec-filings-dashboard")
                    if not channel:
                        continue
                    try:
                        await channel.purge(limit=None)
                        embed = discord.Embed(
                            title="📑 SEC Filings (Past 28 Days)",
                            description="No major SEC filings (10-K, 10-Q, 8-K, S-1) found in the past 28 days.",
                            color=discord.Color.dark_gold(),
                            timestamp=datetime.now(timezone.utc)
                        )
                        embed.set_footer(text="Data from Finnhub")
                        await channel.send(embed=embed)
                    except (discord.Forbidden, discord.HTTPException):
                        pass
                return
            
            # group by filed date
            by_date = {}
            for filing in filings:

                filed_date = filing.get('filedDate', 'Unknown')
                if ' ' in filed_date:
                    filed_date = filed_date.split(' ')[0]
                if filed_date not in by_date:
                    by_date[filed_date] = []
                by_date[filed_date].append(filing)

            for guild in self.bot.guilds:
                channel = discord.utils.get(guild.text_channels, name="sec-filings-dashboard")
                if not channel:
                    continue

                try:
                    await channel.purge(limit=None)

                    # Summary embed
                    summary = discord.Embed(
                        title="📑 SEC Filings (Past 28 Days)",
                        description=f"**Total: {len(filings)} major filings** (10-K, 10-Q, 8-K, S-1)",
                        color=discord.Color.dark_gold(),
                        timestamp=datetime.now(timezone.utc)
                    )

                    # count by form type
                    form_counts = {}
                    for f in filings:
                        form = f.get('form', 'Unknown')
                        form_counts[form] = form_counts.get(form, 0) + 1

                    breakdown = "\n".join(f"• **{form}**: {count}" for form, count in sorted(form_counts.items()))
                    summary.add_field(name="By Form Type", value=breakdown or "None", inline=True)

                    # daily breakdown
                    lines = []
                    for date in sorted(by_date.keys(), reverse=True):
                        try:
                            day_name = datetime.strptime(date, '%Y-%m-%d').strftime('%a %m/%d')
                        except:
                            day_name = date
                        lines.append(f"• **{day_name}**: {len(by_date[date])} filings")

                    summary.add_field(
                        name="By Date",
                        value="\n".join(lines) if lines else "None",
                        inline=True
                    )

                    await channel.send(embed=summary)
                    await asyncio.sleep(0.5)

                    # Post each day's filings (newest first)
                    for date in sorted(by_date.keys(), reverse=True):
                        embeds = self.build_filings_day_embeds(date, by_date[date])
                        for embed in embeds:
                            await channel.send(embed=embed)
                            await asyncio.sleep(0.5)

                    print(f"Posted {len(by_date)} day(s) of SEC filings to {guild.name}")

                except discord.Forbidden:
                    print(f"No permission to post in sec-filings-dashboard in {guild.name}")
                except discord.HTTPException as e:
                    print(f"Failed to post SEC filings in {guild.name}: {e}")

        except Exception as e:
            print(f"SEC filings error: {e}")

    @post_daily_filings.before_loop
    async def before_daily_filings(self):
        """Wait for bot to be ready and channels to be created"""
        await self.bot.wait_until_ready()
        await asyncio.sleep(15)


async def setup(bot):
    await bot.add_cog(SECFilings(bot))