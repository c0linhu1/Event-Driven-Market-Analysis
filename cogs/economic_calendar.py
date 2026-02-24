import discord
from discord.ext import commands, tasks
from discord import app_commands
import aiohttp
from datetime import datetime, timedelta, timezone
import os
from dotenv import load_dotenv
import asyncio

load_dotenv()

FMP_API_KEYS = [os.getenv(f"FMP_API_KEY_{i}") for i in range(1, 9)]


class EconomicCalendar(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.post_daily_economic.start()

    def cog_unload(self):
        self.post_daily_economic.cancel()

    async def fetch_economic_calendar(self, days_ahead = 28):
        """Fetching economic calendar from FMP"""
        today = datetime.now().strftime('%Y-%m-%d')
        future_date = (datetime.now() + timedelta(days=days_ahead)).strftime('%Y-%m-%d')

        for i, api_key in enumerate(FMP_API_KEYS):
            if not api_key:
                continue
            url = f"https://financialmodelingprep.com/api/v3/economic_calendar?from={today}&to={future_date}&apikey={api_key}"
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(url) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            print(f"*** Successfully fetched economic calendar with FMP key {i + 1}")
                            return data
                        elif resp.status == 429:
                            print(f"--FMP economic key {i + 1} rate limited")
                            continue
                        else:
                            text = await resp.text()
                            print(f"-- {resp.status}: FMP economic key {i + 1}: {text}")
                            continue
            except Exception as e:
                print(f"--Error fetching economic calendar with FMP key {i + 1}: {e}")
                continue

        print("---All FMP API keys failed for economic calendar")
        return None

    def get_impact_emoji(self, impact):
        """Emoji based on event impact level"""
        impact = (impact or '').lower()
        if impact == 'high':
            return '🔴'
        elif impact == 'medium':
            return '🟡'
        elif impact == 'low':
            return '🟢'
        return '⚪'

    def build_economic_day_embeds(self, date, events_list):
        """Building Discord embeds for a day's economic events"""
        try:
            date_obj = datetime.strptime(date, '%Y-%m-%d')
            formatted_date = date_obj.strftime('%B %d, %Y (%A)')
        except:
            formatted_date = date

        entries = []
        for event in events_list:
            name = event.get('event', 'Unknown Event')
            country = event.get('country', '')
            impact = event.get('impact', '')
            estimate = event.get('estimate')
            previous = event.get('previous')
            actual = event.get('actual')

            emoji = self.get_impact_emoji(impact)
            entry = f"{emoji} **{name}**"
            if country:
                entry += f" ({country})"

            details = []
            if estimate is not None and estimate != '':
                details.append(f"Est: {estimate}")
            if previous is not None and previous != '':
                details.append(f"Prev: {previous}")
            if actual is not None and actual != '':
                details.append(f"Act: {actual}")
            if details:
                entry += f" • {' | '.join(details)}"

            entries.append(entry)

        embeds = []
        chunk_size = 25

        for i in range(0, len(entries), chunk_size):
            chunk = entries[i:i + chunk_size]
            part_num = (i // chunk_size) + 1
            total_parts = (len(entries) + chunk_size - 1) // chunk_size

            if total_parts > 1:
                title = f"📆 Economic Events: {formatted_date} (Part {part_num}/{total_parts})"
            else:
                title = f"📆 Economic Events: {formatted_date}"

            embed = discord.Embed(
                title=title,
                description=f"**{len(events_list)} events** scheduled" if part_num == 1 else None,
                color=discord.Color.orange(),
                timestamp=datetime.now(timezone.utc)
            )
            embed.add_field(name="Events", value="\n".join(chunk) if chunk else "None", inline=False)
            embed.set_footer(text="🔴 High Impact • 🟡 Medium • 🟢 Low • Data from FMP")
            embeds.append(embed)

        return embeds

    @tasks.loop(hours=24)
    async def post_daily_economic(self):
        """Post economic calendar once daily"""
        await self.bot.wait_until_ready()

        try:
            events = await self.fetch_economic_calendar()

            # handle no data
            if not events:
                for guild in self.bot.guilds:
                    channel = discord.utils.get(guild.text_channels, name="economic-calendar-dashboard")
                    if not channel:
                        continue
                    try:
                        await channel.purge(limit=None)
                        embed = discord.Embed(
                            title="📆 Economic Calendar (Next 28 Days)",
                            description="No economic events found for the next 28 days.",
                            color=discord.Color.orange(),
                            timestamp=datetime.now(timezone.utc)
                        )
                        embed.set_footer(text="Data from FMP")
                        await channel.send(embed=embed)
                    except (discord.Forbidden, discord.HTTPException):
                        pass
                return

            by_date = {}
            for event in events:
                raw_date = event.get('date', 'Unknown')
                d = raw_date.split(' ')[0] if ' ' in raw_date else raw_date
                if d not in by_date:
                    by_date[d] = []
                by_date[d].append(event)

            for guild in self.bot.guilds:
                channel = discord.utils.get(guild.text_channels, name="economic-calendar-dashboard")
                if not channel:
                    continue

                try:
                    await channel.purge(limit=None)

                    # Summary embed
                    summary = discord.Embed(
                        title="📆 Economic Calendar (Next 28 Days)",
                        description=f"**Total: {len(events)} events** scheduled",
                        color=discord.Color.orange(),
                        timestamp=datetime.now(timezone.utc)
                    )

                    # count high impact events
                    high_impact = sum(1 for e in events if (e.get('impact') or '').lower() == 'high')
                    summary.add_field(
                        name="⚠️ High Impact Events",
                        value=f"**{high_impact}** high-impact events this week",
                        inline=False
                    )

                    lines = []
                    for date in sorted(by_date.keys()):
                        try:
                            day_name = datetime.strptime(date, '%Y-%m-%d').strftime('%a %m/%d')
                        except:
                            day_name = date
                        day_high = sum(1 for e in by_date[date] if (e.get('impact') or '').lower() == 'high')
                        high_tag = f" 🔴 {day_high}" if day_high > 0 else ""
                        lines.append(f"• **{day_name}**: {len(by_date[date])} events{high_tag}")

                    summary.add_field(
                        name="Daily Breakdown",
                        value="\n".join(lines) if lines else "No events",
                        inline=False
                    )

                    await channel.send(embed=summary)
                    await asyncio.sleep(0.5)

                    # Post each day's events
                    for date in sorted(by_date.keys()):
                        embeds = self.build_economic_day_embeds(date, by_date[date])
                        for embed in embeds:
                            await channel.send(embed=embed)
                            await asyncio.sleep(0.5)

                    print(f"Posted {len(by_date)} day(s) of economic events to {guild.name}")

                except discord.Forbidden:
                    print(f"No permission to post in economic-calendar-dashboard in {guild.name}")
                except discord.HTTPException as e:
                    print(f"Failed to post economic calendar in {guild.name}: {e}")

        except Exception as e:
            print(f"Economic calendar error: {e}")

    @post_daily_economic.before_loop
    async def before_daily_economic(self):
        """Wait for bot to be ready and channels to be created"""
        await self.bot.wait_until_ready()
        await asyncio.sleep(12)


async def setup(bot):
    await bot.add_cog(EconomicCalendar(bot))