import discord
from discord.ext import commands, tasks
import aiohttp
from datetime import datetime, timedelta, timezone
import os
from dotenv import load_dotenv
import asyncio

load_dotenv()

RAPID_API_KEY = os.getenv("RAPID_API_KEY_1")


class EconomicCalendar(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.post_daily_economic.start()

    def cog_unload(self):
        self.post_daily_economic.cancel()

    async def fetch_economic_calendar(self):
        """Fetching economic calendar from RapidAPI Trader Calendar"""
        url = "https://trader-calendar.p.rapidapi.com/api/calendar"
        headers = {
            "Content-Type": "application/json",
            "x-rapidapi-host": "trader-calendar.p.rapidapi.com",
            "x-rapidapi-key": RAPID_API_KEY
        }
        payload = {"country": "USA"}

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=payload, headers=headers) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        print(f"*** Successfully fetched economic calendar ({len(data)} total events)")
                        return data
                    else:
                        text = await resp.text()
                        print(f"-- Economic calendar API error {resp.status}: {text}")
                        return None
        except Exception as e:
            print(f"--Error fetching economic calendar: {e}")
            return None

    def filter_upcoming_events(self, events, days_ahead=28):
        """Filter events to only include the next 28 days"""
        now = datetime.now(timezone.utc)
        cutoff = now + timedelta(days=days_ahead)

        upcoming = []
        for event in events:
            start_str = event.get('start', '')
            if not start_str:
                continue
            try:
                # parsing ISO format with timezone offset
                event_time = datetime.fromisoformat(start_str)

                if event_time.tzinfo is None:
                    event_time = event_time.replace(tzinfo=timezone.utc)
                # convert to UTC for comparison
                event_utc = event_time.astimezone(timezone.utc)

                if now <= event_utc <= cutoff:
                    event['parsed_date'] = event_utc.strftime('%Y-%m-%d')
                    event['parsed_time'] = event_utc.strftime('%I:%M %p')
                    upcoming.append(event)
            except (ValueError, TypeError):
                continue

        return upcoming

    def get_importance_emoji(self, importance):
        """Emoji based on importance level"""
        if importance >= 5:
            return '🔴'
        elif importance >= 3:
            return '🟡'
        else:
            return '🟢'

    def build_economic_day_embeds(self, date, events_list):
        """Building Discord embeds for a day's economic events"""
        try:
            date_obj = datetime.strptime(date, '%Y-%m-%d')
            formatted_date = date_obj.strftime('%B %d, %Y (%A)')
        except:
            formatted_date = date

        # sort by time
        events_list.sort(key=lambda e: e.get('start', ''))

        entries = []
        for event in events_list:
            title = event.get('title', 'Unknown Event')
            importance = event.get('importance', 0)
            time_str = event.get('parsed_time', '')
            short_desc = event.get('shortDesc', '')

            emoji = self.get_importance_emoji(importance)
            entry = f"{emoji} **{title}**"
            if time_str:
                entry += f" • {time_str} ET"
            if short_desc:
                entry += f"\n   _{short_desc}_"
            entries.append(entry)

        embeds = []
        chunk_size = 15 

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
            embed.set_footer(text="🔴 High Impact • 🟡 Medium • 🟢 Low • Data from Trader Calendar")
            embeds.append(embed)

        return embeds

    @tasks.loop(hours=24)
    async def post_daily_economic(self):
        """Post economic calendar once daily"""
        await self.bot.wait_until_ready()

        try:
            all_events = await self.fetch_economic_calendar()

            if not all_events:
                for guild in self.bot.guilds:
                    channel = discord.utils.get(guild.text_channels, name="economic-calendar-dashboard")
                    if not channel:
                        continue
                    try:
                        await channel.purge(limit=None)
                        embed = discord.Embed(
                            title="📆 Economic Calendar (Next 28 Days)",
                            description="Unable to fetch economic calendar data.",
                            color=discord.Color.orange(),
                            timestamp=datetime.now(timezone.utc)
                        )
                        embed.set_footer(text="Data from Trader Calendar")
                        await channel.send(embed=embed)
                    except (discord.Forbidden, discord.HTTPException):
                        pass
                return

            # filter to next 7 days only
            upcoming = self.filter_upcoming_events(all_events, days_ahead=7)

            if not upcoming:
                for guild in self.bot.guilds:
                    channel = discord.utils.get(guild.text_channels, name="economic-calendar-dashboard")
                    if not channel:
                        continue
                    try:
                        await channel.purge(limit=None)
                        embed = discord.Embed(
                            title="📆 Economic Calendar (Next 28 Days)",
                            description="No economic events scheduled in the next 28 days.",
                            color=discord.Color.orange(),
                            timestamp=datetime.now(timezone.utc)
                        )
                        embed.set_footer(text="Data from Trader Calendar")
                        await channel.send(embed=embed)
                    except (discord.Forbidden, discord.HTTPException):
                        pass
                return

            # group by date
            by_date = {}
            for event in upcoming:
                d = event.get('parsed_date', 'Unknown')
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
                        description=f"**Total: {len(upcoming)} events** scheduled",
                        color=discord.Color.orange(),
                        timestamp=datetime.now(timezone.utc)
                    )

                    high_impact = sum(1 for e in upcoming if e.get('importance', 0) >= 5)
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
                        day_high = sum(1 for e in by_date[date] if e.get('importance', 0) >= 5)
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