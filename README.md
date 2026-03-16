# NOT UPDATED 
# Discord Stock Bot

A Discord bot that provides financial news and stock market information. Implements multiple user commands and features to help beginner traders.

## Features
- Real-time financial news from Finnhub API, Marketaux API, and Twitter API
    - Posts from news sources such as CNBC, CoinDesk, Financial Post, MarketWatch, 
      BusinessWire, financial twitter accounts, etc. 
- Stock price tracking and watchlists
- Automated news updates every few minutes
- Private watchlist channels for users  
- Private portfolio channel for users
- Interactive stock information buttons
- Commands relating to managing watchlists and portfolios 


### Prerequisites
- Python 3.8+
- Discord Bot Token
- Finnhub API Keys
- Marketaux API Keys
- Twitter API Keys


## Required Discord Bot Permissions
- Read Messages/View Channels
- Send Messages
- Manage Channels
- Manage Messages
- Embed Links
- Use Slash Commands

## Commands
- `/watchlist` - Create a private watchlist channel
- `/add_company` - Add a stock to your watchlist
- `/remove_company` - Remove a stock from your watchlist
- `/show_watchlist` - Display your watchlist with interactive buttons
- `/delete_watchlist` - Delete your private watchlist channel

## Twitter API
- The api keys for twitter are the BEARER TOKENS
- Free Twitter API has a 15 min cooldown time for each dev account 
- Prone to account restricting bc I am using multiple api keys - cooldown for a week before running again if one devleoper account gets retricted

## FUTURE IMPLEMENTATIONS
- Applying machine learning to analyze sentiment from articles
- Incorporating a sentiment command and a scale for users to see whether or 
not a stock is a buy hold sell based on sentiment score
- Either use api again or web scrape to make a channel for Top Gainers and Losers of the day. Will try to do for the week 
too - could technically try to do it myself but would require too many api calls when fetching stop price info 
- possibly create graphs based on daily info for past week/month for stock - dont want to make intraday chart / minute chart bc would require a lot of api calls again 
- implement forex/crypto commands 
- implement ipo commands or a channel that shows upcoming ipos
- implement a channel for sec fillings 
- implement personal trading strategy - maybe llm - using 9/20 ema, 50 sma