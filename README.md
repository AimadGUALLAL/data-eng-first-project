# Crypto Data Pipeline

Automated cryptocurrency data collection pipeline that collects data from multiple sources and stores it in AWS S3 with proper partitioning.

## Features

- 📊 **Multiple Data Sources**: CoinGecko, Binance, Coinbase
- 🔄 **Automated Collection**: Scheduled data collection
- ☁️ **AWS S3 Storage**: Organized with date-based partitioning
- 🔧 **Modular Design**: Easy to extend with new collectors
- 📝 **Comprehensive Logging**: Track all operations

## Data Collected

### CoinGecko
- Spot prices (USD, EUR, BTC)
- Market cap and 24h volume
- Top 100 cryptocurrencies market data

### Binance
- 24hr ticker statistics
- OHLCV candlestick data
- Order book depth

### Coinbase
- Order book snapshots
- Ticker information
- 24hr statistics

## Quick Start