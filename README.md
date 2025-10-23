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


## 🔧 Exploitation avec AWS

### Architecture Recommandée
```
┌─────────────────────────────────────────────────────────────┐
│                         S3 Bucket                           │
│  crypto/tabular/spot_prices/...                             │
│  crypto/tabular/tickers/...                                 │
│  crypto/tabular/ohlcv/...                                   │
└─────────────┬───────────────────────────────────────────────┘
              │
              ↓
┌─────────────────────────────────────────────────────────────┐
│              AWS Glue Crawler (Auto-detection)              │
│  - Détecte le schéma des CSV                                │
│  - Crée des tables dans le Data Catalog                     │
│  - Gère le partitionnement (year/month/day)                 │
└─────────────┬───────────────────────────────────────────────┘
              │
              ↓
┌─────────────────────────────────────────────────────────────┐
│              AWS Glue Data Catalog                          │
│  Tables:                                                     │
│  - crypto_spot_prices                                       │
│  - crypto_tickers                                           │
│  - crypto_ohlcv                                             │
└─────────────┬───────────────────────────────────────────────┘
              │
              ↓
┌─────────────────────────────────────────────────────────────┐
│                    AWS Athena                               │
│  Requêtes SQL sur S3 (serverless)                           │
└─────────────┬───────────────────────────────────────────────┘
              │
              ↓
┌─────────────────────────────────────────────────────────────┐
│           Visualisation & Analytics                         │
│  - QuickSight (BI)                                          │
│  - Python/Pandas                                            │
│  - Jupyter Notebooks                                        │
└─────────────────────────────────────────────────────────────┘