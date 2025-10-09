"""
Crypto Data Pipeline - Main Entry Point

Usage:
    python main.py              # Run with default config
    python main.py --once       # Run once and exit
    python main.py --test       # Test configuration only
"""
import sys
import argparse
from datetime import datetime
import pandas as pd
from config import Config
from src.utils.logger import setup_logger
from src.pipeline import DataCollectorWithDedup

# Setup logger
logger = setup_logger(
    name='crypto_pipeline',
    log_level=Config.LOG_LEVEL,
    log_file=f'logs/pipeline_{datetime.now().strftime("%Y%m%d")}.log'
)


# def test_configuration():
#     """Test configuration and connections"""
#     logger.info("Testing configuration...")
    
#     try:
#         Config.validate()
#         Config.print_config()
        
#         # Test S3 connection
#         pipeline = CryptoDataPipeline(
#             bucket_name=Config.S3_BUCKET,
#             region=Config.AWS_REGION,
#             aws_access_key= Config.AWS_ACCESS_KEY_ID,
#             aws_secret_key=Config.AWS_SECRET_ACCESS_KEY
#         )
        
#         logger.info("✓ Configuration test passed!")
#         return True
        
#     except Exception as e:
#         logger.error(f"✗ Configuration test failed: {str(e)}")
#         return False






# Les fonctions qui vont etre consommées par Airflow
def collect_coingecko_prices():
    """Tâche 1: Collecter les prix CoinGecko"""
    logger.info("=== Collecte des prix CoinGecko ===")
    
    collector = DataCollectorWithDedup()
    timestamp = datetime.utcnow()
    
    # Collecter les données
    coin_ids = ['bitcoin', 'ethereum', 'cardano', 'solana', 'binancecoin']
    data = collector.coingecko.get_spot_prices(coin_ids)
    
    # Convertir en format tabulaire
    records = []
    for coin_id, prices in data['data'].items():
        record = {
            'collected_at': timestamp.isoformat(),
            'coin_id': coin_id,
            'price_usd': prices.get('usd'),
            'price_eur': prices.get('eur'),
            'market_cap': prices.get('usd_market_cap'),
            'volume_24h': prices.get('usd_24h_vol'),
            'change_24h_pct': prices.get('usd_24h_change'),
        }
        
        # Hash pour déduplication
        hash_data = {
            'coin': coin_id,
            'price': prices.get('usd'),
            'time': timestamp.strftime('%Y-%m-%d %H:%M')
        }
        record['record_hash'] = collector._generate_hash(hash_data)
        records.append(record)
    
    df = pd.DataFrame(records)
    
    # Déduplication
    s3_prefix = "crypto/raw_data/spot_prices/source=coingecko/"
    existing_hashes = collector._get_recent_hashes(s3_prefix)
    
    if existing_hashes:
        df = df[~df['record_hash'].isin(existing_hashes)]
        logger.info(f"Après dédup: {len(df)} nouvelles lignes")
    
    if len(df) == 0:
        logger.info("Aucune nouvelle donnée")
        return "No new data"
    
    # Upload vers S3
    s3_key = f"{s3_prefix}year={timestamp.year}/month={timestamp.month:02d}/day={timestamp.day:02d}/prices_{timestamp.strftime('%Y%m%d_%H%M%S')}.csv"
    collector._upload_df(df, s3_key)
    
    # Sauvegarder pour résumé
    #['task_instance'].xcom_push(key='prices_count', value=len(df))
    
    return f"Collecté {len(df)} prix"


def collect_binance_tickers():
    """Tâche 2: Collecter les tickers Binance"""
    logger.info("=== Collecte des tickers Binance ===")
    
    collector = DataCollectorWithDedup()
    timestamp = datetime.utcnow()
    
    # Collecter les données
    symbols = ['BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'SOLUSDT', 'ADAUSDT']
    data = collector.binance.get_24hr_ticker(symbols)
    
    # Convertir en format tabulaire
    records = []
    for ticker in data['data']:
        record = {
            'collected_at': timestamp.isoformat(),
            'symbol': ticker['symbol'],
            'price': float(ticker['lastPrice']),
            'price_change_pct': float(ticker['priceChangePercent']),
            'high_24h': float(ticker['highPrice']),
            'low_24h': float(ticker['lowPrice']),
            'volume_24h': float(ticker['volume']),
            'trades_count': int(ticker['count']),
        }
        
        # Hash
        hash_data = {
            'symbol': ticker['symbol'],
            'time': timestamp.strftime('%Y-%m-%d %H:%M')
        }
        record['record_hash'] = collector._generate_hash(hash_data)
        records.append(record)
    
    df = pd.DataFrame(records)
    
    # Déduplication
    s3_prefix = "crypto/raw_data/tickers/source=binance/"
    existing_hashes = collector._get_recent_hashes(s3_prefix)
    
    if existing_hashes:
        df = df[~df['record_hash'].isin(existing_hashes)]
    
    if len(df) == 0:
        logger.info("Aucune nouvelle donnée")
        return "No new data"
    
    # Upload
    s3_key = f"{s3_prefix}year={timestamp.year}/month={timestamp.month:02d}/day={timestamp.day:02d}/tickers_{timestamp.strftime('%Y%m%d_%H%M%S')}.csv"
    collector._upload_df(df, s3_key)
    
    ##['task_instance'].xcom_push(key='tickers_count', value=len(df))
    
    return f"Collecté {len(df)} tickers"


def collect_binance_ohlcv():
    """Tâche 3: Collecter OHLCV Binance"""
    logger.info("=== Collecte OHLCV Binance ===")
    
    collector = DataCollectorWithDedup()
    timestamp = datetime.utcnow()
    
    # Collecter pour plusieurs symboles
    symbols = ['BTCUSDT', 'ETHUSDT']
    all_records = []
    
    for symbol in symbols:
        klines = collector.binance.get_klines(symbol=symbol, interval='1h', limit=24)
        
        for kline in klines:
            record = {
                'collected_at': timestamp.isoformat(),
                'symbol': symbol,
                'interval': '1h',
                'open_time': pd.to_datetime(kline[0], unit='ms'),
                'open': float(kline[1]),
                'high': float(kline[2]),
                'low': float(kline[3]),
                'close': float(kline[4]),
                'volume': float(kline[5]),
                'trades': int(kline[8]),
            }
            
            # Hash basé sur la bougie unique
            hash_data = {
                'symbol': symbol,
                'open_time': kline[0]
            }
            record['record_hash'] = collector._generate_hash(hash_data)
            all_records.append(record)
    
    df = pd.DataFrame(all_records)
    
    # Déduplication
    s3_prefix = "crypto/raw_data/ohlcv/source=binance/"
    existing_hashes = collector._get_recent_hashes(s3_prefix, days=30)
    
    if existing_hashes:
        df = df[~df['record_hash'].isin(existing_hashes)]
    
    if len(df) == 0:
        logger.info("Aucune nouvelle donnée")
        return "No new data"
    
    # Upload
    s3_key = f"{s3_prefix}year={timestamp.year}/month={timestamp.month:02d}/day={timestamp.day:02d}/ohlcv_{timestamp.strftime('%Y%m%d_%H%M%S')}.csv"
    collector._upload_df(df, s3_key)
    
    #['task_instance'].xcom_push(key='ohlcv_count', value=len(df))
    
    return f"Collecté {len(df)} bougies"







def run_once():
    """Run pipeline once and exit"""
    logger.info("Running pipeline once...")
    
    try:
        Config.validate()

        print(collect_coingecko_prices())
        print(collect_binance_tickers())
        print(collect_binance_ohlcv())
        
        #pipeline.collect_all_data()
        logger.info("✓ Single run completed successfully!")
        return True
        
    except Exception as e:
        logger.error(f"✗ Single run failed: {str(e)}", exc_info=True)
        return False
    


if __name__ == '__main__':
    run_once()