"""
Main data collection pipeline orchestrator
"""
import logging
import time
import json
import schedule
import pandas as pd
from datetime import datetime
from io import StringIO
import hashlib
from config import Config
from src.utils.s3_uploader import S3Uploader
from src.collectors import CoinGeckoCollector, BinanceCollector, CoinbaseCollector

logger = logging.getLogger(__name__)

class DataCollectorWithDedup:
    """Collecteur avec déduplication pour Airflow"""
    
    def __init__(self):
        # Charger la config depuis votre .env
        self.s3_uploader = S3Uploader(
            bucket_name=Config.S3_BUCKET,
            aws_access_key=Config.AWS_ACCESS_KEY_ID,
            aws_secret_key=Config.AWS_SECRET_ACCESS_KEY,
            region=Config.AWS_REGION
        )
        self.coingecko = CoinGeckoCollector()
        self.binance = BinanceCollector()
        self.coinbase = CoinbaseCollector()
    
    def _generate_hash(self, data_dict):
        """Générer un hash unique pour détecter les doublons"""
        data_str = json.dumps(data_dict, sort_keys=True)
        return hashlib.md5(data_str.encode()).hexdigest()
    
    def _get_recent_hashes(self, s3_prefix, days=7):
        """Récupérer les hash des derniers jours depuis S3"""
        try:
            # Lister les fichiers récents
            all_keys = self.s3_uploader.list_objects(prefix=s3_prefix)
            
            # Limiter aux 20 derniers fichiers
            recent_keys = sorted(all_keys)[-20:] if all_keys else []
            
            existing_hashes = set()
            for key in recent_keys:
                try:
                    # Télécharger et lire le CSV
                    obj = self.s3_uploader.s3_client.get_object(
                        Bucket=self.s3_uploader.bucket_name,
                        Key=key
                    )
                    df = pd.read_csv(obj['Body'])
                    
                    if 'record_hash' in df.columns:
                        existing_hashes.update(df['record_hash'].tolist())
                except Exception as e:
                    logger.warning(f"Erreur lecture {key}: {e}")
                    continue
            
            logger.info(f"Trouvé {len(existing_hashes)} hash existants")
            return existing_hashes
        
        except Exception as e:
            logger.error(f"Erreur récupération hashes: {e}")
            return set()
    
    def _upload_df(self, df, s3_key):
        """Upload DataFrame en CSV vers S3"""
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        
        self.s3_uploader.upload(
            data=csv_buffer.getvalue().encode('utf-8'),
            s3_key=s3_key,
            content_type='text/csv'
        )
        logger.info(f"✓ Uploadé {len(df)} lignes vers {s3_key}")



# Les fonctions qui vont etre consommées par Airflow
def collect_coingecko_prices(**context):
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
    context['task_instance'].xcom_push(key='prices_count', value=len(df))
    
    return f"Collecté {len(df)} prix"


def collect_binance_tickers(**context):
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
    
    context['task_instance'].xcom_push(key='tickers_count', value=len(df))
    
    return f"Collecté {len(df)} tickers"


def collect_binance_ohlcv(**context):
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
    
    context['task_instance'].xcom_push(key='ohlcv_count', value=len(df))
    
    return f"Collecté {len(df)} bougies"



if __name__ == '__main__':
    print(collect_coingecko_prices())
    print(collect_binance_tickers())
    print(collect_binance_ohlcv())
