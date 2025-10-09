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

from config import Config
from src.utils.s3_uploader import S3Uploader
from src.collectors import CoinGeckoCollector, BinanceCollector, CoinbaseCollector

logger = logging.getLogger(__name__)


class CryptoDataPipeline:
    """Main pipeline orchestrator"""
    
    def __init__(self, bucket_name=None, aws_access_key=None, aws_secret_key=None,
                 region=None, profile_name=None):
        """
        Initialize pipeline with collectors and S3 uploader
        
        Args:
            bucket_name: S3 bucket name
            aws_access_key: AWS access key (optional)
            aws_secret_key: AWS secret key (optional)
            region: AWS region (optional)
            profile_name: AWS profile name (optional)
        """
        self.bucket_name = bucket_name or Config.S3_BUCKET
        self.region = region or Config.AWS_REGION
        
        # Initialize S3 uploader
        self.s3_uploader = S3Uploader(
            bucket_name=self.bucket_name,
            aws_access_key=aws_access_key,
            aws_secret_key=aws_secret_key,
            region=self.region,
            profile_name=profile_name
        )
        
        # Initialize collectors
        self.coingecko = CoinGeckoCollector()
        self.binance = BinanceCollector()
        self.coinbase = CoinbaseCollector()
        
        logger.info("Pipeline initialized with all collectors")
    
    def _upload_json(self, data, s3_key):
        """Upload JSON data to S3"""
        json_str = json.dumps(data, indent=2)
        return self.s3_uploader.upload(
            json_str.encode('utf-8'),
            s3_key,
            content_type='application/json'
        )
    
    def _upload_dataframe(self, df, s3_key):
        """Upload DataFrame as CSV to S3"""
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        return self.s3_uploader.upload(
            csv_buffer.getvalue().encode('utf-8'),
            s3_key,
            content_type='text/csv'
        )
    
    def _generate_s3_key(self, data_type, source, **kwargs):
        """Generate partitioned S3 key"""
        timestamp = datetime.utcnow()
        base_path = f"crypto/{data_type}/source={source}"
        date_path = f"year={timestamp.year}/month={timestamp.month:02d}/day={timestamp.day:02d}"
        
        # Add additional partitions
        extra_parts = '/'.join([f"{k}={v}" for k, v in kwargs.items()])
        if extra_parts:
            base_path = f"{base_path}/{extra_parts}"
        
        filename = f"{data_type}_{timestamp.strftime('%Y%m%d_%H%M%S')}.json"
        
        return f"{base_path}/{date_path}/{filename}"
    
    def collect_coingecko_prices(self):
        """Collect spot prices from CoinGecko"""
        try:
            data = self.coingecko.get_spot_prices()
            s3_key = self._generate_s3_key('spot_prices', 'coingecko')
            self._upload_json(data, s3_key)
            logger.info("✓ Collected CoinGecko spot prices")
        except Exception as e:
            logger.error(f"✗ Failed to collect CoinGecko prices: {str(e)}")
    
    def collect_coingecko_markets(self):
        """Collect market data from CoinGecko"""
        try:
            data = self.coingecko.get_market_data(per_page=100)
            df = pd.DataFrame(data)
            
            timestamp = datetime.utcnow()
            s3_key = f"crypto/market_data/source=coingecko/year={timestamp.year}/month={timestamp.month:02d}/day={timestamp.day:02d}/markets_{timestamp.strftime('%Y%m%d_%H%M%S')}.csv"
            
            self._upload_dataframe(df, s3_key)
            logger.info(f"✓ Collected market data for {len(df)} coins")
        except Exception as e:
            logger.error(f"✗ Failed to collect market data: {str(e)}")
    
    def collect_binance_tickers(self):
        """Collect 24hr tickers from Binance"""
        try:
            data = self.binance.get_24hr_ticker()
            s3_key = self._generate_s3_key('tickers', 'binance')
            self._upload_json(data, s3_key)
            logger.info("✓ Collected Binance 24hr tickers")
        except Exception as e:
            logger.error(f"✗ Failed to collect Binance tickers: {str(e)}")
    
    def collect_binance_klines(self, symbol='BTCUSDT', interval='1h'):
        """Collect OHLCV data from Binance"""
        try:
            data = self.binance.get_klines(symbol=symbol, interval=interval, limit=100)
            df = pd.DataFrame(data, columns=[
                'open_time', 'open', 'high', 'low', 'close', 'volume',
                'close_time', 'quote_volume', 'trades', 'taker_buy_base',
                'taker_buy_quote', 'ignore'
            ])
            
            df['open_time'] = pd.to_datetime(df['open_time'], unit='ms')
            df['close_time'] = pd.to_datetime(df['close_time'], unit='ms')
            
            numeric_cols = ['open', 'high', 'low', 'close', 'volume', 'quote_volume']
            df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric)
            
            timestamp = datetime.utcnow()
            s3_key = f"crypto/ohlcv/source=binance/symbol={symbol}/interval={interval}/year={timestamp.year}/month={timestamp.month:02d}/ohlcv_{timestamp.strftime('%Y%m%d_%H%M%S')}.csv"
            
            self._upload_dataframe(df, s3_key)
            logger.info(f"✓ Collected {len(df)} klines for {symbol}")
        except Exception as e:
            logger.error(f"✗ Failed to collect klines: {str(e)}")
    
    def collect_coinbase_orderbook(self, product_id='BTC-USD'):
        """Collect order book from Coinbase"""
        try:
            data = self.coinbase.get_orderbook(product_id=product_id, level=2)
            s3_key = self._generate_s3_key('orderbook', 'coinbase', product=product_id)
            self._upload_json(data, s3_key)
            logger.info(f"✓ Collected Coinbase orderbook for {product_id}")
        except Exception as e:
            logger.error(f"✗ Failed to collect orderbook: {str(e)}")
    
    def collect_all_data(self):
        """Execute all data collection tasks"""
        logger.info("=" * 70)
        logger.info("Starting data collection cycle")
        logger.info("=" * 70)
        
        # CoinGecko
        self.collect_coingecko_prices()
        time.sleep(2)
        
        self.collect_coingecko_markets()
        time.sleep(2)
        
        # Binance
        self.collect_binance_tickers()
        time.sleep(2)
        
        self.collect_binance_klines()
        time.sleep(2)
        
        # Coinbase
        self.collect_coinbase_orderbook()
        
        logger.info("=" * 70)
        logger.info("Completed data collection cycle")
        logger.info("=" * 70)