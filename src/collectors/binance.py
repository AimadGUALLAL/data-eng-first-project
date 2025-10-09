"""
Binance data collector
API Docs: https://binance-docs.github.io/apidocs/spot/en/
"""
import requests
import logging
import time
from datetime import datetime

logger = logging.getLogger(__name__)


class BinanceCollector:
    """Collect cryptocurrency data from Binance API"""
    
    BASE_URL = "https://api.binance.com/api/v3"
    
    def __init__(self, api_key=None, api_secret=None):
        """
        Initialize Binance collector
        
        Args:
            api_key: Optional API key for authenticated endpoints
            api_secret: Optional API secret
        """
        self.api_key = api_key
        self.api_secret = api_secret
        self.session = requests.Session()
        
        if api_key:
            self.session.headers.update({'X-MBX-APIKEY': api_key})
    
    def get_24hr_ticker(self, symbols=None):
        """
        Get 24hr ticker statistics
        
        Args:
            symbols: List of trading pairs (e.g., ['BTCUSDT', 'ETHUSDT'])
            
        Returns:
            dict: Ticker data with metadata
        """
        if symbols is None:
            symbols = ['BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'SOLUSDT', 'ADAUSDT']
        
        try:
            all_tickers = []
            
            for symbol in symbols:
                url = f"{self.BASE_URL}/ticker/24hr"
                params = {'symbol': symbol}
                
                response = self.session.get(url, params=params, timeout=10)
                response.raise_for_status()
                all_tickers.append(response.json())
                
                time.sleep(0.1)  # Rate limiting
            
            return {
                'collected_at': datetime.utcnow().isoformat(),
                'source': 'binance',
                'data_type': '24hr_ticker',
                'data': all_tickers
            }
            
        except requests.RequestException as e:
            logger.error(f"Binance ticker error: {str(e)}")
            raise
    
    def get_klines(self, symbol='BTCUSDT', interval='1h', limit=100):
        """
        Get candlestick (OHLCV) data
        
        Args:
            symbol: Trading pair
            interval: Timeframe (1m, 5m, 15m, 30m, 1h, 4h, 1d, 1w, 1M)
            limit: Number of candles (max 1000)
            
        Returns:
            list: OHLCV data
        """
        try:
            url = f"{self.BASE_URL}/klines"
            params = {
                'symbol': symbol,
                'interval': interval,
                'limit': min(limit, 1000)
            }
            
            response = self.session.get(url, params=params, timeout=15)
            response.raise_for_status()
            
            return response.json()
            
        except requests.RequestException as e:
            logger.error(f"Binance klines error: {str(e)}")
            raise
    
    def get_orderbook(self, symbol='BTCUSDT', limit=100):
        """
        Get order book depth
        
        Args:
            symbol: Trading pair
            limit: Depth (5, 10, 20, 50, 100, 500, 1000, 5000)
            
        Returns:
            dict: Order book data
        """
        try:
            url = f"{self.BASE_URL}/depth"
            params = {
                'symbol': symbol,
                'limit': limit
            }
            
            response = self.session.get(url, params=params, timeout=10)
            response.raise_for_status()
            
            return response.json()
            
        except requests.RequestException as e:
            logger.error(f"Binance orderbook error: {str(e)}")
            raise
    
    def get_avg_price(self, symbol='BTCUSDT'):
        """
        Get current average price
        
        Args:
            symbol: Trading pair
            
        Returns:
            dict: Average price data
        """
        try:
            url = f"{self.BASE_URL}/avgPrice"
            params = {'symbol': symbol}
            
            response = self.session.get(url, params=params, timeout=10)
            response.raise_for_status()
            
            return response.json()
            
        except requests.RequestException as e:
            logger.error(f"Binance avg price error: {str(e)}")
            raise