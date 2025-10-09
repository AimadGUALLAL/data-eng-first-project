"""
Coinbase data collector
API Docs: https://docs.cloud.coinbase.com/exchange/docs
"""
import requests
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


class CoinbaseCollector:
    """Collect cryptocurrency data from Coinbase Exchange API"""
    
    BASE_URL = "https://api.exchange.coinbase.com"
    
    def __init__(self):
        """Initialize Coinbase collector"""
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': 'CryptoDataPipeline/1.0'})
    
    def get_orderbook(self, product_id='BTC-USD', level=2):
        """
        Get order book for a product
        
        Args:
            product_id: Trading pair (e.g., 'BTC-USD', 'ETH-USD')
            level: Depth level (1: best bid/ask, 2: top 50, 3: full book)
            
        Returns:
            dict: Order book data with metadata
        """
        try:
            url = f"{self.BASE_URL}/products/{product_id}/book"
            params = {'level': level}
            
            response = self.session.get(url, params=params, timeout=10)
            response.raise_for_status()
            
            return {
                'collected_at': datetime.utcnow().isoformat(),
                'source': 'coinbase',
                'product_id': product_id,
                'level': level,
                'data': response.json()
            }
            
        except requests.RequestException as e:
            logger.error(f"Coinbase orderbook error: {str(e)}")
            raise
    
    def get_ticker(self, product_id='BTC-USD'):
        """
        Get ticker information
        
        Args:
            product_id: Trading pair
            
        Returns:
            dict: Ticker data
        """
        try:
            url = f"{self.BASE_URL}/products/{product_id}/ticker"
            
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            
            return response.json()
            
        except requests.RequestException as e:
            logger.error(f"Coinbase ticker error: {str(e)}")
            raise
    
    def get_stats(self, product_id='BTC-USD'):
        """
        Get 24hr stats
        
        Args:
            product_id: Trading pair
            
        Returns:
            dict: 24hr statistics
        """
        try:
            url = f"{self.BASE_URL}/products/{product_id}/stats"
            
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            
            return response.json()
            
        except requests.RequestException as e:
            logger.error(f"Coinbase stats error: {str(e)}")
            raise
    
    def get_products(self):
        """
        Get list of available trading pairs
        
        Returns:
            list: Available products
        """
        try:
            url = f"{self.BASE_URL}/products"
            
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            
            return response.json()
            
        except requests.RequestException as e:
            logger.error(f"Coinbase products error: {str(e)}")
            raise