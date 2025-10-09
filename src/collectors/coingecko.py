"""
CoinGecko data collector
Free API: https://www.coingecko.com/en/api
"""
import requests
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


class CoinGeckoCollector:
    """Collect cryptocurrency data from CoinGecko API"""
    
    BASE_URL = "https://api.coingecko.com/api/v3"
    
    def __init__(self, api_key=None):
        """
        Initialize CoinGecko collector
        
        Args:
            api_key: Optional API key for Pro features
        """
        self.api_key = api_key
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': 'CryptoDataPipeline/1.0'})
        
        if api_key:
            self.session.headers.update({'x-cg-pro-api-key': api_key})
    
    def get_spot_prices(self, coin_ids=None):
        """
        Get current spot prices
        
        Args:
            coin_ids: List of coin IDs (default: top 5)
            
        Returns:
            dict: Price data with metadata
        """
        if coin_ids is None:
            coin_ids = ['bitcoin', 'ethereum', 'cardano', 'solana', 'binancecoin']
        
        try:
            url = f"{self.BASE_URL}/simple/price"
            params = {
                'ids': ','.join(coin_ids),
                'vs_currencies': 'usd,eur,btc',
                'include_market_cap': 'true',
                'include_24hr_vol': 'true',
                'include_24hr_change': 'true',
                'include_last_updated_at': 'true'
            }
            
            response = self.session.get(url, params=params, timeout=10)
            response.raise_for_status()
            
            return {
                'collected_at': datetime.utcnow().isoformat(),
                'source': 'coingecko',
                'data_type': 'spot_prices',
                'data': response.json()
            }
            
        except requests.RequestException as e:
            logger.error(f"CoinGecko API error: {str(e)}")
            raise
    
    def get_market_data(self, vs_currency='usd', per_page=100):
        """
        Get market data for top cryptocurrencies
        
        Args:
            vs_currency: Currency to compare against
            per_page: Number of results (max 250)
            
        Returns:
            list: Market data for each coin
        """
        try:
            url = f"{self.BASE_URL}/coins/markets"
            params = {
                'vs_currency': vs_currency,
                'order': 'market_cap_desc',
                'per_page': min(per_page, 250),
                'page': 1,
                'sparkline': 'false',
                'price_change_percentage': '1h,24h,7d'
            }
            
            response = self.session.get(url, params=params, timeout=15)
            response.raise_for_status()
            
            return response.json()
            
        except requests.RequestException as e:
            logger.error(f"CoinGecko market data error: {str(e)}")
            raise
    
    def get_coin_history(self, coin_id, days=7):
        """
        Get historical market data
        
        Args:
            coin_id: Coin identifier (e.g., 'bitcoin')
            days: Number of days (1, 7, 14, 30, 90, 180, 365, max)
            
        Returns:
            dict: Historical price data
        """
        try:
            url = f"{self.BASE_URL}/coins/{coin_id}/market_chart"
            params = {
                'vs_currency': 'usd',
                'days': days
            }
            
            response = self.session.get(url, params=params, timeout=15)
            response.raise_for_status()
            
            return response.json()
            
        except requests.RequestException as e:
            logger.error(f"CoinGecko history error: {str(e)}")
            raise