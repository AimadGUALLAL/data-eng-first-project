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

from config import Config
from src.utils.logger import setup_logger
from src.pipeline import CryptoDataPipeline

# Setup logger
logger = setup_logger(
    name='crypto_pipeline',
    log_level=Config.LOG_LEVEL,
    log_file=f'logs/pipeline_{datetime.now().strftime("%Y%m%d")}.log'
)


def test_configuration():
    """Test configuration and connections"""
    logger.info("Testing configuration...")
    
    try:
        Config.validate()
        Config.print_config()
        
        # Test S3 connection
        pipeline = CryptoDataPipeline(
            bucket_name=Config.S3_BUCKET,
            region=Config.AWS_REGION,
            aws_access_key= Config.AWS_ACCESS_KEY_ID,
            aws_secret_key=Config.AWS_SECRET_ACCESS_KEY
        )
        
        logger.info("✓ Configuration test passed!")
        return True
        
    except Exception as e:
        logger.error(f"✗ Configuration test failed: {str(e)}")
        return False


def run_once():
    """Run pipeline once and exit"""
    logger.info("Running pipeline once...")
    
    try:
        Config.validate()
        
        pipeline = CryptoDataPipeline(
            bucket_name=Config.S3_BUCKET,
            region=Config.AWS_REGION,
            aws_access_key= Config.AWS_ACCESS_KEY_ID,
            aws_secret_key=Config.AWS_SECRET_ACCESS_KEY
        )
        
        pipeline.collect_all_data()
        logger.info("✓ Single run completed successfully!")
        return True
        
    except Exception as e:
        logger.error(f"✗ Single run failed: {str(e)}", exc_info=True)
        return False
    


if __name__ == '__main__':
    run_once()