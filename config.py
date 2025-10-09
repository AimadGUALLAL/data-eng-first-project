import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

class Config:
    """Configuration management using environment variables"""
    
    # AWS Configuration
    AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
    AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
    AWS_REGION = os.getenv('AWS_DEFAULT_REGION', 'us-east-1')
    S3_BUCKET = os.getenv('AWS_S3_BUCKET')

    # Optional: Use AWS Profile instead
    AWS_PROFILE = os.getenv('AWS_PROFILE', 'default')

    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')

    @classmethod
    def validate(cls):
        """Validate required configuration"""
        required = ['S3_BUCKET']
        missing = [key for key in required if not getattr(cls, key)]
        
        if missing:
            raise ValueError(f"Missing required config: {', '.join(missing)}")
        
        return True