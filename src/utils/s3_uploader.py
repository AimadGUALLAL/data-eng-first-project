
import boto3
import logging
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

class S3Uploader:
    """Handle S3 uploads with proper credential management"""
    
    def __init__(self, bucket_name, aws_access_key=None, aws_secret_key=None, 
                 region=None, profile_name=None):
        """
        Initialize S3 client with flexible credential options
        
        Priority order:
        1. Explicit credentials (aws_access_key, aws_secret_key)
        2. AWS Profile
        3. Environment variables
        4. IAM Role (if running on AWS)
        """
        self.bucket_name = bucket_name
        
        session_params = {}
        
        if profile_name:
            # Use specific AWS profile
            session_params['profile_name'] = profile_name
        
        if region:
            session_params['region_name'] = region
        
        # Create session
        session = boto3.Session(**session_params)
        
        # Create S3 client
        if aws_access_key and aws_secret_key:
            self.s3_client = session.client(
                's3',
                aws_access_key_id=aws_access_key,
                aws_secret_access_key=aws_secret_key
            )
        else:
            # Use default credential chain
            self.s3_client = session.client('s3')
        
        # Verify bucket access
        self._verify_bucket_access()
    
    def _verify_bucket_access(self):
        """Verify we can access the bucket"""
        try:
            self.s3_client.head_bucket(Bucket=self.bucket_name)
            logger.info(f"✓ Successfully connected to bucket: {self.bucket_name}")
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == '404':
                logger.error(f"Bucket {self.bucket_name} does not exist")
            elif error_code == '403':
                logger.error(f"No permission to access bucket {self.bucket_name}")
            else:
                logger.error(f"Error accessing bucket: {str(e)}")
            raise
    
    def upload(self, data, s3_key, content_type='application/json', metadata=None):
        """Upload data to S3"""
        try:
            put_args = {
                'Bucket': self.bucket_name,
                'Key': s3_key,
                'Body': data,
                'ContentType': content_type
            }
            
            if metadata:
                put_args['Metadata'] = metadata
            
            self.s3_client.put_object(**put_args)
            logger.info(f"✓ Uploaded: s3://{self.bucket_name}/{s3_key}")
            return True
        except Exception as e:
            logger.error(f"✗ Upload failed: {str(e)}")
            return False
    

    def list_objects(self, prefix='', max_keys=1000):
        """
        List objects in bucket with given prefix
        
        Args:
            prefix: S3 key prefix to filter
            max_keys: Maximum number of keys to return
            
        Returns:
            list: List of object keys
        """
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=prefix,
                MaxKeys=max_keys
            )
            
            if 'Contents' in response:
                return [obj['Key'] for obj in response['Contents']]
            return []
            
        except Exception as e:
            logger.error(f"Error listing objects: {str(e)}")
            return []