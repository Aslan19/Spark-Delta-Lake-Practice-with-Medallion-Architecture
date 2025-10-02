from dataclasses import dataclass
from typing import Dict, Any
import os
from dotenv import load_dotenv


@dataclass
class ConfigLoader:
    def __init__(self):

        """Configuration for data paths and settings"""
        
        # Data lake paths (can be S3, HDFS, local, etc.)
        RAW_PATH: str = "s3a://datalake/data/raw"
        BRONZE_PATH: str = "s3a://datalake/data/bronze" 
        SILVER_PATH: str = "s3a://datalake/data/silver"
        GOLD_PATH: str = "s3a://datalake/data/gold"
        
        dotenv_path = os.path.join(os.path.dirname(__file__), '../..', '.env')
        load_dotenv(dotenv_path, override=True)
        # Load environment variables
        self.aws_s3_endpoint = os.environ.get("AWS_S3_ENDPOINT")
        self.aws_access_key_id = os.environ.get("AWS_ACCESS_KEY_ID")
        self.aws_secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
        self.lakehouse_s3_path = os.environ.get("LAKEHOUSE_S3_PATH")