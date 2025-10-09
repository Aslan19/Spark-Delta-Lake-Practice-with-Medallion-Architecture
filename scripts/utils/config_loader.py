import os
from dotenv import load_dotenv

class ConfigLoader:
    def __init__(self):
        # Load .env file
        dotenv_path = os.path.join(os.path.dirname(__file__), '../..', '.env')
        load_dotenv(dotenv_path, override=True)
        
        # Load environment variables
        self.aws_s3_endpoint = os.environ.get("MINIO_S3_ENDPOINT")
        self.aws_access_key_id = os.environ.get("MINIO_ACCESS_KEY_ID")
        self.aws_secret_access_key = os.environ.get("MINIO_SECRET_ACCESS_KEY")
        self.lakehouse_s3_tst_path = os.environ.get("LAKEHOUSE_S3_TEST_PATH")
        self.lakehouse_s3_prd_path = os.environ.get("LAKEHOUSE_S3_PROD_PATH")

        # Define the paths to local files
        self.script_dir = os.path.dirname(os.path.abspath(__file__))
        self.base_data_dir = os.path.join(self.script_dir, '../..', 'data')