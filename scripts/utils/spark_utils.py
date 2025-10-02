from pyspark.sql import SparkSession
from dotenv import load_dotenv
import os

# Load .env file. This line assumes the .env file is in the parent directory of the script
load_dotenv(os.path.join(os.path.dirname(__file__), '../..', '.env'))

def create_spark_session():
    return (
        SparkSession.builder
        .appName("Data Ingestion to Bronze with Delta")
        # Delta Lake core
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0")
        # Enable Delta Lake SQL extensions
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        # Set Delta Lake catalog
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # Optional: optimize small file handling
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.files.maxPartitionBytes", "134217728")  # 128 MB
        .getOrCreate()
    )
