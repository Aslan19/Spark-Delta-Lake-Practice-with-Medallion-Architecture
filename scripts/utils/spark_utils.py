from pyspark.sql import SparkSession
from dotenv import load_dotenv
import os

# Load .env file. This line assumes the .env file is in the parent directory of the script
load_dotenv(os.path.join(os.path.dirname(__file__), '../..', '.env'))

def create_spark_session():
    return (
    SparkSession.builder
        .appName("DeltaMedallionMinIO")
        # Delta configs
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # Required packages
        .config(
            "spark.jars.packages",
            ",".join([
                "io.delta:delta-spark_2.13:4.0.0",
                "org.apache.hadoop:hadoop-aws:3.4.0",
                "com.amazonaws:aws-java-sdk-bundle:1.12.262"
            ])
        )
        # MinIO S3A configs
        .config("spark.hadoop.fs.s3a.endpoint", os.environ.get("MINIO_S3_ENDPOINT"))
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY", os.environ.get("MINIO_ACCESS_KEY_ID")))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_SECRET_KEY", os.environ.get("MINIO_SECRET_ACCESS_KEY")))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore")
        .getOrCreate()
)
