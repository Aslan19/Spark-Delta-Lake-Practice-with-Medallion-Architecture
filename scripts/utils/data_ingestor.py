from pyspark.sql import SparkSession
import logging
import os

class DataIngestor:
    def __init__(self, spark: SparkSession, base_path: str = "/opt/spark/data/lakehouse"):
        self.spark = spark
        self.base_path = base_path  # root folder for bronze/silver/gold

    def _get_table_path(self, layer: str, business_entity: str, table_name: str) -> str:
        """Construct path for Delta table."""
        return os.path.join(self.base_path, layer, business_entity, table_name)

    def create_or_replace_delta_table(self, df, layer, business_entity, table_name, partition_by=None):
        """Create or overwrite a Delta table."""
        table_path = self._get_table_path(layer, business_entity, table_name)

        try:
            writer = df.write.format("delta").mode("overwrite")
            if partition_by:
                writer = writer.partitionBy(partition_by)
            writer.save(table_path)

            logging.info(f"Delta table created/replaced at {table_path}")
        except Exception as e:
            logging.error(f"Error creating/replacing Delta table: {e}")
            raise e

    def ingest_file_to_bronze(self, file_path: str, business_entity: str, table_name: str, file_type: str, partition_by=None):
        """Ingest raw file into Bronze Delta Lake."""
        try:
            if file_type == 'csv':
                df = self.spark.read.csv(file_path, header=True, inferSchema=True)
            elif file_type == 'json':
                df = self.spark.read.option("multiLine", "true").json(file_path)
            else:
                raise ValueError(f"Unsupported file type '{file_type}'. Supported types: csv, json")

            # Save as Delta in bronze layer
            self.create_or_replace_delta_table(df, "bronze", business_entity, table_name, partition_by)

            logging.info(
                f"Data ingested successfully from {file_path} "
                f"to Delta table bronze/{business_entity}/{table_name}"
            )

        except Exception as e:
            logging.error(f"Error ingesting file to Delta table: {e}")
            raise e
