from scripts.utils.spark_utils import create_spark_session
from scripts.utils.file_manager import FileManager
from scripts.utils.data_ingestor import DataIngestor
from scripts.utils.config_loader import ConfigLoader

if __name__ == "__main__":
    config = ConfigLoader()
    spark = create_spark_session()
    ingestor = DataIngestor(spark)
    file_manager = FileManager(config.base_data_dir, config.lakehouse_s3_tst_path)

    datafile = file_manager.get_local_file_path("vehicle_health_data", "json")
    ingestor.ingest_file_to_bronze(datafile, "vehicle_health", "logs", "json")
    # file_path: str, business_entity: str, table_name: str, file_type: str, partition_by=None