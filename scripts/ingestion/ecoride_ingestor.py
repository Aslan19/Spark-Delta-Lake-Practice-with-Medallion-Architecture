from scripts.utils.spark_utils import create_spark_session
from scripts.utils.file_manager import FileManager
from scripts.utils.data_ingestor import DataIngestor
from scripts.utils.config_loader import ConfigLoader

if __name__ == "__main__":
    config = ConfigLoader()
    spark = create_spark_session()
    ingestor = DataIngestor(spark)
    file_manager = FileManager(config.base_data_dir, config.lakehouse_s3_tst_path)

    customer_data = file_manager.get_local_file_path("ecoride_customers", "csv")
    ingestor.ingest_file_to_bronze(customer_data, "ecoride", "customers", "csv")

    reviews_data = file_manager.get_local_file_path("ecoride_product_reviews", "json")
    ingestor.ingest_file_to_bronze(reviews_data, "ecoride", "reviews", "json")

    sales_data = file_manager.get_local_file_path("ecoride_sales", "csv")
    ingestor.ingest_file_to_bronze(sales_data, "ecoride", "sales", "csv")

    vehicles_data = file_manager.get_local_file_path("ecoride_vehicles", "csv")
    ingestor.ingest_file_to_bronze(vehicles_data, "ecoride", "vehicles", "csv")