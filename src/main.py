from ingest.load_to_bronze import ingest_csv_to_bronze
from transform.silver.transform_silver_cust_info import transform_cust_info_to_silver

SOURCE_PATH = "data/source"
BRONZE_PATH = "data/bronze"
SILVER_PATH = "data/silver"
# ingest_csv_to_bronze(SOURCE_PATH, BRONZE_PATH)

transform_cust_info_to_silver(BRONZE_PATH, SILVER_PATH)