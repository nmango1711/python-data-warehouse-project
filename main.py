from src.ingest.load_to_bronze import ingest_csv_to_bronze
from src.transform.silver.transform_silver_cust_info import transform_cust_info_to_silver
from src.transform.silver.transform_silver_prd_info import transform_prd_info_to_silver
from src.transform.silver.transform_silver_sales_details import transform_sales_details_to_silver
SOURCE_PATH = "data/source"
BRONZE_PATH = "data/bronze"
SILVER_PATH = "data/silver"
# ingest_csv_to_bronze(SOURCE_PATH, BRONZE_PATH)

# transform_cust_info_to_silver(BRONZE_PATH, SILVER_PATH)
# transform_prd_info_to_silver(BRONZE_PATH, SILVER_PATH)
transform_sales_details_to_silver(BRONZE_PATH, SILVER_PATH)

# from src.config.spark_session import get_spark
# spark = get_spark()
# df = spark.read.parquet("data/bronze/sales_details")
# df.printSchema()