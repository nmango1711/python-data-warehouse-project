from src.ingest.load_to_bronze import ingest_csv_to_bronze
from src.transform.silver.run_silver import run_silver

SOURCE_PATH = "data/source"
BRONZE_PATH = "data/bronze"

#ingest_csv_to_bronze(SOURCE_PATH, BRONZE_PATH)
run_silver()

# from src.config.spark_session import get_spark
# spark = get_spark()
# df = spark.read.parquet("data/bronze/sales_details")
# df.printSchema()