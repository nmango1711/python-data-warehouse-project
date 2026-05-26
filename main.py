from src.ingest.load_to_bronze import ingest_csv_to_bronze
from src.transform.silver.run_silver import run_silver

SOURCE_PATH = "data/source"
BRONZE_PATH = "data/bronze"

#ingest_csv_to_bronze(SOURCE_PATH, BRONZE_PATH)
run_silver()
