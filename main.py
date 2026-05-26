from src.ingest.load_to_bronze import ingest_csv_to_bronze
from src.transform.silver.run_silver import run_silver
from src.transform.gold.create_gold_customers import create_customers_gold
from src.transform.gold.create_gold_products import create_products_gold
from src.transform.gold.create_gold_sales import create_sales_gold

SOURCE_PATH = "data/source"
BRONZE_PATH = "data/bronze"
SILVER_PATH = "data/silver"
GOLD_PATH = "data/gold"

#ingest_csv_to_bronze(SOURCE_PATH, BRONZE_PATH)
# run_silver()
#create_customers_gold(SILVER_PATH, GOLD_PATH)
#create_products_gold(SILVER_PATH, GOLD_PATH)
create_sales_gold(SILVER_PATH, GOLD_PATH)