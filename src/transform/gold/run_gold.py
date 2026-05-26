from src.transform.gold.create_gold_customers import create_customers_gold
from src.transform.gold.create_gold_products import create_products_gold
from src.transform.gold.create_gold_sales import create_sales_gold

SILVER_PATH = "data/silver"
GOLD_PATH = "data/gold"

def run_gold():
    create_customers_gold(SILVER_PATH, GOLD_PATH)
    create_products_gold(SILVER_PATH, GOLD_PATH)
    create_sales_gold(SILVER_PATH, GOLD_PATH)