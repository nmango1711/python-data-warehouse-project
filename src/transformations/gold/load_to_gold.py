from src.transformations.gold.create_gold_customers import create_gold_customers
from src.transformations.gold.create_gold_products import create_gold_products
from src.transformations.gold.create_gold_sales import create_gold_sales

SILVER_PATH = "data/silver"
GOLD_PATH = "data/gold"

def load_to_gold(spark):
    print("****************")
    print("Gold Layer")
    print("****************")
    create_gold_customers(SILVER_PATH, GOLD_PATH, spark)
    create_gold_products(SILVER_PATH, GOLD_PATH, spark)
    create_gold_sales(SILVER_PATH, GOLD_PATH, spark)