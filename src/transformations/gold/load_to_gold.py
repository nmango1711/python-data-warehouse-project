from src.transformations.gold.create_gold_customers import create_gold_customers
from src.transformations.gold.create_gold_products import create_gold_products
from src.transformations.gold.create_gold_sales import create_gold_sales
from src.config.logger import log_layer_start, log_layer_end

SILVER_PATH = "data/silver"
GOLD_PATH = "data/gold"

def load_to_gold(spark, logger):
    log_layer_start(logger, "GOLD")
    create_gold_customers(SILVER_PATH, GOLD_PATH, spark, logger)
    create_gold_products(SILVER_PATH, GOLD_PATH, spark, logger)
    create_gold_sales(SILVER_PATH, GOLD_PATH, spark, logger)
    log_layer_end(logger, "GOLD")