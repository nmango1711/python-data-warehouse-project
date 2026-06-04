from src.transformations.silver.transform_to_silver_cust_info import transform_to_silver_cust_info
from src.transformations.silver.transform_to_silver_prd_info import transform_to_silver_prd_info
from src.transformations.silver.transform_to_silver_sales_details import transform_to_silver_sales_details
from src.transformations.silver.transform_to_silver_cust_az12 import transform_to_silver_cust_az12
from src.transformations.silver.transform_to_silver_loc_a101 import transform_to_silver_loc_a101
from src.transformations.silver.transform_to_silver_px_cat_g1v2 import transform_to_silver_px_cat_g1v2
from src.config.logger import log_layer_start, log_layer_end

BRONZE_PATH = "data/bronze"
SILVER_PATH = "data/silver"

def load_to_silver(spark, logger):
    log_layer_start(logger, "SILVER")
    transform_to_silver_cust_info(BRONZE_PATH, SILVER_PATH, spark, logger)
    transform_to_silver_prd_info(BRONZE_PATH, SILVER_PATH, spark, logger)
    transform_to_silver_sales_details(BRONZE_PATH, SILVER_PATH, spark, logger)
    transform_to_silver_cust_az12(BRONZE_PATH, SILVER_PATH, spark, logger)
    transform_to_silver_loc_a101(BRONZE_PATH, SILVER_PATH, spark, logger)
    transform_to_silver_px_cat_g1v2(BRONZE_PATH, SILVER_PATH, spark, logger)
    log_layer_end(logger, "SILVER")