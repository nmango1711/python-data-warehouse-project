from src.transform.silver.transform_silver_cust_info import transform_cust_info_to_silver
from src.transform.silver.transform_silver_prd_info import transform_prd_info_to_silver
from src.transform.silver.transform_silver_sales_details import transform_sales_details_to_silver
from src.transform.silver.transform_silver_cust_az12 import transform_cust_az12_to_silver
from src.transform.silver.transform_silver_loc_a101 import transform_loc_a101_to_silver
from src.transform.silver.transform_silver_px_cat_g1v2 import transform_px_cat_g1v2_to_silver
SOURCE_PATH = "data/source"
BRONZE_PATH = "data/bronze"
SILVER_PATH = "data/silver"

def run_silver():
    transform_cust_info_to_silver(BRONZE_PATH, SILVER_PATH)
    transform_prd_info_to_silver(BRONZE_PATH, SILVER_PATH)
    transform_sales_details_to_silver(BRONZE_PATH, SILVER_PATH)
    transform_cust_az12_to_silver(BRONZE_PATH, SILVER_PATH)
    transform_loc_a101_to_silver(BRONZE_PATH, SILVER_PATH)
    transform_px_cat_g1v2_to_silver(BRONZE_PATH, SILVER_PATH)