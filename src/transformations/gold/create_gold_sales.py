from src.transformations.gold.base_gold import read_silver_file, write_gold_file
from pyspark.sql.functions import col
from src.sql.create_view import create_view_if_not_exists
import time
from src.config.logger import log_file_started

def create_gold_sales(silver_path, gold_path, spark, logger):

    file_name = "fact_sales"

    start_time = log_file_started(logger, file_name, "G", ["sales_details", "dim_products", "dim_customers"])

    df_sales_details = read_silver_file("sales_details", silver_path, spark)
    df_dim_products = read_silver_file("dim_products", gold_path, spark)
    df_dim_customers = read_silver_file("dim_customers", gold_path, spark)

    df = (
        df_sales_details.alias("sd")
        .join(
            df_dim_products.alias("pr"), 
            col("pr.product_number") == col("sd.sls_prd_key"),
            "left"
        )
        .join(
            df_dim_customers.alias("cu"),
            col("cu.customer_id") == col("sd.sls_cust_id"),
            "left"
        )
    )

    df_sales = df.select(
        col("sls_ord_num").alias("order_number"),
        col("product_key"),
        col("customer_key"),
        col("sls_order_dt").alias("order_date"),
        col("sls_ship_dt").alias("shipping_date"),
        col("sls_due_dt").alias("due_date"),
        col("sls_sales").alias("sales_amount"),
        col("sls_quantity").alias("quantity"),
        col("sls_price").alias("price"),
    )

    load_time = time.time() - start_time
    write_gold_file(df_sales, file_name, gold_path, load_time, logger)
    create_view_if_not_exists(file_name, df_sales, logger)