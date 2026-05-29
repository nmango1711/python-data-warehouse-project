from src.transformations.gold.base_gold import read_silver_file, write_gold_file
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window
from src.sql.create_view import create_view_if_not_exists
import time

def create_gold_products(silver_path, gold_path, spark):

    start_time = time.time()

    df_prd_info = read_silver_file("prd_info", silver_path, spark)
    df_px_cat_g1v2 = read_silver_file("PX_CAT_G1V2", silver_path, spark)
   
    df = (
        df_prd_info.alias("pi") 
        .join(df_px_cat_g1v2.alias("pcg"), 
            col("pi.cat_id") == col("pcg.id"),
            "left"
        )
        .filter(col("pi.prd_end_dt").isNull())
        .withColumn("product_key",
            row_number().over(
            Window.orderBy(col("pi.prd_start_dt"), col("pi.prd_key")))
        )       
    )

    df_products = df.select(
        col("product_key"),
        col("prd_id").alias("customer_id"),
        col("prd_key").alias("product_number"),
        col("prd_nm").alias("product_name"),
        col("cat_id").alias("category_id"),
        col("cat").alias("category"),
        col("subcat").alias("subcategory"),
        col("maintenance"),
        col("prd_cost").alias("cost"),
        col("prd_line").alias("product_line"),
        col("prd_start_dt").alias("start_date"),
    )

    load_time = time.time() - start_time
    write_gold_file(df_products, "dim_products", gold_path, load_time)
    create_view_if_not_exists("dim_products", df_products)