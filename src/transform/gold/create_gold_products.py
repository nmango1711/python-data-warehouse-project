from src.transform.gold.base_gold import read_silver, write_gold, create_temp_view
from pyspark.sql.functions import *
from pyspark.sql.window import Window

def create_products_gold(silver_path, gold_path):

    df_prd_info = read_silver("prd_info", silver_path)
    df_px_cat_g1v2 = read_silver("PX_CAT_G1V2", silver_path)
   
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
        col("maintenance").alias("maintenance"),
        col("prd_cost").alias("cost"),
        col("prd_line").alias("product_line"),
        col("prd_start_dt").alias("start_date"),
    )

    write_gold(df_products, "dim_products", gold_path)

    # create_temp_view(df_customers, "dim_customers")
