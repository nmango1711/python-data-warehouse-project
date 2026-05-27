from src.transformations.silver.base_silver import read_bronze_file, write_silver_file
from pyspark.sql.functions import *
import time 

def transform_to_silver_sales_details(bronze_path, silver_path, spark):

    start_time = time.time()
    
    df = (
        read_bronze_file("sales_details", bronze_path, spark)

        .withColumn("sls_order_dt",
            when(
                (length(col("sls_order_dt").cast("string")) != 8) | 
                (col("sls_order_dt").isNull()) | 
                (col("sls_order_dt").cast("int") == 0),
                lit(None))
            .otherwise(to_date(col("sls_order_dt"), "yyyyMMdd")
            )
        )
        .withColumn("sls_ship_dt",
            when(
                (length(col("sls_ship_dt").cast("string")) != 8) | 
                (col("sls_ship_dt").isNull()) | 
                (col("sls_ship_dt").cast("int") == 0),
                lit(None))
            .otherwise(to_date(col("sls_ship_dt"), "yyyyMMdd")
            )
        )
        .withColumn("sls_due_dt",
            when(
                (length(col("sls_due_dt").cast("string")) != 8) | 
                (col("sls_due_dt").isNull()) | 
                (col("sls_due_dt").cast("int") == 0),
                lit(None))
            .otherwise(to_date(col("sls_due_dt"), "yyyyMMdd")
            )
        )
        .withColumn("sls_sales",
            when(
                (col("sls_sales").isNull()) | 
                (col("sls_sales") <= 0) |
                (col("sls_sales") != col("sls_quantity") * abs(col("sls_price"))),
                col("sls_quantity") * abs(col("sls_price")))
            .otherwise(col("sls_sales")
            )
        )
        .withColumn("sls_price",
            when(
                (col("sls_price") <= 0) | 
                (col("sls_price").isNull()),
                col("sls_sales") / when(col("sls_quantity") != 0, col("sls_quantity")))
            .otherwise(col("sls_price")
            )
        )
        .withColumn("cleaned_time", current_timestamp())
    )

    load_time = time.time() - start_time
    write_silver_file(df, "sales_details", silver_path, load_time)