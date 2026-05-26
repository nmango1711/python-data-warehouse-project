from src.transform.silver.base_silver import read_bronze, write_silver 
from pyspark.sql.functions import *
from pyspark.sql.window import Window

def transform_sales_details_to_silver(bronze_path, silver_path):

    df = (
        read_bronze("sales_details", bronze_path)

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
                (col("sls_price") <= 0) | (col("sls_price").isNull()),
                col("sls_sales") / when(col("sls_quantity") != 0, col("sls_quantity")))
            .otherwise(col("sls_price")
            )
        )
        .withColumn("cleaned_date", current_timestamp())
    )

    print("**************************")
    print(f"Cleaned file: sales_details")
    print(f"Total Rows: {df.count()}")
    print("**************************")
    write_silver(df, "sales_details", silver_path)