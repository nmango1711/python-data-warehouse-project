from src.transform.silver.base_silver import read_bronze, write_silver 
from pyspark.sql.functions import *
from pyspark.sql.window import Window

def transform_prd_info_to_silver(bronze_path, silver_path):

    df = (
        read_bronze("prd_info", bronze_path)
        .withColumn("cat_id", 
            regexp_replace(substring(col("prd_key"), 1, 5), "-", "_")
        )
        .withColumn("prd_key", 
            substring(col("prd_key"), 7, length(col("prd_key")))
        )
        .withColumn("prd_cost",
            coalesce(col("prd_cost"), lit(0)))
        .withColumn("prd_line",
            when(upper(trim(col("prd_line"))) == "M", "Mountain")
            .when(upper(trim(col("prd_line"))) == "R", "Road")
            .when(upper(trim(col("prd_line"))) == "S", "Other sales")
            .when(upper(trim(col("prd_line"))) == "T", "Touring")
            .otherwise("n/a")
        )
        .withColumn("prd_start_dt",
            date_sub(lead("prd_start_dt").over(
            Window.partitionBy("cst_id").orderBy(col("cst_create_date").desc())
            ), 1)
        )
    )

    print("**************************")
    print(f"Cleaned file: prd_info")
    print(f"Total Rows: {df.count()}")
    print("**************************")
    write_silver(df, "prd_info", silver_path)