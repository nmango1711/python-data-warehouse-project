import os
from transform.silver.base_silver import read_bronze, write_silver 
from pyspark.sql.functions import *
from pyspark.sql.window import Window

def transform_cust_info_to_silver(bronze_path, silver_path):

    df = (
        read_bronze("cust_info", bronze_path)
        .filter(col("cst_id").isNotNull())
        .withColumn("show_duplicates", 
            row_number().over(
            Window.partitionBy("cst_id").orderBy(col("cst_create_date").desc())
            )
        )
        .filter(col("show_duplicates") == 1).drop("show_duplicates")
        .withColumn("cst_firstname", trim(col("cst_firstname")))
        .withColumn("cst_lastname", trim(col("cst_lastname")))
        .withColumn("cst_marital_status",
            when(upper(trim(col("cst_marital_status"))) == "S", "Single")
            .when(upper(trim(col("cst_marital_status"))) == "M", "Married")
            .otherwise("n/a")
        )
        .withColumn("cst_gndr",
            when(upper(col("cst_gndr")) == "F", "Female")
            .when(upper(col("cst_gndr")) == "M", "Male")
            .otherwise("n/a")
        )
        .withColumn("cleaned_date", current_timestamp())
    )

    print(f"Cleaned file: cust_info")
    write_silver(df, "cust_info", silver_path)