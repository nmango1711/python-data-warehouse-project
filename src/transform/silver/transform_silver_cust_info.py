import os
from transform.silver.base_silver import read_bronze, write_silver 
from pyspark.sql.functions import *

def transform_cust_info_to_silver(bronze_path, silver_path):
    
    df = read_bronze("cust_info", bronze_path)

    df = df.withColumnRenamed("cst_firstname", "customer_name").withColumn("customer_name", trim(col("customer_name")))
    
    cleaned_df = df
    print(f"Cleaned file: cust_info")
    write_silver(cleaned_df, "cust_info", silver_path)