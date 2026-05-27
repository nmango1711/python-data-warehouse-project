from src.transformations.gold.base_gold import read_silver_file, write_gold_file
from pyspark.sql.functions import row_number, col, coalesce, lit, when
from pyspark.sql.window import Window
import time 

def create_gold_customers(silver_path, gold_path, spark):
    
    start_time = time.time()

    df_cust_info = read_silver_file("cust_info", silver_path, spark)
    df_cust_az12 = read_silver_file("CUST_AZ12", silver_path, spark)
    df_loc_a101 = read_silver_file("LOC_A101", silver_path, spark)
   
    df = (
        df_cust_info.alias("ci") 
        .join(df_cust_az12.alias("ca"), 
            col("ca.cid") == col("ci.cst_key"), 
            "left") 
        .join(df_loc_a101.alias("la"), 
            col("la.cid") == col("ci.cst_key"), 
            "left") 
        .withColumn("customer_key",
            row_number().over(
            Window.orderBy(col("ci.cst_id")))
        ) 
        .withColumn("gender",
            when(
                col("ci.cst_gndr") != "n/a", 
                col("ci.cst_gndr")
            )
            .otherwise(
                coalesce(col("ca.gen"), lit("n/a")))
        )        
    )
    
    df_customers = df.select(
        col("customer_key"),
        col("cst_id").alias("customer_id"),
        col("cst_key").alias("customer_number"),
        col("cst_firstname").alias("first_name"),
        col("cst_lastname").alias("last_name"),
        col("cntry").alias("country"),
        col("cst_marital_status").alias("marital_status"),
        col("gender"),
        col("bdate").alias("birthdate"),
        col("cst_create_date").alias("create_date"),
    )

    load_time = time.time() - start_time
    write_gold_file(df_customers, "dim_customers", gold_path, load_time)