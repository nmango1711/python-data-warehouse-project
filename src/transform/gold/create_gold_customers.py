from src.transform.gold.base_gold import read_silver, write_gold, create_temp_view
from pyspark.sql.functions import *
from pyspark.sql.window import Window

def create_customers_gold(silver_path, gold_path):

    df_cust_info = read_silver("cust_info", silver_path)
    df_cust_az12 = read_silver("CUST_AZ12", silver_path)
    df_loc_a101 = read_silver("LOC_A101", silver_path)
   
    df = (
        df_cust_info.alias("ci") 
        .join(df_cust_az12.alias("ca"), col("ca.cid") == col("ci.cst_key"), "left") 
        .join(df_loc_a101.alias("la"), col("la.cid") == col("ci.cst_key"), "left") 
        .withColumn("customer_key",
            row_number().over(Window.orderBy(col("ci.cst_id")))
        ) 
        .withColumn("gender",
            when(
                col("ci.cst_gndr") != "n/a", 
                col("ci.cst_gndr"))
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

    write_gold(df_customers, "dim_customers", gold_path)

    # create_temp_view(df_customers, "dim_customers")
