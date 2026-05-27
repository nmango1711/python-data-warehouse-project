from src.transformations.silver.base_silver import read_bronze_file, write_silver_file 
from pyspark.sql.functions import col, substring, regexp_replace, lit, length, trim, upper, when, current_timestamp, to_date, date_sub, coalesce, lead
from pyspark.sql.window import Window
import time 

def transform_to_silver_prd_info(bronze_path, silver_path, spark):

    start_time = time.time()
    
    df = (
        read_bronze_file("prd_info", bronze_path, spark)

        .withColumn("cat_id", 
            regexp_replace(substring(col("prd_key"), 1, 5), "-", "_")
        )
        .withColumn("prd_key", 
            substring(col("prd_key"), 7, length(col("prd_key")))
        )
        .withColumn("prd_cost",
            coalesce(col("prd_cost"), lit(0)))
        .withColumn("prd_line",
            when(
                upper(trim(col("prd_line"))) == "M", 
                "Mountain"
            )
            .when(
                upper(trim(col("prd_line"))) == "R", 
                "Road"
            )
            .when(
                upper(trim(col("prd_line"))) == "S", 
                "Other sales"
            )
            .when(
                upper(trim(col("prd_line"))) == "T", 
                "Touring"
            )
            .otherwise("n/a")
        )
        .withColumn("prd_end_dt",
            to_date(        
                date_sub(lead("prd_start_dt").over(
                Window.partitionBy("prd_key").orderBy(col("prd_start_dt"))
                ), 1)
            )
        )
        .withColumn("cleaned_time", current_timestamp())
    )

    load_time = time.time() - start_time
    write_silver_file(df, "prd_info", silver_path, load_time)