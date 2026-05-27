from src.transformations.silver.base_silver import read_bronze_file, write_silver_file 
from pyspark.sql.functions import col, regexp_replace, trim, when, current_timestamp
import time 

def transform_to_silver_loc_a101(bronze_path, silver_path, spark):

    start_time = time.time()
    
    clean_country = (trim(
        regexp_replace(
            regexp_replace(col("cntry"), "\r", ""), 
        "\n", "")))

    df = (
        read_bronze_file("LOC_A101", bronze_path, spark)

        .withColumn("cid",
            regexp_replace(col("cid"), "-", "")
        )
        .withColumn("cntry",
            when(
                clean_country == "DE", 
                "Germany")
            .when(
                clean_country.isin("US", "USA"), 
                "United States") 
            .when(
                clean_country.isNull() | (clean_country == ""),
                "n/a")      
            .otherwise(clean_country) 
        )
        .withColumn("cleaned_time", current_timestamp())
    )

    load_time = time.time() - start_time
    write_silver_file(df, "LOC_A101", silver_path, load_time)