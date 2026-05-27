from src.transformations.silver.base_silver import read_bronze_file, write_silver_file 
from pyspark.sql.functions import col, substring, current_date, regexp_replace, lit, length, trim, upper, when, current_timestamp
import time 

def transform_to_silver_cust_az12(bronze_path, silver_path, spark):

    start_time = time.time()

    df = (
        read_bronze_file("CUST_AZ12", bronze_path, spark)

        .withColumn("cid",
            when(
                col("cid").startswith("NAS"), 
                substring(col("cid"), 4, length(col("cid")) - 3))
            .otherwise(col("cid")
            )
        )
        .withColumn("bdate",
            when(
                col("bdate") > current_date(),
                lit(None))
            .otherwise(col("bdate")
            )
        )
        .withColumn("gen",
            when(
                upper(trim(regexp_replace(col("gen"), "\r", ""))).isin("F", "FEMALE"),
                "Female")
            .when(
                upper(trim(regexp_replace(col("gen"), "\r", ""))).isin("M", "MALE"),
                "Male")       
            .otherwise("n/a") 
        )
        .withColumn("cleaned_time", current_timestamp())
    )

    load_time = time.time() - start_time
    write_silver_file(df, "CUST_AZ12", silver_path, load_time)