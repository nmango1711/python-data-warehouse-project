from src.transform.silver.base_silver import read_bronze, write_silver 
from pyspark.sql.functions import *
from pyspark.sql.window import Window

def transform_loc_a101_to_silver(bronze_path, silver_path):

    clean_country = (trim(
        regexp_replace(
            regexp_replace(col("cntry"), "\r", ""), 
        "\n", "")))

    df = (
        read_bronze("LOC_A101", bronze_path)

        .withColumn("cid",
            regexp_replace(col("cid"), "-", "")
        )
        .withColumn("cntry",
            when(
                clean_country == "DE", "Germany")
            .when(
                clean_country.isin("US", "USA"), "United States") 
            .when(
                clean_country.isNull() | (clean_country == ""),
                "n/a")      
            .otherwise(clean_country) 
        )
        .withColumn("cleaned_date", current_timestamp())
    )

    print("**************************")
    print(f"Cleaned file: LOC_A101")
    print(f"Total Rows: {df.count()}")
    print("**************************")
    write_silver(df, "LOC_A101", silver_path)