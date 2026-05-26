from src.transform.silver.base_silver import read_bronze, write_silver 
from pyspark.sql.functions import *
from pyspark.sql.window import Window

def transform_cust_az12_to_silver(bronze_path, silver_path):

    df = (
        read_bronze("CUST_AZ12", bronze_path)

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
        .withColumn("cleaned_date", current_timestamp())
    )

    print("**************************")
    print(f"Cleaned file: CUST_AZ12")
    print(f"Total Rows: {df.count()}")
    print("**************************")
    write_silver(df, "CUST_AZ12", silver_path)