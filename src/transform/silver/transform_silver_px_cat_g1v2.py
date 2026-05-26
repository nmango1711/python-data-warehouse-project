from src.transform.silver.base_silver import read_bronze, write_silver 
from pyspark.sql.functions import *
from pyspark.sql.window import Window

def transform_px_cat_g1v2_to_silver(bronze_path, silver_path):

    df = (
        read_bronze("PX_CAT_G1V2", bronze_path)
        .withColumn("maintenance", 
            trim(
                regexp_replace(
                    regexp_replace(col("maintenance"), "\r", ""),
                "\n", "")
            )
            
        )
        .withColumn("cleaned_date", current_timestamp())
    )

    print("**************************")
    print(f"Cleaned file: PX_CAT_G1V2")
    print(f"Total Rows: {df.count()}")
    print("**************************")
    write_silver(df, "PX_CAT_G1V2", silver_path)