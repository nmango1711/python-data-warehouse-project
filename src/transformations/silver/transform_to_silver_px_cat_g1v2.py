from src.transformations.silver.base_silver import read_bronze_file, write_silver_file
from pyspark.sql.functions import trim, regexp_replace, current_timestamp, col
import time
from src.config.logger import log_file_started

def transform_to_silver_px_cat_g1v2(bronze_path, silver_path, spark, logger):

    file_name = "PX_CAT_G1V2"

    start_time = log_file_started(logger, file_name, "S")
    
    df = (
        read_bronze_file(file_name, bronze_path, spark)

        .withColumn("maintenance", 
            trim(
                regexp_replace(
                    regexp_replace(col("maintenance"), "\r", ""),
                "\n", "")
            ) 
        )
        .withColumn("cleaned_time", current_timestamp())
    )

    load_time = time.time() - start_time
    write_silver_file(df, file_name, silver_path, load_time, logger)