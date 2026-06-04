from src.transformations.silver.base_silver import read_bronze_file, write_silver_file
from pyspark.sql.functions import col,trim, upper, when, current_timestamp, row_number
from pyspark.sql.window import Window
import time 
from src.config.logger import log_file_started

def transform_to_silver_cust_info(bronze_path, silver_path, spark, logger):

    file_name = "cust_info"

    start_time = log_file_started(logger, file_name, "S")

    df = (
        read_bronze_file(file_name, bronze_path, spark)

        .filter(col("cst_id").isNotNull())
        .withColumn("show_duplicates", 
            row_number().over(
            Window.partitionBy("cst_id").orderBy(col("cst_create_date").desc())
            )
        )
        .filter(col("show_duplicates") == 1).drop("show_duplicates")
        .withColumn("cst_firstname", trim(col("cst_firstname")))
        .withColumn("cst_lastname", trim(col("cst_lastname")))
        .withColumn("cst_marital_status",
            when(
                upper(trim(col("cst_marital_status"))) == "S", 
                "Single"
            )
            .when(
                upper(trim(col("cst_marital_status"))) == "M", 
                "Married"
            )
            .otherwise("n/a")
        )
        .withColumn("cst_gndr",
            when(
                upper(col("cst_gndr")) == "F", 
                "Female"
            )
            .when(
                upper(col("cst_gndr")) == "M", 
                "Male"
            )
            .otherwise("n/a")
        )
        .withColumn("cleaned_time", current_timestamp())
    )

    load_time = time.time() - start_time
    write_silver_file(df, file_name, silver_path, load_time, logger)