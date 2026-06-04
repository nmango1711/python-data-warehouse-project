import os, time
from src.config.logger import log_layer_start, log_file_started, log_file_finished, log_layer_end

def load_to_bronze(source_path, bronze_path, spark, logger):

    log_layer_start(logger, "BRONZE")

    for file in os.listdir(source_path):
        start_time = log_file_started(logger, file, "B")
        if file.endswith(".csv"):
            df = spark.read \
                .option("header", True) \
                .option("inferSchema", True) \
                .option("delimiter", ",") \
                .csv(f"{source_path}/{file}")
            
        bronze_file = file.replace(".csv", "")
        df.write.mode("overwrite").parquet(f"{bronze_path}/{bronze_file}") 

        load_time = time.time()
        
        log_file_finished(logger, df, bronze_file, load_time - start_time, "B")

    log_layer_end(logger, "BRONZE")