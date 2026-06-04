from src.config.logger import log_file_finished

def read_bronze_file(bronze_file, bronze_path, spark):
    path = f"{bronze_path}/{bronze_file}"
    return spark.read.parquet(path)

def write_silver_file(df, silver_file, silver_path, load_time, logger):
    output_path = f"{silver_path}/{silver_file}"
    df.write.mode("overwrite").parquet(output_path)    
    log_file_finished(logger, df, silver_file, load_time, "S")