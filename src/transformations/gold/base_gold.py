from src.config.logger import log_file_finished

def read_silver_file(silver_file, silver_path, spark):
    path = f"{silver_path}/{silver_file}"
    return spark.read.parquet(path)

def write_gold_file(df, gold_file , gold_path, load_time, logger):
    output_path = f"{gold_path}/{gold_file}"
    df.write.mode("overwrite").parquet(output_path)
    log_file_finished(logger, df, gold_file, load_time, "G")