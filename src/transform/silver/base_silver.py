from src.config.spark_session import get_spark

spark = get_spark()

def read_bronze(bronze_file, bronze_path):
    path = f"{bronze_path}/{bronze_file}"
    return spark.read.parquet(path)

def write_silver(df, bronze_file, silver_path):
    output_path = f"{silver_path}/{bronze_file}"
    df.write.mode("overwrite").parquet(output_path)
