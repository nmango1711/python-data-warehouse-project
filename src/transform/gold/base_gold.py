from src.config.spark_session import get_spark

spark = get_spark()

def read_silver(silver_file, silver_path):
    path = f"{silver_path}/{silver_file}"

    return spark.read.parquet(path)

def write_gold(df, silver_file , gold_path):
    output_path = f"{gold_path}/{silver_file}"
    df.write.mode("overwrite").parquet(output_path)
    print("****************")
    print(f"Created gold file: {silver_file}")
    print(f"Total Rows: {df.count()}")
    print("****************")

def create_temp_view(df, view_name):
    df.createOrReplaceTempView(view_name)
    spark.catalog.listTables()
    print("****************")
    print("Created view Customers")
    print("****************")
