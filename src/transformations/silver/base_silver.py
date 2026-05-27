def read_bronze_file(bronze_file, bronze_path, spark):
    path = f"{bronze_path}/{bronze_file}"
    return spark.read.parquet(path)

def write_silver_file(df, silver_file, silver_path, load_time):
    output_path = f"{silver_path}/{silver_file}"
    df.write.mode("overwrite").parquet(output_path)
    
    print()
    print("****************")
    print(f"Created silver file: {silver_file}")
    print(f"Total Rows: {df.count()}")
    print(f"Total Load Time: {load_time:.2f} seconds")
    print("****************")
