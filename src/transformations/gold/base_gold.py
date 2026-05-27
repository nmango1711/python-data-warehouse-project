def read_silver_file(silver_file, silver_path, spark):
    path = f"{silver_path}/{silver_file}"
    return spark.read.parquet(path)

def write_gold_file(df, gold_file , gold_path):
    output_path = f"{gold_path}/{gold_file}"
    df.write.mode("overwrite").parquet(output_path)
    
    print()
    print("****************")
    print(f"Created gold file: {gold_file}")
    print(f"Total Rows: {df.count()}")
    print("****************")

