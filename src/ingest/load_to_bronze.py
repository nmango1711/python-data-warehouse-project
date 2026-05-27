import os, time

def load_to_bronze(source_path, bronze_path, spark):

    print()
    print("****************")
    print("Bronze Layer")
    print("****************")

    for file in os.listdir(source_path):
        start_time = time.time()
        if file.endswith(".csv"):
            df = spark.read \
                .option("header", True) \
                .option("inferSchema", True) \
                .option("delimiter", ",") \
                .csv(f"{source_path}/{file}")
            
        bronze_file = file.replace(".csv", "")
        df.write.mode("overwrite").parquet(f"{bronze_path}/{bronze_file}") 

        load_time = time.time()
        print("----------------")
        print(f"Created bronze file: {bronze_file}")
        print(f"Total Rows: {df.count()}")
        print(f"Total Load Time: {(load_time-start_time):.2f} seconds")
        print("----------------")
        print()