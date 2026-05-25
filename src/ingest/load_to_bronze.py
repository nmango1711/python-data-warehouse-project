from config.spark_session import get_spark
import os 

def ingest_csv_to_bronze(input_path, output_path):
    spark = get_spark()

    for file in os.listdir(input_path):
        if file.endswith(".csv"):
            df = spark.read \
                .option("header", True) \
                .option("inferSchema", True) \
                .option("delimiter", ",") \
                .csv(f"{input_path}/{file}")
            
        output_file = file.replace(".csv", "")
        df.write.mode("overwrite").parquet(f"{output_path}/{output_file}") 
        
    spark.stop()