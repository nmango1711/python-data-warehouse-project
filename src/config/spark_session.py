from pyspark.sql import SparkSession

def get_spark():
    spark =  SparkSession.builder \
        .appName("python-data-warehouse") \
        .master("local[*]") \
        .getOrCreate()
    
    return spark
