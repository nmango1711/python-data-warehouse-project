from pyspark.sql import SparkSession

def get_spark():
    spark =  SparkSession.builder \
        .appName("python-data-warehouse") \
        .master("local[*]") \
        .config("spark.jars.packages", "com.microsoft.sqlserver:mssql-jdbc:12.6.1.jre11") \
        .getOrCreate() 
    
    spark.sparkContext.setLogLevel("ERROR")
    
    return spark
