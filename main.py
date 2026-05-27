from src.ingest.load_to_bronze import load_to_bronze
from src.transformations.silver.load_to_silver import load_to_silver
from src.transformations.gold.load_to_gold import load_to_gold
from src.config.spark_session import get_spark

spark = get_spark()
spark.sparkContext.setLogLevel("ERROR")

load_to_bronze("data/source", "data/bronze", spark)
load_to_silver(spark)
load_to_gold(spark)

spark.stop()