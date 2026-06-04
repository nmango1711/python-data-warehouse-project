from src.ingest.load_to_bronze import load_to_bronze
from src.transformations.silver.load_to_silver import load_to_silver
from src.transformations.gold.load_to_gold import load_to_gold
from src.config.spark_session import get_spark
from src.config.logger import get_logger, get_blank_line

spark = get_spark()

logger = get_logger("MAIN")

get_blank_line(logger)

logger.info("Pipeline started")

load_to_bronze("data/source", "data/bronze", spark, logger)
load_to_silver(spark, logger)
load_to_gold(spark, logger)

spark.stop()

logger.info("Pipeline finished")