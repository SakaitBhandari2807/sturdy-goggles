from pyspark.sql import SparkSession
from lib.logger import Log4j
from lib.utils import  get_spark_app_config

if __name__ == "__main__":

    conf = get_spark_app_config()
    spark = SparkSession.builder\
        .config(conf = conf)\
        .getOrCreate()

    logger = Log4j(spark)
    logger.info('Starting helloSpark')
    logger.info('Finished hellSpark')
    spark.stop()

