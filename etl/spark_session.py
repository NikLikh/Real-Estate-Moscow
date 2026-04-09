from pyspark.sql import SparkSession

from config.settings import JARS_PATH


def create_spark(app_name="silver_etl", cores=2, driver_mem="4g"):
    return (
        SparkSession.builder.appName(app_name)
        .master(f"local[{cores}]")
        .config("spark.jars", JARS_PATH)
        .config("spark.driver.extraClassPath", JARS_PATH)
        .config("spark.executor.extraClassPath", JARS_PATH)
        .config("spark.driver.memory", driver_mem)
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .getOrCreate()
    )
