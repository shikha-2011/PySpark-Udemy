from pyspark.sql import SparkSession

from lib.logger import Log4J

if __name__ == "__main__":
    spark = SparkSession \
            .builder \
            .appName("SparkSchemaDemo") \
            .master("local[3]") \
            .getOrCreate()

    logger = Log4J(spark)

    flightTimeCsvDf = spark.read \
    .format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("data/flight*.csv")

    flightTimeCsvDf.show(5)
    logger.info("CSV Schema:" + flightTimeCsvDf.schema.simpleString())

    flightTimeJsonDf = spark.read \
        .format("json") \
        .load("data/flight*.json")

    flightTimeJsonDf.show(5)
    logger.info("JSON Schema:" + flightTimeJsonDf.schema.simpleString())

    flightTimeParDf = spark.read \
            .format("parquet") \
            .load("data/flight*.parquet")

    flightTimeParDf.show(5)
    logger.info("PARQUET Schema:" + flightTimeParDf.schema.simpleString())
