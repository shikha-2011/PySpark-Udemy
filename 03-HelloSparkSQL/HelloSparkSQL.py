import sys
from pyspark.sql import SparkSession
from lib.logger import Log4J

if __name__ == "__main__":
    spark = SparkSession \
            .builder \
            .appName("HelloSparkSQL") \
            .master("local[3]") \
            .getOrCreate()

    logger = Log4J(spark)

    if len(sys.argv) != 2:
        logger.error("Usage: HelloSpark <filename>")
        sys.exit(-1)

    surveyDf = spark.read \
                .option("header", True) \
                .option("inferSchema", True) \
                .csv(sys.argv[1])

    # SQL expression can be performed only on the table or a view
    surveyDf.createOrReplaceTempView("survey_tbl")
    countDf = spark.sql("select Country, count(1) as count from survey_tbl where Age<40 group by Country")
    countDf.show()