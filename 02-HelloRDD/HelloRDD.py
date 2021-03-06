import sys
from collections import namedtuple

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

from lib.logger import Log4J


SurveyRecord = namedtuple("SurveyRecord", ["Age","Gender","Country","State"])
if __name__ == "__main__":
    conf = SparkConf() \
        .setMaster("local[3") \
        .setAppName("HelloRDD")

    # RDDs are created using SparkContext unlikely DataFrames which are created using SparkSession

    # sc = SparkContext(conf=conf)
    spark = SparkSession.builder \
        .config(conf=conf) \
        .getOrCreate()
    sc = spark.sparkContext
    logger = Log4J(spark)

    if sys.argv != 2:
        logger.error("Usage: Hellospark <filename>")
        sys.exit(-1)

    linesRDD = sc.textFile(sys.argv[1])
    partitionedRDD = linesRDD.repartition(2)
    colsRDD = partitionedRDD.map(lambda line: line.replace('"', '').split(","))
    selectRDD = colsRDD.map(lambda cols: SurveyRecord(int(cols[1]), cols[2], cols[3], cols[4]))
    filteredRDD = selectRDD.filter(lambda r: r.Age < 40)
    kvRDD = filteredRDD.map(lambda r: (r.Country, 1))
    countRDD = kvRDD.reduceByKey(lambda v1, v2: v1 + v2)
    # countRDD = kvRDD.reduceByKey(add) ### without using lambda function
    colList = countRDD.collect()
    for x in colList:
        logger.info(x)
