import re

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, expr
from pyspark.sql.types import StringType


def parse_gender(gender):
    female_pattern = r'^f$|f.m|w.m'
    male_pattern = r'^m$|ma|m.l'
    if re.search(female_pattern, gender.lower()):
        return "Female"
    elif re.search(male_pattern, gender.lower()):
        return "Male"
    else:
        return "Unknown"


if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .master("local[2]") \
        .appName("UDF Demo") \
        .getOrCreate()

    survey_df = spark.read \
        .format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load("./survey.csv")
    survey_df.show(10)

    # new_survey_df = survey_df.withColumn("Gender", parse_gender("Gender"))
    # you cannot use the user defined function directly in spark
    # you need to register it with the driver and make it udf

    parse_gender_udf = udf(parse_gender, StringType())
    [print(f) for f in spark.catalog.listFunctions() if "parse_gender" in f.name]

    # here we are using udf in a column object expression
    new_survey_df = survey_df.withColumn("Gender", parse_gender_udf("Gender"))
    new_survey_df.show(10)

    # other way is to use udf in string or sql expression
    # for this the registration process is different

    spark.udf.register("parse_gender_udf", parse_gender, StringType())
    [print(f) for f in spark.catalog.listFunctions() if "parse_gender" in f.name]

    new_survey_df2 = survey_df.withColumn("Gender", expr("parse_gender_udf(Gender)"))
    new_survey_df2.show(10)
