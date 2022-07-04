from pyspark.sql import SparkSession
from pyspark.sql.functions import column, col, concat

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .master("local[3]") \
        .appName("ColumnDemo") \
        .getOrCreate()

    survey_df = spark.read \
                .format("csv") \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .load("./survey.csv")

    survey_df.printSchema()
    survey_df.show(10)

    # accessing columns using column strings
    survey_df.select("Timestamp", "Country", "comments").show(10)

    # using column objects
    survey_df.select(column("Timestamp"), col("Country"), survey_df.state).show(10)

    # expressions in columns
    survey_df.select("Timestamp", "Age", concat("Country", "state").alias("ConState")).show(10)
    