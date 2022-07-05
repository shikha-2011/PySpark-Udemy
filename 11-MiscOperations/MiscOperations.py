from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark import *

if __name__ == "__main__":
    spark = SparkSession \
                .builder \
                .master("local[3]") \
                .appName("Misc operations demo") \
                .getOrCreate()

    data_list = [ ("Atul", 8,8,2002),
                  ("Aditi", 6,15,1996),
                  ("Mona", 9,7,97),
                  ("Kirti", 7,1,81),
                  ("Atul", 8,8,2002)]

    # creating a quick dataframe along with column names
    raw_df = spark.createDataFrame(data_list).toDF("name", "day", "month", "year").repartition(3)
    raw_df.printSchema()

    # adding a new id column using the monotonically increasing id func
    df1 = raw_df.withColumn("id", monotonically_increasing_id())
    df1.show()

    # using case condition
    df2 = df1.withColumn("year", expr("""
                case when year < 21 then year + 2000
                when year < 100 then year + 1900
                else year
                end"""))
    df2.printSchema()
    df2.show()

    # casting your data
    # 1. inline cast
    # 2. change the schema
    df3 = df1.withColumn("year", expr("""
                    case when year < 21 then cast(year as int) + 2000
                    when year < 100 then cast(year as int) + 1900
                    else year
                    end"""))
    df3.printSchema()
    df3.show()

    df4 = df1.withColumn("year", expr("""
                        case when year < 21 then year + 2000
                        when year < 100 then year + 1900
                        else year
                        end""").cast(IntegerType()))
    df4.printSchema()
    df4.show()

    # Setting the datatype should be done at the beginning to avoid problems
    df5 = df1.withColumn("day", col("day").cast(IntegerType())) \
             .withColumn("month", col("month").cast(IntegerType())) \
             .withColumn("year", col("year").cast(IntegerType()))
    df5 = df1.withColumn("year", expr("""
                            case when year < 21 then year + 2000
                            when year < 100 then year + 1900
                            else year
                            end"""))
    df5.printSchema()
    df5.show()

    df6 = df5.withColumn("dob", expr("to_date(concat(day,'/',month,'/',year), 'd/M/y')"))
    df6.show()

    # dropping columns in a dataframe
    df7 = df5.withColumn("dob", expr("to_date(concat(day,'/',month,'/',year), 'd/M/y')")) \
            .drop("day", "month", "year") \
            .dropDuplicates(["name", "dob"]) \
            .sort(expr("dob desc" ))
    df7.show()
