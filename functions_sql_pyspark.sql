
Mathematical Functions:
----------------------------------------------------------------------------------------------------
from pyspark.sql import SparkSession
from pyspark.sql.functions import sqrt, abs

spark = SparkSession.builder.appName("MathFunctions").getOrCreate()

data = [(4,), (9,), (16,)]
df = spark.createDataFrame(data, ["number"])

# Square root of a column
df.withColumn("sqrt_number", sqrt("number")).show()

# Absolute value of a column
df.withColumn("abs_number", abs("number")).show()
----------------------------------------------------------------------------------------------------
String Functions:
----------------------------------------------------------------------------------------------------
from pyspark.sql.functions import concat, substring, lower

data = [("karthik", "chintu"), ("kishore", "bala")]
df = spark.createDataFrame(data, ["first_name", "last_name"])

# Concatenating two columns
df.withColumn("full_name", concat("first_name", "last_name")).show()

# Extracting substring
df.withColumn("sub_name", substring("first_name", 1, 2)).show()

# Converting strings to lower case
df.withColumn("lower_first_name", lower("first_name")).show()

----------------------------------------------------------------------------------------------------
Date and Time Functions:
----------------------------------------------------------------------------------------------------
from pyspark.sql.functions import current_date, date_add, year

data = [("2023-01-01",), ("2023-05-15",)]
df = spark.createDataFrame(data, ["date"])

# Getting the current date
df.withColumn("current_date", current_date()).show()

# Adding days to a date
df.withColumn("date_after_5_days", date_add("date", 5)).show()

# Extracting year from a date
df.withColumn("year", year("date")).show()
----------------------------------------------------------------------------------------------------
Aggregation Functions:
----------------------------------------------------------------------------------------------------
from pyspark.sql.functions import sum, avg, max

data = [(1,), (2,), (3,), (4,)]
df = spark.createDataFrame(data, ["value"])

# Sum of values in a column
df.select(sum("value")).show()

# Average of values in a column
df.select(avg("value")).show()

# Maximum value in a column
df.select(max("value")).show()
