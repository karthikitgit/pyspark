# Databricks notebook source
help(spark.read.csv)

# COMMAND ----------

df = spark.read.csv(path = 'dbfs:/FileStore/tables/FileStore/text_file.txt')
display(df)

# COMMAND ----------

from pyspark.sql.types import *

Schema = (
    StructType()
    .add("Firstnm", StringType())
    .add("Lastnm", StringType())
    .add("phonenumber", LongType())
)

df = spark.read.csv("dbfs:/FileStore/tables/FileStore/sample_data.csv", schema=Schema, header=True)
display(df)
df.printSchema()


# COMMAND ----------

from pyspark.sql.types import *
struct = (
            StructType()
            .add("Firstnm", StringType())
            .add("Lastnm", StringType())
            .add("phonenumber", LongType())
            )

df = spark.read.csv(path ="dbfs:/FileStore/tables/output.csv",)
display(df)
df.printstruct()


# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = SparkSession.builder.appName("ReadCSV").getOrCreate()

schema = StructType([
    StructField('Firstnm', StringType(), True),
    StructField('Lastnm', StringType(), True),
    StructField('phonenumber', StringType(), True)  # Using StringType() instead of LongType()
])

df = spark.read.csv("dbfs:/FileStore/tables/output.csv", schema=schema, header=True)
df.show()
df.printSchema()


# COMMAND ----------

from pyspark.
