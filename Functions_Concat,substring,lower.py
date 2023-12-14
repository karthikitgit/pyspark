# Databricks notebook source
from pyspark.sql.functions import concat, substring, lower

data = [("karthik", "chintu"), ("kishore", "bala")]
df = spark.createDataFrame(data, ["first_name", "last_name"])

# Concatenating two columns
df.withColumn("full_name", concat("first_name", "last_name")).show()

# Extracting substring
df.withColumn("sub_name", substring("first_name", 1, 2)).show()

# Converting strings to lower case
df.withColumn("lower_first_name", lower("first_name")).show()

# COMMAND ----------


