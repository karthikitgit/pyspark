# Databricks notebook source


data = [(1,"karthik"),(2,"chintu")]
schema = ["id","name"]

df = spark.createDataFrame(data=data,schema=schema,)
display(df)


# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql import *

df.write.mode("overwrite").csv(path ="dbfs:/FileStore/tables/FileStore/sample_data.csv",header = True )

# COMMAND ----------

df = spark.read.csv(path="dbfs:/FileStore/tables/FileStore/sample_data.csv",header =True)
df.show()

# COMMAND ----------


