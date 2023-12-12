# Databricks notebook source
from pyspark.sql.types import StructType,StructField,StringType,IntegerType

data = [(1,"karthik",5000),(2,"chintu",4000)]
schema = StructType([
          StructField('ID', IntegerType(), True),
          StructField('Name', StringType(), True),
          StructField('Salary', IntegerType(), True)
])

df = spark.createDataFrame(data=data,schema=schema)
df.show()
df.printSchema()

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,StringType,IntegerType

data = [(1,('Bala','karthik'),5000),(2,('Karthik','chintu'),4000)]

struct = StructType([
                    StructField ("fristName",StringType(),True),
                    StructField ("lastName",StringType(),True)
])



schema = StructType([
          StructField('ID', IntegerType(), True),
          StructField('Name',struct),
          StructField('Salary', IntegerType(), True)
])

df = spark.createDataFrame(data,schema)
display(df)
df.printSchema()

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

data = [(1, ("Bala", "karthik"), 5000), (2, ("Karthik", "chintu"), 4000)]

nested_struct = StructType([
    StructField("firstName", StringType(), True),
    StructField("lastName", StringType(), True)
])

schema = StructType([
    StructField('ID', IntegerType(), True),
    StructField('Name', nested_struct),
    StructField('Salary', IntegerType(), True)
])

df = spark.createDataFrame(data=data, schema=schema)

df.show()

df.printSchema()


# COMMAND ----------


