# Databricks notebook source
from pyspark.sql.types import StructType,StructField,IntegerType,StringType,ArrayType

data = [("ABC",[1,2]),("MNO",[3,4]),("IJK",[5,6]),("XYZ",[7,8])]

schema = StructType([
    StructField("id", StringType(), True),
    StructField("Numbers", ArrayType(IntegerType()), True)
])

df = spark.createDataFrame(data = data,schema = schema)

display(df)
df.printSchema()
df.show()



# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, ArrayType


data = [("ABC", [1, 2]), ("MNO", [3, 4]), ("IJK", [5, 6]), ("XYZ", [7, 8])]

# Define the schema with StructType and StructField
schema = StructType([
    StructField("id", StringType(), True),
    StructField("Numbers", ArrayType(IntegerType()), True)
])

df = spark.createDataFrame(data=data, schema=schema)

df.show()

df.printSchema()


# COMMAND ----------

df.withColumn("Firstname",df.Numbers[0]).show()

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, array

data = [(1,2,3),(4,5,6)]
schema = ['num1','num2','num3']


df = spark.createDataFrame(data=data,schema=schema)

df1 = df.withColumn("Numbers",array(col('num1'),col('num2'),col('num3'))[0])
df.show()
display(df1)


# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, array

data = [(1, 2, 3), (4, 5, 6)]
schema = ['num1', 'num2', 'num3']

df = spark.createDataFrame(data=data, schema=schema)

df = df.withColumn("Numbers", array(col('num1'), col('num2'), col('num3'))[1])

df.show()


# COMMAND ----------



# COMMAND ----------


