# Databricks notebook source
help(spark.read.json)

# COMMAND ----------

df = spark.read.json("dbfs:/FileStore/tables/FileStore/records1.json")
df.show()

# COMMAND ----------


df = spark.read.json("dbfs:/FileStore/tables/FileStore/records2.json")
df.show()
df.printSchema()

# COMMAND ----------



file_paths = [
    "dbfs:/FileStore/tables/FileStore/records1.json",
    "dbfs:/FileStore/tables/FileStore/records2.json"
]

df = spark.read.json(file_paths)
display(df)


# COMMAND ----------


