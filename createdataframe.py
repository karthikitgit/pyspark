# Databricks notebook source
help(spark.createDataFrame)

# COMMAND ----------

data = [(1,"karthik"),(2,"chintu")]
schema = ["id","name"]

df = spark.createDataFrame(data=data,schema =schema )
df.show(



# COMMAND ----------


data = [{'Id':1,'Name':'karthik'},
        {'Id':2,'Name':'chintu'}]
df = spark.createDataFrame(data=data)
df.show()
df.printSchema()


# COMMAND ----------


