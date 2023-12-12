# Databricks notebook source
data = [(1,"karthik","5000"),(2,"chintu","4000")]
columns = ["Id","Name","salary"]

df = spark.createDataFrame(data=data,schema=columns)

df1 = df.withColumnRenamed("salary","updated_salary")
df1.show()

# COMMAND ----------


