# Databricks notebook source
data = [(101,"kiran","50000","male"),
        (102,"dracula","60000","female"),
        (103,"kishore","40000","male"),
        (104,"mounika","30000","female")
        ]
schema = ["Id","Name","Salary","Gender"]
#path="dbfs:/FileStore/tables/FileStore/records2.json"
df = spark.createDataFrame(data=data,schema=schema)
display(df)

# COMMAND ----------

path="dbfs:/FileStore/tables/FileStore/records2.json"

df1 = df.write.json(path=path,mode="overwrite")
display(df1)


# COMMAND ----------

display(spark.read.json("dbfs:/FileStore/tables/FileStore/records2.json"))

# COMMAND ----------


