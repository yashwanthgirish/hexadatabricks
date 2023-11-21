# Databricks notebook source
df=spark.read.json("dbfs:/FileStore/tables/F1/drivers.json")

# COMMAND ----------

df.display()

# COMMAND ----------

from pyspark.sql.functions import *
df_final=df.withColumn("forename",col("name.forename"))\
.withColumn("surname",col("name.surname"))\
.drop("name","url")


# COMMAND ----------

df_final.display()

# COMMAND ----------


