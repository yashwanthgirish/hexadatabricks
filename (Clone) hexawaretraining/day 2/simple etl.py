# Databricks notebook source
# MAGIC %fs ls

# COMMAND ----------

dbutils.fs.mkdirs("dbfs:/FileStore/")

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/tables/

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/tables/Formula 1/circuits.csv/

# COMMAND ----------

df=spark.read.csv("dbfs:/FileStore/tables/Formula 1/",header=True)
