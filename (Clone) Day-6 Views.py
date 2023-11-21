# Databricks notebook source
# MAGIC %sql
# MAGIC use iotdata

# COMMAND ----------

# MAGIC %sql
# MAGIC create view tempabove25 as(select * from sample where temperature > 25)

# COMMAND ----------

# MAGIC %sql
# MAGIC create temp view tempabove25 as(select * from sample where temperature > 25)

# COMMAND ----------

# MAGIC %sql
# MAGIC show views

# COMMAND ----------

# MAGIC %python
# MAGIC df=spark.read.option("header",True).option("inferschema",True).csv("dbfs:/mnt/asadlsad/processeddata/raw/Baby_Names.csv")

# COMMAND ----------

df.createOrReplaceTempView("namespace")

# COMMAND ----------


