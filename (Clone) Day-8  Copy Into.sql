-- Databricks notebook source
-- MAGIC %sql
-- MAGIC create table tableusingcopyinto

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select * from tableusingcopyinto

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/mnt/asadlsad/processeddata/inputstream/csv/
-- MAGIC

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC COPY INTO tableusingcopyinto
-- MAGIC FROM 'dbfs:/mnt/asadlsad/processeddata/inputstream/csv'
-- MAGIC FILEFORMAT = CSV
-- MAGIC FORMAT_OPTIONS ('header' = 'true')
-- MAGIC COPY_OPTIONS ('mergeSchema' = 'true')

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select * from tableusingcopyinto

-- COMMAND ----------


