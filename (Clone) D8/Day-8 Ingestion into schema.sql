-- Databricks notebook source
create schema if not exists project;
use project

-- COMMAND ----------

create table if not exists bronze

-- COMMAND ----------

COPY INTO bronze
FROM 'dbfs:/mnt/asadlsad/processeddata/inputproject/json'
FILEFORMAT = JSON
FORMAT_OPTIONS ('multiline' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true')

-- COMMAND ----------


