# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS yvdelta.people10m(id INT,  firstName STRING,  middleName STRING,  lastName STRING,  gender STRING,  birthDate TIMESTAMP,  ssn STRING,  salary INT) USING DELTA

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema yvdelta;
# MAGIC use yvdelta;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended yvdelta.people10m

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history yvdelta.people10m

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS yvdelta.people20m (
# MAGIC   id INT,
# MAGIC   firstName STRING,
# MAGIC   middleName STRING,
# MAGIC   lastName STRING,
# MAGIC   gender STRING,
# MAGIC   birthDate TIMESTAMP,
# MAGIC   ssn STRING,
# MAGIC   salary INT
# MAGIC ) LOCATION 'dbfs:/mnt/yvadlshexaware/processed/delta/yashwanth'

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into yvdelta.people20m values(1,'Virat','R','K','M',2023-11-14,"123",1500)
# MAGIC
# MAGIC

# COMMAND ----------


