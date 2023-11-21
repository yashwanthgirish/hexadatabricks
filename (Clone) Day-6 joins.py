# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------


df_drivers=spark.read.json("dbfs:/mnt/asadlsad/processeddata/raw/drivers.json")
df_pitstop=spark.read.option("multiline",True).json("dbfs:/mnt/asadlsad/processeddata/raw/pit_stops.json")

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/asadlsad/processeddata/raw/
# MAGIC

# COMMAND ----------

df_drivers.join(df_pitstop).display()

# COMMAND ----------

df_drivers.join(df_pitstop,"driverId").display()

# COMMAND ----------

df_drivers.join(df_pitstop,"driverId","left").select("driverId","code","lap").display()

# COMMAND ----------

df=spark.read.csv("dbfs:/mnt/asadlsad/processeddata/raw/Nulls.csv",header=True)

# COMMAND ----------

df.dropDuplicates().display()
df1=df.dropDuplicates()

# COMMAND ----------

df1.dropna("all",subset="id").display()


# COMMAND ----------

df=spark.read.csv("dbfs:/mnt/asadlsad/processeddata/raw/Nulls.csv",header=True,inferSchema=True)

# COMMAND ----------

df1.display()

# COMMAND ----------

df1.fillna({"id":0,"name":"unknown","Marks":49,"placed":False}).display()

# COMMAND ----------


