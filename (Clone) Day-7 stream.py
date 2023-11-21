# Databricks notebook source
inputpath="dbfs:/mnt/asadlsad/processeddata/inputstream/csv/"

# COMMAND ----------

from pyspark.sql.types import *

# COMMAND ----------

users_sch=StructType([StructField("Id",IntegerType()),
                      StructField("Name",StringType()),
                      StructField("Gender",StringType()),
                      StructField("Salary",IntegerType()),
                      StructField("Country",StringType()),
                      StructField("Date",StringType()),
])

# COMMAND ----------

df=spark.readStream.option("header",True).schema(users_sch).csv(f"{inputpath}")

# COMMAND ----------

display(df)

# COMMAND ----------

from pyspark.sql.functions import *






# COMMAND ----------

df1=df.withColumn("ingestiondate", current_timestamp())

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema stream;

# COMMAND ----------

outputpath="dbfs:/mnt/asadlsad/processeddata/outputstream"

# COMMAND ----------

df1.writeStream.option("path",f"{outputpath}/yashwanth/teststream/files").option("checkpointLocation",f"{outputpath}/yashwanth/teststream/checkpoint").trigger (processingTime="1 minute").toTable("stream.teststream")

# COMMAND ----------

spark.streams.active



# COMMAND ----------

for i in spark.streams.active:
    i.stop()

# COMMAND ----------


