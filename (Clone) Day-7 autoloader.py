# Databricks notebook source
inputpath="dbfs:/mnt/asadlsad/processeddata/inputstream/csv/"
outputpath="dbfs:/mnt/asadlsad/processeddata/outputautoloader"

# COMMAND ----------

(spark.readStream
.format("cloudFiles")
.option("cloudFiles.format","csv")
.option("cloudFiles.schemaLocation",f"{outputpath}/yashwanth/schemalocation")
.load(f"{inputpath}")
.writeStream
.option("checkpointLocation",f"{outputpath}/yashwanth/checkpoint")
.option("path",f"{outputpath}yashwanth/files")
.table("stream.firstauto")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table stream.firstauto

# COMMAND ----------

(spark.readStream
.format("cloudFiles")
.option("cloudFiles.format","csv")
.option("cloudFiles.schemaLocation",f"{outputpath}/yashwanth/schemalocation")
.option("cloudFiles.inferColumnTypes",True)
.load(f"{inputpath}")
.writeStream
.option("checkpointLocation",f"{outputpath}/yashwanth/checkpoint")
.option("path",f"{outputpath}/yashwanth/files")
.options("mergeSchema",True)
.table("stream.firstauto")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from stream.firstauto

# COMMAND ----------


