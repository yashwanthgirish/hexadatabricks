# Databricks notebook source
# MAGIC %sql
# MAGIC select * from json.`dbfs:/mnt/blobadhex/testblobcontainer/raw/16.8.23.json`

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists iotdata;
# MAGIC use iotdata

# COMMAND ----------

# MAGIC %sql
# MAGIC Create table iotdata.sample as
# MAGIC (select * from json.`dbfs:/mnt/blobadhex/testblobcontainer/raw/16.8.23.json`)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from parquet.`dbfs:/mnt/asadlsad/processeddata/iotdata/yashwanth`

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.rahulhexaware.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.rahulhexaware.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.provider.type.rahulhexaware.dfs.core.windows.net", "?sv=2022-11-02&ss=bfqt&srt=c&sp=rlx&se=2023-11-10T14:07:39Z&st=2023-11-10T06:07:39Z&spr=https&sig=i083TPS6Rb2k8I%2FU%2Fz5DyNgfNynDmLQHyedH1a8zPYI%3D")

# COMMAND ----------


