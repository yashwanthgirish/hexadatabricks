# Databricks notebook source
# MAGIC %fs ls dbfs:/mnt/blobadhex/testblobcontainer/raw/
# MAGIC

# COMMAND ----------

df=spark.read.json("dbfs:/mnt/blobadhex/testblobcontainer/raw/16.8.23.json")

# COMMAND ----------

df.show()

# COMMAND ----------

df.display()

# COMMAND ----------

from pyspark.sql.types import *
users_schema=StructType([StructField("sensor_id",StringType()),
                         StructField("temperature",FloatType()),
                         StructField("humidity",FloatType()),
                         StructField("pressure",FloatType()),
                         StructField("timestamp",StringType()),
                         StructField("test",IntegerType())])

# COMMAND ----------

df = spark.read.schema(users_schema).json("dbfs:/mnt/blobadhex/testblobcontainer/raw/16.8.23.json")

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df1=df.withColumn("path",input_file_name()).drop("test")

# COMMAND ----------

df1.display()

# COMMAND ----------

dbutils.fs.mount(
  source = "wasbs://processeddata@asadlsad.blob.core.windows.net",
  mount_point = "/mnt/asadlsad/processeddata",
  extra_configs = {"fs.azure.account.key.asadlsad.blob.core.windows.net":"CgjjI6wlHTDiuYdRjWwkN0akzNZMWLd5H1dfezAgt9/1QxEsBftLQvagg5iz3uh5+LkRQctEgYQK+ASt+jU+rA=="})

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/asadlsad/processeddata/iotdata/
# MAGIC

# COMMAND ----------

outputpath= "dbfs:/mnt/asadlsad/processeddata/iotdata/"

# COMMAND ----------

df1.write.mode("overwrite").parquet(f"{outputpath}yashwanth")

# COMMAND ----------


