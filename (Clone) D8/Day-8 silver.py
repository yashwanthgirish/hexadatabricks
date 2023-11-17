# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %sql
# MAGIC use project;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze

# COMMAND ----------

# MAGIC %python
# MAGIC
# MAGIC df=spark.read.table("bronze")
# MAGIC df_final=df.withColumn("products",explode("products"))\
# MAGIC .withColumn("price",col("products.price"))\
# MAGIC .withColumn("product_id",col("products.product_id"))\
# MAGIC .withColumn("product_name",col("products.product_name"))\
# MAGIC .withColumn("quantity",col("products.quantity"))\
# MAGIC .withColumn("timestamp",col("timestamp").cast("timestamp"))\
# MAGIC .drop("products")
# MAGIC df_final.write.mode("overwrite").saveAsTable("project.silver")
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver

# COMMAND ----------


