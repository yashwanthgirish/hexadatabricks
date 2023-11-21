# Databricks notebook source
df=spark.read.option("multiline",True).json("dbfs:/mnt/asadlsad/processeddata/inputproject/json")

# COMMAND ----------

display(df)

# COMMAND ----------

from pyspark.sql.functions import  *

# COMMAND ----------

df.withColumn("products",explode("products")).display()

# COMMAND ----------

df.withColumn("products",explode("products"))\
.withColumn("price",col("products.price"))\
.withColumn("product_id",col("products.product_id"))\
.withColumn("product_name",col("products.product_name"))\
.withColumn("quantity",col("products.quantity"))\
.drop("products")\
.display()

# COMMAND ----------


