# Databricks notebook source
spark

# COMMAND ----------

# MAGIC %md
# MAGIC #####spark(spark session):spark is an entry point to start your driver program

# COMMAND ----------

users=[(1,"a",30),(2,"b",32)]
sampledf=spark.createDataFrame(users)

# COMMAND ----------

sampledf.show()

# COMMAND ----------

users_schema_str="id int,name string,age int"
users=[(1,"a",30),(2,"b",32)]
df1=spark.createDataFrame(users,users_schema_str)
df1.show()


# COMMAND ----------


