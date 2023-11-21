# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS yvdelta.employee(id INT,  emp_name STRING, gender STRING)
# MAGIC location "dbfs:/mnt/asadlsad/processeddata/delta/emp"

# COMMAND ----------

from delta import *

# COMMAND ----------

DeltaTable.createOrReplace(spark)\
    .tableName("yvdelta.employee")\
    .addColumn("emp_id","int")\
    .addColumn("emp_name","String")\
    .addColumn("gender","String")\
    .location("dbfs:/mnt/asadlsad/processeddata/delta/emp")\
    .execute()

# COMMAND ----------

from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into yvdelta.employee values(1,"Sachin","M")

# COMMAND ----------

# MAGIC %python
# MAGIC data=[(2,'Rohit',"M")]
# MAGIC schema=StructType([StructField("emp_id",IntegerType()),
# MAGIC                    StructField("emp_name",StringType()),
# MAGIC                    StructField("gender",StringType()),
# MAGIC ])
# MAGIC (spark.createDataFrame(data,schema).write.mode("append").saveAsTable("yvdelta.employee"))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from yvdelta.employee

# COMMAND ----------

# MAGIC %python
# MAGIC df=spark.read.table("yvdelta.employee")

# COMMAND ----------

 df1=df.dropDuplicates()

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %python
# MAGIC data=[(3,'virat',"M","Batsman")]
# MAGIC schema=StructType([StructField("emp_id",IntegerType()),
# MAGIC                    StructField("emp_name",StringType()),
# MAGIC                    StructField("gender",StringType()),
# MAGIC                    StructField("dept",StringType())
# MAGIC ])
# MAGIC (spark.createDataFrame(data,schema).write.mode("append").option("mergeSchema","true").saveAsTable("yvdelta.employee"))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from yvdelta.employee

# COMMAND ----------


