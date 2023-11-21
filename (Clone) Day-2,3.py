# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC This notebook will show you how to create and query a table or DataFrame that you uploaded to DBFS. [DBFS](https://docs.databricks.com/user-guide/dbfs-databricks-file-system.html) is a Databricks File System that allows you to store data for querying inside of Databricks. This notebook assumes that you have a file already inside of DBFS that you would like to read from.
# MAGIC
# MAGIC This notebook is written in **Python** so the default cell type is Python. However, you can use different languages by using the `%LANGUAGE` syntax. Python, Scala, SQL, and R are all supported.

# COMMAND ----------

# File location and type
file_location = "/FileStore/tables/F1/circuits.csv"
file_type = "csv"

# CSV options
infer_schema = "false"
first_row_is_header = "false"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

display(df)

# COMMAND ----------

# Create a view or table

temp_table_name = "circuits_csv"

df.createOrReplaceTempView(temp_table_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC /* Query the created temp table in a SQL cell */
# MAGIC
# MAGIC select * from `circuits_csv`

# COMMAND ----------

# With this registered as a temp view, it will only be available to this particular notebook. If you'd like other users to be able to query this table, you can also create a table from the DataFrame.
# Once saved, this table will persist across cluster restarts as well as allow various users across different notebooks to query this data.
# To do so, choose your table name and uncomment the bottom line.

permanent_table_name = "circuits_csv"

# df.write.format("parquet").saveAsTable(permanent_table_name)

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/tables/F1/

# COMMAND ----------

df=spark.read.csv("dbfs:/FileStore/tables/F1/circuits.csv",header=True)

# COMMAND ----------

df.display()

# COMMAND ----------

df=spark.read.csv("dbfs:/FileStore/tables/F1/circuits.csv",header=True,inferSchema=True)
df.write.mode("overwrite").saveAsTable("F1.circuits")

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists F1;
# MAGIC use F1;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1.circuits where circuitRef="monaco"

# COMMAND ----------

df=spark.read.option("header",True).option("inferschema",True).csv("dbfs:/FileStore/tables/F1/circuits.csv")

# COMMAND ----------

df.select("circuitId","circuitRef","name").display()

# COMMAND ----------

from pyspark.sql.functions import col
df.select(col("circuitid").alias("circuit_id")).display()

# COMMAND ----------

df.select(col("circuitid"),"circuitref",df.name,df["location"]).display()

# COMMAND ----------

df.rdd.getNumPartitions()

# COMMAND ----------

from pyspark.sql.functions import *
df.select(concat("location","country").alias("new column")).display()
df.select("*", concat("location",lit(" "),"country").alias("new column")).display()

# COMMAND ----------

help(df.withColumnRenamed)

# COMMAND ----------

df.withColumnRenamed("circuitid","circuit_id").display()

# COMMAND ----------

df.columns

# COMMAND ----------

newcolumns=['circuitId',
 'circuitRef',
 'name',
 'location',
 'country',
 'lat',
 'lng',
 'alt',
 'url']

# COMMAND ----------

df.toDF(*newcolumns).display()

# COMMAND ----------

df.withColumn("formula1",lit("formula1 data")).display()

# COMMAND ----------

df.withColumn("ingestiondate",current_date()).display()

# COMMAND ----------

df.withColumn("date",current_timestamp()).display()

# COMMAND ----------

df.columns

# COMMAND ----------

df.where("circuitID=1").display()


# COMMAND ----------

df.filter(col("circuitID")==1).display()

# COMMAND ----------

df.filter("circuitID > 10 and country ='UK'").display()

# COMMAND ----------

df.filter((col("circuitID") > 10) & (col("country") =='UK')).display()

# COMMAND ----------

df.orderBy(col("circuitid").desc()).display()

# COMMAND ----------

df.orderBy(desc("circuitid")).display()

# COMMAND ----------

df.sort("country","location").select("country","location").display()

# COMMAND ----------


