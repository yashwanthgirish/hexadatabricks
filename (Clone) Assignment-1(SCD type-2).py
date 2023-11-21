# Databricks notebook source
# MAGIC %python
# MAGIC emp=[(1, "Scott", "Tiger", 1000.0,
# MAGIC                       "united states"
# MAGIC                      )]
# MAGIC df = spark. \
# MAGIC     createDataFrame(emp,
# MAGIC                     schema="""employee_id INT, first_name STRING,
# MAGIC                     last_name STRING, salary FLOAT, nationality STRING
# MAGIC                     """
# MAGIC                    )
# MAGIC display(df)

# COMMAND ----------

df.createOrReplaceTempView("source_view")

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC select * from source_view

# COMMAND ----------

# MAGIC %sql
# MAGIC create table emptarget(
# MAGIC    employee_id int,
# MAGIC    first_name string,
# MAGIC    last_name string,
# MAGIC    salary int,
# MAGIC    nationality string,
# MAGIC    startDate date,
# MAGIC    endDate date
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO emptarget a
# MAGIC using
# MAGIC (
# MAGIC     select employee_id as mergeKey,* from source_view
# MAGIC     union all
# MAGIC     select NULL as mergeKey,a.* from source_view a join emptarget b on a.employee_id = b.employee_id
# MAGIC )b
# MAGIC ON a.employee_id = b.mergeKey
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC    a.endDate = current_date() - 1
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (
# MAGIC     employee_id,
# MAGIC     first_name,
# MAGIC     last_name,
# MAGIC     salary,
# MAGIC     nationality,
# MAGIC     startDate,
# MAGIC     endDate
# MAGIC   )VALUES (
# MAGIC     employee_id,
# MAGIC     first_name,
# MAGIC     last_name,
# MAGIC     salary,
# MAGIC     nationality,
# MAGIC     current_date(),
# MAGIC     '9999-12-31'
# MAGIC  
# MAGIC   )

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from emptarget

# COMMAND ----------

emp = [(1, "Scott", "Tiger", 1000.0,
                      "India"
                     ),
             (2,"John","Clair",2000.0,"UK"),
             (3,"Max","Jean",3000.0,"Mexico"),
             (4,"Robin","Hood",4000.0,"Africa"),
             (5,"Den","Mark",5000.0,"Canada")]
df = spark. \
    createDataFrame(emp,
                    schema="""employee_id INT, first_name STRING,
                    last_name STRING, salary FLOAT, nationality STRING
                    """
                   )
df.createOrReplaceTempView("source_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from emptarget

# COMMAND ----------


