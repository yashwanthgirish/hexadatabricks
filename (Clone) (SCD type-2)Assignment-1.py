# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %sql
# MAGIC truncate table employee;

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table employees(employee_id INT, first_name STRING,
# MAGIC                     last_name STRING, salary FLOAT, nationality STRING,current boolean,start_date date,end_date date)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from employees

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC insert into employees values (1, "Scott", "Tiger", 1000.0,
# MAGIC                       "united states",1,current_date(),'9999-12-31')

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table emp_source(employee_id INT, first_name STRING,last_name STRING, salary FLOAT, nationality STRING,start_date date)
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into emp_source values(1, "Scott", "Tiger", 1000.0,"UK",current_date())

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from emp_source

# COMMAND ----------

# MAGIC %sql
# MAGIC select emp_source.employee_id as mergekey,emp_source.* from emp_source
# MAGIC union all
# MAGIC select null as mergekey,emp_source.* from emp_source join employees on emp_source.employee_id=employees.employee_id where employees.current=1 and emp_source.nationality <> employees.nationality
# MAGIC

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO employees as target
# MAGIC USING(select emp_source.employee_id as mergekey,emp_source.* from emp_source
# MAGIC union all
# MAGIC select null as mergekey,emp_source.* from emp_source join employees on emp_source.employee_id=employees.employee_id where employees.current=1 and emp_source.nationality <> employees.nationality) updates on target.employee_id=mergekey
# MAGIC
# MAGIC WHEN MATCHED and target.current=1 and target.nationality <> updates.nationality
# MAGIC
# MAGIC THEN
# MAGIC   UPDATE SET current=0 , end_date=updates.start_date
# MAGIC   WHEN NOT MATCHED
# MAGIC   THEN INSERT (
# MAGIC     employee_id,
# MAGIC     first_name,
# MAGIC     last_name,
# MAGIC     salary,
# MAGIC     nationality,
# MAGIC     current,
# MAGIC     start_date,
# MAGIC     end_date
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     updates.employee_id,
# MAGIC     updates.first_name,
# MAGIC     updates.last_name,
# MAGIC     updates.salary,
# MAGIC     updates.nationality,
# MAGIC     1,
# MAGIC     updates.start_date,
# MAGIC     '9999-12-31'
# MAGIC   )

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from employees

# COMMAND ----------


