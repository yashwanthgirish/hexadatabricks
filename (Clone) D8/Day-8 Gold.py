# Databricks notebook source
# MAGIC %sql
# MAGIC use project;

# COMMAND ----------

# MAGIC %sql
# MAGIC Create or replace table project.gold as (select product_name, sum(quantity) as totalquantity from silver group by all order by totalquantity desc)

# COMMAND ----------


