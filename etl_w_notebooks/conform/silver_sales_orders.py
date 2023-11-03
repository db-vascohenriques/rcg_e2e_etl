# Databricks notebook source
import pyspark.sql.functions as F

# COMMAND ----------

dbutils.widgets.dropdown('env', 'dev',['dev', 'stg', 'uat', 'prod'])
dbutils.widgets.text('company', 'acme')

# COMMAND ----------

env, company = 'dev', 'acme'
if dbutils and hasattr(dbutils, 'widgets'):
  env = dbutils.widgets.get('env')
  company = dbutils.widgets.get('company')

# COMMAND ----------


