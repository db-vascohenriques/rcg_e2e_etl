# Databricks notebook source
dbutils.widgets.dropdown('env', 'dev',['dev', 'stg', 'uat', 'prod'])
dbutils.widgets.text('company', 'acme')
dbutils.widgets.text('sql_conn_name', 'dwadventureworks')

# COMMAND ----------

env, company, sql_conn_name = 'dev', 'acme', 'dwadventureworks'
if dbutils and hasattr(dbutils, 'widgets'):
  env = dbutils.widgets.get('env')
  company = dbutils.widgets.get('company')
  sql_conn_name = dbutils.widgets.get('sql_conn_name')

# COMMAND ----------

catalogs_to_drop = [
  f"{company}_ext_{env}_erp",
  f"{company}_{env}_data_engineering",
  f"{company}_{env}_data_science",
  f"{company}_{env}_business_intelligence"
]

# COMMAND ----------

for c in catalogs_to_drop:
  spark.sql(f"""DROP CATALOG IF EXISTS {c} CASCADE;""")
