# Databricks notebook source
dbutils.widgets.dropdown('env', 'dev',['dev', 'stg', 'uat', 'prod'])
dbutils.widgets.text('company', 'acme')

# COMMAND ----------

env, company = 'dev', 'acme'
if dbutils and hasattr(dbutils, 'widgets'):
  env = dbutils.widgets.get('env')
  company = dbutils.widgets.get('company')

# COMMAND ----------

schemas_to_create = [
 {"schema":"bronze_erp", "catalog":f"{company}_{env}_data_engineering"},
 {"schema":"conformed_dims", "catalog":f"{company}_{env}_data_engineering"},
 {"schema":"sales_facts", "catalog":f"{company}_{env}_data_engineering"},
 {"schema":"common_information_model", "catalog":f"{company}_{env}_business_intelligence"}
]

# COMMAND ----------

for s in schemas_to_create:
  spark.sql(f"""CREATE SCHEMA IF NOT EXISTS {s['catalog']}.{s['schema']};""")
