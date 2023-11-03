# Databricks notebook source
# MAGIC %md
# MAGIC # Data Definition Notebook
# MAGIC ## Schema Creation
# MAGIC This notebook handles creating the necessary schemas for running the demo. It assumes that a company name and environment are provided

# COMMAND ----------

# Declare the parameters needed for this notebook
dbutils.widgets.dropdown('env', 'dev',['dev', 'stg', 'uat', 'prod'])
dbutils.widgets.text('company', 'acme')

# COMMAND ----------

# MAGIC %run ../_setup

# COMMAND ----------

# List the internal catalogs to create, one for each 
# data quality layer and table type
schemas_to_create = [
 {"schema":"bronze_erp", "catalog":f"{company}_{env}_data_engineering"},
 {"schema":"conformed_dims", "catalog":f"{company}_{env}_data_engineering"},
 {"schema":"sales_facts", "catalog":f"{company}_{env}_data_engineering"},
 {"schema":"common_information_model", "catalog":f"{company}_{env}_business_intelligence"}
]

# COMMAND ----------

# Loop through each schema and CREATE
for s in schemas_to_create:
  spark.sql(f"""CREATE SCHEMA IF NOT EXISTS {s['catalog']}.{s['schema']};""")
