# Databricks notebook source
# MAGIC %md
# MAGIC # Data Definition Notebook
# MAGIC ## Catalog Cleanup
# MAGIC This notebook handles removing the necessary catalogs after running the demo. It assumes that a company name and environment are provided

# COMMAND ----------

# Declare the parameters needed for this notebook
dbutils.widgets.dropdown('env', 'dev',['dev', 'stg', 'uat', 'prod'])
dbutils.widgets.text('company', 'acme')

# COMMAND ----------

# MAGIC %run ../_setup

# COMMAND ----------

# List the internal catalogs to drop
catalogs_to_drop = [
  f"{company}_ext_{env}_erp",
  f"{company}_{env}_data_engineering",
  f"{company}_{env}_data_science",
  f"{company}_{env}_business_intelligence"
]

# COMMAND ----------

# Loop through each catalog and DROP
for c in catalogs_to_drop:
  spark.sql(f"""DROP CATALOG IF EXISTS {c} CASCADE;""")
