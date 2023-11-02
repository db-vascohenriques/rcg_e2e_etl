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

foreign_catalogs_to_create = [
  {"name": f"{company}_ext_{env}_erp", "connection": sql_conn_name}
]

# COMMAND ----------

internal_catalogs_to_create = [
 f"{company}_{env}_data_engineering",
 f"{company}_{env}_data_science",
 f"{company}_{env}_business_intelligence"
]

# COMMAND ----------

for c in foreign_catalogs_to_create:
  spark.sql(f"""CREATE FOREIGN CATALOG IF NOT EXISTS {c['name']} USING CONNECTION {c['connection']} OPTIONS (database 'adventureworks');""")

# COMMAND ----------

for c in internal_catalogs_to_create:
  spark.sql(f"""CREATE CATALOG IF NOT EXISTS {c};""")
