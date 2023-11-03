# Databricks notebook source
# MAGIC %md
# MAGIC # Data Definition Notebook
# MAGIC ## Catalog Creation
# MAGIC This notebook handles creating the necessary catalogs for running the demo. It assumes that a company name and environment are provided

# COMMAND ----------

# Declare the parameters needed for this notebook, this notebook has
# one additional parameter than ususal, the 'sql_conn_name' parameter 
# which takes the name of the connection to the database containing 
# the adventureworks model
dbutils.widgets.dropdown('env', 'dev',['dev', 'stg', 'uat', 'prod'])
dbutils.widgets.text('company', 'acme')
dbutils.widgets.text('sql_conn_name', 'dwadventureworks')

# COMMAND ----------

# MAGIC %run ../_setup

# COMMAND ----------

# Parse the additional 'sql_conn_name' parameter and assert not empty
sql_conn_name = 'dwadventureworks'
if dbutils and hasattr(dbutils, 'widgets'):
  sql_conn_name = dbutils.widgets.get('sql_conn_name')
assert sql_conn_name, "Please pass a correct connection name to the parameter 'sql_conn_name'"

# COMMAND ----------

# List the foreign catalogs to create, along with the connection name
foreign_catalogs_to_create = [
  {"name": f"{company}_ext_{env}_erp", "connection": sql_conn_name}
]

# COMMAND ----------

# List the internal catalogs to create, one for each business domain/persona 
# per environment
internal_catalogs_to_create = [
 f"{company}_{env}_data_engineering",
 f"{company}_{env}_data_science",
 f"{company}_{env}_business_intelligence"
]

# COMMAND ----------

# Loop through each foreign catalog and CREATE
for c in foreign_catalogs_to_create:
  spark.sql(f"""CREATE FOREIGN CATALOG IF NOT EXISTS {c['name']} USING CONNECTION {c['connection']} OPTIONS (database 'adventureworks');""")

# COMMAND ----------

# Loop through each internal catalog and CREATE
for c in internal_catalogs_to_create:
  spark.sql(f"""CREATE CATALOG IF NOT EXISTS {c};""")
