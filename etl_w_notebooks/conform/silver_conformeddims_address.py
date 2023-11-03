# Databricks notebook source
# MAGIC %md
# MAGIC # Conform Addres dimension
# MAGIC ## Silver layer
# MAGIC This notebook unions all sources containing information about the address conformed dimension

# COMMAND ----------

# Import the necessary libraries for data transformation 
# and writing to the silver layer
import pyspark.sql.functions as F
from delta import DeltaTable

# COMMAND ----------

# Declare the parameters needed for this notebook
dbutils.widgets.dropdown('env', 'dev',['dev', 'stg', 'uat', 'prod'])
dbutils.widgets.text('company', 'acme')

# COMMAND ----------

# MAGIC %run ../_setup

# COMMAND ----------

# Set the right execution context for writing to the 
# conformed dimensions schema
spark.sql(f"USE CATALOG {company}_{env}_data_engineering;")
spark.sql(f"USE conformed_dims;")

# COMMAND ----------

# Read the bronze addresses table from the erp schema
bronze_df = spark.read.table('bronze_erp.address')

# COMMAND ----------

# Adapt the erp's schema to a conformed schema which could support
# additional sources in the future. Remember the silver layer should
# be source agnostic and use case agnostic
conformed_df = (
  bronze_df
  .select(
    F.sha2(F.concat(F.lit(f'{env}_erp'), F.col('AddressID').cast('string')), 0).alias('id'),
    F.coalesce(F.concat(F.col('AddressLine1')), F.lit('\n'), F.col('AddressLine2'), F.col('AddressLine1')).alias('full_address'),
    F.col('City').alias('city'),
    F.col('StateProvince').alias('state'),
    F.col('CountryRegion').alias('country'),
    F.col('PostalCode').alias('postcode'),
    F.lit(None).cast('MAP<string,double>').alias('_num_attrs'),
    F.lit(None).cast('MAP<string,date>').alias('_date_attrs'),
    F.lit(None).cast('MAP<string,string>').alias('_text_attrs'),
    F.lit(f'{env}_erp').alias('_source_id'),
    F.col('ModifiedDate').alias('_source_modstamp'),  
    F.current_timestamp().alias('_row_modstamp')
  )
)

# COMMAND ----------

# Declare the target addresses table to write to and create it
# if it does not exist
target_dt = DeltaTable.createIfNotExists(spark).tableName('conformed_dims.addresses').addColumns(conformed_df.schema).execute()

# COMMAND ----------

# Find the latest modstamp in the target for which data was upserted
latest_modstamp = (
  target_dt.toDF().select(
    F.coalesce(F.max(F.col('_source_modstamp')),F.lit(0).cast('timestamp')).alias('watermark')
  ).take(1)[0].watermark
)
# Use the latest modstamp to filter only for new data
conf_updates_df = (
  conformed_df
  .where(
    F.col('_source_modstamp') > latest_modstamp
  )
)

# COMMAND ----------

# Merge the resulting updates df into the target customers table 
# on the `id` column
merge_result = target_dt.alias('target').merge(
  conf_updates_df.alias('source'),
  condition=f'target.`id` = source.`id`'
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
