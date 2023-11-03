# Databricks notebook source
# MAGIC %md
# MAGIC # Conform product dimension
# MAGIC ## Silver layer
# MAGIC This notebook unions all sources containing information about the product conformed dimension

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

# Read the bronze products table from the erp schema as
# well as all supporting tables to enrich product with
bronze_df = spark.read.table('bronze_erp.product')
pc_df = spark.read.table('bronze_erp.product_category')
pm_df = spark.read.table('bronze_erp.product_model')
pd_df = spark.read.table('bronze_erp.product_description')
pre_pmd_df = spark.read.table('bronze_erp.product_model_description')

# COMMAND ----------

# Nest the model descriptions per language into a map type column
# such that said column could be used in the conformed product dim 
# for description purposes
pmd_df = (
  pre_pmd_df.alias('pmd1').join(
    pd_df.alias('pd1'), 
    on=[F.col('pmd1.ProductDescriptionID') == F.col('pd1.ProductDescriptionID')],
    how='left'
  )
  .groupBy('pmd1.ProductModelID')
  .agg(F.map_from_arrays(
    F.collect_list(F.col('pmd1.Culture')),
    F.collect_list(F.col('pd1.Description'))
  ).alias('descriptions'))
)

# COMMAND ----------

# Adapt the erp's schema to a conformed schema which could support
# additional sources in the future. Remember the silver layer should
# be source agnostic and use case agnostic
conformed_df = (
  bronze_df.alias('p')
  .join(pc_df.alias('pc'),   on=[F.col('p.ProductCategoryID') == F.col('pc.ProductCategoryID')],         how='left')
  .join(pc_df.alias('ppc'),  on=[F.col('pc.ParentProductCategoryID') == F.col('ppc.ProductCategoryID')], how='left')
  .join(pm_df.alias('pm'),   on=[F.col('p.ProductModelID') == F.col('pm.ProductModelID')],               how='left')
  .join(pmd_df.alias('pmd'), on=[F.col('p.ProductModelID') == F.col('pmd.ProductModelID')],              how='left')
  .select(
    F.sha2(F.concat(F.lit(f'{env}_erp'), F.col('p.ProductID').cast('string')), 0).alias('id'),
    F.col('p.ProductNumber').alias('sku'),
    F.col('p.Name').alias('name'),
    F.col('p.ListPrice').alias('list_price'),
    F.col('pc.Name').alias('category_name'),
    F.col('ppc.Name').alias('parent_category_name'),
    F.col('pm.Name').alias('model_name'),
    F.col('pmd.descriptions').alias('model_descriptions'), 
    F.create_map(
      F.lit('cost'), F.col('p.StandardCost').cast('double'),
      F.lit('weight'), F.col('p.Weight')
    ).cast('MAP<string,double>').alias('_num_attrs'),
    F.create_map(
      F.lit('sell_start_ts'), F.col('p.SellStartDate'),
      F.lit('sell_end_ts'), F.col('p.SellEndDate'),
      F.lit('discontinued_ts'), F.col('p.DiscontinuedDate'),
    ).cast('MAP<string,date>').alias('_date_attrs'),
    F.create_map(
      F.lit('color'), F.col('p.Color'),
      F.lit('size'), F.col('p.Size')
    ).cast('MAP<string,string>').alias('_text_attrs'),
    F.lit(f'{env}_erp').alias('_source_id'),
    F.col('p.ModifiedDate').alias('_source_modstamp'),  
    F.current_timestamp().alias('_row_modstamp')
  )
)

# COMMAND ----------

# Declare the target products table to write to and create it
# if it does not exist
target_dt = DeltaTable.createIfNotExists(spark).tableName('conformed_dims.products').addColumns(conformed_df.schema).execute()

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
