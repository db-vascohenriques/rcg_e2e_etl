# Databricks notebook source
# MAGIC %md
# MAGIC # Conform sales order
# MAGIC ## Silver layer
# MAGIC This notebook unions all sources containing information about sales orders

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
# sales facts schema
spark.sql(f"USE CATALOG {company}_{env}_data_engineering;")
spark.sql(f"USE sales_facts;")

# COMMAND ----------

# Read the bronze sales order header and detail tables
# from the erp schema
bronze_df = spark.read.table('bronze_erp.sales_order_header')
sod_df = spark.read.table('bronze_erp.sales_order_detail')

# COMMAND ----------

# Nest each line item in the sales order detail dataframe to create
# an array type column containing each line item per order
nli_df = (
  sod_df
  .groupBy('SalesOrderID')
  .agg(F.collect_list(
    F.struct(
      F.sha2(F.concat(F.lit(f'{env}_erp'), F.col('SalesOrderDetailID').cast('string')), 0).alias('line_item_id'),
      F.sha2(F.concat(F.lit(f'{env}_erp'), F.col('ProductID').cast('string')), 0).alias('product_fk'),
      F.col('OrderQty').cast('double').alias('quantity'),
      F.col('UnitPrice').cast('double').alias('unit_price'),
      F.col('UnitPriceDiscount').cast('double').alias('discount')
    )
  ).alias('line_items'))
)

# COMMAND ----------

# Adapt the erps's schema to a conformed schema which could support
# additional sources in the future. Remember the silver layer should
# be source agnostic and use case agnostic
conformed_df = (
  bronze_df.alias('h')
  .join(nli_df.alias('li'), on=[F.col('h.SalesOrderID') == F.col('li.SalesOrderID')])
  .select(
    F.sha2(F.concat(F.lit(f'{env}_erp'), F.col('h.SalesOrderID').cast('string')), 0).alias('id'),
    F.sha2(F.concat(F.lit(f'{env}_erp'), F.col('h.CustomerID').cast('string')), 0).alias('customer_fk'),
    F.sha2(F.concat(F.lit(f'{env}_erp'), F.col('h.ShipToAddressID').cast('string')), 0).alias('shto_address_fk'),
    F.sha2(F.concat(F.lit(f'{env}_erp'), F.col('h.BillToAddressID').cast('string')), 0).alias('blto_address_fk'),
    F.col('h.OrderDate').cast('date').alias('order_date'),
    F.col('li.line_items').alias('line_items'),
    F.col('h.Status').alias('order_status'),
    F.col('h.OnlineOrderFlag').alias('was_online_order'),
    F.col('h.ShipMethod').alias('shipping_method'),
    F.create_map(
      F.lit('revision_number'), F.col('h.RevisionNumber').cast('double')
    ).cast('MAP<string,double>').alias('_num_attrs'),
    F.create_map(
      F.lit('ship_date'), F.col('h.ShipDate'),
      F.lit('due_date'), F.col('h.DueDate')
    ).cast('MAP<string,date>').alias('_date_attrs'),
    F.create_map(
      F.lit('sales_order_no'), F.col('h.SalesOrderNumber'),
      F.lit('purchase_order_no'), F.col('h.PurchaseOrderNumber'),
      F.lit('account_number'), F.col('h.AccountNumber')
    ).cast('MAP<string,string>').alias('_text_attrs'),
    F.lit(f'{env}_erp').alias('_source_id'),
    F.col('h.ModifiedDate').alias('_source_modstamp'),  
    F.current_timestamp().alias('_row_modstamp')
  )
)

# COMMAND ----------

# Declare the target addresses table to write to and create it
# if it does not exist
target_dt = DeltaTable.createIfNotExists(spark).tableName('sales_facts.orders').addColumns(conformed_df.schema).execute()

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
