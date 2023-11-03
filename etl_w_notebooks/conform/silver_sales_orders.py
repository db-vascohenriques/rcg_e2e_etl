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

spark.sql(f"USE CATALOG {company}_{env}_data_engineering;")
spark.sql(f"USE sales_facts;")

# COMMAND ----------

bronze_df = spark.read.table('bronze_erp.sales_order_header')
sod_df = spark.read.table('bronze_erp.product_category')

# COMMAND ----------

nli_df = (
  sod_df
  .groupBy('SalesOrderID')
  .agg(F.collect_list(
    F.struct(
      F.sha2(F.concat(F.lit(f'{env}_erp'), F.col('SalesOrderDetailID').cast('string')), 0).alias(''),
      F.sha2(F.concat(F.lit(f'{env}_erp'), F.col('ProductID').cast('string')), 0).alias(''),
      F.col('quantity').alias('quantity'),
      F.col('unit_price').alias('unit_price'),
      F.col('discount').alias('discount')
    )
  ).alias('line_items'))
)

# COMMAND ----------

conformed_df = (
  bronze_df.alias('h')
  .join(nli_df.alias('li'), on=[F.col('h.SalesOrderID') == F.col('li.SalesOrderID')])
  .select(
    F.sha2(F.concat(F.lit(f'{env}_erp'), F.col('p.ProductID').cast('string')), 0).alias('id'),
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

target_dt = DeltaTable.createIfNotExists(spark).tableName('sales_facts.orders').addColumns(conformed_df.schema).execute()

# COMMAND ----------

latest_modstamp = (
  target_dt.toDF().select(
    F.coalesce(F.max(F.col('_source_modstamp')),F.lit(0).cast('timestamp')).alias('watermark')
  ).take(1)[0].watermark
)
conf_updates_df = (
  conformed_df
  .where(
    F.col('_source_modstamp') > latest_modstamp
  )
)

# COMMAND ----------

merge_result = target_dt.alias('target').merge(
  conf_updates_df.alias('source'),
  condition=f'target.`id` = source.`id`'
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
