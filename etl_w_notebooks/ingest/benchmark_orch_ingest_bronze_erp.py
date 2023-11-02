# Databricks notebook source
import pyspark.sql.functions as F
from multiprocessing.pool import ThreadPool
import json, uuid

# COMMAND ----------

dbutils.widgets.dropdown('env', 'dev',['dev', 'stg', 'uat', 'prod'])
dbutils.widgets.text('company', 'acme')

# COMMAND ----------

env, company, run_id = 'dev', 'acme', str(uuid.uuid4()).split('-')[0]
if dbutils and hasattr(dbutils, 'widgets'):
  env = dbutils.widgets.get('env')
  company = dbutils.widgets.get('company')

# COMMAND ----------

tables_in_source = [i.tableName
  for i in spark.sql(f"SHOW TABLES IN {company}_ext_{env}_erp.saleslt").collect()
]

# COMMAND ----------

tables_to_replicate = [
  {"src_name":"address", "name": "address", "watermark_column": "ModifiedDate", "merge_on": ["AddressID"]},
  {"src_name":"customer", "name": "customer", "watermark_column": "ModifiedDate", "merge_on": ["CustomerID"]},
  {"src_name":"customeraddress", "name": "customer_address", "watermark_column": "ModifiedDate", "merge_on": ["CustomerID","AddressID"]},
  {"src_name":"product", "name": "product", "watermark_column": "ModifiedDate", "merge_on": ["ProductID"]},
  {"src_name":"productcategory", "name": "product_category", "watermark_column": "ModifiedDate", "merge_on": ["ProductCategoryID"]},
  {"src_name":"productdescription", "name": "product_description", "watermark_column": "ModifiedDate", "merge_on": ["ProductDescriptionID"]},
  {"src_name":"productmodel", "name": "product_model", "watermark_column": "ModifiedDate", "merge_on": ["ProductModelID"]},
  {"src_name":"productmodelproductdescription", "name": "product_model_description", "watermark_column": "ModifiedDate", "merge_on": ["ProductModelID","ProductDescriptionID"]},
  {"src_name":"salesorderdetail", "name": "salesorderdetail", "watermark_column": "ModifiedDate", "merge_on": ["SalesOrderId","SalesOrderDetailID"]},
  {"src_name":"salesorderheader", "name": "salesorderheader", "watermark_column": "ModifiedDate", "merge_on": ["SalesOrderId"]}
]

# COMMAND ----------

def create_schema_if_not_exists(fqn):
  return spark.sql(f"CREATE SCHEMA IF NOT EXISTS {fqn};")
create_schema_if_not_exists(f'{company}_{env}_data_engineering.bronze_erp')

# COMMAND ----------

def clean_all_tables():
  for i in [c['name'] for c in tables_to_replicate if c['src_name'] in tables_in_source]:
    spark.sql(f"DELETE FROM {company}_{env}_data_engineering.bronze_erp.{i}")

# COMMAND ----------

bench_r = []
for epoch in range(1, 20, 1):
  for i in range(1, 11, 1):
    print("Epoch:", epoch, "Test Concurrency:", i)
    pool = ThreadPool(i)
    confs = [c for c in tables_to_replicate if c['src_name'] in tables_in_source]
    r_raw = pool.map(lambda c: dbutils.notebook.run('./exec_ingest_bronze_erp', timeout_seconds=3600, arguments={"conf": json.dumps(c), "company": company, "env": env})
      , confs
    )

    r_flat = []
    for r in r_raw:
      r_tmp = json.loads(r)
      r_tmp["epoch"] = epoch
      r_tmp["concurrency"] = i
      r_flat.append(r_tmp)

    r_df = spark.createDataFrame(r_flat)
    write_df = (
      r_df
      .select(
        F.lit(run_id).alias('run_id'),
        F.col('target'), F.col('concurrency'), F.col('epoch'),
        (F.col('t_create')-F.col('t_start')).alias('create_time'),
        (F.col('t_last_watermark')-F.col('t_create')).alias('last_watermark_time'),
        (F.col('t_compute_incrementals')-F.col('t_last_watermark')).alias('compute_incrementals_time'),
        (F.col('t_count_updates')-F.col('t_compute_incrementals')).alias('count_updates_time'),
        (F.col('t_merge')-F.col('t_count_updates')).alias('merge_time'),
        (F.col('t_merge')-F.col('t_start')).alias('total_time'),
        F.current_timestamp().alias('row_insert_ts')
      )
    )
    write_df.write.mode('append').saveAsTable('vh_catalog.benchmarks.rcg_e2e_etl_bronze_ingest')
    clean_all_tables()

# COMMAND ----------


