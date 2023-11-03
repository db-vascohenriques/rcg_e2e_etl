# Databricks notebook source
import pyspark.sql.functions as F
from multiprocessing.pool import ThreadPool
import json

# COMMAND ----------

dbutils.widgets.dropdown('env', 'dev',['dev', 'stg', 'uat', 'prod'])
dbutils.widgets.text('company', 'acme')

# COMMAND ----------

env, company = 'dev', 'acme'
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
  {"src_name":"salesorderdetail", "name": "sales_order_detail", "watermark_column": "ModifiedDate", "merge_on": ["SalesOrderId","SalesOrderDetailID"]},
  {"src_name":"salesorderheader", "name": "sales_order_header", "watermark_column": "ModifiedDate", "merge_on": ["SalesOrderId"]}
]

# COMMAND ----------

def create_schema_if_not_exists(fqn):
  return spark.sql(f"CREATE SCHEMA IF NOT EXISTS {fqn};")
create_schema_if_not_exists(f'{company}_{env}_data_engineering.bronze_erp')

# COMMAND ----------

pool = ThreadPool(9)
confs = [c for c in tables_to_replicate if c['src_name'] in tables_in_source]
r_raw = pool.map(lambda c: dbutils.notebook.run('./exec_ingest_bronze_erp', timeout_seconds=3600, arguments={"conf": json.dumps(c), "company": company, "env": env})
  , confs
)
