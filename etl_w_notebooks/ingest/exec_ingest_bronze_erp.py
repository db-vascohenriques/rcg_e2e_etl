# Databricks notebook source
import time, json
import pyspark.sql.functions as F
from delta import DeltaTable

# COMMAND ----------

dbutils.widgets.dropdown('env', 'dev',['dev', 'stg', 'uat', 'prod'])
dbutils.widgets.text('company', 'acme')
dbutils.widgets.text('conf', '{}')

# COMMAND ----------

conf, env, company = {}, 'dev', 'acme'
if dbutils and hasattr(dbutils, 'widgets'):
  conf = json.loads(dbutils.widgets.get('conf'))
  env = dbutils.widgets.get('env')
  company = dbutils.widgets.get('company')

# COMMAND ----------

def create_empty_table_if_not_exists(source_fqn, target_fqn):
  return spark.sql(f"CREATE TABLE IF NOT EXISTS {target_fqn} AS SELECT * FROM {source_fqn} LIMIT 0;")

# COMMAND ----------

t_start = time.time()
print(f'Working on source: {conf["src_name"]} -> target: {conf["name"]}')
# Declare the fully qualified namespace references for the source table and the target table
source_fqn = f'{company}_ext_{env}_erp.saleslt.{conf["src_name"]}'
print("\tsource_fqn:", source_fqn)
target_fqn = f'{company}_{env}_data_engineering.bronze_erp.{conf["name"]}'
print("\ttarget_fqn:", target_fqn)

# If the target table does not exist, create it
table_created = create_empty_table_if_not_exists(source_fqn, target_fqn)
t_create = time.time()

# Read both tables as dataframes
source_df = spark.read.table(source_fqn)
target_df = spark.read.table(target_fqn)

last_watermark = target_df.select(
  F.coalesce(F.max(F.col(conf['watermark_column'])),F.lit(0).cast('timestamp')).alias('watermark')
).take(1)[0].watermark
t_last_watermark = time.time()

print('\tlast_watermark:', last_watermark)

# Compute incremental changes
updates_df = source_df.where(
  F.col(conf['watermark_column']) > last_watermark
)
t_compute_incrementals = time.time()

print('\tupdates_count:', updates_df.count())
print('\tMERGING... ',end='')
t_count_updates = time.time()
# Merge
target_dt = DeltaTable.forName(spark, target_fqn)
merge_result = target_dt.alias('target').merge(
  updates_df.alias('source'),
  condition=' AND '.join([f'target.{c} = source.{c}' for c in conf['merge_on']])
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
t_merge = time.time()
print('DONE! Took', t_merge-t_count_updates, 'seconds')

# COMMAND ----------

output = {
  "target": conf['name'],
  "t_start": t_start,
  "t_create": t_create,
  "t_last_watermark": t_last_watermark,
  "t_compute_incrementals": t_compute_incrementals,
  "t_count_updates": t_count_updates,
  "t_merge": t_merge,
}

# COMMAND ----------

dbutils.notebook.exit(json.dumps(output))
