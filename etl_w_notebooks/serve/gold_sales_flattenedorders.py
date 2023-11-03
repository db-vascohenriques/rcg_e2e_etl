# Databricks notebook source
# MAGIC %md
# MAGIC # Denormalise orders
# MAGIC ## Gold layer
# MAGIC This notebook joins all comformed dimensions with the orders silver table to produce a denormalised version of the data for reporting

# COMMAND ----------

# Import the necessary libraries for data transformation
import pyspark.sql.functions as F

# COMMAND ----------

# Declare the parameters needed for this notebook
dbutils.widgets.dropdown('env', 'dev',['dev', 'stg', 'uat', 'prod'])
dbutils.widgets.text('company', 'acme')

# COMMAND ----------

# MAGIC %run ../_setup

# COMMAND ----------

# Set the right execution context for writing to the CIM
spark.sql(f"USE CATALOG {company}_{env}_business_intelligence;")
spark.sql(f"USE common_information_model;")

# COMMAND ----------

# Read the orders fact from silver
silver_df = spark.read.table(f'{company}_{env}_data_engineering.sales_facts.orders')

# COMMAND ----------

# Explode the line items column, such that each line item in 
# the array becomes an individual row in the exploded_df
orders_exploded_df = (
  silver_df
  .withColumn('line_item', F.explode(F.col('line_items')))
  .drop('line_items')
)

# COMMAND ----------

# Expand each line item from a struct into its individual components
orders_expanded_df = (
  orders_exploded_df
  .withColumn('product_fk', F.col('line_item.product_fk'))
  .withColumn('quantity', F.col('line_item.quantity'))
  .withColumn('unit_price', F.col('line_item.unit_price'))
  .withColumn('discount', F.col('line_item.discount'))
  .drop('line_item')
)

# COMMAND ----------

# Reorganise the orders dataframe to expose certain columns but not all
orders_reorg = (
  orders_expanded_df
  .select(
    'order_date', 'product_fk', 'customer_fk',
    'shto_address_fk', 'blto_address_fk',
    'quantity', 'unit_price', 'discount',
    'was_online_order', 'shipping_method',
    F.col('_date_attrs.ship_date').cast('date').alias('ship_date'),
    F.col('_date_attrs.due_date').cast('date').alias('due_date')
  )
  .where(F.col('_source_id') == F.lit(f'{env}_erp'))
)

# COMMAND ----------

# Fetch conformed dimentions into individual dfs
p_df  = spark.read.table(f'{company}_{env}_data_engineering.conformed_dims.products')
c_df  = spark.read.table(f'{company}_{env}_data_engineering.conformed_dims.customers')
sa_df = spark.read.table(f'{company}_{env}_data_engineering.conformed_dims.addresses')
ba_df = spark.read.table(f'{company}_{env}_data_engineering.conformed_dims.addresses')

# COMMAND ----------

# Perform the join and select the necessary columns. Choosing 
# the necessary columns always comes down to what the business 
# needs, so the BI team must make sure the deliver on the brief
orders_joined = (
  orders_reorg.alias('o')
  .join(p_df.alias('p'),   on=[F.col('o.product_fk') == F.col('p.id')],       how='left')
  .join(c_df.alias('c'),   on=[F.col('o.customer_fk') == F.col('c.id')],       how='left')
  .join(sa_df.alias('sa'), on=[F.col('o.shto_address_fk') == F.col('sa.id')], how='left')
  .join(ba_df.alias('ba'), on=[F.col('o.blto_address_fk') == F.col('ba.id')], how='left')
  .select(
    *[F.col(f'o.{c}') for c in orders_reorg.columns if c not in ['product_fk', 'customer_fk', 'shto_address_fk', 'blto_address_fk']],
    F.col('p.name').alias('product_name'),
    *[F.col(f'p.{c}') for c in p_df.columns if c not in ['id', 'name', 'model_descriptions', '_num_attrs', '_date_attrs', '_text_attrs', '_source_id', '_source_modstamp', '_row_modstamp']],
    F.col('p.model_descriptions.en').alias('model_description'),
    F.col('c.name').alias('customer_name'),
    *[F.col(f'c.{c}') for c in c_df.columns if c not in ['id', 'name', 'email', 'phone', '_num_attrs', '_date_attrs', '_text_attrs', '_source_id', '_source_modstamp', '_row_modstamp']],
    F.col('sa.full_address').alias('shipment_address'),
    F.col('sa.city').alias('shipment_city'),
    F.col('sa.state').alias('shipment_state'),
    F.col('sa.country').alias('shipment_country'),
    F.col('sa.postcode').alias('shipment_postcode'),
    F.col('ba.full_address').alias('billing_address'),
    F.col('ba.city').alias('billing_city'),
    F.col('ba.state').alias('billing_state'),
    F.col('ba.country').alias('billing_country'),
    F.col('ba.postcode').alias('billing_postcode')
  )
)

# COMMAND ----------

# Write the joined result to the gold table
orders_joined.write.mode('overwrite').saveAsTable('orders_flattened')
