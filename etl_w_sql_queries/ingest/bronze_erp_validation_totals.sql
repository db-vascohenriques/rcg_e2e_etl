-- Creating the bronze layer from source systems is a responsibility of Data Engineering. 
-- Therefore bronze tables live in the data engineering catalog
USE CATALOG {{company}}_{{env}}_data_engineering; 
-- Use an appropriate schema for replication
-- Here the naming convention is up to data engineers to decide
USE bronze_erp; 

CREATE OR REPLACE TEMPORARY VIEW _val_totals_ungrouped AS
SELECT 'customer' as table_name, NULL as source_count, COUNT(*) AS bronze_count FROM customer
UNION
SELECT 'customer', COUNT(*), NULL FROM {{company}}_ext_{{env}}_erp.saleslt.customer
UNION
SELECT 'address', NULL, COUNT(*) FROM address
UNION
SELECT 'address', COUNT(*), NULL FROM {{company}}_ext_{{env}}_erp.saleslt.address
UNION
SELECT 'product', NULL, COUNT(*) FROM product
UNION
SELECT 'product', COUNT(*), NULL FROM {{company}}_ext_{{env}}_erp.saleslt.product
UNION
SELECT 'customer_address', NULL, COUNT(*) FROM customer_address
UNION
SELECT 'customer_address', COUNT(*), NULL FROM {{company}}_ext_{{env}}_erp.saleslt.customeraddress
UNION
SELECT 'product_category', NULL, COUNT(*) FROM product_category
UNION
SELECT 'product_category', COUNT(*), NULL FROM {{company}}_ext_{{env}}_erp.saleslt.productcategory
UNION
SELECT 'product_model', NULL, COUNT(*) FROM product_model
UNION
SELECT 'product_model', COUNT(*), NULL FROM {{company}}_ext_{{env}}_erp.saleslt.productmodel
UNION
SELECT 'product_description', NULL, COUNT(*) FROM product_description
UNION
SELECT 'product_description', COUNT(*), NULL FROM {{company}}_ext_{{env}}_erp.saleslt.productdescription
UNION
SELECT 'product_model_description', NULL, COUNT(*) FROM product_model_description
UNION
SELECT 'product_model_description', COUNT(*), NULL FROM {{company}}_ext_{{env}}_erp.saleslt.productmodelproductdescription
UNION
SELECT 'sales_order_header', NULL, COUNT(*) FROM sales_order_header
UNION
SELECT 'sales_order_header', COUNT(*), NULL FROM {{company}}_ext_{{env}}_erp.saleslt.salesorderheader
UNION
SELECT 'sales_order_detail', NULL, COUNT(*) FROM sales_order_detail
UNION
SELECT 'sales_order_detail', COUNT(*), NULL FROM {{company}}_ext_{{env}}_erp.saleslt.salesorderdetail;

CREATE OR REPLACE TABLE _val_totals AS
SELECT 
    table_name,
    MAX(source_count) as source_count,
    MAX(bronze_count) AS bronze_count,
    MAX(source_count) = MAX(bronze_count) AS totals_do_match,
    current_timestamp() AS validation_ts
FROM _val_totals_ungrouped
GROUP BY table_name ORDER BY table_name ASC;

SELECT 
  CASE WHEN (REDUCE(COLLECT_LIST(totals_do_match), TRUE, (a, x) -> x and a))
  THEN 'SUCCESS' ELSE raise_error('Validation failed for counts in source v.s. bronze') END 
FROM princes_prod_data_engineering.bronze_erp._val_totals;