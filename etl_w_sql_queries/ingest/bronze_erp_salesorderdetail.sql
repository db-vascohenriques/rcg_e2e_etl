-- Creating the bronze layer from source systems is a responsibility of Data Engineering. 
-- Therefore bronze tables live in the data engineering catalog
USE CATALOG {{company}}_{{env}}_data_engineering; 
-- Use an appropriate schema for replication
-- Here the naming convention is up to data engineers to decide
USE bronze_erp; 

-- Create a cloud storage replica of the sales order detail table in the ERP, 
-- if it doesnt exist already
CREATE TABLE IF NOT EXISTS sales_order_detail AS
SELECT * FROM {{company}}_ext_{{env}}_erp.saleslt.salesorderdetail LIMIT 0;

-- Find what changes have happened to the sales order detail table in the ERP by 
-- checking the modification timestamp for the last time we updated the bronze table
CREATE OR REPLACE TEMPORARY VIEW sales_order_detail_updates AS
SELECT * FROM {{company}}_ext_{{env}}_erp.saleslt.salesorderdetail
WHERE ModifiedDate > COALESCE(
  (SELECT MAX(ModifiedDate) FROM sales_order_detail), 
  TIMESTAMP('1970-01-01T00:00:00.000')
);

-- Upsert everything that is new and everything that changed
MERGE INTO sales_order_detail AS Hot
USING sales_order_detail_updates AS New
  ON Hot.SalesOrderId = New.SalesOrderId
  AND Hot.SalesOrderDetailID = New.SalesOrderDetailID
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;