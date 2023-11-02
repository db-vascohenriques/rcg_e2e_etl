-- Creating the bronze layer from source systems is a responsibility of Data Engineering. 
-- Therefore bronze tables live in the data engineering catalog
USE CATALOG {{company}}_{{env}}_data_engineering; 
-- Use an appropriate schema for replication
-- Here the naming convention is up to data engineers to decide
USE bronze_erp; 

-- Create a cloud storage replica of the product table in the ERP, if it doesnt exist already
CREATE TABLE IF NOT EXISTS product AS
SELECT * FROM {{company}}_ext_{{env}}_erp.saleslt.product LIMIT 0;

-- Find what changes have happened to the product table in the ERP by checking the modification
-- timestamp for the last time we updated the bronze table
CREATE OR REPLACE TEMPORARY VIEW product_updates AS
SELECT * FROM {{company}}_ext_{{env}}_erp.saleslt.product
WHERE ModifiedDate > COALESCE(
  (SELECT MAX(ModifiedDate) FROM product), 
  TIMESTAMP('1970-01-01T00:00:00.000')
);

-- Upsert everything that is new and everything that changed
MERGE INTO product AS Hot
USING product_updates AS New
  ON Hot.ProductID = New.ProductID
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;