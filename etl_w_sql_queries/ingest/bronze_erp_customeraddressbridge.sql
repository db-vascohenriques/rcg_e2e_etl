-- Creating the bronze layer from source systems is a responsibility of Data Engineering. 
-- Therefore bronze tables live in the data engineering catalog
USE CATALOG {{company}}_{{env}}_data_engineering; 
-- Use an appropriate schema for replication
-- Here the naming convention is up to data engineers to decide
USE bronze_erp; 

-- Create a cloud storage replica of the customer address table in the ERP, if it doesnt exist already
CREATE TABLE IF NOT EXISTS customer_address AS
SELECT * FROM {{company}}_ext_{{env}}_erp.saleslt.customeraddress LIMIT 0;

-- Find what changes have happened to the customer address table in the ERP by checking the modification
-- timestamp for the last time we updated the bronze table
CREATE OR REPLACE TEMPORARY VIEW customer_address_updates AS
SELECT * FROM {{company}}_ext_{{env}}_erp.saleslt.customeraddress
WHERE ModifiedDate > COALESCE(
  (SELECT MAX(ModifiedDate) FROM customer_address), 
  TIMESTAMP('1970-01-01T00:00:00.000')
);

-- Upsert everything that is new and everything that changed
MERGE INTO customer_address AS Hot
USING customer_address_updates AS New
  ON Hot.CustomerID = New.CustomerID
  AND Hot.AddressID = New.AddressID
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;