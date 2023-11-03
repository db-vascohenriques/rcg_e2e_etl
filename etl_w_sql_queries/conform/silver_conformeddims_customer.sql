-- Creating the silver layer from bronze is a responsibility of Data Engineering. 
-- Therefore silver tables live in the data engineering catalog
USE CATALOG {{company}}_{{env}}_data_engineering; 
-- Use an appropriate schema for replication
-- Here the naming convention is up to data engineers to decide
USE conformed_dims;

-- This view is created to extract customer data from the ERP and conform it 
-- for data warehousing purposes. The view coalesces the data into a format that
-- is consistent with the schema of the customers table.
CREATE OR REPLACE TEMP VIEW bronze_erp_customer_conformed AS
SELECT 
  SHA2(CONCAT('{{env}}_erp_', CAST(CustomerID AS STRING)), 0) AS `id`,
  COALESCE(FirstName, '') || ' ' || COALESCE(MiddleName, '') || ' ' || COALESCE(LastName, '') AS name,
  EmailAddress AS email,
  Phone AS phone,
  CompanyName AS company,
  CAST(NULL AS MAP<string, double>) AS _num_attrs,
  CAST(NULL AS MAP<string, date>) AS _date_attrs,
  MAP('title', Title, 'suffix', Suffix, 'sales_person', SalesPerson) AS _text_attrs,
  '{{env}}_erp' AS _source_id,
  ModifiedDate AS _source_modstamp,
  current_timestamp() AS _row_modstamp
FROM bronze_erp.customer;

-- Create the destination table if it doesnt exist using the schema from the conformed view
CREATE TABLE IF NOT EXISTS customers AS
SELECT * FROM bronze_erp_customer_conformed LIMIT 0;

-- Make sure only changes are applied
CREATE OR REPLACE TEMP VIEW bronze_erp_customer_updates AS
SELECT * FROM bronze_erp_customer_conformed
WHERE _source_modstamp > COALESCE((SELECT MAX(_source_modstamp) FROM customers), TIMESTAMP('1970-01-01T00:00:00.000'));

-- Update changes and insert new data
MERGE INTO customers AS target
USING bronze_erp_customer_updates AS source
  ON target.`id` = source.`id`
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *