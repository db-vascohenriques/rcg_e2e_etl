-- Creating the silver layer from bronze is a responsibility of Data Engineering. 
-- Therefore silver tables live in the data engineering catalog
USE CATALOG {{company}}_{{env}}_data_engineering; 
-- Use an appropriate schema for replication
-- Here the naming convention is up to data engineers to decide
USE sales_facts;

-- Create the correct orders table structure by nesting line items into an
-- array of structs
CREATE OR REPLACE TEMP VIEW bronze_erp_order_conformed AS
WITH NESTED_LINE_ITEMS AS (
  SELECT SalesOrderID, collect_list(named_struct(
    'line_item_id', SHA2(CONCAT('{{env}}_erp_', CAST(SalesOrderDetailID AS STRING)),0),
    'product_fk', SHA2(CONCAT('{{env}}_erp_', CAST(ProductID AS STRING)),0),
    'quantity', OrderQty,
    'unit_price', UnitPrice,
    'discount', UnitPriceDiscount
  )) AS line_items
  FROM bronze_erp.sales_order_detail
  GROUP BY SalesOrderID
)
SELECT 
  SHA2(CONCAT('{{env}}_erp_', CAST(h.SalesOrderID AS STRING)), 0) AS `id`,
  SHA2(CONCAT('{{env}}_erp_', CAST(h.CustomerID AS STRING)),0) AS customer_fk,
  SHA2(CONCAT('{{env}}_erp_', CAST(h.ShipToAddressID AS STRING)),0) AS shto_address_fk,
  SHA2(CONCAT('{{env}}_erp_', CAST(h.BillToAddressID AS STRING)),0) AS blto_address_fk,
  DATE(h.OrderDate) AS order_date,
  li.line_items,
  h.Status AS order_status,
  h.OnlineOrderFlag AS was_online_order,
  h.ShipMethod AS shipping_method,
  MAP('revision_number', h.RevisionNumber) AS _num_attrs,
  MAP('ship_date', h.ShipDate, 'due_date', h.DueDate) AS _date_attrs,
  MAP('sales_order_no',h.SalesOrderNumber,'purchase_order_no',h.PurchaseOrderNumber,'account_number',h.AccountNumber) AS _text_attrs,
  '{{env}}_erp' AS _source_id,
  h.ModifiedDate AS _source_modstamp,
  current_timestamp() AS _row_modstamp
FROM bronze_erp.sales_order_header AS h
JOIN NESTED_LINE_ITEMS AS li
  ON h.SalesOrderID = li.SalesOrderID;

-- Create the destination table if it doesnt exist using the schema from the conformed view
CREATE TABLE IF NOT EXISTS orders AS
SELECT * FROM bronze_erp_order_conformed LIMIT 0;

-- Make sure only changes are applied
CREATE OR REPLACE TEMP VIEW bronze_erp_order_updates AS
SELECT * FROM bronze_erp_order_conformed
WHERE _source_modstamp > COALESCE((SELECT MAX(_source_modstamp) FROM orders), TIMESTAMP('1970-01-01T00:00:00.000'));

-- Update changes and insert new data
MERGE INTO orders AS target
USING bronze_erp_order_updates AS source
  ON target.`id` = source.`id`
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *