-- Creating the bronze layer from source systems is a responsibility of Data Engineering. 
-- Therefore bronze tables live in the data engineering catalog
USE CATALOG {{company}}_{{env}}_data_engineering; 
-- Use an appropriate schema for replication
-- Here the naming convention is up to data engineers to decide
USE conformed_dims;

-- This view is created to extract product data from the ERP and conform it 
-- for data warehousing purposes. The view joins various tables from the source system and
-- coalesces the data into a format that is consistent with the schema of the products table.
CREATE OR REPLACE TEMP VIEW bronze_erp_product_conformed AS
SELECT 
  SHA2(CONCAT('prod_erp_', CAST(p.ProductID AS STRING)), 0) AS `id`,
  p.ProductNumber AS sku,
  p.Name as name,
  p.ListPrice AS list_price,
  pc.Name AS category_name,
  ppc.Name AS parent_category_name,
  pm.Name AS model_name,
  pmd.descriptions AS model_descriptions,
  MAP('cost', CAST(p.StandardCost AS DOUBLE), 'weight', p.Weight) AS _num_attrs,
  MAP('sell_start_ts', p.SellStartDate, 'sell_end_ts', p.SellEndDate, 'discontinued_ts', p.DiscontinuedDate) AS _date_attrs,
  MAP('color', p.Color, 'size', p.`Size`) AS _text_attrs,
  'prod_erp' AS _source_id,
  p.ModifiedDate AS _source_modstamp,
  current_timestamp() AS _row_modstamp
FROM bronze_erp.product AS p
LEFT JOIN bronze_erp.product_category AS pc
  ON p.ProductCategoryID = pc.ProductCategoryID
LEFT JOIN bronze_erp.product_category AS ppc
  ON pc.ParentProductCategoryID = ppc.ProductCategoryID
LEFT JOIN bronze_erp.product_model AS pm
  ON p.ProductModelID = pm.ProductModelID
LEFT JOIN (
  SELECT 
    pmd1.ProductModelID, 
    map_from_arrays(collect_list(pmd1.Culture), collect_list(pd1.Description)) AS descriptions
  FROM bronze_erp.product_model_description AS pmd1
  LEFT JOIN bronze_erp.product_description AS pd1
    ON pmd1.ProductDescriptionID = pd1.ProductDescriptionID
  GROUP BY ALL
) AS pmd
  ON p.ProductModelID = pmd.ProductModelID;

-- Create the destination table if it doesnt exist using the schema from the conformed view
CREATE TABLE IF NOT EXISTS products AS
SELECT * FROM bronze_erp_product_conformed LIMIT 0;

-- Make sure only changes are applied
CREATE OR REPLACE TEMP VIEW bronze_erp_product_updates AS
SELECT * FROM bronze_erp_product_conformed
WHERE _source_modstamp > COALESCE((SELECT MAX(_source_modstamp) FROM products), TIMESTAMP('1970-01-01T00:00:00.000'));

-- Update changes and insert new data
MERGE INTO products AS target
USING bronze_erp_product_updates AS source
  ON target.`id` = source.`id`
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *