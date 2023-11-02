-- Creating the bronze layer from source systems is a responsibility of Data Engineering. 
-- Therefore bronze tables live in the data engineering catalog
USE CATALOG {{company}}_{{env}}_data_engineering; 
-- Use an appropriate schema for replication
-- Here the naming convention is up to data engineers to decide
USE bronze_erp; 

-- Use a full outer join on the primary key or the foreign key set for each table to 
-- identify which keys are missing and where
CREATE OR REPLACE TEMPORARY VIEW _val_pk_ungrouped AS
SELECT -- customer
  'customer' as table_name, COALESCE(bc.CustomerID, sc.CustomerID) as pk,
  CASE
    WHEN bc.CustomerID IS NULL AND sc.CustomerID IS NOT NULL THEN 'target'
    WHEN sc.CustomerID IS NULL AND bc.CustomerID IS NOT NULL THEN 'source' 
    WHEN sc.CustomerID IS NULL AND bc.CustomerID IS NULL THEN 'both' -- This should never happen
    ELSE NULL
  END AS missing_in
FROM customer AS bc
FULL OUTER JOIN {{company}}_ext_{{env}}_erp.saleslt.customer AS sc
  ON bc.CustomerID = sc.CustomerID
WHERE bc.CustomerID IS NULL OR sc.CustomerID IS NULL
UNION
SELECT
  'address', COALESCE(ba.AddressID, sa.AddressID),
  CASE
    WHEN ba.AddressID IS NULL AND sa.AddressID IS NOT NULL THEN 'target'
    WHEN sa.AddressID IS NULL AND ba.AddressID IS NOT NULL THEN 'source' 
    WHEN sa.AddressID IS NULL AND ba.AddressID IS NULL THEN 'both' 
    ELSE NULL
  END
FROM address as ba
FULL OUTER JOIN {{company}}_ext_{{env}}_erp.saleslt.address as sa
  ON ba.AddressID = sa.AddressID
WHERE ba.AddressID IS NULL OR sa.AddressID IS NULL
UNION
SELECT
  'product', COALESCE(bp.ProductID, sp.ProductID),
  CASE
    WHEN bp.ProductID IS NULL AND sp.ProductID IS NOT NULL THEN 'target'
    WHEN sp.ProductID IS NULL AND bp.ProductID IS NOT NULL THEN 'source' 
    WHEN sp.ProductID IS NULL AND bp.ProductID IS NULL THEN 'both' 
    ELSE NULL
  END
FROM product as bp
FULL OUTER JOIN {{company}}_ext_{{env}}_erp.saleslt.product as sp
  ON bp.ProductID = sp.ProductID
WHERE bp.ProductID IS NULL OR sp.ProductID IS NULL
UNION
SELECT
  'customer_address', COALESCE(bca.CustomerID || bca.AddressID, sca.CustomerID || sca.AddressID),
  CASE
    WHEN bca.CustomerID || bca.AddressID IS NULL AND sca.CustomerID || sca.AddressID IS NOT NULL THEN 'target'
    WHEN sca.CustomerID || sca.AddressID IS NULL AND bca.CustomerID || bca.AddressID IS NOT NULL THEN 'source' 
    WHEN sca.CustomerID || sca.AddressID IS NULL AND bca.CustomerID || bca.AddressID IS NULL THEN 'both' 
    ELSE NULL
  END
FROM customer_address as bca
FULL OUTER JOIN {{company}}_ext_{{env}}_erp.saleslt.customeraddress as sca
  ON bca.CustomerID = sca.CustomerID
  AND bca.AddressID = sca.AddressID
WHERE (bca.CustomerID IS NULL AND bca.AddressID IS NULL) OR (sca.CustomerID IS NULL AND sca.AddressID IS NULL)
UNION
SELECT
  'product_category', COALESCE(bpc.ProductCategoryID, spc.ProductCategoryID),
  CASE
    WHEN bpc.ProductCategoryID IS NULL AND spc.ProductCategoryID IS NOT NULL THEN 'target'
    WHEN spc.ProductCategoryID IS NULL AND bpc.ProductCategoryID IS NOT NULL THEN 'source' 
    WHEN spc.ProductCategoryID IS NULL AND bpc.ProductCategoryID IS NULL THEN 'both' 
    ELSE NULL
  END
FROM product_category as bpc
FULL OUTER JOIN {{company}}_ext_{{env}}_erp.saleslt.productcategory as spc
  ON bpc.ProductCategoryID = spc.ProductCategoryID
WHERE bpc.ProductCategoryID  IS NULL OR spc.ProductCategoryID IS NULL
UNION
SELECT
  'product_model', COALESCE(bpm.ProductModelID, spm.ProductModelID),
  CASE
    WHEN bpm.ProductModelID IS NULL AND spm.ProductModelID IS NOT NULL THEN 'target'
    WHEN spm.ProductModelID IS NULL AND bpm.ProductModelID IS NOT NULL THEN 'source' 
    WHEN spm.ProductModelID IS NULL AND bpm.ProductModelID IS NULL THEN 'both' 
    ELSE NULL
  END
FROM product_model as bpm
FULL OUTER JOIN {{company}}_ext_{{env}}_erp.saleslt.productmodel as spm
  ON bpm.ProductModelID = spm.ProductModelID
WHERE bpm.ProductModelID IS NULL OR spm.ProductModelID IS NULL
UNION
SELECT
  'product_description', COALESCE(bpd.ProductDescriptionID, spd.ProductDescriptionID),
  CASE
    WHEN bpd.ProductDescriptionID IS NULL AND spd.ProductDescriptionID IS NOT NULL THEN 'target'
    WHEN spd.ProductDescriptionID IS NULL AND bpd.ProductDescriptionID IS NOT NULL THEN 'source' 
    WHEN spd.ProductDescriptionID IS NULL AND bpd.ProductDescriptionID IS NULL THEN 'both' 
    ELSE NULL
  END
FROM product_description as bpd
FULL OUTER JOIN {{company}}_ext_{{env}}_erp.saleslt.productdescription as spd
  ON bpd.ProductDescriptionID = spd.ProductDescriptionID
WHERE bpd.ProductDescriptionID IS NULL OR spd.ProductDescriptionID IS NULL
UNION
SELECT
  'product_model_description', COALESCE(bpmd.ProductModelID || bpmd.ProductDescriptionID, spmd.ProductModelID || spmd.ProductDescriptionID),
  CASE
    WHEN bpmd.ProductModelID || bpmd.ProductDescriptionID IS NULL AND spmd.ProductModelID || spmd.ProductDescriptionID IS NOT NULL THEN 'target'
    WHEN spmd.ProductModelID || spmd.ProductDescriptionID IS NULL AND bpmd.ProductModelID || bpmd.ProductDescriptionID IS NOT NULL THEN 'source' 
    WHEN spmd.ProductModelID || spmd.ProductDescriptionID IS NULL AND bpmd.ProductModelID || bpmd.ProductDescriptionID IS NULL THEN 'both' 
    ELSE NULL
  END
FROM product_model_description as bpmd
FULL OUTER JOIN {{company}}_ext_{{env}}_erp.saleslt.productmodelproductdescription as spmd
  ON bpmd.ProductModelID = spmd.ProductModelID
  AND bpmd.ProductDescriptionID = spmd.ProductDescriptionID
WHERE (bpmd.ProductModelID IS NULL AND bpmd.ProductDescriptionID IS NULL)
OR (spmd.ProductModelID IS NULL AND spmd.ProductDescriptionID IS NULL)
UNION
SELECT
  'sales_order_header', COALESCE(bsoh.SalesOrderID, ssoh.SalesOrderID),
  CASE
    WHEN bsoh.SalesOrderID IS NULL AND ssoh.SalesOrderID IS NOT NULL THEN 'target'
    WHEN ssoh.SalesOrderID IS NULL AND bsoh.SalesOrderID IS NOT NULL THEN 'source' 
    WHEN ssoh.SalesOrderID IS NULL AND bsoh.SalesOrderID IS NULL THEN 'both' 
    ELSE NULL
  END
FROM sales_order_header as bsoh
FULL OUTER JOIN {{company}}_ext_{{env}}_erp.saleslt.salesorderheader as ssoh
  ON bsoh.SalesOrderID = ssoh.SalesOrderID
WHERE bsoh.SalesOrderID IS NULL OR ssoh.SalesOrderID IS NULL
UNION
SELECT
  'sales_order_detail', COALESCE(bsod.SalesOrderID || bsod.SalesOrderDetailID, ssod.SalesOrderID || ssod.SalesOrderDetailID),
  CASE
    WHEN bsod.SalesOrderID || bsod.SalesOrderDetailID IS NULL AND ssod.SalesOrderID || ssod.SalesOrderDetailID IS NOT NULL THEN 'target'
    WHEN ssod.SalesOrderID || ssod.SalesOrderDetailID IS NULL AND bsod.SalesOrderID || bsod.SalesOrderDetailID IS NOT NULL THEN 'source' 
    WHEN ssod.SalesOrderID || ssod.SalesOrderDetailID IS NULL AND bsod.SalesOrderID || bsod.SalesOrderDetailID IS NULL THEN 'both' 
    ELSE NULL
  END
FROM sales_order_detail as bsod
FULL OUTER JOIN {{company}}_ext_{{env}}_erp.saleslt.salesorderdetail as ssod
  ON bsod.SalesOrderID = ssod.SalesOrderID
  AND bsod.SalesOrderDetailID = ssod.SalesOrderDetailID
WHERE (bsod.SalesOrderID IS NULL AND bsod.SalesOrderDetailID IS NULL) OR (ssod.SalesOrderID IS NULL AND ssod.SalesOrderDetailID IS NULL);

-- Store the results in a validation table
CREATE OR REPLACE TABLE _val_pk_foj AS
SELECT table_name, pk, MAX(missing_in) AS missing_in, current_timestamp() AS validation_ts
FROM _val_pk_ungrouped
GROUP BY table_name, pk ORDER BY table_name, pk;