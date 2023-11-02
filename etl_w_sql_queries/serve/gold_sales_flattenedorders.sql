-- Creating the gold layer from silver is a responsibility of BI and Analytics
-- Therefore gold tables live in the business intelligence catalog
USE CATALOG {{company}}_{{env}}_business_intelligence;
-- Use an appropriate schema for replication
-- Here the naming convention is up to data analysts to decide
USE common_information_model;

-- Explode each line item into multiple rows
CREATE OR REPLACE TEMP VIEW orders_exploded AS
SELECT EXPLODE(line_items) as line_item, * EXCEPT(line_items)
FROM {{company}}_{{env}}_data_engineering.sales_facts.orders;

-- Expand line items into individual columns
CREATE OR REPLACE TEMP VIEW orders_expanded AS
SELECT line_item.product_fk, line_item.quantity, line_item.unit_price, line_item.discount, * EXCEPT(line_item)
FROM orders_exploded;

-- Reorganise columns before joining
CREATE OR REPLACE TEMP VIEW orders_reorg AS 
SELECT 
  order_date, 
  product_fk, customer_fk,
  shto_address_fk, blto_address_fk,
  quantity, unit_price, discount,
  was_online_order, shipping_method,
  DATE(_date_attrs.ship_date) AS ship_date,
  DATE(_date_attrs.due_date) AS due_date
FROM orders_expanded
WHERE _source_id = '{{env}}_erp';

-- Join and remove unnecessary columns
CREATE OR REPLACE TEMP VIEW orders_joined AS 
SELECT 
  O.* EXCEPT (
    product_fk, customer_fk, shto_address_fk, blto_address_fk
  ),
  P.name AS product_name,
  P.* EXCEPT (
    id, name, model_descriptions,
    _num_attrs, _date_attrs, _text_attrs, 
    _source_id, _source_modstamp, _row_modstamp
  ),
  P.model_descriptions.en AS model_description,
  C.name AS customer_name,
  C.* EXCEPT (
    id, name, email, phone,
    _num_attrs, _date_attrs, _text_attrs, 
    _source_id, _source_modstamp, _row_modstamp
  ),
  SA.full_address as shipment_address,
  SA.city as shipment_city,
  SA.state as shipment_state,
  SA.country as shipment_country,
  SA.postcode as shipment_postcode,
  BA.full_address as billing_address,
  BA.city as billing_city,
  BA.state as billing_state,
  BA.country as billing_country,
  BA.postcode as billing_postcode
FROM orders_reorg AS O
LEFT JOIN {{company}}_{{env}}_data_engineering.conformed_dims.products AS P
  ON O.product_fk = P.id
LEFT JOIN {{company}}_{{env}}_data_engineering.conformed_dims.customers AS C
  ON O.customer_fk = C.id
LEFT JOIN {{company}}_{{env}}_data_engineering.conformed_dims.addresses AS SA
  ON O.shto_address_fk = SA.id
LEFT JOIN {{company}}_{{env}}_data_engineering.conformed_dims.addresses AS BA
  ON O.blto_address_fk = BA.id
;

-- Replace the original table
CREATE OR REPLACE TABLE orders_flattened AS
SELECT * FROM orders_joined;