-- ## Create Internal Catalogs ##
-- Owner: Databricks, Inc
-- Created Date: 2023-11-02
-- License: Apache 2.0

-- ***** Data Engineering Catalog *****
-- Any bronze-level tables replicating non-federated sources by means 
-- of incoming files in a landing zone or custom integrations with systems 
-- that aren't supported by Lakehouse Federation. Should also contain the silver
-- tables representing the engineering scemantic model that feeds the CIM and
-- the silver feature tables found in the data science catalog. Said engineering
-- scemantic model should be source-agnostic and use-case agnostic while supporting
-- infinite attributes and measures by means of map type columns
CREATE CATALOG IF NOT EXISTS {{company}}_{{env}}_data_engineering;

-- ***** Business Intelligence Catalog *****
-- Any gold-level denormalized aggregates created by business analysts, SQL analysts or 
-- report builders who want to take ownership of creating their own tables or views to be
-- used by the wider org. These 3 catalogs should not contain data integration schemas, 
-- i.e. shouldnt contain ingestion tables or normalized scemantic models. Catalog fed by 
-- the CIM, Data Engineering and Data Science tables
CREATE CATALOG IF NOT EXISTS {{company}}_{{env}}_business_intelligence;

-- ***** Data Science Catalog *****
-- Contains silver feature tables, models and inference tables for different data 
-- science use cases. The job of DEng is to provide the feature tables and keep them 
-- refreshed while data science train and deploy models with the highest accuracy / score
-- possible. Once appropriate inference tables are created, BI users may leverage these 
-- tables to create gold aggregates for reporting. Typically silver tables in data science are
-- denormalized, therefore, in cases where inferencing results are to be integrated into 
-- the CIM, working with DEng is required
CREATE CATALOG IF NOT EXISTS {{company}}_{{env}}_data_science;