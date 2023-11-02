-- ## Create Foreign Catalogs ##
-- Owner: Databricks, Inc
-- Created Date: 2023-11-02
-- License: Apache 2.0

-- ***** ERP External Catalog *****
-- Reflects a connection to the adventureworks sql server and db
CREATE FOREIGN CATALOG IF NOT EXISTS {{company}}_ext_{{env}}_erp
USING CONNECTION {{sql_conn_name}}
OPTIONS (database 'adventureworks');