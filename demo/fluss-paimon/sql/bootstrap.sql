CREATE CATALOG fluss_catalog WITH (
  'type' = 'fluss',
  'bootstrap.servers' = 'coordinator-server:9123',
  'paimon.s3.access-key' = 'flussadmin',
  'paimon.s3.secret-key' = 'flussadmin'
);

USE CATALOG fluss_catalog;

CREATE DATABASE IF NOT EXISTS fluss;
USE fluss;

DROP TABLE IF EXISTS e2e_orders;
DROP TABLE IF EXISTS e2e_customers;

CREATE TABLE e2e_orders (
  order_id BIGINT,
  customer_id INT,
  amount DECIMAL(15, 2),
  status STRING
) WITH (
  'bucket.num' = '1',
  'table.log.format' = 'indexed',
  'table.datalake.enabled' = 'true',
  'table.datalake.freshness' = '30s'
);

CREATE TABLE e2e_customers (
  customer_id BIGINT,
  customer_name STRING,
  customer_tier STRING,
  PRIMARY KEY (customer_id) NOT ENFORCED
) WITH (
  'bucket.num' = '1',
  'table.kv.format' = 'indexed'
);
