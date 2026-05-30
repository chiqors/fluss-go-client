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
DROP TABLE IF EXISTS e2e_customer_orders;
DROP TABLE IF EXISTS e2e_all_types;
DROP TABLE IF EXISTS e2e_orders_arrow;

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
  'table.kv.format' = 'indexed',
  'table.kv.format-version' = '2'
);

CREATE TABLE e2e_customer_orders (
  customer_id BIGINT,
  customer_name STRING,
  order_id BIGINT,
  order_status STRING,
  PRIMARY KEY (customer_id, customer_name, order_id) NOT ENFORCED
) WITH (
  'bucket.num' = '1',
  'bucket.key' = 'customer_id,customer_name',
  'table.kv.format' = 'indexed',
  'table.kv.format-version' = '2'
);

CREATE TABLE e2e_all_types (
  event_id BIGINT,
  bool_flag BOOLEAN,
  tiny_value TINYINT,
  small_value SMALLINT,
  int_value INT,
  big_value BIGINT,
  float_value FLOAT,
  double_value DOUBLE,
  name STRING,
  raw_bytes BYTES,
  amount DECIMAL(10, 2),
  event_date DATE,
  event_time TIME(3),
  event_ts TIMESTAMP(6),
  event_ts_ltz TIMESTAMP_LTZ(6),
  score_history ARRAY<INT>,
  label_counts MAP<STRING, BIGINT>,
  nested_payload ROW<note STRING, rank_value INT, tags ARRAY<STRING>>
) WITH (
  'bucket.num' = '1',
  'table.log.format' = 'indexed'
);

CREATE TABLE e2e_orders_arrow (
  order_id BIGINT,
  customer_id INT,
  amount DECIMAL(15, 2),
  status STRING
) WITH (
  'bucket.num' = '1',
  'table.log.format' = 'arrow',
  'table.log.arrow.compression.type' = 'NONE'
);
