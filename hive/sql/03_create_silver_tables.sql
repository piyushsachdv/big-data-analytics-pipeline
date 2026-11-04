-- The pipeline registers the current run's Parquet paths itself. This DDL is
-- a reference for the same schema; provide silver_location at execution time.
CREATE EXTERNAL TABLE IF NOT EXISTS silver_transactions_reference (
  order_id STRING,
  customer_id STRING,
  customer_name STRING,
  product STRING,
  category STRING,
  quantity INT,
  unit_price DECIMAL(18,2),
  order_date DATE,
  region STRING,
  payment_method STRING,
  order_status STRING,
  transaction_value DECIMAL(20,2)
)
STORED AS PARQUET
LOCATION '${hivevar:silver_location}';
