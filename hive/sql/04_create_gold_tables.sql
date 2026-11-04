-- Gold tables are registered by the Spark pipeline after each successful run.
-- This reference DDL illustrates monthly revenue. Supply the run's location.
CREATE EXTERNAL TABLE IF NOT EXISTS gold_sales_by_month_reference (
  order_year INT,
  order_month INT,
  total_orders BIGINT,
  gross_transaction_value DECIMAL(30,2),
  delivered_revenue DECIMAL(30,2)
)
STORED AS PARQUET
LOCATION '${hivevar:gold_month_location}';
