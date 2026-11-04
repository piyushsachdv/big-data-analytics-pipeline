-- Parameterize the location with: --hivevar bronze_location=hdfs://.../path.csv
-- CSV files with quoted separators should be read by Spark's CSV reader (the
-- pipeline does this); this simple external table is for basic SQL inspection.
CREATE EXTERNAL TABLE IF NOT EXISTS bronze_transactions_reference (
  order_id STRING,
  customer_id STRING,
  customer_name STRING,
  product STRING,
  category STRING,
  quantity STRING,
  unit_price STRING,
  order_date STRING,
  region STRING,
  payment_method STRING,
  order_status STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
STORED AS TEXTFILE
LOCATION '${hivevar:bronze_location}';
