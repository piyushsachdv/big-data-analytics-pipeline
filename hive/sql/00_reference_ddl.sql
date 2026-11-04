-- Reference DDL only. Review and adapt to the selected Hive version before execution.
CREATE DATABASE IF NOT EXISTS ecommerce_analytics;
USE ecommerce_analytics;

-- Bronze landing table: read source CSV fields as strings first.
-- Configure LOCATION to the actual HDFS landing directory created by the pipeline.
CREATE EXTERNAL TABLE IF NOT EXISTS bronze_transactions (
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
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

-- Silver and Gold tables should be created by the pipeline as Parquet tables
-- after schema validation and transformation. Do not assume this raw CSV DDL
-- handles quoted commas; use an appropriate CSV SerDe or Spark CSV reader
-- when the uploaded data may contain quoted delimiters.
