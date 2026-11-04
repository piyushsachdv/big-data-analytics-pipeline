# User and pipeline workflow

```mermaid
flowchart TD
  A[Start HDFS and YARN] --> B[Open Streamlit dashboard]
  B --> C[Inspect one of the starter CSVs]
  C --> D{Schema valid?}
  D -- No --> E[Show validation error; do not upload]
  D -- Yes --> F[Upload original to unique Bronze path]
  F --> G[Submit Spark job to YARN]
  G --> H[Normalize and validate records]
  H --> I[Write valid Silver and rejected rows]
  I --> J[Write seven Gold Parquet aggregates]
  J --> K[Register Hive-compatible tables]
  K --> L[Compare SQL revenue with Spark result]
  L --> M[Verify YARN state and HDFS outputs]
  M --> N[Explore analytics, quality and logs]
  N --> O[Download supported result tables]
```

For the faculty demonstration, use `data/raw/ecommerce_transactions_50000.csv`, run Pipeline Monitor, then visit Sales Analytics, Customer Analytics and Data Quality. Pipeline IDs, YARN application IDs, stages and output locations are recorded in `.runtime/logs/` and HDFS. Do not format HDFS during normal startup.
