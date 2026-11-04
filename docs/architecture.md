# Architecture

## System view

```mermaid
flowchart LR
  CSV[Starter or uploaded CSV] --> PY[Python validation and orchestration]
  PY --> B[(HDFS Bronze: original CSV)]
  PY --> Y[YARN ResourceManager]
  Y --> S[Spark on YARN]
  B --> S
  S --> C[Validate, deduplicate, clean, aggregate]
  C --> V[(HDFS Silver Parquet)]
  C --> R[(HDFS rejected Parquet + reasons)]
  C --> G[(HDFS Gold Parquet aggregates)]
  S --> H[Hive-compatible Spark SQL catalog]
  V --> H
  G --> H
  V --> A[Dashboard Spark reads latest successful HDFS paths]
  G --> A
  A --> UI[Streamlit + Plotly: six pages]
```

## Storage layers

```mermaid
flowchart TB
  subgraph HDFS
    B[/ecommerce/bronze/transactions/run_id: unchanged CSV/]
    S[/ecommerce/silver/transactions/run_id: cleaned Parquet/]
    R[/ecommerce/silver/rejected/run_id: raw rejected rows + reason/]
    G[/ecommerce/gold/runs/run_id: seven Parquet aggregates/]
    M[/ecommerce/metadata/run_id: quality metrics and run summary/]
  end
  B --> S
  B --> R
  S --> G
  S --> M
```

## YARN and Spark execution

The Python orchestrator validates a CSV, copies it to a unique Bronze location and starts `spark-submit --master yarn --deploy-mode client`. The client driver runs in the invoking process; YARN schedules the executor on the single NodeManager. Spark reads Bronze through Hadoop filesystem support, computes typed records and rejects with DataFrame transformations, writes Parquet outputs, registers tables and checks a delivered revenue SQL result against the Spark aggregate. The orchestrator checks the YARN final state and expected HDFS paths before marking the run successful.

The YARN driver is the only application process that opens the embedded Derby-backed Hive catalog. Pipeline and benchmark catalog writers use a kernel-managed exclusive lock, which serializes Derby clients across Streamlit sessions and processes without deleting lock files. Dashboard analytics use a separate local Spark session without Hive support and read the immutable HDFS paths recorded by the last successful run. A new run writes to unique paths; the latest-run pointer changes only after output and SQL checks succeed. The UI can therefore continue reading a complete prior run while a new job executes without opening Derby concurrently.

```mermaid
sequenceDiagram
  participant U as User / Streamlit
  participant P as Python orchestrator
  participant H as HDFS
  participant Y as YARN
  participant S as Spark driver/executor
  participant C as Hive-compatible catalog (YARN driver only)
  participant A as Dashboard Spark reader (no Hive client)
  U->>P: Select CSV and run
  P->>P: Validate schema
  P->>H: Upload unchanged CSV to Bronze
  P->>Y: Submit Spark application
  Y->>S: Schedule executor
  S->>H: Read Bronze and write Silver/Gold
  S->>C: Register Parquet tables and query revenue
  S-->>P: Stats JSON and exit status
  P->>Y: Confirm FINISHED/SUCCEEDED
  P->>H: Verify output locations
  A->>H: Read last successful immutable Parquet paths
  A-->>U: Charts and bounded previews
  P-->>U: Stage events, metrics and result
```

## Warehouse choice

Spark uses its bundled Hive 2.3.10 support and persistent embedded Derby metastore in this resource-limited local environment. There is no independent Hive Metastore daemon, HiveServer2 or Beeline endpoint. Catalog operations and SQL are real and verified in Spark SQL. This boundary is documented to avoid describing the embedded catalog as an external Hive service.
