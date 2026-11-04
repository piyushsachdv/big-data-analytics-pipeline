# Tests and verification record

## Automated tests

Run from the workspace root:

```bash
./.venv/bin/pytest -q
```

Latest recorded result: **13 passed**. Coverage includes CSV schema validation and previews, the provided edge-case fixture, deduplication/rejection rules, clean fixture handling, deterministic synthetic generation, delivered versus gross revenue, Gold aggregation calculations, and path-based dashboard analytics. Spark tests use a local Spark session; they do not replace infrastructure tests.

## Infrastructure integration

- HDFS NameNode/DataNode, safe mode, block health and HDFS paths checked after initialization.
- YARN ResourceManager/NodeManager and available memory/vcores checked.
- Separate Spark `--master yarn` smoke application completed successfully and wrote Parquet to HDFS.
- Spark SQL embedded Hive catalog query verified.
- 1,000-row corrupted fixture completed through the Python orchestrator and verified output paths plus SQL revenue.
- Provided 50,000-row demonstration completed on YARN; detailed observed metrics are in [research observations](research_observations.md).
- 10,000-row generated benchmark completed and recorded.
- All six Streamlit pages rendered successfully against the latest catalog output using Streamlit's in-process app harness.
- Pipeline run `run_20261104T185933Z_74f1a4ed` completed with YARN application `application_1793812534425_0006` in `SUCCEEDED`; catalog SQL delivered revenue matched Spark at 15,830,247.41.

The separate HiveServer2/Beeline path is not configured, so it is not claimed as passed.

## Failure handling

Schema failures stop before Bronze upload. Spark and YARN failures are recorded as failed stages with logs and application IDs when available. HDFS format or destructive recovery is never an automatic retry strategy.
