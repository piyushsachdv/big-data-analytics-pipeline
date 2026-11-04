# Troubleshooting

## Hadoop and YARN commands are not found

Load the project runtime environment in each shell before using Hadoop/Spark commands:

```bash
source scripts/bigdata_env.sh
```

The script sets the project-local Hadoop distribution, PySpark Spark binaries, configuration paths, and log/temp locations.

## HDFS is not initialized

`start_bigdata.sh` checks for `.runtime/data/dfs/name/current`. For a new, empty NameNode directory only, inspect the path and follow the one-time steps in [setup and operations](setup_guide.md#first-time-hdfs-initialization). If metadata already exists, do not format; inspect NameNode logs under `.runtime/logs/` and verify service status with `./scripts/verify_bigdata.sh`.

## A daemon is not running

Check `./scripts/verify_bigdata.sh`, then inspect `.runtime/logs/hadoop/` and `.runtime/logs/yarn/`. Startup is idempotent and reports existing daemon PIDs. Use `./scripts/stop_bigdata.sh` followed by `./scripts/start_bigdata.sh` only when a service restart is appropriate; this preserves NameNode and DataNode directories.

## Spark/YARN application fails

Open the run's `spark_submit.log` and `result.json` under `.runtime/logs/pipeline_runs/<run_id>/`. The Pipeline Monitor reports the failed stage and YARN application ID, with detailed logs in an expander. Check the YARN application report and the underlying `Caused by` exception before changing state or data.

## Derby `XSDB6` embedded metastore lock

Derby permits one embedded JVM owner at a time. The dashboard uses a Spark session without Hive support, while pipeline and benchmark catalog writers serialize through a kernel-managed lock. If an external Spark process still uses `.runtime/metastore/metastore_db`, wait for it to exit and inspect its log/process before retrying. Do not delete Derby lock files or the metastore directory while a process may be using them; do not recreate the catalog as an automatic recovery.

## Dashboard has no current analytics

Analytics use HDFS paths from the latest successful run. Start HDFS, check `./scripts/verify_bigdata.sh`, and complete a successful pipeline before opening analytics. Failed or in-progress runs do not replace the last successful run.

## Python/Spark test gateway cannot bind

Spark's local Py4J gateway requires loopback socket access. Run tests in a normal WSL terminal with loopback networking available:

```bash
source scripts/bigdata_env.sh
.venv/bin/pytest -q
```

If a test points at `hdfs://localhost` for a local fixture, confirm the test session has its local filesystem setting and that the project Hadoop environment is loaded consistently.
