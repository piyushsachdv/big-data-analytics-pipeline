# Infrastructure setup and verification record

## Environment measured

- WSL2, Ubuntu 22.04.5 LTS (the planning note expected Ubuntu 24.04).
- OpenJDK 17.0.20.1; Python 3.10.12.
- WSL reported 7.5 GiB RAM, 5.3 GiB available at inspection, and 2 GiB swap.
- The project filesystem reported 945 GiB free at inspection.
- Starter assets listed in `ASSET_MANIFEST.json` match their SHA-256 values. `README_ASSETS.md` is listed in the manifest but is absent from the workspace.

## Selected runtime

| Component | Selected version / mode | Reason and notes |
|---|---|---|
| Hadoop | 3.5.0 | Apache documents Java 17 support; HDFS and YARN use a single-node pseudo-distributed setup. |
| Spark / PySpark | 4.2.1 | The Hadoop 3.5 binary is the selected distribution. It runs on the installed Java 17 and Python 3.10 runtimes. |
| Hive catalog | Spark bundled Hive 2.3.10 with embedded Derby | The YARN pipeline driver registers tables and verifies SQL. A separate Hive Metastore daemon, HiveServer2, and Beeline server connection are not part of this 8 GB local deployment. |
| YARN allocation | 3 GiB, 2 vcores | Leaves memory for WSL, HDFS, the Spark client, and the dashboard. Spark uses one 768 MiB executor with 384 MiB overhead. |

Spark 4.2.1 also documents support for connecting to Hive Metastore 4.1.0. This setup uses Spark's bundled Hive catalog instead, so it does not claim to have validated a standalone Hive 4.1.0 server. The project Hive configuration uses HDFS for table data and Derby under `.runtime/metastore/` for persistent catalog metadata.

## HDFS initialization and safety

Before formatting, both configured directories were checked and found absent, with no existing NameNode metadata or DataNode blocks:

- NameNode: `.runtime/data/dfs/name`
- DataNode: `.runtime/data/dfs/data`

The directories were created empty and the guarded `scripts/initialize_hdfs.sh --confirm-first-format` command formatted the NameNode once on 2026-11-04. `scripts/setup_bigdata.sh` and `scripts/start_bigdata.sh` contain no format operation. Startup checks the existing NameNode metadata and skips already-running daemons.

The first daemon launch was cleaned up when the command process group exited. The startup script was adjusted to detach daemon launches with `setsid`; repeat startup then skipped the live PIDs without changing HDFS metadata.

## Verification results

- Hadoop CLI reported Hadoop 3.5.0.
- Spark CLI reported Spark 4.2.1, Scala 2.13.18, and OpenJDK 17.0.20.1.
- A local Spark job created a Parquet table through the Hive catalog, queried it, and returned `5` for `2 + 3`.
- NameNode and DataNode were live; HDFS safe mode was OFF. HDFS reported one live DataNode, zero missing blocks, zero corrupt replicas, and zero under-replicated blocks.
- HDFS `/`, `/tmp`, `/user`, and `/apps` were accessible.
- ResourceManager and NodeManager were live. YARN reported one node in `RUNNING` state with 3072 MiB and 2 vcores.
- NameNode UI `http://localhost:9870` and ResourceManager UI `http://localhost:8088` responded with HTTP 302 redirects.
- Spark YARN application `application_1793812534425_0001` finished with final status `SUCCEEDED`. Its Hive query returned `rows=3, total=750` and its Parquet output is at `hdfs://localhost:9000/tmp/spark-yarn-check/2d970e9c8396`.
- Detailed runtime logs are under the ignored `.runtime/logs/` directory.

## Known limitations

This is a single-node educational environment, not evidence of multi-machine scaling. Ubuntu 22.04 differs from the intended 24.04. Dashboard analytics read the latest successful Parquet paths without opening an embedded Hive client, avoiding concurrent Derby access. Pipeline and benchmark catalog writers serialize access with an OS-managed lock. HiveServer2 and Beeline were not deployed.

## Apache documentation

- [Hadoop 3.5.0 documentation](https://hadoop.apache.org/docs/r3.5.0/)
- [Spark 4.2.1 overview](https://spark.apache.org/docs/4.2.1/)
- [Spark 4.2.1 YARN guide](https://spark.apache.org/docs/4.2.1/running-on-yarn.html)
- [Spark 4.2.1 configuration reference](https://spark.apache.org/docs/4.2.1/configuration.html)
- [Hive release compatibility notes](https://hive.apache.org/general/downloads/)
