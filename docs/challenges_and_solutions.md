# Challenges and solutions

| Challenge | Implemented solution | Evidence and remaining limit |
|---|---|---|
| Java 25 failed with the supplied older PySpark setup | Selected Java 17 with Spark/PySpark 4.2.1 after checking official compatibility documentation | The Spark-on-YARN smoke and pipeline jobs completed; see [infrastructure record](infrastructure_verification.md). |
| WSL host exposes about 7.5 GiB RAM | Configured a small YARN single-node resource pool and one constrained Spark executor | It runs the provided files; large benchmarks may be slow. |
| No Hadoop stack was preinstalled | Added a project-local Hadoop runtime, XML configuration and guarded lifecycle scripts | All four daemons were verified. |
| HDFS initialization can destroy existing namespace metadata | Kept formatting out of setup/startup; verified the configured directory was empty and explicitly formatted once after user approval | Never format on subsequent starts. |
| Corrupted data includes duplicate IDs and invalid values | Spark parses typed columns, applies explicit required-field rules, deduplicates deterministically and writes rejected raw columns with reason strings | 50k demonstration measured 250 duplicates and 1,893 rejected rows. |
| Spark and Hive version combinations are constrained on a small machine | Use Spark's bundled Hive 2.3.10 and persistent embedded Derby catalog | Actual Spark SQL catalog verified. No separate Hive 4 metastore, HS2 or Beeline service is present. |
| Fresh JVM and metastore initialization add overhead | Record ingestion, Spark stages and total wall-clock time separately | Measurements are from one single-node run; not controlled repeated trials. |
| Streamlit reruns can re-execute page code | Run actions are explicit button events and benchmark sizes are manual | A Streamlit interaction smoke check is part of final verification. |

The dataset is synthetic and the cluster has one worker. Results do not establish production readiness, multi-node scaling or causal performance comparisons.
