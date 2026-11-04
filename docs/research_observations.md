# Research observations and measured results

## Experimental setup

Single-node WSL2 Ubuntu 22.04.5 environment; OpenJDK 17.0.20.1; Hadoop 3.5.0; Spark/PySpark 4.2.1; Python 3.10.12; about 7.47 GiB reported host RAM. Spark ran through YARN with a single NodeManager. The catalog uses Spark's embedded Hive 2.3.10/Derby implementation. Data is fictional.

## Complete 50,000-row demonstration

Run ID: `run_20261104T174947Z_b603d56e`; YARN application: `application_1793812534425_0003`; status: `SUCCEEDED`.

| Measure | Observed result |
|---|---:|
| Source file rows including injected duplicate rows | 50,250 |
| Valid Silver records | 48,357 |
| Rejected records | 1,893 |
| Duplicate IDs removed | 250 |
| Spark job time | 180.481 s |
| Cleaning stage | 49.867 s |
| Silver write | 30.334 s |
| Gold aggregation/write | 18.138 s |
| Hive registration and validation | 31.378 s |
| Bronze upload | 12.052 s |
| Delivered revenue | ₹791,026,472.09 |
| Hive SQL delivered revenue | ₹791,026,472.09 (matched Spark) |

The complete wall time from the recorded orchestration events was about 231 seconds, including startup and output verification. See `.runtime/logs/pipeline_runs/run_20261104T174947Z_b603d56e/spark_stats.json` for the full measurements.

## Selected 10,000-row benchmark

Run ID: `run_20261104T175411Z_786940d6`; YARN application: `application_1793812534425_0004`; status: `SUCCEEDED`; one repetition.

| Measure | Observed result |
|---|---:|
| Generated input rows | 10,000 |
| Bronze upload | 11.504 s |
| Spark job time | 165.114 s |
| Cleaning | 44.859 s |
| Gold aggregation/write | 16.300 s |
| Hive registration/validation | 25.936 s |
| End-to-end benchmark time | 233.311 s |
| Throughput (rows / end-to-end seconds) | 42.86 rows/s |
| Silver + rejects + Gold HDFS bytes | 1,064,597 |

This is one observation, not an average or a scaling curve. JVM startup, metastore activity, WSL I/O and run order influence elapsed time. It does not support a claim that Spark is faster than another engine or that performance scales across machines.

## Data quality interpretation

The data quality metrics overlap: a record can violate multiple rules, so the number of rule violations should not be summed to infer rejected record count. Rejected records retain original field values plus an explicit reason. Delivered revenue excludes cancelled, returned, processing and shipped transactions, following the starter data dictionary.

## Research paper framing

The project demonstrates an end-to-end design and observable operational behavior. Any literature review claims should be backed by independently verified papers; no unsupported citations are added here. A stronger performance study would need identical workloads, multiple repetitions, warm-up policy, controlled configurations and preferably multi-node hardware.
