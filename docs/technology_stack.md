# Technology stack and compatibility

| Layer | Selected component | Role |
|---|---|---|
| OS | Ubuntu 22.04 under WSL2 | Local development and single-node runtime |
| JVM | OpenJDK 17 | Hadoop and Spark runtime |
| Storage/resource manager | Hadoop 3.5.0 | HDFS, YARN ResourceManager and NodeManager |
| Distributed processing | Apache Spark / PySpark 4.2.1 | DataFrames, SQL and YARN jobs |
| SQL catalog | Spark bundled Hive 2.3.10 with embedded Derby | Persistent catalog and SQL over Parquet |
| Ingestion/orchestration | Python 3.10, Pandas | Schema validation, HDFS upload and YARN status tracking |
| Dashboard | Streamlit, Plotly, Pandas | Local navigation, charts, filtering and export |
| Tests | Pytest | Unit and local Spark validation |

Official compatibility references checked while selecting versions: [Hadoop 3.5.0 documentation](https://hadoop.apache.org/docs/r3.5.0/), [Spark 4.2.1 documentation](https://spark.apache.org/docs/4.2.1/), [Spark configuration](https://spark.apache.org/docs/4.2.1/configuration.html), and [Hive downloads and compatibility](https://hive.apache.org/general/downloads/). Spark's documented distribution options include Hadoop 3.5 client compatibility; selected Spark/PySpark support the installed Java and Python runtimes. The project records actual runtime versions in [infrastructure verification](infrastructure_verification.md).

The target prompt proposed Hive 4.0.1, but that is not the runtime used here. The project deliberately uses Spark's bundled Hive catalog rather than mixing an independent metastore version into the constrained 8 GiB environment. A separate metastore/HS2 deployment remains future work.
