# Local environment notes

The verified project environment is Ubuntu 22.04 under WSL2 with OpenJDK 17, Python 3.10, Hadoop 3.5.0, and Spark/PySpark 4.2.1. Detailed setup steps are maintained in [setup and operations](setup_guide.md), and measured versions and service checks are in [infrastructure verification](infrastructure_verification.md).

## WSL guidance

- Keep the project on the Linux filesystem (for example, under `~/projects`) and use VS Code Remote - WSL when editing from Windows.
- Run HDFS and YARN daemons inside WSL/Linux, not native Windows.
- Use the project `scripts/bigdata_env.sh` to select project-local Hadoop and Spark paths in each shell.
- Keep `.runtime/` and `.venv/` local. They contain machine-specific runtime data and are excluded from the repository.
- HDFS is a persistent namespace. Format only once when initializing a new, empty NameNode directory; never format as part of routine startup or troubleshooting.

## Portability

The scripts resolve the project root dynamically. The Java fallback in `scripts/bigdata_env.sh` is the standard Ubuntu OpenJDK 17 path; set `JAVA_HOME` before sourcing the script if the JDK is installed elsewhere. The default HDFS URI is `hdfs://localhost:9000` and may be overridden with `HDFS_URI` where the rest of the local cluster configuration is adjusted accordingly.
