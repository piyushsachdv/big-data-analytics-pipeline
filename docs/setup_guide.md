# Setup and operations guide

## Tested environment and requirements

The recorded environment is WSL2 Ubuntu 22.04.5, OpenJDK 17, Python 3.10, Hadoop 3.5.0, Spark/PySpark 4.2.1, and approximately 8 GiB of RAM. The configuration allocates 3 GiB and 2 vcores to YARN. Other Linux environments may need adjustments to Java paths and resource limits.

Install system tools on a fresh Ubuntu environment:

```bash
sudo apt update
sudo apt install -y openjdk-17-jdk python3.10 python3.10-venv python3-pip curl tar
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
```

Create `.venv` only if it does not already exist, then install Python requirements:

```bash
python3.10 -m venv .venv   # skip if .venv already exists
source .venv/bin/activate
python -m pip install -r requirements.txt
```

## Download external runtime archives

`scripts/setup_bigdata.sh` does not download files. It expects the Hadoop release archive and checksum, and the PySpark source distribution, in `.runtime/archives/`. These files are ignored by Git because they are large runtime packages.

```bash
mkdir -p .runtime/archives
curl -fL 'https://archive.apache.org/dist/hadoop/common/hadoop-3.5.0/hadoop-3.5.0.tar.gz' \
  -o .runtime/archives/hadoop-3.5.0.tar.gz
curl -fL 'https://archive.apache.org/dist/hadoop/common/hadoop-3.5.0/hadoop-3.5.0.tar.gz.sha512' \
  -o .runtime/archives/hadoop-3.5.0.tar.gz.sha512
python -m pip download --no-deps --no-binary=:all: --dest .runtime/archives pyspark==4.2.1
```

Prepare the local runtime:

```bash
./scripts/setup_bigdata.sh
```

The script checks the Hadoop SHA-512 checksum, installs the local PySpark 4.2.1 and Py4J packages, unpacks Hadoop under `.runtime/hadoop/`, and copies the version-controlled XML/config files. It never formats HDFS. See [infrastructure verification](infrastructure_verification.md) for the compatibility choices and recorded results.

## First-time HDFS initialization

HDFS data lives under `.runtime/data/dfs/`; the configured NameNode directory is `.runtime/data/dfs/name`. Inspect it before formatting:

```bash
find .runtime/data/dfs/name -mindepth 1 -maxdepth 1 -print
```

Only when this is a brand-new cluster and the NameNode directory has no metadata, run the explicit one-time command:

```bash
./scripts/initialize_hdfs.sh --confirm-first-format
```

The initializer refuses a nonempty NameNode directory. Never use it to troubleshoot or restart an existing cluster. Preserve both `.runtime/data/dfs/name` and `.runtime/data/dfs/data`.

## Start, verify, and stop

```bash
./scripts/start_bigdata.sh
./scripts/verify_bigdata.sh
```

Startup requires an initialized NameNode, starts only stopped daemons, and creates the needed HDFS working directories. It does not format HDFS. The NameNode UI is normally `http://localhost:9870`; the ResourceManager UI is normally `http://localhost:8088`.

Stop services without deleting HDFS data:

```bash
./scripts/stop_bigdata.sh
```

Restart with `./scripts/start_bigdata.sh`. Do not remove `.runtime/data/` when restarting the same namespace.

## Run the application

With HDFS and YARN running:

```bash
./scripts/run_pipeline.sh data/raw/ecommerce_transactions_50000.csv
./scripts/run_dashboard.sh
```

The dashboard's Pipeline Monitor also accepts a selected fixture, upload, or generated input. The dashboard URL printed by Streamlit is normally `http://localhost:8501`.

## Benchmark

```bash
./scripts/run_benchmark.sh 10000
```

Supported row counts: 10,000, 50,000, 100,000, 250,000, and 500,000. Larger runs can take several minutes and consume cluster memory. Benchmarking is an explicit action, not part of startup.

## Tests

```bash
source scripts/bigdata_env.sh
.venv/bin/pytest -q
```

Tests create local Spark sessions and do not initialize or format HDFS. Use `./scripts/verify_bigdata.sh` for the HDFS/YARN service status. Additional caveats are in [testing](testing.md) and [troubleshooting](troubleshooting.md).
