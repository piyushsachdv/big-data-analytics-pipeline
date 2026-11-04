#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT/scripts/bigdata_env.sh"

echo "Java: $(java -version 2>&1 | head -1)"
echo "Hadoop: $(hadoop version | head -1)"
echo "Spark: $(spark-submit --version 2>&1 | grep -m1 'version' || true)"
echo "Hive catalog: Spark's built-in Hive support"
echo
echo "HDFS report:"
hdfs dfsadmin -report
echo
echo "YARN nodes:"
yarn node -list -all
