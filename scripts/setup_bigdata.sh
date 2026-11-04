#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ARCHIVES="$ROOT/.runtime/archives"
RUNTIME="$ROOT/.runtime"

require_file() {
  if [[ ! -f "$1" ]]; then
    echo "Required file is missing: $1" >&2
    exit 1
  fi
}

require_file "$ARCHIVES/hadoop-3.5.0.tar.gz"
require_file "$ARCHIVES/hadoop-3.5.0.tar.gz.sha512"
require_file "$ARCHIVES/pyspark-4.2.1.tar.gz"

echo "Verifying Apache release checksums..."
(cd "$ARCHIVES" && sha512sum --check hadoop-3.5.0.tar.gz.sha512)

echo "Installing the locally downloaded Apache PySpark distribution into .venv..."
"$ROOT/.venv/bin/python" -m pip install --no-index --no-build-isolation --find-links "$ARCHIVES" pyspark==4.2.1 py4j==0.10.9.9

mkdir -p "$RUNTIME/hadoop" "$RUNTIME/spark" "$RUNTIME/metastore"
if [[ ! -x "$RUNTIME/hadoop/bin/hdfs" ]]; then
  tar -xzf "$ARCHIVES/hadoop-3.5.0.tar.gz" -C "$RUNTIME/hadoop" --strip-components=1
fi
mkdir -p "$RUNTIME/data" "$RUNTIME/logs" "$RUNTIME/pids" "$RUNTIME/metastore"
cp "$ROOT/config/hadoop/core-site.xml" "$RUNTIME/hadoop/etc/hadoop/core-site.xml"
cp "$ROOT/config/hadoop/hdfs-site.xml" "$RUNTIME/hadoop/etc/hadoop/hdfs-site.xml"
cp "$ROOT/config/hadoop/yarn-site.xml" "$RUNTIME/hadoop/etc/hadoop/yarn-site.xml"
cp "$ROOT/config/hadoop/mapred-site.xml" "$RUNTIME/hadoop/etc/hadoop/mapred-site.xml"
SPARK_HOME="$("$ROOT/.venv/bin/python" -c 'import pyspark; print(pyspark.__path__[0])')"
mkdir -p "$SPARK_HOME/conf"
cp "$ROOT/config/spark/spark-defaults.conf" "$SPARK_HOME/conf/spark-defaults.conf"
cp "$ROOT/config/hive/hive-site.xml" "$SPARK_HOME/conf/hive-site.xml"
for config_file in \
  "$RUNTIME/hadoop/etc/hadoop/core-site.xml" \
  "$RUNTIME/hadoop/etc/hadoop/hdfs-site.xml" \
  "$RUNTIME/hadoop/etc/hadoop/yarn-site.xml" \
  "$RUNTIME/hadoop/etc/hadoop/mapred-site.xml" \
  "$SPARK_HOME/conf/hive-site.xml"; do
  sed -i "s|__RUNTIME__|$RUNTIME|g" "$config_file"
done

for env_file in "$RUNTIME/hadoop/etc/hadoop/hadoop-env.sh" "$RUNTIME/hadoop/etc/hadoop/yarn-env.sh"; do
  if ! grep -Fq "export JAVA_HOME=\"${JAVA_HOME:-/usr/lib/jvm/java-17-openjdk-amd64}\"" "$env_file"; then
    printf '\nexport JAVA_HOME="%s"\n' "${JAVA_HOME:-/usr/lib/jvm/java-17-openjdk-amd64}" >> "$env_file"
  fi
done

cat > "$RUNTIME/hadoop/etc/hadoop/workers" <<'EOF'
localhost
EOF

echo "Apache Hadoop and PySpark installed under $RUNTIME and .venv."
echo "Hive support uses Spark's bundled Hive metastore with persistent Derby metadata."
echo "HDFS data is kept in $RUNTIME/data; this script never formats the NameNode."
