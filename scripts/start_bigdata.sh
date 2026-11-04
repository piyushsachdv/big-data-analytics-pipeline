#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT/scripts/bigdata_env.sh"

start_if_stopped() {
  local name="$1" pid_file="$2" main_class="$3"
  shift 3
  if [[ -r "$pid_file" ]]; then
    local pid command_line
    pid="$(cat "$pid_file")"
    if [[ "$pid" =~ ^[0-9]+$ ]] && kill -0 "$pid" 2>/dev/null; then
      command_line="$(ps -p "$pid" -o args= 2>/dev/null || true)"
      if [[ "$command_line" == *"$main_class"* ]]; then
        echo "$name already running (pid $pid)"
        return 0
      fi
    fi
  fi
  setsid "$@"
}

NAME_DIR="$BIGDATA_ROOT/data/dfs/name"
if [[ ! -d "$NAME_DIR/current" ]]; then
  echo "HDFS is not initialized. Review the new NameNode directory, then run scripts/initialize_hdfs.sh --confirm-first-format once." >&2
  exit 2
fi

start_if_stopped NameNode "$HADOOP_PID_DIR/hadoop-${USER}-namenode.pid" \
  org.apache.hadoop.hdfs.server.namenode.NameNode hdfs --daemon start namenode
start_if_stopped DataNode "$HADOOP_PID_DIR/hadoop-${USER}-datanode.pid" \
  org.apache.hadoop.hdfs.server.datanode.DataNode hdfs --daemon start datanode
for _ in {1..30}; do
  if hdfs dfsadmin -safemode get 2>/dev/null | grep -q 'OFF'; then break; fi
  sleep 2
done
start_if_stopped ResourceManager "$YARN_PID_DIR/hadoop-${USER}-resourcemanager.pid" \
  org.apache.hadoop.yarn.server.resourcemanager.ResourceManager yarn --daemon start resourcemanager
start_if_stopped NodeManager "$YARN_PID_DIR/hadoop-${USER}-nodemanager.pid" \
  org.apache.hadoop.yarn.server.nodemanager.NodeManager yarn --daemon start nodemanager

hdfs dfs -mkdir -p /tmp /user/"$(id -un)" /user/hive/warehouse /apps/spark
hdfs dfs -chmod 1777 /tmp
hdfs dfs -chmod 775 /user/hive/warehouse

echo "HDFS and YARN startup commands were issued. Spark creates the persistent Hive catalog on first query."
echo "NameNode UI: http://localhost:9870 | ResourceManager UI: http://localhost:8088"
