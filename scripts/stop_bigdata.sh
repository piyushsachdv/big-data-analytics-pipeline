#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT/scripts/bigdata_env.sh"

stop_if_running() {
  local pid_file="$1" main_class="$2"
  shift 2
  if [[ -r "$pid_file" ]]; then
    local pid command_line
    pid="$(cat "$pid_file")"
    command_line="$(ps -p "$pid" -o args= 2>/dev/null || true)"
    if [[ "$command_line" == *"$main_class"* ]]; then
      "$@"
    fi
  fi
}

stop_if_running "$YARN_PID_DIR/hadoop-${USER}-nodemanager.pid" org.apache.hadoop.yarn.server.nodemanager.NodeManager yarn --daemon stop nodemanager
stop_if_running "$YARN_PID_DIR/hadoop-${USER}-resourcemanager.pid" org.apache.hadoop.yarn.server.resourcemanager.ResourceManager yarn --daemon stop resourcemanager
stop_if_running "$HADOOP_PID_DIR/hadoop-${USER}-datanode.pid" org.apache.hadoop.hdfs.server.datanode.DataNode hdfs --daemon stop datanode
stop_if_running "$HADOOP_PID_DIR/hadoop-${USER}-namenode.pid" org.apache.hadoop.hdfs.server.namenode.NameNode hdfs --daemon stop namenode
echo "HDFS and YARN daemons stopped. NameNode data was preserved."
