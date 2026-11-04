#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT/scripts/bigdata_env.sh"

if [[ "${1:-}" != "--confirm-first-format" ]]; then
  echo "Refusing to format HDFS. Run with --confirm-first-format only for a new, empty NameNode." >&2
  exit 2
fi
NAME_DIR="$BIGDATA_ROOT/data/dfs/name"
if [[ -n "$(find "$NAME_DIR" -mindepth 1 -maxdepth 1 -print -quit 2>/dev/null || true)" ]]; then
  echo "NameNode directory is not empty; refusing to format: $NAME_DIR" >&2
  exit 3
fi
hdfs namenode -format ecommerce-analytics
