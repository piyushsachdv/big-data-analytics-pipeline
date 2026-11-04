#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT/scripts/bigdata_env.sh"
exec "$ROOT/scripts/verify_bigdata.sh"
