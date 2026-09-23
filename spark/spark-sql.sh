#!/usr/bin/env bash
# Usage: ./spark/spark-sql.sh "<SQL>"
set -euo pipefail
export MSYS_NO_PATHCONV=1 MSYS2_ARG_CONV_EXCL='*'
cd "$(dirname "$0")/.."

sql="${1:?Usage: ./spark/spark-sql.sh \"<SQL>\"}"

docker compose exec -T spark-master /opt/spark/bin/spark-sql \
  --master spark://spark-master:7077 \
  -e "$sql"