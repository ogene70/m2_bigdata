#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

docker compose up -d --scale spark-worker=0 spark-master 2>/dev/null || true
docker compose stop spark-master spark-worker spark-connect

echo "Cluster Spark arrêté (le reste de la stack reste en place)."