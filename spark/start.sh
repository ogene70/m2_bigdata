#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

docker compose up -d --build --scale spark-worker=3 dashboard spark-master spark-worker spark-connect

echo ""
echo "Spark UI :      http://localhost:8080"
echo "Spark Connect : grpc://localhost:15002"
echo ""
docker compose ps --format "table {{.Name}}\t{{.Service}}\t{{.Status}}"