#!/usr/bin/env bash
set -euo pipefail

# Ensure the Iceberg warehouse bucket exists on MinIO.
docker exec m2-bigdata-s3 mc alias set local http://localhost:9000 root root-134 >/dev/null 2>&1
docker exec m2-bigdata-s3 mc mb local/warehouse --ignore-existing >/dev/null 2>&1 || true

echo "Bucket 'warehouse' ready. Running Iceberg ingestion..."

docker compose exec dashboard python ingest.py