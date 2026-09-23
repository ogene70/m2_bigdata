#!/usr/bin/env bash
set -euo pipefail

SQL="${1:-SELECT role, count(*) AS n FROM sessions GROUP BY role ORDER BY n DESC}"

docker compose exec dashboard python query.py "$SQL"