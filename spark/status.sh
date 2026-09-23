#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

echo "== Spark cluster =="
docker compose ps --filter "name=spark" --format "table {{.Name}}\t{{.Service}}\t{{.Status}}"

echo ""
echo "== Spark Master =="
curl -fsS http://localhost:8080/json/ 2>/dev/null \
  | python -c "
import sys, json
try:
    d = json.load(sys.stdin)
    alive = sum(1 for w in d.get('workers',[]) if w.get('state')=='ALIVE')
    total = len(d.get('workers',[]))
    print(f'workers : {alive}/{total} alive')
except Exception:
    print('(master UI indisponible)')
" 2>/dev/null || echo "(master UI indisponible)"