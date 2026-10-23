#!/usr/bin/env bash
# Idempotent pipeline registration against the Arroyo API.
set -euo pipefail

API="${ARROYO_API:-http://arroyo:5115/api/v1}"
NAME="${PIPELINE_NAME:-session-topics}"
QUERY_FILE="${PIPELINE_SQL:-/opt/arroyo/pipeline.sql}"

python3 - "$QUERY_FILE" "$NAME" "$API" <<'PY'
import json
import sys
import time
import urllib.error
import urllib.request

query_file, name, api = sys.argv[1], sys.argv[2], sys.argv[3]
with open(query_file, encoding="utf-8") as handle:
    query = handle.read()


def request(method, path, payload=None):
    data = json.dumps(payload).encode("utf-8") if payload is not None else None
    req = urllib.request.Request(
        api + path,
        data=data,
        method=method,
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=60) as resp:
        body = resp.read().decode("utf-8")
        return resp.status, (json.loads(body) if body.strip() else None)


for _ in range(90):
    try:
        request("GET", "/pipelines")
        break
    except Exception as err:  # noqa: BLE001
        print(f"[arroyo-init] waiting for api: {err}", flush=True)
        time.sleep(2)
else:
    sys.exit(f"[arroyo-init] arroyo api unreachable at {api}")

status, listing = request("GET", "/pipelines")
pipelines = (listing or {}).get("data", []) or []
existing = next((p for p in pipelines if p.get("name") == name), None)
payload = {"name": name, "query": query, "parallelism": 1}


def is_running(pipeline_id):
    try:
        _, jobs = request("GET", f"/pipelines/{pipeline_id}/jobs")
    except Exception:  # noqa: BLE001
        return True
    data = (jobs or {}).get("data", []) or []
    if not data:
        return False
    return data[-1].get("state") == "Running"


if existing:
    pipeline_id = existing.get("id")
    print(f"[arroyo-init] updating pipeline {pipeline_id} ({name})", flush=True)
    try:
        request("PATCH", f"/pipelines/{pipeline_id}", payload)
    except urllib.error.HTTPError as err:
        print(f"[arroyo-init] update failed ({err}) -> recreating", flush=True)
        try:
            request("POST", f"/pipelines/{pipeline_id}/stop")
            time.sleep(3)
        except urllib.error.HTTPError:
            pass
        try:
            request("DELETE", f"/pipelines/{pipeline_id}")
        except urllib.error.HTTPError as del_err:
            print(f"[arroyo-init] delete failed ({del_err})", flush=True)
        status, created = request("POST", "/pipelines", payload)
        pipeline_id = (created or {}).get("id", pipeline_id)
else:
    print(f"[arroyo-init] creating pipeline {name}", flush=True)
    status, created = request("POST", "/pipelines", payload)
    pipeline_id = (created or {}).get("id")

if pipeline_id and not is_running(pipeline_id):
    print(f"[arroyo-init] starting pipeline {pipeline_id}", flush=True)
    try:
        request("POST", f"/pipelines/{pipeline_id}/start")
    except urllib.error.HTTPError as err:
        print(f"[arroyo-init] start failed ({err})", flush=True)

print(f"[arroyo-init] pipeline {name} ready", flush=True)
PY
