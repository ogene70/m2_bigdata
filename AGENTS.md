# AGENTS.md

Local big data playground with a dockerized MinIO S3 server, Redpanda streaming, Iceberg (Nessie) tables and Spark. Helper scripts for object upload/download still exist; session archiving is now streaming-driven (Redpanda + Redpanda Connect).

## Commands

### Start environment

```bash
docker compose up -d
```

Starts MinIO, Nessie, Redpanda, Redpanda Console, Redpanda Connect (1 pipeline), Spark cluster, the Streamlit dashboard, the Iceberg maintenance service, the Arroyo stream engine, the topic-push service, the HTML real-time dashboard and the docs site. Buckets `data`, `sessions` and `warehouse` are created automatically by the `minio-init` one-shot service.

### Stop environment

```bash
docker compose down
```

### Web UIs

| Service | URL |
|----------|-----|
| MinIO console | http://localhost:9001 |
| Redpanda Console | http://localhost:10080 |
| Spark master UI | http://localhost:8080 |
| Dashboard (Streamlit) | http://localhost:4000 |
| Dashboard (HTML, temps réel) | http://localhost:4001 |
| Arroyo UI / API | http://localhost:5115 |
| Docsify (documentation) | http://localhost:2000 |

La doc docsify est copiée dans l'image nginx (pas de bind mount) : après toute modification de `docs/`, reconstruire le service — `docker compose up -d --build documentation`.

Credentials (MinIO + Redpanda):

```text
username: root
password: root-134
```

### Upload Object

```bash
./post.sh <file> [bucket] [object-key]
```

Defaults:

- bucket: `data`
- object-key: source filename

Behavior:

- Creates the bucket automatically if it does not already exist.
- Uploads the specified file to MinIO.

Examples:

```bash
./post.sh report.pdf
./post.sh report.pdf backups
./post.sh report.pdf backups reports/2026/report.pdf
```

### Download Object

```bash
./get.sh <object-key> [bucket] [destination]
```

Defaults:

- bucket: `data`
- destination: basename of object-key

Examples:

```bash
./get.sh reports/2026/report.pdf
./get.sh reports/2026/report.pdf backups
./get.sh reports/2026/report.pdf backups report-local.pdf
```

---

## Environment

### MinIO Endpoint

```text
http://localhost:9000
```

### Region

```text
us-east-1
```

### Authentication

```text
Access Key: root
Secret Key: root-134
```

Authentication uses AWS Signature Version 4.

Always use the existing helper scripts where possible.

Do not implement custom signing logic if `post.sh` or `get.sh` can perform the operation.

### Redpanda

```text
Kafka endpoint (host):     localhost:9093   # listener EXTERNAL, annonce localhost:9093
Kafka endpoint (Docker):   redpanda:9092    # listener PLAIN, annonce redpanda:9092
Schema Registry:           localhost:8081
Admin API:                 localhost:9644
Redpanda Console:          http://localhost:10080
```

Redpanda is Kafka API-compatible. Topic `sessions` receives one JSON message per archived row.

Les deux listeners portent des noms distincts (`PLAIN` et `EXTERNAL`) : un client qui se connecte à `localhost:9092` depuis l'hôte reçoit des métadonnées pointant vers `redpanda:9092` (nom interne Docker, non résolvable). **Tout client hôte (plugin opencode) doit utiliser `localhost:9093`.**

---

## Storage Layout

### Default Bucket

```text
data
```

### Session Archive Bucket

```text
sessions
```

### Iceberg Warehouse Bucket

```text
warehouse
```

All three buckets are created automatically (no manual step required).

---

## Session Persistence Policy

At the end of every chat session, the entire conversation must be archived; the plugin actually archives it live, and the final flush guarantees completeness.

The archive flows: opencode plugin → Redpanda topic `sessions` → Redpanda Connect → Parquet files in MinIO `sessions`. Spark (`iceberg-ingest`, streaming) reads those Parquet snapshots and merges them into Iceberg (`analytics.sessions`). Spark reads Iceberg for the dashboard.

### Automatic Archival

An opencode plugin automates the publish step:

- Entry point: `.opencode/plugins/session-archive.js` — exported as an **OpenCode V2 plugin** (`export default { id, setup(ctx) }`). It subscribes to `ctx.event.subscribe()` and archives live (`session.inbox.enqueued`, `session.text.ended`, `session.tool.success`, … debounce 250 ms), with a final flush on `session.execution.succeeded` / `failed` / `interrupted`.
- Core logic: `.opencode/lib/session-archive.mjs` — reads the transcript with `ctx.session.context({ sessionID })` (V2 messages: `{ type, time.created (ms), content[] }`), builds the decision-tree rows and publishes each row as one JSON message to Redpanda (topic `sessions`, key = `session_id`) via `kafkajs`.
- Broker: the plugin runs on the **host**, not inside Docker, so it uses the host-facing listener `localhost:9093` (`EXTERNAL://localhost:9093` in `docker-compose.yml`). `localhost:9092` advertises the Docker-internal address `redpanda:9092` and is therefore not usable from the host. Brokers are resolved/filtered at runtime (`brokerCandidates()`), with `REDPANDA_BROKERS` honoured as an extra candidate.
- Archive journal: `<project>/.opencode/logs/session-archive.log` (the V2 plugin context has no logger, so the plugin appends its own file).
- Redpanda Connect sinks (`.opencode` not involved):
  - `redpanda-connect-archive` → Parquet snapshots in MinIO bucket `sessions` (path `YYYY/MM/DD/<session-id>-<uuid>.parquet`), batches of up to 2000 rows / 15 s, zstd compression (`redpanda-connect/s3-archive.yaml`).
- Spark (`iceberg-ingest`, structured streaming) reads the Parquet snapshots from `s3a://sessions/` and merges them into the Iceberg table `analytics.sessions` (Nessie REST catalog `http://nessie:19120/iceberg/`, warehouse `warehouse`), 10 s trigger, table partitioned by `days(timestamp)`.
- Spark (`iceberg-maintenance`) compacts the Iceberg table every 30 minutes (`spark/jobs/maintain-sessions.py`, `rewrite_data_files`, run in `local[1]` so it does not take cluster cores). Snapshot expiry and orphan removal are deliberately excluded: with the Nessie catalog each commit exposes a single snapshot and older commits still reference those files (Nessie purges history with its own GC tool).
- Performance tuning (batching, trigger, partitioning, maintenance) is documented in `docs/performance.md`.

The plugin archives continuously while the session runs (debounce 250 ms) and flushes at the end of the execution; no manual step is required. Never run `opencode service restart` casually: the service caches plugin modules per location and does not watch the WSL/UNC path, so a restart is the only way to load a changed plugin — it interrupts the running session.

### Required Workflow

1. Collect the complete conversation transcript.
2. Normalize messages into structured records (rows with 17 columns, one row per node).
3. Publish each row as a JSON message to Redpanda topic `sessions` (key `session_id`).
4. Redpanda Connect persists rows to S3 Parquet (`sessions` bucket).
5. Spark (`iceberg-ingest`) ingests the Parquet snapshots into Iceberg (`analytics.sessions`).
6. Confirm publish success and report topic + count.
7. Optionally verify the Iceberg table via `./spark/spark-sql.sh` and Parquet objects via `get.sh`.

If publishing fails, preserve the generated Parquet file locally.

---

## Real-Time Topic Dashboard

A second, low-latency path publishes the LLM-extracted subject of every user message to a live HTML dashboard.

```text
opencode plugin (topic-extractor)
  → Redpanda topic `sessions` (row republished with column `topic`)
  → Arroyo pipeline `session-topics` (arroyo/pipeline.sql)
  → topic-push (POST /ingest, in-memory buffer, SSE fan-out)
  → dashboard-html (nginx, proxies /events without buffering)
```

### Components

| Path | Role |
|------|------|
| `.opencode/lib/topic-extractor.mjs` | LLM subject extraction (3-6 words, French) via `ctx.generate.text` — **no session is created**, so nothing to archive; heuristic fallback |
| `.opencode/plugins/session-archive.js` | Triggers extraction on `session.inbox.enqueued`, republishes the row once the subject is known |
| `arroyo/pipeline.sql` | Stream definition: Kafka source `sessions` → filter `role='user' AND topic<>''` → webhook sink |
| `arroyo/init.sh` | Idempotent registration of the pipeline through the Arroyo REST API |
| `topic-push/server.js` | `POST /ingest`, `GET /events` (SSE), `GET /sessions`, `GET /healthz` |
| `dashboard-html/index.html` | Single-file HTML dashboard (SVG timeline), served by nginx on `:4001` |

### Rules

- Subject extraction uses `ctx.generate.text({ model, prompt })`: it creates no session, invokes no tool and adds nothing to any history, so there is no recursion risk and no internal session to skip.
- The session model is tried first; if the provider refuses the transient call (the `opencode` free tier answers *"free tier can only be used from within OpenCode"*), the extractor retries up to two models from `ctx.model.list()` (non-`opencode` providers first, `:free`/flash/mini preferred) and caches the one that works. Three attempts maximum, then the heuristic fallback.
- Topics are keyed by `ord:<n>` (rank of the user message in the session) and mirrored under `txt:<sha1>` so a shifting message index cannot lose the subject.
- Compaction removes the `user` messages from the transcript returned by `ctx.session.context()`. The plugin records every prompt on `session.inbox.enqueued` and re-injects the missing ones as root `user` rows (`buildRows(…, { prompts })`), so a compacted session keeps its tree roots and subjects.
- Subject extraction is best-effort: a heuristic (first significant words) is used when the model call fails or the extraction is disabled.
- The `topic` column is part of the archive schema (Parquet + Iceberg `analytics.sessions`).
- The dashboard is read-only: it never writes to MinIO, Redpanda or Iceberg.
- `arroyo/pipeline.sql` is the reference for the stream; change it there, then re-run `arroyo-init` (`docker compose up arroyo-init`).

---

## Parquet Schema

Each message must be stored as a separate row.

| Column | Type |
|----------|----------|
| session_id | string |
| timestamp | timestamp |
| role | string |
| content | string |

Optional metadata columns:

| Column | Type |
|----------|----------|
| repository | string |
| model | string |
| message_index | integer |
| tokens_in | integer |
| tokens_out | integer |
| harness | string |
| topic | string |

`harness` identifies the agent harness that produced the row (`opencode`).
`topic` holds the LLM-extracted subject of a user message (3-6 words, French). It is empty for non-user rows and for archives produced before the extraction feature.

### Decision Tree Columns

Since the agent's real decisions (steps, reasoning, tool calls) must be captured, the archive also stores the conversation as a tree. Every node (message, step, reasoning, tool) is one row.

| Column | Type | Meaning |
|----------|----------|----------|
| decision_node_id | integer | unique node id within the session |
| decision_parent_id | integer | parent node id (`0` = root / user request) |
| decision_node_type | string | `message`, `step`, `reasoning`, or `tool` |
| decision_status | string | step outcome (`stop`, `tool-calls`, `started`…) or tool status (`completed`, `error`…) |
| tool_name | string | tool used for `tool` nodes (`bash`, `glob`, …) |
| tool_input | string | command/arguments of the tool call |

`decision_node_type` is `message` for rows missing the column (legacy archives).

---

## Session Identifier

Generate a unique session identifier for every session.

Recommended format:

```text
YYYYMMDD-HHMMSS-UUID
```

Example:

```text
20260917-132455-a7f29587c3d1
```

(The UUID part is the first 12 hex characters of a random UUID.)

Use the same identifier in:

- Redpanda message key
- Parquet archive name
- parquet session_id field and Iceberg `session_id` column

---

## Object Key Convention

Redpanda Connect writes Parquet archives to the `sessions` bucket using:

```text
YYYY/MM/DD/<session-id>-<uuid>.parquet
```

Example:

```text
2026/10/21/20261021-152825-a926b56e8ac8-daf63846-749c-4f20-8877-7be41deea23f.parquet
```

The date prefix is derived from the message timestamp; a random UUID suffix prevents collisions when batches span sessions.

---

## Local File Convention

On publish failure only, the plugin preserves the archive locally at:

```text
./archives/<archive-session-id>.parquet
```

The file is named with the **archive** session id (`YYYYMMDD-HHMMSS-UUID`, printed in the journal as `session ses_… / <archive-id>`), not with the opencode `ses_…` id.

Manual recovery is possible via:

```bash
./post.sh archives/<archive-session-id>.parquet sessions <object-key>
```

---

## Verification

After a session ends:

1. Verify the publish succeeded (the plugin journal `.opencode/logs/session-archive.log` logs `archived <n>/<m> rows -> sessions (session ses_… / <archive-id>)`).
2. Verify rows reached the Iceberg table:

   ```bash
   ./spark/spark-sql.sh "SELECT count(*), min(decision_node_type), max(decision_node_type) FROM analytics.sessions WHERE session_id = '<session-id>' GROUP BY 1"
   ```

3. Verify the Parquet object exists in the `sessions` bucket:

   ```bash
   ./get.sh 2026/09/17/<session-id>-<uuid>.parquet sessions
   ```

Expected result:

```text
topic: sessions
count: <n rows>
log: archived <n>/<m> rows -> sessions
```

---

## Failure Handling

If row building fails:

- report the error
- stop the archival workflow
- do not delete intermediate data

If Redpanda publish fails:

- report the error
- retain the local Parquet file under `./archives/<archive-session-id>.parquet`
- include the intended object key
- include the bucket name (`sessions`)

Example:

```text
Session archive publish failed.

Bucket:
sessions

Intended object key:
2026/10/21/20261021-152825-a926b56e8ac8.parquet

Local File:
archives/20261021-152825-a926b56e8ac8.parquet
```

---

## Gotchas

- This is MinIO, not AWS S3.
- Endpoint must always be:

  ```text
  http://localhost:9000
  ```

- Region must always be:

  ```text
  us-east-1
  ```

- Credentials are:

  ```text
  root / root-134
  ```

- The `iceberg-ingest` Spark job reads the Parquet snapshots from `s3a://sessions/` (Hadoop S3A, path-style `http://s3:9000`, creds `root / root-134`) and writes to the Iceberg table `analytics.sessions` via the Nessie REST catalog at `http://nessie:19120/iceberg/`, warehouse `warehouse`.
- Use `post.sh` and `get.sh` whenever possible.
- Do not replace the helper scripts with custom implementations unless explicitly requested.
- `get.sh` displays `get-file.sh` in its usage message. This is a cosmetic mismatch and should be ignored.
- Data is stored in the Docker volumes:

  ```text
  s3-data
  nessie-data
  redpanda-data
  arroyo-data
  ```

- Running:

  ```bash
  docker compose down -v
  ```

  permanently removes all stored objects: MinIO objects, Nessie catalog metadata, and Redpanda topics.

---

## Agent Rules

- Prefer existing repository scripts over new tooling.
- Treat Redpanda as the ingestion buffer and MinIO/Iceberg as the source of truth for archived sessions.
- Store every completed session in Parquet format.
- Publish every completed session to the Redpanda `sessions` topic.
- Never upload session archives to the default `data` bucket.
- Keep date-partitioned object keys.
- Preserve publish logs and error details when failures occur.
- Report the publish result after a successful archive.
- Do not use external cloud storage services.
- Do not use AWS endpoints.
- Use only the local Dockerized MinIO instance.
- Do not modify `redpanda-connect/s3-archive.yaml` Parquet schema or the `iceberg-ingest` Spark job / table layout without coordinating with the dashboard and Spark config. 
- Always create subagents to execute multitasks and delete them after task ended 
