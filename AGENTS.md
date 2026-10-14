# AGENTS.md

Local big data playground with a dockerized MinIO S3 server, Redpanda streaming, Iceberg (Nessie) tables and Spark. Helper scripts for object upload/download still exist; session archiving is now streaming-driven (Redpanda + Redpanda Connect).

## Commands

### Start environment

```bash
docker compose up -d
```

Starts MinIO, Nessie, Redpanda, Redpanda Console, Redpanda Connect (1 pipeline), Spark cluster, the Streamlit dashboard and the docs site. Buckets `data`, `sessions` and `warehouse` are created automatically by the `minio-init` one-shot service.

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
Kafka endpoint:      localhost:9092
Schema Registry:     localhost:8081
Admin API:           localhost:9644
Redpanda Console:    http://localhost:10080
```

Redpanda is Kafka API-compatible. Topic `sessions` receives one JSON message per archived row.

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

At the completion of every chat session, archive the entire conversation.

The archive flows: opencode plugin → Redpanda topic `sessions` → Redpanda Connect → Parquet files in MinIO `sessions`. Spark (`iceberg-ingest`, streaming) reads those Parquet snapshots and merges them into Iceberg (`analytics.sessions`). Spark reads Iceberg for the dashboard.

### Automatic Archival

An opencode plugin automates the publish step:

- Hook: `session.idle` in `.opencode/plugins/session-archive.js`
- Core logic: `.opencode/lib/session-archive.mjs` — builds rows and publishes each row as one JSON message to Redpanda (topic `sessions`, key = `session_id`) via `kafkajs`. Broker: `localhost:9092` (override with env `REDPANDA_BROKERS`).
- Redpanda Connect sinks (`.opencode` not involved):
  - `redpanda-connect-archive` → Parquet snapshots in MinIO bucket `sessions` (path `YYYY/MM/DD/<session-id>-<uuid>.parquet`), ~10s batching.
- Spark (`iceberg-ingest`, structured streaming) reads the Parquet snapshots from `s3a://sessions/` and appends them to the Iceberg table `analytics.sessions` (Nessie REST catalog `http://nessie:19120/iceberg/`, warehouse `warehouse`), ~10s trigger.

The plugin runs automatically when a conversation ends. No manual step is required.

### Required Workflow

1. Collect the complete conversation transcript.
2. Normalize messages into structured records (rows with 13 columns, one row per node).
3. Publish each row as a JSON message to Redpanda topic `sessions` (key `session_id`).
4. Redpanda Connect persists rows to S3 Parquet (`sessions` bucket).
5. Spark (`iceberg-ingest`) ingests the Parquet snapshots into Iceberg (`analytics.sessions`).
6. Confirm publish success and report topic + count.
7. Optionally verify the Iceberg table via `./spark/spark-sql.sh` and Parquet objects via `get.sh`.

If publishing fails, preserve the generated Parquet file locally.

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
20260917-132455-a7f29587
```

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
2026/09/17/20260917-132455-a7f29587-3f9a21cc.parquet
```

The date prefix is derived from the message timestamp; a random UUID suffix prevents collisions when batches span sessions.

---

## Local File Convention

On publish failure only, the plugin preserves the archive locally at:

```text
./archives/<session-id>.parquet
```

Manual recovery is possible via:

```bash
./post.sh archives/<session-id>.parquet sessions <object-key>
```

---

## Verification

After a session ends:

1. Verify the publish succeeded (plugin logs `status: published`).
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
status: published
```

---

## Failure Handling

If row building fails:

- report the error
- stop the archival workflow
- do not delete intermediate data

If Redpanda publish fails:

- report the error
- retain the local Parquet file under `./archives/<session-id>.parquet`
- include the intended object key
- include the bucket name (`sessions`)

Example:

```text
Session archive publish failed.

Bucket:
sessions

Intended object key:
2026/09/17/20260917-132455-a7f29587-3f9a21cc.parquet

Local File:
archives/20260917-132455-a7f29587.parquet
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