# AGENTS.md

Local MinIO S3 playground. No application code, tests, lint, or build config. This repository provides a dockerized MinIO S3 server and helper scripts for object upload/download.

## Commands

### Start S3

```bash
docker compose up -d
```

### Stop S3

```bash
docker compose down
```

### Web Console

URL:

```text
http://localhost:9001
```

Credentials:

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

Create the bucket automatically when required.

---

## Session Persistence Policy

At the completion of every chat session, archive the entire conversation to MinIO.

The archive must be written as an Apache Parquet file and uploaded to the local MinIO instance.

### Automatic Archival

An opencode plugin automates this workflow:

- Hook: `session.idle` in `.opencode/plugins/session-archive.js`
- Core logic: `.opencode/lib/session-archive.mjs` (Parquet generation via `parquetjs-lite` + upload via `post.sh`)
- Bucket: `sessions`

The plugin runs automatically when a conversation ends. No manual step is required.

### Required Workflow

1. Collect the complete conversation transcript.
2. Normalize messages into structured records.
3. Generate a Parquet file.
4. Upload the Parquet file to MinIO.
5. Confirm upload success.
6. Report the object key used for storage.

If upload fails, report the failure and preserve the generated Parquet file locally.

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

- filename
- object key
- parquet session_id field

---

## Object Key Convention

Store archives using:

```text
YYYY/MM/DD/<session-id>.parquet
```

Example:

```text
2026/09/17/20260917-132455-a7f29587.parquet
```

---

## Local File Convention

Before upload, create:

```text
./archives/<session-id>.parquet
```

Example:

```text
./archives/20260917-132455-a7f29587.parquet
```

---

## Upload Command

Upload session archives using:

```bash
./post.sh <parquet-file> sessions <object-key>
```

Example:

```bash
./post.sh \
  archives/20260917-132455-a7f29587.parquet \
  sessions \
  2026/09/17/20260917-132455-a7f29587.parquet
```

---

## Verification

After upload:

1. Verify the upload completed successfully.
2. Verify the object exists in the target bucket.
3. Record the bucket and object key used.

Expected result:

```text
bucket: sessions
object-key: YYYY/MM/DD/<session-id>.parquet
status: uploaded
```

---

## Failure Handling

If Parquet generation fails:

- report the error
- stop the archival workflow
- do not delete intermediate data

If upload fails:

- report the error
- retain the local Parquet file
- include the intended object key
- include the bucket name

Example:

```text
Session archive upload failed.

Bucket:
sessions

Object Key:
2026/09/17/20260917-132455-a7f29587.parquet

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

- Use `post.sh` and `get.sh` whenever possible.
- Do not replace the helper scripts with custom implementations unless explicitly requested.
- `get.sh` displays `get-file.sh` in its usage message. This is a cosmetic mismatch and should be ignored.
- Data is stored in the Docker volume:

  ```text
  s3-data
  ```

- Running:

  ```bash
  docker compose down -v
  ```

  permanently removes all stored objects, including archived session Parquet files.

---

## Agent Rules

- Prefer existing repository scripts over new tooling.
- Treat MinIO as the source of truth for archived sessions.
- Store every completed session in Parquet format.
- Upload every completed session to the `sessions` bucket.
- Never upload session archives to the default `data` bucket.
- Always use date-partitioned object keys.
- Preserve upload logs and error details when failures occur.
- Report the final archive location after a successful upload.
- Do not use external cloud storage services.
- Do not use AWS endpoints.
- Use only the local Dockerized MinIO instance.