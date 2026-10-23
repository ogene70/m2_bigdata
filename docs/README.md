# m2_bigdata — Documentation

Local big-data playground: a Dockerized **MinIO** S3 server, **Redpanda** streaming, an **Iceberg** lakehouse (Nessie catalog), a standalone **Apache Spark** cluster and a **Streamlit** dashboard. Session archives flow automatically from the opencode agent into Redpanda, are persisted as Parquet by Redpanda Connect, ingested into Iceberg by Spark, and explored through the dashboard.

## Pipeline at a glance

```text
opencode plugin → Redpanda (topic sessions) ─┬→ Redpanda Connect → Parquet (MinIO sessions)
                                             │                              ↓
                                             │          Spark iceberg-ingest (streaming)
                                             │                              ↓
                                             │            Iceberg analytics.sessions (Nessie)
                                             │                              ↓
                                             │                Streamlit dashboard (:4000)
                                             │
                                             └→ Arroyo (session-topics) → topic-push (SSE)
                                                                              ↓
                                                             dashboard-html (:4001)
```

See [Project architecture](/architecture.md) for the full diagram and data-flow description.

A second, low-latency path publishes the subject of each user message to a live
HTML dashboard: `opencode plugin → Redpanda (topic sessions) → Arroyo
(session-topics) → topic-push (SSE) → dashboard-html`. It shows what a session is
about, turn after turn, without touching the lakehouse. See
[Real-time dashboard](/real-time-dashboard.md).

## Quick start

```bash
docker compose up -d
```

Starts MinIO, Nessie, Redpanda, Redpanda Console, Redpanda Connect, the Spark cluster, the Streamlit dashboard, the Arroyo stream engine, the topic-push service, the HTML real-time dashboard and this documentation site. The `minio-init` one-shot service creates the `data`, `sessions` and `warehouse` buckets automatically.

Stop everything with:

```bash
docker compose down
```

## Web UIs

| Service | URL |
|---|---|
| Dashboard (Streamlit) | http://localhost:4000 |
| Dashboard (HTML, real-time) | http://localhost:4001 |
| Arroyo UI / API | http://localhost:5115 |
| topic-push API | http://localhost:8090 |
| Documentation | http://localhost:2000 |
| MinIO console | http://localhost:9001 |
| Redpanda Console | http://localhost:10080 |
| Spark master UI | http://localhost:8080 |
| Nessie REST catalog | http://localhost:19120 |

Credentials (MinIO + Redpanda): `root` / `root-134`.

## Sections

- [Project architecture](/architecture.md) — services, data flow, storage layout and endpoints.
- [Real-time dashboard](/real-time-dashboard.md) — the HTML timeline of conversation subjects: UI, SSE contract, topic-push API and Arroyo pipeline.
- [Performance](/performance.md) — the archival pipeline optimisation: the problem, the initial state (measured), the changes and their measured effect.
- [The Bigdata](/introduction-to-bigdata/) — background on storage, distributed compute and traceability.
- [Redline project](/redline-project/) — project ideas.
