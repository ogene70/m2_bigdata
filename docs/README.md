# m2_bigdata — Documentation

Local big-data playground: a Dockerized **MinIO** S3 server, **Redpanda** streaming, an **Iceberg** lakehouse (Nessie catalog), a standalone **Apache Spark** cluster and a **Streamlit** dashboard. Session archives flow automatically from the opencode agent into Redpanda, are persisted as Parquet by Redpanda Connect, ingested into Iceberg by Spark, and explored through the dashboard.

## Pipeline at a glance

```text
opencode plugin → Redpanda (topic sessions) → Redpanda Connect → Parquet (MinIO sessions)
                                                       ↓
                                  Spark iceberg-ingest (streaming)
                                                       ↓
                                        Iceberg analytics.sessions (Nessie)
                                                       ↓
                                              Streamlit dashboard
```

See [Project architecture](/architecture.md) for the full diagram and data-flow description.

## Quick start

```bash
docker compose up -d
```

Starts MinIO, Nessie, Redpanda, Redpanda Console, Redpanda Connect, the Spark cluster, the Streamlit dashboard and this documentation site. The `minio-init` one-shot service creates the `data`, `sessions` and `warehouse` buckets automatically.

Stop everything with:

```bash
docker compose down
```

## Web UIs

| Service | URL |
|---|---|
| Dashboard (Streamlit) | http://localhost:4000 |
| Documentation | http://localhost:2000 |
| MinIO console | http://localhost:9001 |
| Redpanda Console | http://localhost:10080 |
| Spark master UI | http://localhost:8080 |
| Nessie REST catalog | http://localhost:19120 |

Credentials (MinIO + Redpanda): `root` / `root-134`.

## Sections

- [Project architecture](/architecture.md) — services, data flow, storage layout and endpoints.
- [The Bigdata](/introduction-to-bigdata/) — background on storage, distributed compute and traceability.
- [Redline project](/redline-project/) — project ideas.
