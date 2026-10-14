# Project Architecture

Local big-data playground built around a Dockerized MinIO S3 server, Redpanda streaming, an Iceberg lakehouse (Nessie catalog), a standalone Apache Spark cluster, and a Streamlit dashboard. Session archives flow automatically from the opencode agent into Redpanda (topic `sessions`); Redpanda Connect persists them as Parquet snapshots in MinIO `sessions`; a Spark structured-streaming job (`iceberg-ingest`) ingests those snapshots into the Iceberg lakehouse (Nessie catalog); and the dashboard reads Iceberg via Spark for visualization and SQL.

---

## High-level overview

```mermaid
flowchart LR
    subgraph Host["Host machine"]
        OP["opencode agent<br/>(session-archive plugin)"]
        SH["Helper scripts<br/>post.sh · get.sh"]
        ARC["./archives/*.parquet<br/>(publish failure fallback only)"]
    end

    subgraph Stack["Docker Compose stack (m2-bigdata)"]
        subgraph Streaming["Streaming layer"]
            RP["Redpanda broker<br/>m2-bigdata-redpanda<br/>:9092 (Kafka) · :8081 (SR) · :9644 (admin)"]
            RPC["Redpanda Connect<br/>s3-archive pipeline"]
            CPC["Redpanda Console<br/>:10080"]
        end
        subgraph Storage["Storage layer"]
            S3["MinIO S3<br/>m2-bigdata-s3<br/>:9000 / :9001"]
        end
        subgraph Catalog["Catalog layer"]
            NESSIE["Nessie catalog<br/>m2-bigdata-nessie<br/>:19120"]
        end
        subgraph Compute["Compute layer"]
            SM["spark-master<br/>:8080 / :7077"]
            SW["spark-worker ×N<br/>(2 cores / 2g each)"]
            SC["spark-connect<br/>:15002 (gRPC)"]
            III["iceberg-ingest<br/>(Parquet → Iceberg streaming)"]
        end
        subgraph App["Application layer"]
            DASH["dashboard (Streamlit)<br/>m2-bigdata-dashboard<br/>:4000 → :8501"]
        end
    end

    UI["Browser<br/>:4000"]

    OP -->|"kafkajs → topic sessions"| RP
    RP -->|"redpanda input"| RPC
    RPC -->|"parquet → s3://sessions/"| S3
    III -->|"read s3a://sessions/"| S3
    III -->|"append (REST catalog)"| NESSIE
    NESSIE -->|"warehouse data<br/>s3://warehouse/"| S3
    SM --> SW
    SC -->|"gRPC"| SM
    DASH -->|"Spark Connect → Iceberg"| SC
    UI --> DASH
    SH -->|S3 SigV4 HTTP| S3
```

---

## Docker services (from `docker-compose.yml`)

| Service | Image / Build | Container | Host ports | Role |
|---|---|---|---|---|
| `s3` | `quay.io/minio/minio:latest` | `m2-bigdata-s3` | `9000`, `9001` | Object storage (S3 API + console) |
| `nessie` | `ghcr.io/projectnessie/nessie:latest` | `m2-bigdata-nessie` | `19120` | Iceberg REST catalog (RocksDB store) |
| `minio-init` | `quay.io/minio/minio:latest` | `m2-bigdata-minio-init` | — | One-shot bucket creation (`data`, `sessions`, `warehouse`) |
| `redpanda` | `docker.redpanda.com/redpandadata/redpanda:v26.2.2` | `m2-bigdata-redpanda` | `9092`, `8081`, `9644` | Kafka-compatible stream broker |
| `redpanda-console` | `docker.redpanda.com/redpandadata/console:v3.11.0` | `m2-bigdata-redpanda-console` | `10080` | Redpanda web UI |
| `redpanda-connect-archive` | `./redpanda-connect` | `m2-bigdata-redpanda-connect-archive` | — | Topic `sessions` → Parquet snapshots in MinIO `sessions` (10s batching) |
| `iceberg-ingest` | `./spark` | `m2-bigdata-iceberg-ingest` | — | Spark streaming: Parquet `s3a://sessions/` → Iceberg `analytics.sessions` (10s trigger) |
| `dashboard` | `./dashboard` | `m2-bigdata-dashboard` | `4000 → 8501` | Streamlit UI + query scripts |
| `spark-master` | `./spark` | `m2-bigdata-spark-master` | `8080`, `7077` | Spark standalone master |
| `spark-worker` | `./spark` | (scaled, default 1; `start.sh` uses 3) | — | Spark workers (2 cores / 2 GB) |
| `spark-connect` | `./spark` | `m2-bigdata-spark-connect` | `15002` | Spark Connect gRPC server |
| `documentation` | `./docs` | `m2_bigdata-documentation-1` | `2000 → 80` | Docsify documentation site (nginx) |

Shared environment anchor (`x-spark-env`): `ICEBERG_VERSION=1.11.0`, `AWS_REGION=us-east-1`.

Volumes: `s3-data` (MinIO objects), `nessie-data` (catalog metadata), `redpanda-data` (topics/logs).

### Dependency graph

```mermaid
flowchart TD
    S3["s3 (MinIO)"]
    NESSIE["nessie"]
    RP["redpanda"]
    RPCA["redpanda-connect-archive"]
    CPC["redpanda-console"]
    SM["spark-master"]
    SW["spark-worker"]
    SC["spark-connect"]
    III["iceberg-ingest"]
    DASH["dashboard"]
    DOC["documentation"]
    MI["minio-init"]

    NESSIE --> S3
    MI --> S3
    RPCA --> RP
    RPCA --> MI
    CPC --> RP
    SM --> S3
    SM --> NESSIE
    SW --> SM
    SC --> SM
    III --> SM
    III --> NESSIE
    DASH --> S3
    DASH --> NESSIE
    DASH --> SC
```

---

## Data flow: session archival → lakehouse → dashboard

```mermaid
sequenceDiagram
    participant A as opencode agent
    participant P as session-archive plugin<br/>.opencode/plugins/session-archive.js
    participant L as lib/session-archive.mjs<br/>(kafkajs)
    participant R as Redpanda (topic sessions)
    participant A2 as redpanda-connect-archive<br/>(s3-archive.yaml)
    participant M as MinIO (bucket sessions)
    participant III as iceberg-ingest<br/>(Spark streaming)
    participant N as Nessie catalog
    participant W as MinIO (bucket warehouse)
    participant D as dashboard/app.py
    participant S as Spark (Connect)

    A->>P: session.idle
    P->>L: normalize transcript + decision tree (13 cols)
    L->>R: send() key=session_id, value=JSON row × n
    R->>A2: consume group s3-archive
    A2->>M: parquet_encode + PUT s3://sessions/YYYY/MM/DD/<id>-<uuid>.parquet
    III-->>M: read Parquet snapshots (s3a://sessions/)
    III->>N: append rows (REST catalog)
    N->>W: write data files (s3://warehouse/)
    S->>N: SELECT * FROM iceberg.analytics.sessions
    D->>S: Spark Connect (sc://spark-connect:15002)
    D-->>D: KPIs, charts, decision tree, SQL console
```

---

## Storage layout (MinIO buckets)

```mermaid
flowchart LR
    subgraph MinIO["MinIO — s3-data volume"]
        subgraph B1["bucket: data"]
            D1["default uploads<br/>(post.sh default bucket)"]
        end
        subgraph B2["bucket: sessions"]
            S1["YYYY/MM/DD/&lt;session-id&gt;-&lt;uuid&gt;.parquet<br/>(Redpanda Connect parquet snapshots)"]
        end
        subgraph B3["bucket: warehouse"]
            W1["Iceberg table data files<br/>analytics.sessions"]
        end
    end

    POST["post.sh"] --> D1
    RPC["redpanda-connect-archive"] --> S1
    S1 -->|"Spark streaming<br/>iceberg-ingest"| W1
    NESSIE["Nessie (catalog metadata)"] -.->|tracks| W1
```

Rules: session archives always go to the `sessions` bucket with date-partitioned keys; never to `data`. The `warehouse` bucket is owned by the Iceberg/Nessie layer. The live Iceberg table is written by the `iceberg-ingest` Spark streaming job, not by a batch script.

---

## Query engines

```mermaid
flowchart TD
    subgraph DASH["dashboard container"]
        APP["app.py (Streamlit)"]
    end

    subgraph Engines["Query engines"]
        SPK["Apache Spark 3.5.4<br/>(via Spark Connect gRPC) — primary"]
        PY["PyIceberg<br/>(table.scan()) — fallback"]
        S3FS["s3fs / pandas<br/>(raw parquet fallback)"]
    end

    APP --> SPK
    APP --> PY
    APP --> S3FS

    SPK --> SM["spark-master → spark-worker(s)"]
    SM --> NESSIE["Nessie REST catalog"]
    SM --> WMINIO["MinIO (warehouse)"]
```

- **Spark Connect** — primary engine for the dashboard, reads `iceberg.analytics.sessions` (catalog `iceberg` = Nessie REST). Needs `./spark/start.sh`.
- **PyIceberg** — fallback once the table exists and Spark is down.
- **s3fs** — last-resort raw-Parquet reader when no Iceberg table exists yet.

---

## Repository structure

```mermaid
flowchart TD
    ROOT["m2_bigdata/"]
    DC["docker-compose.yml<br/>stack definition"]
    RPCF["redpanda-connect/<br/>s3-archive.yaml"]
    P1["post.sh · get.sh<br/>S3 upload/download helpers"]
    DOCS["docs/<br/>docsify site"]
    DASHF["dashboard/<br/>app.py<br/>Dockerfile · requirements.txt"]
    SPARKF["spark/<br/>Dockerfile · conf/ · jobs/<br/>start/stop/status/spark-sql.sh"]
    OPP[".opencode/<br/>plugins/session-archive.js<br/>lib/session-archive.mjs"]
    ARC["archives/<br/>local parquet fallback (publish failure)"]
    AG["AGENTS.md<br/>agent conventions"]

    ROOT --> DC
    ROOT --> RPCF
    ROOT --> P1
    ROOT --> DOCS
    ROOT --> DASHF
    ROOT --> SPARKF
    ROOT --> OPP
    ROOT --> ARC
    ROOT --> AG
```

---

## Technology stack summary

| Layer | Technology |
|---|---|
| Object storage | MinIO (S3-compatible, SigV4, path-style, `us-east-1`) |
| Stream broker | Redpanda 26.2 (Kafka API-compatible) |
| Connect / streaming ETL | Redpanda Connect 4.x (s3 parquet archive sink) |
| Table format | Apache Iceberg 1.11.0 |
| Catalog | Project Nessie (REST, RocksDB) |
| Batch/SQL compute | Apache Spark 3.5.4 (standalone, Java 17) |
| Iceberg ingestion | Spark structured streaming (`iceberg-ingest`: Parquet → Iceberg) |
| Remote compute API | Spark Connect (gRPC :15002) |
| Dashboard | Streamlit + Plotly + pandas |
| Archival pipeline | opencode plugin → kafkajs → Redpanda → Redpanda Connect → Spark |
| Docs | Docsify (`docs/index.html`) |

---

## Exposed endpoints

| URL | Service |
|---|---|
| `http://localhost:4000` | Streamlit dashboard |
| `http://localhost:2000` | Documentation site (docsify) |
| `http://localhost:9000` | MinIO S3 API |
| `http://localhost:9001` | MinIO console (`root` / `root-134`) |
| `http://localhost:10080` | Redpanda Console |
| `http://localhost:19120` | Nessie REST catalog |
| `http://localhost:9092` | Redpanda Kafka API (internal) |
| `http://localhost:9093` | Redpanda Kafka API (external-advertised) |
| `http://localhost:8081` | Redpanda Schema Registry |
| `http://localhost:9644` | Redpanda Admin API |
| `http://localhost:8080` | Spark master UI |
| `spark://localhost:7077` | Spark master |
| `sc://localhost:15002` | Spark Connect |
