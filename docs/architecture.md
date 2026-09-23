# Project Architecture

Local big-data playground built around a Dockerized MinIO S3 server, an Iceberg lakehouse (Nessie catalog), a standalone Apache Spark cluster, and a Streamlit dashboard. Session archives (Parquet) flow automatically from the opencode agent into MinIO, get ingested into an Iceberg table, and are visualized/queried from the dashboard.

---

## High-level overview

```mermaid
flowchart LR
    subgraph Host["Host machine"]
        OP["opencode agent<br/>(session-archive plugin)"]
        SH["Helper scripts<br/>post.sh · get.sh · iceberg-ingest.sh · iceberg-query.sh"]
        ARC["./archives/*.parquet"]
    end

    subgraph Stack["Docker Compose stack (m2-bigdata)"]
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
        end
        subgraph App["Application layer"]
            DASH["dashboard (Streamlit)<br/>m2-bigdata-dashboard<br/>:4000 → :8501"]
        end
    end

    UI["Browser<br/>:4000"]

    OP -->|"session.idle hook"| ARC
    ARC -->|"post.sh → s3://sessions/"| S3
    SH -->|S3 SigV4 HTTP| S3
    SH -->|"docker compose exec"| DASH

    NESSIE -->|"warehouse data<br/>s3://warehouse/"| S3
    SM --> SW
    SC -->|"gRPC"| SM
    DASH -->|"Iceberg REST catalog"| NESSIE
    DASH -->|"S3FS / DuckDB / PyIceberg"| S3
    DASH -->|"Spark Connect"| SC
    UI --> DASH
```

---

## Docker services (from `docker-compose.yml`)

| Service | Image / Build | Container | Host ports | Role |
|---|---|---|---|---|
| `s3` | `quay.io/minio/minio:latest` | `m2-bigdata-s3` | `9000`, `9001` | Object storage (S3 API + console) |
| `nessie` | `ghcr.io/projectnessie/nessie:latest` | `m2-bigdata-nessie` | `19120` | Iceberg REST catalog (RocksDB store) |
| `dashboard` | `./dashboard` | `m2-bigdata-dashboard` | `4000 → 8501` | Streamlit UI + ingest/query scripts |
| `spark-master` | `./spark` | `m2-bigdata-spark-master` | `8080`, `7077` | Spark standalone master |
| `spark-worker` | `./spark` | (scaled, default 1; `start.sh` uses 3) | — | Spark workers (2 cores / 2 GB) |
| `spark-connect` | `./spark` | `m2-bigdata-spark-connect` | `15002` | Spark Connect gRPC server |

Shared environment anchor (`x-spark-env`): `ICEBERG_VERSION=1.11.0`, `AWS_REGION=us-east-1`.

Volumes: `s3-data` (MinIO objects), `nessie-data` (catalog metadata).

### Dependency graph

```mermaid
flowchart TD
    S3["s3 (MinIO)"]
    NESSIE["nessie"]
    SM["spark-master"]
    SW["spark-worker"]
    SC["spark-connect"]
    DASH["dashboard"]

    NESSIE --> S3
    SM --> S3
    SM --> NESSIE
    SW --> SM
    SC --> SM
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
    participant L as lib/session-archive.mjs<br/>(parquetjs-lite)
    participant F as ./archives/*.parquet
    participant S as post.sh
    participant M as MinIO (bucket sessions)
    participant I as iceberg-ingest.sh<br/>dashboard/ingest.py
    participant N as Nessie catalog
    participant W as MinIO (bucket warehouse)
    participant D as dashboard/app.py

    A->>P: session.idle
    P->>L: normalize transcript + decision tree
    L->>F: write Parquet (session_id, decision_node_*)
    F->>S: ./post.sh archives/… sessions YYYY/MM/DD/…
    S->>M: PUT s3://sessions/YYYY/MM/DD/<id>.parquet
    I->>M: list + read all sessions/*.parquet
    I->>N: create_namespace / drop / create table analytics.sessions
    I->>N: append Arrow table
    N->>W: write Parquet data files (s3://warehouse/)
    D->>N: load_catalog(REST) → table scan
    D->>W: PyIceberg / DuckDB iceberg_scan / s3fs fallback
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
            S1["YYYY/MM/DD/&lt;session-id&gt;.parquet<br/>(session archives)"]
        end
        subgraph B3["bucket: warehouse"]
            W1["Iceberg table data files<br/>analytics.sessions"]
        end
    end

    POST["post.sh"] --> D1
    PLUGIN["session archive"] --> S1
    S1 -->|"iceberg-ingest.sh"| W1
    NESSIE["Nessie (catalog metadata)"] -.->|tracks| W1
```

Rules: session archives always go to the `sessions` bucket with date-partitioned keys (`YYYY/MM/DD/<session-id>.parquet`); never to `data`. The `warehouse` bucket is owned by the Iceberg/Nessie layer.

---

## Query engines

```mermaid
flowchart TD
    subgraph DASH["dashboard container"]
        APP["app.py (Streamlit)"]
        ING["ingest.py"]
        QRY["query.py"]
    end

    subgraph Engines["Query engines"]
        DB["DuckDB<br/>(in-process, iceberg_scan)"]
        PY["PyIceberg<br/>(table.scan())"]
        S3FS["s3fs / pandas<br/>(raw parquet fallback)"]
        SPK["Apache Spark 3.5.4<br/>(via Spark Connect gRPC)"]
    end

    APP --> PY
    APP --> DB
    APP --> S3FS
    APP --> SPK
    ING --> PY
    QRY --> DB

    SPK --> SM["spark-master → spark-worker(s)"]
    SM --> NESSIE["Nessie REST catalog"]
    SM --> WMINIO["MinIO (warehouse)"]
```

- **DuckDB** — ad-hoc SQL over `iceberg_scan('<table location>')` (default engine in the SQL console).
- **PyIceberg** — catalog metadata + Arrow scans (dashboard KPIs, `ingest.py`).
- **Spark Connect** — optional distributed engine (`./spark/start.sh`), catalog `iceberg` = Nessie REST.
- **s3fs** — fallback raw-Parquet reader when no Iceberg table exists yet.

---

## Repository structure

```mermaid
flowchart TD
    ROOT["m2_bigdata/"]
    DC["docker-compose.yml<br/>stack definition"]
    P1["post.sh · get.sh<br/>S3 upload/download helpers"]
    P2["iceberg-ingest.sh<br/>parquet → Iceberg"]
    P3["iceberg-query.sh<br/>CLI SQL via query.py"]
    DOCS["docs/<br/>docsify site"]
    DASHF["dashboard/<br/>app.py · ingest.py · query.py<br/>Dockerfile · requirements.txt"]
    SPARKF["spark/<br/>Dockerfile · conf/<br/>start/stop/status/spark-sql.sh"]
    OPP[".opencode/<br/>plugins/session-archive.js<br/>lib/session-archive.mjs"]
    ARC["archives/<br/>local parquet staging"]
    AG["AGENTS.md<br/>agent conventions"]

    ROOT --> DC
    ROOT --> P1
    ROOT --> P2
    ROOT --> P3
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
| Table format | Apache Iceberg 1.11.0 |
| Catalog | Project Nessie (REST, RocksDB) |
| Batch/SQL compute | Apache Spark 3.5.4 (standalone, Java 17) |
| Remote compute API | Spark Connect (gRPC :15002) |
| In-process SQL | DuckDB |
| Dashboard | Streamlit + Plotly + pandas |
| Archival pipeline | opencode plugin → parquetjs-lite → post.sh |
| Docs | Docsify (`docs/index.html`) |

---

## Exposed endpoints

| URL | Service |
|---|---|
| `http://localhost:4000` | Streamlit dashboard |
| `http://localhost:9000` | MinIO S3 API |
| `http://localhost:9001` | MinIO console (`root` / `root-134`) |
| `http://localhost:19120` | Nessie REST catalog |
| `http://localhost:8080` | Spark master UI |
| `spark://localhost:7077` | Spark master |
| `grpc://localhost:15002` | Spark Connect |
