import io
import os

import pandas as pd
import pyarrow as pa
import s3fs
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.schema import Schema
from pyiceberg.types import (
    IntegerType,
    NestedField,
    StringType,
    TimestamptzType,
)

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "s3:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "root")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "root-134")
NESSIE_URI = os.getenv("NESSIE_URI", "http://nessie:19120/iceberg/")
NESSIE_WAREHOUSE = os.getenv("NESSIE_WAREHOUSE", "warehouse")
SOURCE_BUCKET = "sessions"
TABLE_ID = "analytics.sessions"

SCHEMA = Schema(
    NestedField(field_id=1, name="session_id", field_type=StringType(), required=False),
    NestedField(field_id=2, name="timestamp", field_type=TimestamptzType(), required=False),
    NestedField(field_id=3, name="role", field_type=StringType(), required=False),
    NestedField(field_id=4, name="content", field_type=StringType(), required=False),
    NestedField(field_id=5, name="repository", field_type=StringType(), required=False),
    NestedField(field_id=6, name="model", field_type=StringType(), required=False),
    NestedField(field_id=7, name="message_index", field_type=IntegerType(), required=False),
    NestedField(field_id=8, name="decision_node_id", field_type=IntegerType(), required=False),
    NestedField(field_id=9, name="decision_parent_id", field_type=IntegerType(), required=False),
    NestedField(field_id=10, name="decision_node_type", field_type=StringType(), required=False),
    NestedField(field_id=11, name="decision_status", field_type=StringType(), required=False),
    NestedField(field_id=12, name="tool_name", field_type=StringType(), required=False),
    NestedField(field_id=13, name="tool_input", field_type=StringType(), required=False),
)

COLUMNS = [
    "session_id",
    "timestamp",
    "role",
    "content",
    "repository",
    "model",
    "message_index",
    "decision_node_id",
    "decision_parent_id",
    "decision_node_type",
    "decision_status",
    "tool_name",
    "tool_input",
]


def sanitize_timestamps(df):
    if "timestamp" not in df.columns or df["timestamp"].isna().all():
        return df
    ts = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
    try:
        secs = ts.astype("int64") / 1e6
    except Exception:
        return df
    valid = (secs >= 946684800) & (secs <= 4102444800)
    if not valid.all():
        df = df[valid].copy()
    df["timestamp"] = pd.to_datetime(secs, unit="s", utc=True)
    return df


def load_source():
    fs = s3fs.S3FileSystem(
        key=MINIO_ACCESS_KEY,
        secret=MINIO_SECRET_KEY,
        client_kwargs={"endpoint_url": f"http://{MINIO_ENDPOINT}"},
        config_kwargs={"signature_version": "s3v4"},
    )
    paths = sorted(p for p in fs.find(f"{SOURCE_BUCKET}/") if p.endswith(".parquet"))
    if not paths:
        print(f"No parquet files under {SOURCE_BUCKET}/")
        return pd.DataFrame(columns=COLUMNS)
    frames = []
    for path in paths:
        with fs.open(path, "rb") as f:
            df = sanitize_timestamps(pd.read_parquet(io.BytesIO(f.read())))
        for col in COLUMNS:
            if col not in df.columns:
                df[col] = None
        df = df[COLUMNS]
        frames.append(df)
        print(f"  read {path} -> {len(df)} rows")
    return pd.concat(frames, ignore_index=True)


def catalog():
    return load_catalog(
        "nessie",
        **{
            "type": "rest",
            "uri": NESSIE_URI,
            "warehouse": NESSIE_WAREHOUSE,
            "s3.endpoint": f"http://{MINIO_ENDPOINT}",
            "s3.access-key-id": MINIO_ACCESS_KEY,
            "s3.secret-access-key": MINIO_SECRET_KEY,
            "s3.region": "us-east-1",
            "s3.path-style-access": "true",
        },
    )


def main():
    print(f"Ingesting {SOURCE_BUCKET}/*.parquet -> Iceberg table {TABLE_ID}")
    df = load_source()
    if df.empty:
        print("Nothing to ingest.")
        return

    cat = catalog()
    try:
        cat.create_namespace(TABLE_ID.split(".")[0])
    except Exception:
        pass

    try:
        cat.drop_table(TABLE_ID)
        print(f"Dropped existing table {TABLE_ID}")
    except NoSuchTableError:
        pass
    except Exception as e:
        print(f"Table does not exist or cannot be dropped: {e}")

    table = cat.create_table(TABLE_ID, schema=SCHEMA)
    print(f"Created table {TABLE_ID}")

    for col in df.select_dtypes(include=["datetime64", "datetimetz"]).columns:
        df[col] = pd.to_datetime(df[col], utc=True).astype("datetime64[us, UTC]")
    for col in ["message_index", "decision_node_id", "decision_parent_id"]:
        if col in df.columns:
            df[col] = df[col].fillna(0).astype("int32")

    arrow = pa.Table.from_pandas(df, preserve_index=False)
    table.append(arrow)
    print(f"Appended {df.shape[0]} rows")

    print(f"Table location: {table.location()}")


if __name__ == "__main__":
    main()