import os
import sys

import duckdb
from pyiceberg.catalog import load_catalog

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "s3:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "root")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "root-134")
NESSIE_URI = os.getenv("NESSIE_URI", "http://nessie:19120/iceberg/")
NESSIE_WAREHOUSE = os.getenv("NESSIE_WAREHOUSE", "warehouse")
TABLE_ID = "analytics.sessions"

DEFAULT_SQL = "SELECT * FROM sessions LIMIT 5"


def main():
    sql = " ".join(sys.argv[1:]) if len(sys.argv) > 1 else DEFAULT_SQL

    cat = load_catalog(
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
    table = cat.load_table(TABLE_ID)
    location = table.location()
    print(f"-- table : {TABLE_ID}\n-- location : {location}\n")

    con = duckdb.connect()
    con.execute("SET unsafe_enable_version_guessing = true;")
    con.execute(
        "CREATE SECRET minio ("
        f"TYPE S3, KEY_ID '{MINIO_ACCESS_KEY}', SECRET '{MINIO_SECRET_KEY}', "
        f"REGION 'us-east-1', ENDPOINT '{MINIO_ENDPOINT}', "
        f"URL_STYLE 'path', USE_SSL false)"
    )
    con.execute(
        f"CREATE OR REPLACE TEMP VIEW sessions AS SELECT * FROM iceberg_scan('{location}')"
    )
    df = con.execute(sql).fetchdf()
    print(df.to_string(index=False))


if __name__ == "__main__":
    main()