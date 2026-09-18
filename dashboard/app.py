import io
import os
from datetime import datetime

import duckdb
import pandas as pd
import plotly.express as px
import s3fs
import streamlit as st
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NoSuchTableError

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "s3:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "root")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "root-134")
NESSIE_URI = os.getenv("NESSIE_URI", "http://nessie:19120/iceberg/")
NESSIE_WAREHOUSE = os.getenv("NESSIE_WAREHOUSE", "warehouse")
ICEBERG_TABLE = "analytics.sessions"
BUCKET = "sessions"
REGION = "us-east-1"

st.set_page_config(
    page_title="Sessions Dashboard",
    page_icon="📊",
    layout="wide",
)


def get_fs():
    fs = s3fs.S3FileSystem(
        key=MINIO_ACCESS_KEY,
        secret=MINIO_SECRET_KEY,
        client_kwargs={"endpoint_url": f"http://{MINIO_ENDPOINT}"},
        config_kwargs={"signature_version": "s3v4"},
    )
    return fs


@st.cache_resource
def get_s3_filesystem():
    return get_fs()


def list_parquet_paths(fs):
    prefix = f"{BUCKET}/"
    try:
        entries = fs.find(prefix)
        parquets = [p for p in entries if p.endswith(".parquet")]
        return sorted(parquets)
    except Exception:
        return []


@st.cache_data(show_spinner=False, ttl=60)
def load_all_sessions(paths):
    fs = get_s3_filesystem()
    frames = []
    for path in paths:
        try:
            with fs.open(path, "rb") as f:
                data = f.read()
            df = pd.read_parquet(io.BytesIO(data))
            df = sanitize_timestamps(df)
            df["object_key"] = path
            frames.append(df)
        except Exception:
            continue
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


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
    restored = pd.to_datetime(secs, unit="s", utc=True)
    df["timestamp"] = restored
    return df


def iceberg_catalog():
    return load_catalog(
        "nessie",
        **{
            "type": "rest",
            "uri": NESSIE_URI,
            "warehouse": NESSIE_WAREHOUSE,
            "s3.endpoint": f"http://{MINIO_ENDPOINT}",
            "s3.access-key-id": MINIO_ACCESS_KEY,
            "s3.secret-access-key": MINIO_SECRET_KEY,
            "s3.region": REGION,
            "s3.path-style-access": "true",
        },
    )


@st.cache_data(show_spinner=False, ttl=60)
def load_iceberg_sessions():
    try:
        cat = iceberg_catalog()
        table = cat.load_table(ICEBERG_TABLE)
        df = table.scan().to_arrow().to_pandas()
        if "timestamp" in df.columns:
            df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
        return df
    except (NoSuchTableError, FileNotFoundError, RuntimeError, ValueError, OSError):
        return pd.DataFrame()
    except Exception:
        return pd.DataFrame()


@st.cache_resource(show_spinner=False)
def get_duckdb():
    con = duckdb.connect(database=":memory:")
    con.execute("SET unsafe_enable_version_guessing = true;")
    con.execute(
        "CREATE SECRET minio ("
        f"TYPE S3, KEY_ID '{MINIO_ACCESS_KEY}', SECRET '{MINIO_SECRET_KEY}', "
        f"REGION '{REGION}', ENDPOINT '{MINIO_ENDPOINT}', "
        f"URL_STYLE 'path', USE_SSL false)"
    )
    return con


@st.cache_data(show_spinner=False, ttl=60)
def iceberg_location():
    try:
        cat = iceberg_catalog()
        return cat.load_table(ICEBERG_TABLE).location()
    except Exception:
        return None


def run_sql(sql, df):
    con = get_duckdb()
    loc = iceberg_location()
    if loc:
        con.execute(f"CREATE OR REPLACE TEMP VIEW sessions AS SELECT * FROM iceberg_scan('{loc}')")
    else:
        con.register("df", df)
        con.execute("CREATE OR REPLACE TEMP VIEW sessions AS SELECT * FROM df")
    return con.execute(sql).fetchdf()


fs = get_s3_filesystem()
parquet_paths = list_parquet_paths(fs)

df = load_iceberg_sessions()
from_iceberg = not df.empty

st.sidebar.title("📊 Sessions")
if from_iceberg:
    source_label = "Iceberg"
    st.sidebar.caption(f"Table Iceberg : `{ICEBERG_TABLE}`")
else:
    source_label = "MinIO (parquet)"
    st.sidebar.caption(f"Bucket : `{BUCKET}` ({len(parquet_paths)} fichier(s))")

if df.empty and parquet_paths:
    with st.spinner("Chargement des sessions depuis MinIO (parquet)..."):
        df = load_all_sessions(tuple(parquet_paths))

if df.empty:
    st.warning("Aucune donnée de session trouvée.")
    if from_iceberg:
        st.info("Exécutez `./iceberg-ingest.sh` après avoir archivé des sessions.")
    else:
        st.info("Démarrez des conversations et elles seront archivées automatiquement.")
    st.stop()

# ---- Sidebar filters ----
session_ids = sorted(df["session_id"].unique())
selected_id = st.sidebar.selectbox("Session", session_ids)

min_date = pd.Timestamp(df["timestamp"].min()).date()
max_date = pd.Timestamp(df["timestamp"].max()).date()
date_range = st.sidebar.date_input(
    "Filtre de date",
    value=(min_date, max_date),
    min_value=min_date,
    max_value=max_date,
)

if isinstance(date_range, tuple) and len(date_range) == 2:
    start_date, end_date = date_range
else:
    start_date = end_date = date_range

ts_naive = df["timestamp"].dt.tz_localize(None)
day_start = pd.Timestamp(start_date)
day_end = pd.Timestamp(end_date) + pd.Timedelta(days=1)
mask = (ts_naive >= day_start) & (ts_naive < day_end)
filtered = df[mask]

repo_filter = st.sidebar.multiselect(
    "Dépôts",
    options=sorted(filtered["repository"].dropna().unique() if filtered["repository"].notna().any() else []),
)
if repo_filter:
    filtered = filtered[filtered["repository"].isin(repo_filter)]

role_filter = st.sidebar.multiselect(
    "Rôles",
    options=sorted(filtered["role"].unique()),
    default=sorted(filtered["role"].unique()),
)
if role_filter:
    filtered = filtered[filtered["role"].isin(role_filter)]

# ---- Header ----
st.title("📊 Tableau de bord des sessions")

c1, c2, c3, c4 = st.columns(4)
c1.metric("Sessions", filtered["session_id"].nunique())
c2.metric("Messages", len(filtered))
c3.metric("Modèles", filtered["model"].dropna().nunique() if "model" in filtered.columns else 0)
c4.metric("Dépôts", filtered["repository"].dropna().nunique() if "repository" in filtered.columns else 0)

st.divider()

# ---- Charts ----
col_a, col_b = st.columns(2)

with col_a:
    st.subheader("Messages par session")
    msg_per_session = (
        filtered.groupby("session_id")
        .size()
        .reset_index(name="count")
        .sort_values("count", ascending=False)
    )
    if not msg_per_session.empty:
        fig = px.bar(msg_per_session, x="session_id", y="count", labels={"session_id": "", "count": "Messages"})
        fig.update_layout(showlegend=False, xaxis_tickangle=-45, height=400)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Aucune donnée")

with col_b:
    st.subheader("Répartition user / assistant")
    role_counts = filtered["role"].value_counts().reset_index()
    role_counts.columns = ["role", "count"]
    if not role_counts.empty:
        fig = px.pie(role_counts, names="role", values="count", hole=0.4)
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Aucune donnée")

col_c, col_d = st.columns(2)

with col_c:
    st.subheader("Activité temporelle")
    ts = filtered.set_index("timestamp")
    daily = ts.resample("D").size().reset_index(name="count")
    daily.columns = ["date", "count"]
    if not daily.empty:
        fig = px.line(daily, x="date", y="count", markers=True)
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Aucune donnée")

with col_d:
    st.subheader("Modèles utilisés")
    model_cols = filtered["model"].dropna()
    if not model_cols.empty:
        model_counts = model_cols.value_counts().reset_index()
        model_counts.columns = ["model", "count"]
        fig = px.bar(model_counts, x="model", y="count", orientation="h")
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Aucune donnée")

st.divider()

# ---- Session table ----
st.subheader("Sessions")
session_table = (
    filtered.groupby("session_id")
    .agg(
        n_messages=("content", "count"),
        n_user=("role", lambda x: (x == "user").sum()),
        n_assistant=("role", lambda x: (x == "assistant").sum()),
        first_msg=("timestamp", "min"),
        last_msg=("timestamp", "max"),
    )
    .reset_index()
)
session_table["first_msg"] = pd.to_datetime(session_table["first_msg"]).dt.strftime("%Y-%m-%d %H:%M")
session_table["last_msg"] = pd.to_datetime(session_table["last_msg"]).dt.strftime("%Y-%m-%d %H:%M")
session_table.columns = [
    "session_id",
    "messages",
    "user",
    "assistant",
    "premier message",
    "dernier message",
]
st.dataframe(session_table, use_container_width=True, hide_index=True)

# ---- Requête SQL ----
st.divider()
st.subheader("🛠️ Requête SQL (DuckDB sur Iceberg)")

SQL_EXAMPLES = {
    "Messages par rôle": "SELECT role, count(*) AS n FROM sessions GROUP BY role ORDER BY n DESC",
    "Messages par session": "SELECT session_id, count(*) AS messages FROM sessions GROUP BY session_id ORDER BY messages DESC",
    "Modèles utilisés": "SELECT model, count(*) AS n, avg(length(content)) AS avg_len FROM sessions GROUP BY model ORDER BY n DESC",
    "Activité maximale par jour": "SELECT CAST(timestamp AS DATE) AS jour, count(*) AS messages FROM sessions GROUP BY jour ORDER BY jour",
    "Perimetre (toutes les données)": "SELECT * FROM sessions LIMIT 10",
    "Gabarit (les 5 plus longues réponses)": "SELECT session_id, role, length(content) AS taille, left(content, 80) AS extrait FROM sessions ORDER BY taille DESC LIMIT 5",
}

example_name = st.selectbox("Exemple", list(SQL_EXAMPLES.keys()))
sql_text = st.text_area(
    "Écrivez votre requête SQL",
    value=SQL_EXAMPLES[example_name],
    height=150,
    help="La table Iceberg est exposée comme vue `sessions`. Ex : SELECT * FROM sessions WHERE role = 'assistant' LIMIT 10",
)

run_clicked = st.button("▶ Exécuter", type="primary")

if run_clicked and sql_text.strip():
    try:
        result = run_sql(sql_text, df)
        st.success(f"Requête exécutée — {len(result)} ligne(s)")
        st.dataframe(result, use_container_width=True, hide_index=True)
    except Exception as e:
        st.error(f"Échec de la requête : {e}")
elif run_clicked and not sql_text.strip():
    st.warning("Requête SQL vide.")

# ---- Selected session chat view ----
st.divider()
st.subheader(f"💬 Conversation : {selected_id}")

session_messages = df[df["session_id"] == selected_id].sort_values("message_index")

for _, row in session_messages.iterrows():
    is_user = row.get("role") == "user"
    author = "🧑‍💻 User" if is_user else "🤖 Assistant"
    model = row.get("model") or ""
    ts = pd.to_datetime(row["timestamp"]).strftime("%Y-%m-%d %H:%M:%S")
    with st.chat_message("user" if is_user else "assistant"):
        st.caption(f"{author} · {ts}" + (f" · {model}" if model else ""))
        st.markdown(row["content"])

st.caption("--- Fin de la conversation ---")