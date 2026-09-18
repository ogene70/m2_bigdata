import io
import os
from datetime import datetime

import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
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


def run_sql(sql, df, session_id=None):
    con = get_duckdb()
    loc = iceberg_location()
    if loc:
        base = f"SELECT * FROM iceberg_scan('{loc}')"
    else:
        con.register("df", df)
        base = "SELECT * FROM df"
    where = f" WHERE session_id = '{session_id}'" if session_id else ""
    con.execute(f"CREATE OR REPLACE TEMP VIEW sessions AS {base}{where}")
    con.execute(
        "CREATE OR REPLACE TEMP VIEW messages AS "
        "SELECT * FROM sessions WHERE decision_node_type = 'message' OR decision_node_type IS NULL"
    )
    return con.execute(sql).fetchdf()


def _tree_layout(df):
    ids = df["decision_node_id"].tolist()
    parents = df["decision_parent_id"].tolist()
    idx_of = {rid: i for i, rid in enumerate(ids)}

    children = {i: [] for i in range(len(ids))}
    roots = []
    for i, p in enumerate(parents):
        pi = idx_of.get(p)
        if p is None or pd.isna(p) or p == 0 or pi is None:
            roots.append(i)
        else:
            children[pi].append(i)

    depth = [0] * len(ids)
    def set_depth(i, d):
        depth[i] = d
        for c in children[i]:
            set_depth(c, d + 1)
    for r in roots:
        set_depth(r, 0)

    pos = [0.0] * len(ids)
    counter = {"n": 0}
    def assign(i):
        if not children[i]:
            pos[i] = counter["n"]
            counter["n"] += 1
            return pos[i]
        vals = [assign(c) for c in children[i]]
        pos[i] = sum(vals) / len(vals)
        return pos[i]
    for r in roots:
        assign(r)

    return depth, pos


def _tree_label(row):
    ntype = row.get("decision_node_type") or "message"
    content = str(row.get("content") or "").replace("\n", " ").strip()
    if ntype == "tool":
        label = f"{row.get('tool_name') or '(outil)'} · {row.get('decision_status') or ''}".strip()
        if content:
            label += f" — {content[:60]}"
        return label
    if ntype == "reasoning":
        return content[:80] if content else "Réflexion"
    if ntype == "step":
        return f"Décision · {row.get('decision_status') or ''}".strip()
    author = "Utilisateur" if str(row.get("role")) == "user" else "Assistant"
    return f"{author} : {content[:80]}"


def decision_tree_figure(df, session_id):
    tree = df[df["session_id"] == session_id].sort_values(
        ["message_index", "decision_node_id"]
    )
    if tree.empty:
        return None

    depth, pos = _tree_layout(tree)

    colors = {
        "message": "#64748b",
        "step": "#f59e0b",
        "reasoning": "#38bdf8",
        "tool": "#10b981",
        "autre": "#94a3b8",
    }
    symbols = {
        "message": "circle",
        "step": "diamond",
        "reasoning": "circle",
        "tool": "square",
        "autre": "circle",
    }
    values = {v: [] for v in colors.values()}
    labels = {}
    hover = {}
    for i, (_, row) in enumerate(tree.iterrows()):
        ntype = row.get("decision_node_type") or "message"
        t = colors.get(ntype, "#94a3b8")
        values[t].append(i)
        labels[i] = _tree_label(row)
        detail = str(row.get("tool_input") or "") if ntype == "tool" else str(row.get("content") or "")
        if len(detail) > 400:
            detail = detail[:400] + "…"
        hover[i] = f"{labels[i]}<br>{detail}" if detail else labels[i]

    fig = go.Figure()
    xs, ys = [], []
    for i, (_, row) in enumerate(tree.iterrows()):
        p = row.get("decision_parent_id")
        if p is None or pd.isna(p) or p == 0:
            continue
        pi = tree.index[tree["decision_node_id"] == p]
        if pi.empty:
            continue
        pi = pi[0]
        j = tree.index.get_loc(pi)
        xs += [depth[j], depth[i], depth[i], None]
        ys += [pos[j], pos[j], pos[i], None]
    fig.add_trace(
        go.Scatter(
            x=xs,
            y=ys,
            mode="lines",
            line=dict(color="#94a3b8", width=1.2),
            hoverinfo="skip",
            showlegend=False,
        )
    )

    for color, node_idx in values.items():
        htext = []
        for i in node_idx:
            htext.append(hover[i])
        fig.add_trace(
            go.Scatter(
                x=[depth[i] for i in node_idx],
                y=[pos[i] for i in node_idx],
                mode="markers+text",
                text=[labels[i][:40] for i in node_idx],
                textposition="middle right",
                textfont=dict(size=10, color="#334155"),
                marker=dict(
                    size=14,
                    color=color,
                    line=dict(width=1, color="#0f172a"),
                    symbol=symbols.get(color2type.get(color), "circle"),
                ),
                customdata=list(node_idx),
                hovertemplate="%{hovertext}<extra></extra>",
                hovertext=htext,
                name=color2name.get(color, color),
            )
        )

    n_rows = int(max(pos)) + 1 if pos else 1
    height = min(1600, max(480, 22 * n_rows))
    fig.update_layout(
        height=height,
        margin=dict(l=30, r=30, t=40, b=10),
        title=f"🌳 Arbre décisionnel — {session_id}",
        xaxis=dict(showgrid=False, zeroline=False, visible=False),
        yaxis=dict(showgrid=False, zeroline=False, visible=False, autorange="reversed"),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0),
        hoverlabel=dict(font_size=11),
    )
    return fig


def _node_type_color(row):
    ntype = row.get("decision_node_type") or "message"
    return colors_of.get(ntype, "#94a3b8")


colors_of = {
    "message": "#64748b",
    "step": "#f59e0b",
    "reasoning": "#38bdf8",
    "tool": "#10b981",
}


def decision_process_swimlane(df, session_id):
    proc = (
        df[df["session_id"] == session_id]
        .sort_values(["message_index", "decision_node_id"])
        .reset_index(drop=True)
    )
    if proc.empty:
        return None

    lane_names = ["🧑 Demande", "🧠 Raisonnement", "⚙️ Décision", "🛠️ Outil", "🤖 Réponse"]

    def lane_of(row):
        ntype = row.get("decision_node_type") or "message"
        role = str(row.get("role") or "")
        if ntype == "message":
            return 0 if role == "user" else 4
        if ntype == "reasoning":
            return 1
        if ntype == "tool":
            return 3
        return 2

    x = list(range(len(proc)))
    y = proc.apply(lane_of, axis=1).astype(float).tolist()

    xs_edge, ys_edge = [], []
    for i in range(len(proc) - 1):
        xs_edge += [x[i], x[i + 1], None]
        ys_edge += [y[i], y[i + 1], None]

    hover = []
    for i, (_, row) in enumerate(proc.iterrows()):
        ntype = row.get("decision_node_type") or "message"
        role = str(row.get("role") or "")
        label = _tree_label(row)
        ts = pd.to_datetime(row["timestamp"]).strftime("%Y-%m-%d %H:%M:%S")
        extra = ""
        if ntype == "tool":
            extra = f" · `{row.get('tool_name') or ''}` ({row.get('decision_status') or ''})"
        elif ntype == "step":
            extra = f" · {row.get('decision_status') or ''}"
        hover.append(f"#{i} · {role} · {ts}<br>{label}{extra}")

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=xs_edge, y=ys_edge, mode="lines",
            line=dict(color="#cbd5e1", width=1.5), hoverinfo="skip", showlegend=False,
        )
    )
    for color in colors_of.values():
        idxs = [i for i in range(len(proc)) if _node_type_color(proc.iloc[i]) == color]
        if not idxs:
            continue
        fig.add_trace(
            go.Scatter(
                x=[x[i] for i in idxs],
                y=[y[i] for i in idxs],
                mode="markers",
                marker=dict(size=11, color=color, line=dict(width=1, color="#0f172a")),
                hovertext=[hover[i] for i in idxs],
                hovertemplate="%{hovertext}<extra></extra>",
                customdata=list(idxs),
                name=color2name.get(color, color),
            )
        )

    fig.update_layout(
        height=320,
        margin=dict(l=10, r=20, t=40, b=30),
        title=f"🧬 Processus de décision — {session_id}",
        xaxis=dict(
            showgrid=True, gridcolor="#f1f5f9",
            zeroline=False, title="",
            tickmode="array",
            tickvals=list(range(0, len(proc), max(1, len(proc) // 12))),
            ticktext=[f"#{i}" for i in range(0, len(proc), max(1, len(proc) // 12))],
            range=[-0.5, len(proc) - 0.5],
        ),
        yaxis=dict(
            showgrid=True, gridcolor="#f1f5f9", zeroline=False,
            tickmode="array",
            tickvals=list(range(len(lane_names))),
            ticktext=lane_names,
        ),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0),
        hoverlabel=dict(font_size=11),
    )
    return fig


def decision_process_sankey(df, session_id):
    ses = df[df["session_id"] == session_id]
    if ses.empty:
        return None

    def is_type(t):
        return ses["decision_node_type"].eq(t)

    users = ses[is_type("message") & ses["role"].eq("user")]
    assts = ses[is_type("message") & ses["role"].eq("assistant")]
    steps = ses[is_type("step")]
    reasonings = ses[is_type("reasoning")]
    tools = ses[is_type("tool")]

    labels = []
    colors = {}
    links = []

    def add_node(label, color):
        if label not in colors:
            labels.append(label)
            colors[label] = color
        return labels.index(label)

    def add_link(s, t, value, color=None):
        if value <= 0:
            return
        links.append((add_node(s, color), add_node(t, color), int(value)))

    N_SESSION, N_USER, N_ASST, N_STEP, N_REASON = (
        "Session", "Demande utilisateur", "Réponse assistant", "Étape de décision", "Raisonnement",
    )
    add_node(N_SESSION, "#0f172a")
    add_node(N_USER, "#94a3b8")
    add_node(N_ASST, "#64748b")
    add_node(N_STEP, "#f59e0b")
    add_node(N_REASON, "#38bdf8")

    add_link(N_SESSION, N_USER, len(users))
    add_link(N_USER, N_ASST, len(assts))
    add_link(N_ASST, N_STEP, len(steps))
    add_link(N_STEP, N_REASON, len(reasonings))

    tool_counts = tools["tool_name"].fillna("(vide)").value_counts()
    top_tools = tool_counts.head(8)
    for name, cnt in top_tools.items():
        add_link(N_STEP, f"Outil : {name}", cnt, "#10b981")
        sub_status = tools[tools["tool_name"].fillna("(vide)") == name]["decision_status"].fillna("(inconnu)").value_counts()
        for st, c in sub_status.items():
            add_link(f"Outil : {name}", f"Outil → {st}", c, "#34d399")
    rest = tool_counts.sum() - top_tools.sum()
    if rest > 0:
        add_link(N_STEP, "Outil : autres", rest, "#10b981")

    for st, cnt in steps["decision_status"].fillna("(fin)").value_counts().items():
        add_link(N_STEP, f"Étape → {st or '(fin)'}", cnt, "#fbbf24")

    node_colors = [colors.get(l, "#94a3b8") for l in labels]
    fig = go.Figure(go.Sankey(
        arrangement="snap",
        node=dict(
            pad=15, thickness=18, label=labels, color=node_colors,
            line=dict(color="#0f172a", width=0.5),
        ),
        link=dict(
            source=[e[0] for e in links],
            target=[e[1] for e in links],
            value=[e[2] for e in links],
            color="rgba(148,163,184,0.35)",
        ),
    ))
    fig.update_layout(
        height=max(420, 90 * max(1, (1 + len(tool_counts)))),
        margin=dict(l=10, r=10, t=40, b=10),
        title=f"🔀 Flux de décision — {session_id}",
        font=dict(size=11),
    )
    return fig


def show_node_details(selection_key, df):
    sel = st.session_state.get(selection_key)
    if sel is None:
        return
    if isinstance(sel, dict):
        points = list(sel.get("points") or [])
    else:
        points = list(getattr(sel, "points", None) or [])
    if not points:
        return
    shown = False
    for p in points:
        idx = p.get("customdata")
        if idx is None or idx == "":
            continue
        try:
            row = df.iloc[int(idx)]
        except Exception:
            continue
        if not shown:
            st.markdown("#### 🔎 Détail(s) sélectionné(s)")
            shown = True
        ntype = str(row.get("decision_node_type") or "message")
        role = str(row.get("role") or "")
        author = "Utilisateur" if role == "user" else ("Assistant" if role == "assistant" else role)
        ts = pd.to_datetime(row["timestamp"]).strftime("%Y-%m-%d %H:%M:%S")
        badge = color2name.get(_node_type_color(row), ntype)
        head = f"**`{ntype}`** · {badge} · {author} · {ts}"
        if ntype == "tool":
            st.markdown(f"**Outil :** `{row.get('tool_name') or '(inconnu)'}` · statut `{row.get('decision_status') or '(—)'}`")
        elif ntype == "step":
            st.markdown(f"**Statut de l'étape :** `{row.get('decision_status') or '(—)'}`")
        st.markdown(f"**{head}**")
        tool_input = str(row.get("tool_input") or "")
        if tool_input:
            st.markdown("**Entrée de l'outil :**")
            st.code(tool_input, language=None)
        content = str(row.get("content") or "")
        if content:
            st.markdown(str(row.get("content") or ""))
        st.caption(
            f"position={int(idx)} · nœud id={row.get('decision_node_id')} · "
            f"parent={row.get('decision_parent_id')}"
        )
        st.divider()


color2type = {
    "#64748b": "message",
    "#f59e0b": "step",
    "#38bdf8": "reasoning",
    "#10b981": "tool",
    "#94a3b8": "autre",
}
color2name = {
    "#64748b": "Message",
    "#f59e0b": "Décision (étape)",
    "#38bdf8": "Raisonnement",
    "#10b981": "Appel d'outil",
    "#94a3b8": "Autre",
}


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

# ---- Normalisation des colonnes "arbre décisionnel" ----
DECISION_COLS = [
    "decision_node_id",
    "decision_parent_id",
    "decision_node_type",
    "decision_status",
    "tool_name",
    "tool_input",
]
for _c in DECISION_COLS:
    if _c not in df.columns:
        df[_c] = None
df["decision_node_type"] = df["decision_node_type"].fillna("message")

msg_mask = df["decision_node_type"].eq("message")
msg_df = df[msg_mask].copy()

# ---- Sidebar filters ----
ALL_SESSIONS = "Tous (toutes les sessions)"
session_ids = [ALL_SESSIONS] + sorted(df["session_id"].unique())
selected_id = st.sidebar.selectbox("Session", session_ids, index=0)
show_all = selected_id == ALL_SESSIONS

min_date = pd.Timestamp(msg_df["timestamp"].min()).date()
max_date = pd.Timestamp(msg_df["timestamp"].max()).date()
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

ts_naive = msg_df["timestamp"].dt.tz_localize(None)
day_start = pd.Timestamp(start_date)
day_end = pd.Timestamp(end_date) + pd.Timedelta(days=1)
mask = (ts_naive >= day_start) & (ts_naive < day_end)
filtered = msg_df[mask]

all_ts_naive = df["timestamp"].dt.tz_localize(None)
tree_mask = (all_ts_naive >= day_start) & (all_ts_naive < day_end)
tree_filtered = df[tree_mask]

repo_filter = st.sidebar.multiselect(
    "Dépôts",
    options=sorted(filtered["repository"].dropna().unique() if filtered["repository"].notna().any() else []),
)
if repo_filter:
    filtered = filtered[filtered["repository"].isin(repo_filter)]
    tree_filtered = tree_filtered[tree_filtered["repository"].isin(repo_filter)]

role_filter = st.sidebar.multiselect(
    "Rôles",
    options=sorted(filtered["role"].unique()),
    default=sorted(filtered["role"].unique()),
)
if role_filter:
    filtered = filtered[filtered["role"].isin(role_filter)]
    tree_filtered = tree_filtered[tree_filtered["role"].isin(role_filter)]

# Applique la session sélectionnée à tout le dashboard (KPI, graphiques,
# table, arbre et requêtes SQL).
if not show_all:
    filtered = filtered[filtered["session_id"] == selected_id]
    tree_filtered = tree_filtered[tree_filtered["session_id"] == selected_id]

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

# ---- Arbre décisionnel de l'agent ----
with st.expander("🌳 Arbre décisionnel de l'agent", expanded=False):
    tree_session_ids = sorted(df["session_id"].unique())
    if show_all:
        tree_session = st.selectbox(
            "Session à explorer",
            options=tree_session_ids,
            index=(len(tree_session_ids) - 1) if tree_session_ids else 0,
        )
    else:
        tree_session = selected_id

    tree_data = tree_filtered[tree_filtered["session_id"] == tree_session]
    if tree_data.empty:
        st.info("Aucun nœud pour ce filtre.")
    elif "decision_node_type" not in df.columns or tree_data["decision_node_type"].notna().all() and (
        tree_data["decision_node_type"] == "message"
    ).all():
        st.caption(
            "Cette session a été archivée avant l'ajout des décisions (étapes, raisonnements, outils). "
            "Seuls les messages sont disponibles — ré-archiver la session pour obtenir l'arbre complet."
        )
        fig = decision_tree_figure(tree_data, tree_session)
        if fig:
            st.plotly_chart(
                fig,
                use_container_width=True,
                key=f"tree_chart_{tree_session}",
                on_select="rerun",
                selection_mode="points",
            )
            show_node_details(f"tree_chart_{tree_session}", tree_data)
    else:
        fig = decision_tree_figure(tree_data, tree_session)
        if fig:
            st.caption(
                "Lecture de **gauche à droite** : chaque **demande utilisateur** ouvre une branche ; chaque "
                "**décision (étape)** déplie ses raisonnements 🧠 et ses **appels d'outils** 🛠️, avec l'**état** "
                "terminal de l'étape (`stop` = réponse donnée, `tool-calls` = nouvelles actions). "
                "Survoler un nœud pour le détail, **cliquer sur un nœud** pour afficher son détail complet."
            )
            st.plotly_chart(
                fig,
                use_container_width=True,
                key=f"tree_chart_{tree_session}",
                on_select="rerun",
                selection_mode="points",
            )
            show_node_details(f"tree_chart_{tree_session}", tree_data)
        else:
            st.info("Aucun nœud pour cette session.")

st.divider()

# ---- Processus de décision de l'agent ----
with st.expander("🧬 Processus de décision de l'agent", expanded=False):
    proc_data = tree_filtered[tree_filtered["session_id"] == tree_session]
    if proc_data.empty:
        st.info("Aucun nœud pour ce filtre.")
    elif "decision_node_type" not in df.columns or proc_data["decision_node_type"].eq("message").all():
        st.caption(
            "Cette session a été archivée avant l'ajout des décisions (étapes, raisonnements, outils). "
            "Seuls les messages sont disponibles — ré-archiver la session pour obtenir le processus complet."
        )
        fig = decision_process_swimlane(proc_data, tree_session)
        if fig:
            st.plotly_chart(
                fig,
                use_container_width=True,
                key=f"process_chart_{tree_session}",
                on_select="rerun",
                selection_mode="points",
            )
            show_node_details(f"process_chart_{tree_session}", proc_data)
    else:
        swim = decision_process_swimlane(proc_data, tree_session)
        if swim:
            st.caption(
                "**Processus chronologique (swimlane)** — lecture de gauche à droite : les activités de l'agent "
                "🧠 raisonner → ⚙️ décider → 🛠️ utiliser un outil → 🤖 répondre, positionnées dans le temps. "
                "**Cliquer sur un point** pour afficher son détail complet."
            )
            st.plotly_chart(
                swim,
                use_container_width=True,
                key=f"process_chart_{tree_session}",
                on_select="rerun",
                selection_mode="points",
            )
            show_node_details(f"process_chart_{tree_session}", proc_data)
        sankey = decision_process_sankey(proc_data, tree_session)
        if sankey:
            st.caption(
                "**Flux de décision (Sankey)** — cheminements agrégés : des demandes utilisateur jusqu'aux "
                "issues des étapes (`stop` / `tool-calls`) et des appels d'outils (`completed` / `error`)."
            )
            st.plotly_chart(sankey, use_container_width=True)

st.divider()

# ---- Session table ----
with st.expander("📋 Sessions", expanded=False):
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

with st.expander("🛠️ Requête SQL (DuckDB sur Iceberg)", expanded=False):
    SQL_EXAMPLES = {
        "Messages par rôle": "SELECT role, count(*) AS n FROM messages GROUP BY role ORDER BY n DESC",
        "Messages par session": "SELECT session_id, count(*) AS messages FROM messages GROUP BY session_id ORDER BY messages DESC",
        "Modèles utilisés": "SELECT model, count(*) AS n, avg(length(content)) AS avg_len FROM messages GROUP BY model ORDER BY n DESC",
        "Activité maximale par jour": "SELECT CAST(timestamp AS DATE) AS jour, count(*) AS messages FROM messages GROUP BY jour ORDER BY jour",
        "Outils utilisés par l'agent": "SELECT tool_name, decision_status, count(*) AS n FROM sessions WHERE decision_node_type = 'tool' GROUP BY tool_name, decision_status ORDER BY n DESC",
        "Perimetre (toutes les données)": "SELECT * FROM sessions LIMIT 10",
        "Gabarit (les 5 plus longues réponses)": "SELECT session_id, role, length(content) AS taille, left(content, 80) AS extrait FROM messages ORDER BY taille DESC LIMIT 5",
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
            result = run_sql(sql_text, df, session_id=(None if show_all else selected_id))
            st.success(f"Requête exécutée — {len(result)} ligne(s)")
            st.dataframe(result, use_container_width=True, hide_index=True)
        except Exception as e:
            st.error(f"Échec de la requête : {e}")
    elif run_clicked and not sql_text.strip():
        st.warning("Requête SQL vide.")

# ---- Selected session chat view / global view ----
st.divider()

if show_all:
    chat_title = f"💬 Toutes les conversations ({len(filtered)} message(s))"
else:
    chat_title = f"💬 Conversation : {selected_id}"

with st.expander(chat_title, expanded=True):
    if show_all:
        chat_messages = filtered.sort_values(["timestamp", "message_index"])
    else:
        chat_messages = msg_df[msg_df["session_id"] == selected_id].sort_values("message_index")

    for _, row in chat_messages.iterrows():
        is_user = row.get("role") == "user"
        author = "🧑‍💻 User" if is_user else "🤖 Assistant"
        model = row.get("model") or ""
        ts = pd.to_datetime(row["timestamp"]).strftime("%Y-%m-%d %H:%M:%S")
        extra = f"session `{row['session_id']}` · " if show_all else ""
        with st.chat_message("user" if is_user else "assistant"):
            st.caption(f"{extra}{author} · {ts}" + (f" · {model}" if model else ""))
            st.markdown(row["content"])

    st.caption("--- Fin de la conversation ---")