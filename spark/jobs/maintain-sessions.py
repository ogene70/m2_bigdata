"""Maintenance de la table Iceberg analytics.sessions.

Execute par le service `iceberg-maintenance` (boucle toutes les 30 minutes) :
compaction des petits fichiers via `rewrite_data_files`.

Lance en mode local (--master local[1]) : la maintenance ne prend pas les
coeurs du cluster, deja utilises par l'ingestion et Spark Connect.

Pourquoi pas `expire_snapshots` / `remove_orphan_files` ?
Le catalogue Nessie ecrit un nouveau fichier de metadonnees a chaque commit et
n'expose que le snapshot courant. `expire_snapshots` n'a donc rien a expirer,
et `remove_orphan_files` considererait comme orphelins des fichiers encore
references par les commits precedents de Nessie (perte de l'historique). La
purge de l'historique Nessie se fait avec son propre outil de GC, pas ici.
"""
from pyspark.sql import SparkSession

TABLE = "analytics.sessions"

spark = SparkSession.builder.appName("iceberg-maintenance").getOrCreate()


def call(label, sql):
    try:
        spark.sql(sql)
        print(f"[maintenance] {label}: ok", flush=True)
    except Exception as err:
        print(f"[maintenance] {label}: {type(err).__name__}: {err}", flush=True)


# Compaction : fusionne les groupes de >= 5 petits fichiers, cible 128 Mo.
call(
    "rewrite_data_files",
    f"""
    CALL iceberg.system.rewrite_data_files(
        table => '{TABLE}',
        options => map(
            'min-input-files', '5',
            'target-file-size-bytes', '134217728'
        )
    )
    """,
)

print("[maintenance] termine", flush=True)
spark.stop()
