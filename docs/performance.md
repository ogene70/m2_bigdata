# Performance de la chaîne d'archivage

Cette page documente l'optimisation de la chaîne
`plugin → Redpanda → Redpanda Connect → Parquet → Spark → Iceberg → dashboard` :
le problème constaté, l'état initial mesuré, les changements appliqués et leur
effet réel.

Mesures effectuées le **23/10/2026** sur le stack local, avant et après
redéploiement. Toutes les valeurs sont issues de l'instance en cours, pas
d'estimations.

---

## 1. Le problème

| # | Symptôme | Où | Conséquence |
|---|---|---|---|
| 1 | Un objet Parquet toutes les **300 ms** | bucket `sessions` | 528 objets pour 2,9 Mo ; médiane de **5,5 Ko** par fichier |
| 2 | Micro-batch Spark toutes les **1 s** | `iceberg-ingest` | un `MERGE` (réécriture de fichiers) dès qu'une ligne arrive |
| 3 | Table **non partitionnée** | `analytics.sessions` | chaque `MERGE` et chaque requête parcourt toute la table |
| 4 | Aucune compaction | warehouse Iceberg | 52 fichiers de données (7,6 Ko de moyenne) pour 679 lignes, jamais fusionnés |
| 5 | Dashboard qui relit tout | Streamlit (`:4000`) | listing complet du bucket + `SELECT *` toutes les 2 s |

Le facteur commun : **beaucoup de petits objets et beaucoup de commits** pour
peu de données utiles. Le coût fixe (ouvrir un fichier S3, planifier une
requête, committer un snapshot) domine le coût variable (les lignes).

---

## 2. État initial du système (avant)

### 2.1 Paramètres

| Composant | Paramètre | Valeur initiale |
|---|---|---|
| Redpanda Connect | `batching.period` | `300ms` |
| Redpanda Connect | `batching.count` | `0` (illimité) |
| Redpanda Connect | `parquet_encode.default_compression` | `snappy` |
| Spark `iceberg-ingest` | `trigger(processingTime=…)` | `1 second` |
| Spark `iceberg-ingest` | `maxFilesPerTrigger` | `50` |
| Table `analytics.sessions` | partitionnement | aucun (`spec: []`) |
| Maintenance Iceberg | fréquence | aucune (manuelle) |
| Dashboard Streamlit | `DASHBOARD_REFRESH_SECONDS` | `2` |

### 2.2 Volumétrie mesurée (08:00 UTC)

| Indicateur | Valeur |
|---|---|
| Objets Parquet dans `s3://sessions/` | **528** |
| Taille totale | **2,9 Mo** (3 003 134 octets) |
| Taille moyenne / médiane | **5 688 o** / **5 493 o** |
| Plus gros objet | 14 489 o |
| Lignes dans `analytics.sessions` | **679** |
| Fichiers de données Iceberg | **52** |
| Taille moyenne d'un fichier Iceberg | **7 597 o** |
| Partitionnement de la table | **aucun** |
| Sessions archivées | 7 |

Autrement dit : ~5,7 Ko par objet côté MinIO et ~7,6 Ko par fichier côté
Iceberg, pour une table de quelques centaines de lignes.

---

## 3. Solutions apportées

| # | Problème | Changement | Fichier | Avant → Après |
|---|---|---|---|---|
| 1 | Petits Parquet | Batching temporel + plafond de lignes + `zstd` | `redpanda-connect/s3-archive.yaml` | `300ms` / `count: 0` / `snappy` → **`15s` / `count: 2000` / `zstd`** |
| 2 | Commits trop fréquents | Trigger du streaming | `spark/jobs/ingest-sessions.py` | `1 s` → **`10 s`** |
| 3 | Rattrapage lent | Fichiers par micro-batch | `spark/jobs/ingest-sessions.py` | `50` → **`100`** |
| 4 | Table non partitionnée | Évolution du spec de partition | `spark/jobs/ingest-sessions.py` | `spec: []` → **`days(timestamp)`** |
| 5 | Petits fichiers Iceberg | Compaction toutes les 30 min | `spark/jobs/maintain-sessions.py` + `docker-compose.yml` | aucun → **`rewrite_data_files`** |
| 6 | Dashboard trop bavard | Période de rafraîchissement | `docker-compose.yml` | `2 s` → **`5 s`** |

Détail des changements non triviaux :

- **Compression `zstd`.** Validée par `redpanda-connect lint` (un
  `default_compression: bogus` est bien rejeté par le linter) et lue nativement
  par Spark 3.5.
- **Partitionnement par jour.** `ALTER TABLE … ADD PARTITION FIELD
  days(timestamp)` est une **évolution de spec** : les fichiers déjà écrits
  restent non partitionnés, les nouveaux sont rangés par jour. Aucune
  réécriture de l'existant, aucune perte de données.
- **Maintenance en `local[1]`.** Elle tourne dans son propre conteneur, en mode
  local : elle ne prend pas les cœurs du worker, déjà réservés par
  `iceberg-ingest` (jusqu'à 2) et `spark-connect` (2) sur un worker de 4.

### Pourquoi la maintenance ne fait *que* compacter

`expire_snapshots` et `remove_orphan_files` ont été testés puis écartés, pour
une raison propre au catalogue Nessie : **il écrit un nouveau fichier de
métadonnées à chaque commit et n'expose que le snapshot courant**. Constaté sur
l'instance :

| Observation | Valeur (08:37 UTC) |
|---|---|
| Fichiers `*.metadata.json` dans le répertoire de la table | **549** (un par commit) |
| Snapshots visibles dans le fichier de métadonnées courant | **1** |
| `last-sequence-number` du même fichier | **543** |

Ces compteurs augmentent à chaque commit : ce sont des ordres de grandeur, pas des
constantes.

Conséquence : `expire_snapshots` n'a rien à expirer (il ne voit qu'un
snapshot), et `remove_orphan_files` considérerait comme orphelins des fichiers
encore référencés par les commits précédents de Nessie — donc destruction de
l'historique. La purge de l'historique Nessie relève de son propre outil de GC,
pas de la maintenance Iceberg. Seule la compaction est à la fois utile et sûre.

---

## 4. Résultats mesurés (après) — mesures à 08:10–08:37 UTC

| Indicateur | Avant | Après |
|---|---|---|
| Fenêtre de batching Connect | 300 ms | **15 s** (plafond 2 000 lignes) |
| Objets Parquet max / heure (théorique, topic à 1 partition) | 12 000 | **240** |
| Cadence observée | irrégulière (par rafale) | **1 objet / 15 s** — écarts mesurés : `15,0 s` |
| Compression Parquet | snappy | **zstd** (vérifié : `COMPRESSION ['ZSTD']`) |
| Cadence des micro-batches Spark | 1 / s | **1 / 10 s** |
| Partitionnement | aucun | **`day(timestamp)`** (spec `1000`) |
| Fichiers de données Iceberg | 52 | **3** juste après compaction — **12** en régime courant (08:37) |
| Taille moyenne d'un fichier Iceberg | 7 597 o | **23 337 o** après compaction — **11 070 o** en régime courant |
| Lignes dans la table | 679 | **679** (aucune perte) |
| Maintenance | manuelle | **compaction toutes les 30 min** |
| Rafraîchissement dashboard | 2 s | **5 s** |

« Régime courant » = mesure à 08:37 après plusieurs cycles d'ingestion : la
compaction ramène périodiquement la table à quelques fichiers, l'ingestion la
fait remonter entre deux passes.

Vérifications exécutées :

```text
[maintenance] rewrite_data_files: ok
[maintenance] termine
```

| Contrôle | Résultat |
|---|---|
| Spec de la table | `[1000: timestamp_day: day(2)]` |
| Lignes avant / après compaction | 679 / 679 |
| Ingestion | fichier Parquet de 08:37:39 présent dans la table (décalage ≤ 1 trigger) |
| Lag du consumer `s3-archive` | 11 messages, soit moins d'une fenêtre de batching |
| Dashboard Streamlit | HTTP 200 sur `:4000`, `DASHBOARD_REFRESH_SECONDS=5` |
| Dashboard temps réel | HTTP 200 sur `:4001` (non modifié) |

### Le compromis assumé

Regrouper 15 s de lignes **ajoute de la latence** : un objet Parquet apparaît
au plus 15 s après publication, puis Iceberg 10 s plus tard.

| Chemin | Latence avant | Latence après |
|---|---|---|
| Redpanda → Parquet (`sessions`) | ≤ 0,3 s | **≤ 15 s** |
| Parquet → Iceberg (`analytics.sessions`) | ≤ 1 s | **≤ 10 s** |
| **Total** | **≤ 1,5 s** | **≤ 25 s** |

Ces valeurs sont des **bornes de configuration** (période de batching + trigger),
pas une mesure bout-en-bout : la latence réelle dépend du moment où la ligne
arrive dans la fenêtre courante.

Ce délai ne concerne **que** la persistance analytique. Le dashboard temps réel
(`:4001`, chemin Arroyo → topic-push → SSE) ne lit pas S3 : il continue d'être
poussé en continu, sans subir cette fenêtre.

### Ce que le changement n'a pas amélioré

La taille moyenne des objets Parquet est restée à ~5,7 Ko (5 747 o mesurés sur
86 objets produits depuis le changement). La raison est mesurable : le trafic
actuel (2 à 12 nouvelles lignes par tranche de 15 s, médiane ~7) est trop faible
pour remplir un fichier. Le gain réel porte sur le **plafond** : quel que soit le
débit, on ne produit plus qu'un objet toutes les 15 s au lieu d'un toutes les
300 ms. C'est la compaction qui traite ensuite la taille des fichiers.

---

## 5. Ce qui n'a pas été changé (et pourquoi)

| Piste | Décision | Raison |
|---|---|---|
| Augmenter les partitions du topic `sessions` | non | le volume actuel tient sur une partition ; à faire si le débit augmente (la clé `session_id` préserve l'ordre par session) |
| Augmenter `smp` / mémoire de Redpanda | non | Redpanda n'est pas le goulot : lag du consumer `s3-archive` mesuré à 11 messages, soit moins d'une fenêtre de batching |
| Plus de workers Spark | non | paralléliser un coût fixe n'apporte rien ; `iceberg-ingest` (jusqu'à 2 cœurs) et `spark-connect` (2 cœurs) occupent déjà le worker de 4 |
| Réécrire les requêtes du dashboard (`SELECT *` → agrégats) | non | refonte de l'UI hors périmètre ; seule la période de rafraîchissement a été ajustée |
| Tronquer `content` / `tool_input` | non | perte d'information dans l'archive ; `zstd` couvre déjà une partie du gain |
| Passer de `MERGE` à un `append` pur | non | les lignes sont republiées (sujet, contenu en cours d'écriture) : le `MERGE` est ce qui garantit l'idempotence |
| `expire_snapshots` + `remove_orphan_files` | écarté | avec Nessie, un seul snapshot est exposé par commit ; l'orphelinage supprimerait des fichiers encore référencés par l'historique (voir §3) |

---

## 6. Où régler quoi

| Réglage | Fichier | Valeur actuelle |
|---|---|---|
| Taille / fréquence des Parquet | `redpanda-connect/s3-archive.yaml` (`batching.period`, `batching.count`) | `15s`, `2000` |
| Compression Parquet | `redpanda-connect/s3-archive.yaml` (`default_compression`) | `zstd` |
| Fréquence d'ingestion | `spark/jobs/ingest-sessions.py` (`trigger`) | `10 seconds` |
| Fichiers par micro-batch | `spark/jobs/ingest-sessions.py` (`maxFilesPerTrigger`) | `100` |
| Fréquence de maintenance | `docker-compose.yml` (`sleep 1800`) | 30 min |
| Cible de compaction | `spark/jobs/maintain-sessions.py` (`target-file-size-bytes`) | 128 Mo |
| Rafraîchissement dashboard | `docker-compose.yml` (`DASHBOARD_REFRESH_SECONDS`) | `5` |

Après modification de `s3-archive.yaml`, valider avant de redémarrer :

```bash
docker compose exec redpanda-connect-archive /redpanda-connect lint /config/s3-archive.yaml
```

---

## 7. Vérifier l'état courant

```bash
# Dernière exécution de la maintenance
docker logs --tail 20 m2-bigdata-iceberg-maintenance | grep maintenance

# Fichiers et snapshots de la table
docker exec m2-bigdata-dashboard python -c "
from pyiceberg.catalog import load_catalog
cat = load_catalog('nessie', type='rest', uri='http://nessie:19120/iceberg/', warehouse='warehouse',
    **{'s3.endpoint':'http://s3:9000','s3.access-key-id':'root','s3.secret-access-key':'root-134',
       's3.region':'us-east-1','s3.path-style-access':'true'})
t = cat.load_table('analytics.sessions')
print('spec:', t.spec())
print('fichiers:', len(list(t.scan().plan_files())))
"

# Volume du bucket
docker exec m2-bigdata-s3 mc du local/sessions
```

Les deux dashboards restent les indicateurs de bout en bout :
<http://localhost:4000> (analytique) et <http://localhost:4001> (temps réel).
