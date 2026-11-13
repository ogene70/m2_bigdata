from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    TimestampType,
)
from pyspark.sql.window import Window

spark = (
    SparkSession.builder.appName("iceberg-ingest")
    .config("spark.cores.max", "2")
    .config("spark.executor.cores", "1")
    .config("spark.executor.instances", "1")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .getOrCreate()
)

spark.conf.set("spark.sql.streaming.fileSource.list.as-recursive", "true")
spark.conf.set("spark.sql.streaming.fileSource.maxFileAgeMs", str(1577847600000))

spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.analytics")

spark.sql(
    """
CREATE TABLE IF NOT EXISTS iceberg.analytics.sessions (
    session_id string,
    timestamp timestamp,
    role string,
    content string,
    repository string,
    model string,
    message_index int,
    decision_node_id int,
    decision_parent_id int,
    decision_node_type string,
    decision_status string,
    tool_name string,
    tool_input string,
    tokens_in int,
    tokens_out int,
    harness string,
    topic string
) USING iceberg
"""
)

# Tables creees avant l'introduction de la colonne `topic` : on l'ajoute.
try:
    spark.sql("ALTER TABLE iceberg.analytics.sessions ADD COLUMNS (topic string)")
except Exception:
    pass

# Partitionnement par jour : un MERGE ne touche alors que les fichiers des jours
# concernes au lieu de scanner toute la table. Sans effet sur les donnees deja
# ecrites (evolution du spec de partitionnement).
try:
    spark.sql("ALTER TABLE iceberg.analytics.sessions ADD PARTITION FIELD days(`timestamp`)")
except Exception:
    pass

schema = StructType(
    [
        StructField("session_id", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("role", StringType(), True),
        StructField("content", StringType(), True),
        StructField("repository", StringType(), True),
        StructField("model", StringType(), True),
        StructField("message_index", IntegerType(), True),
        StructField("decision_node_id", IntegerType(), True),
        StructField("decision_parent_id", IntegerType(), True),
        StructField("decision_node_type", StringType(), True),
        StructField("decision_status", StringType(), True),
        StructField("tool_name", StringType(), True),
        StructField("tool_input", StringType(), True),
        StructField("tokens_in", IntegerType(), True),
        StructField("tokens_out", IntegerType(), True),
        StructField("harness", StringType(), True),
        StructField("topic", StringType(), True),
    ]
)

stream = (
    spark.readStream.format("parquet")
    .schema(schema)
    .option("pathGlobFilter", "*.parquet")
    .option("maxFilesPerTrigger", 100)
    .option("maxFileAgeMs", 1577847600000)
    .load("s3a://sessions/*/*/*/")
)


def upsert_to_iceberg(batch_df, batch_id):
    # Une seule fenetre suffit : l'ordre par timestamp desc garantit le "dernier
    # ecrit gagne" (deux republications d'un meme noeud -> la plus recente gagne).
    deduped = (
        batch_df.withColumn(
            "_rn",
            F.row_number().over(
                Window.partitionBy("session_id", "decision_node_id")
                .orderBy(F.col("timestamp").desc(), F.col("message_index").desc())
            ),
        )
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )
    sess = batch_df.sparkSession
    sess.conf.set("spark.sql.shuffle.partitions", "4")
    sess.conf.set("spark.sql.adaptive.enabled", "true")
    deduped.createOrReplaceTempView("sessions_stage")
    sess.sql(
        """
        MERGE INTO iceberg.analytics.sessions AS t
        USING sessions_stage AS s
        ON  t.session_id = s.session_id
        AND t.decision_node_id = s.decision_node_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )


query = (
    stream.writeStream.foreachBatch(upsert_to_iceberg)
    .option("checkpointLocation", "s3a://warehouse/_checkpoints/iceberg-ingest")
    .trigger(processingTime="10 seconds")
    .start()
)

query.awaitTermination()