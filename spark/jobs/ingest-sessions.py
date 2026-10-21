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
    harness string
) USING iceberg
"""
)

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
    ]
)

stream = (
    spark.readStream.format("parquet")
    .schema(schema)
    .option("pathGlobFilter", "*.parquet")
    .option("maxFilesPerTrigger", 50)
    .option("maxFileAgeMs", 1577847600000)
    .load("s3a://sessions/*/*/*/")
    .select(F.col("*"), F.input_file_name().alias("_file"))
)


def _file_mtimes(files):
    jvm = spark._jvm
    hadoop_conf = spark._jsc.hadoopConfiguration()
    path_cls = jvm.org.apache.hadoop.fs.Path
    result = {}
    for file_path in files:
        try:
            path = path_cls(file_path)
            fs = path.getFileSystem(hadoop_conf)
            result[file_path] = int(fs.getFileStatus(path).getModificationTime())
        except Exception:
            result[file_path] = 0
    return result


def upsert_to_iceberg(batch_df, batch_id):
    files = list({row[0] for row in batch_df.select("_file").collect()})
    if not files:
        return
    mtimes_map = _file_mtimes(files)

    def _lookup(path):
        return mtimes_map.get(path, 0)

    lookup_udf = F.udf(_lookup, "long")
    deduped = (
        batch_df.withColumn("_mod_ts", lookup_udf(F.col("_file")))
        .withColumn(
            "_rn",
            F.row_number().over(
                Window.partitionBy("session_id", "decision_node_id")
                .orderBy(F.col("_mod_ts").desc())
            ),
        )
        .filter(F.col("_rn") == 1)
        .drop("_rn", "_mod_ts", "_file")
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
    .trigger(processingTime="1 second")
    .start()
)

query.awaitTermination()