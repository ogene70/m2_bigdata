from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    TimestampType,
)

spark = SparkSession.builder.appName("iceberg-ingest").getOrCreate()

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
    tool_input string
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
    ]
)

stream = (
    spark.readStream.format("parquet")
    .schema(schema)
    .option("pathGlobFilter", "*.parquet")
    .option("maxFilesPerTrigger", 50)
    .option("maxFileAgeMs", 1577847600000)
    .load("s3a://sessions/*/*/*/")
)

query = (
    stream.writeStream.format("iceberg")
    .outputMode("append")
    .option("checkpointLocation", "s3a://warehouse/_checkpoints/iceberg-ingest")
    .trigger(processingTime="10 seconds")
    .start("analytics.sessions")
)

query.awaitTermination()