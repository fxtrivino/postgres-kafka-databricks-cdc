# Databricks notebook source
# Databricks notebook source

from pyspark.sql import functions as F
from delta.tables import DeltaTable

VOLUME_PATH = "/Volumes/workspace/bronze/cdc_files"
BRONZE_TABLE = "workspace.bronze.customers_cdc_events"

jsonl_files = [
    file for file in dbutils.fs.ls(VOLUME_PATH)
    if file.name.endswith(".jsonl")
]

print(f"JSONL files found: {len(jsonl_files)}")

if not jsonl_files:
    dbutils.notebook.exit("NO_INPUT_FILES")

df_raw = (
    spark.read
    .option("multiLine", "false")
    .json(f"{VOLUME_PATH}/*.jsonl")
)

# COMMAND ----------

df_bronze = (
    df_raw
    .select(
        F.col("topic").cast("string").alias("topic"),
        F.col("partition").cast("int").alias("partition"),
        F.col("offset").cast("long").alias("offset"),
        F.col("kafka_timestamp_ms").cast("long").alias("kafka_timestamp_ms"),
        F.col("is_tombstone").cast("boolean").alias("is_tombstone"),
        F.to_json(F.col("key")).alias("event_key"),
        F.to_json(F.col("value")).alias("event_value"),
        F.col("value.payload.op").alias("operation"),
        F.current_timestamp().alias("ingested_at")
    )
)

# COMMAND ----------

KEY_COLUMNS = ["topic", "partition", "offset"]

# Validar claves técnicas obligatorias
invalid_keys = df_bronze.filter(
    F.col("topic").isNull()
    | F.col("partition").isNull()
    | F.col("offset").isNull()
)

if invalid_keys.limit(1).count() > 0:
    raise RuntimeError("Bronze contains events with null Kafka keys")

# Detectar eventos contradictorios para un mismo offset.
# Usamos una representación JSON de los campos técnicos y del payload.
df_fingerprints = df_bronze.withColumn(
    "_fingerprint",
    F.to_json(
        F.struct(
            "kafka_timestamp_ms",
            "is_tombstone",
            "event_key",
            "event_value",
            "operation"
        ),
        options={"ignoreNullFields": "false"}
    )
)

conflicts = (
    df_fingerprints
    .groupBy(*KEY_COLUMNS)
    .agg(F.countDistinct("_fingerprint").alias("distinct_versions"))
    .filter(F.col("distinct_versions") > 1)
)

if conflicts.limit(1).count() > 0:
    raise RuntimeError(
        "Conflicting CDC events for the same Kafka offset"
    )

df_source = df_bronze.dropDuplicates(KEY_COLUMNS)

print(f"Source events after deduplication: {df_source.count()}")

# COMMAND ----------

bronze_table = DeltaTable.forName(
    spark,
    BRONZE_TABLE
)

(
    bronze_table.alias("target")
    .merge(
        df_source.alias("source"),
        """
        target.topic = source.topic
        AND target.partition = source.partition
        AND target.offset = source.offset
        """
    )
    .whenNotMatchedInsertAll()
    .execute()
)

print("Bronze CDC MERGE completed")