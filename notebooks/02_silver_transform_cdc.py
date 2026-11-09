# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

BRONZE_TABLE = "workspace.bronze.customers_cdc_events"
SILVER_TABLE = "workspace.silver.customers_history"

bronze = spark.table(BRONZE_TABLE)
silver = spark.table(SILVER_TABLE)

df_pending = (
    bronze.alias("b")
    .filter(F.col("b.operation").isin("c", "u", "d", "r"))
    .join(
        silver.alias("s"),
        (F.col("b.topic") == F.col("s.source_topic")) &
        (F.col("b.partition") == F.col("s.source_partition")) &
        (F.col("b.offset") == F.col("s.source_offset")),
        "left_anti"
    )
)

pending_count = df_pending.count()

print(f"Pending CDC events: {pending_count}")

if pending_count == 0:
    dbutils.notebook.exit("NO_PENDING_EVENTS")

# COMMAND ----------

df_affected_customers = (
    df_pending
    .withColumn(
        "customer_id",
        F.coalesce(
            F.get_json_object(
                "event_value",
                "$.payload.after.customer_id"
            ),
            F.get_json_object(
                "event_value",
                "$.payload.before.customer_id"
            )
        ).cast("long")
    )
    .select("customer_id")
    .distinct()
)

affected_count = df_affected_customers.count()

print(f"Affected customers: {affected_count}")

if df_affected_customers.filter(
    F.col("customer_id").isNull()
).limit(1).count() > 0:
    raise RuntimeError(
        "CDC events contain null customer_id"
    )

# COMMAND ----------

df_customer_events = (
    bronze
    .filter(F.col("operation").isin("c", "u", "d", "r"))
    .withColumn(
        "customer_id",
        F.coalesce(
            F.get_json_object(
                "event_value",
                "$.payload.after.customer_id"
            ),
            F.get_json_object(
                "event_value",
                "$.payload.before.customer_id"
            )
        ).cast("long")
    )
    .join(
        df_affected_customers,
        "customer_id",
        "inner"
    )
)

df_changes = (
    df_customer_events
    .select(
        "customer_id",
        F.get_json_object(
            "event_value", "$.payload.after.name"
        ).alias("name"),
        F.get_json_object(
            "event_value", "$.payload.after.email"
        ).alias("email"),
        F.get_json_object(
            "event_value", "$.payload.after.country"
        ).alias("country"),
        "operation",
        (F.col("operation") == "d").alias("is_deleted"),
        F.get_json_object(
            "event_value", "$.payload.source.ts_ms"
        ).cast("long").alias("source_timestamp_ms"),
        F.col("topic").alias("source_topic"),
        F.col("partition").alias("source_partition"),
        F.col("offset").alias("source_offset")
    )
)

window = (
    Window.partitionBy("customer_id")
    .orderBy("source_partition", "source_offset")
)

df_history = (
    df_changes
    .withColumn(
        "version",
        F.row_number().over(window)
    )
    .withColumn(
        "valid_from",
        F.expr("timestamp_millis(source_timestamp_ms)")
    )
    .withColumn(
        "valid_to",
        F.lead("valid_from").over(window)
    )
    .withColumn(
        "is_current",
        F.col("valid_to").isNull()
    )
    .withColumn(
        "processed_at",
        F.current_timestamp()
    )
    .drop("source_timestamp_ms")
)

# COMMAND ----------

df_silver_merge = df_history.select(
    "customer_id",
    "version",
    "name",
    "email",
    "country",
    "operation",
    "is_deleted",
    "is_current",
    "valid_from",
    "valid_to",
    "source_topic",
    "source_partition",
    "source_offset",
    "processed_at"
)

invalid_history = (
    df_silver_merge
    .groupBy("customer_id")
    .agg(
        F.sum(
            F.when(F.col("is_current"), 1).otherwise(0)
        ).alias("current_versions")
    )
    .filter(F.col("current_versions") != 1)
)

if invalid_history.limit(1).count() > 0:
    raise RuntimeError(
        "Invalid SCD Type 2 history: multiple or missing current versions"
    )

source_count = df_silver_merge.count()

print(f"Silver MERGE source rows: {source_count}")

# COMMAND ----------

silver_table = DeltaTable.forName(
    spark,
    SILVER_TABLE
)

(
    silver_table.alias("target")
    .merge(
        df_silver_merge.alias("source"),
        """
        target.source_topic = source.source_topic
        AND target.source_partition = source.source_partition
        AND target.source_offset = source.source_offset
        """
    )
    .whenMatchedUpdate(
        condition="""
            NOT (target.valid_to <=> source.valid_to)
            OR NOT (target.is_current <=> source.is_current)
        """,
        set={
            "valid_to": "source.valid_to",
            "is_current": "source.is_current"
        }
    )
    .whenNotMatchedInsertAll()
    .execute()
)

print("Silver CDC MERGE completed")