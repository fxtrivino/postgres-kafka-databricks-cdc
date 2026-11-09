# Databricks notebook source
from pyspark.sql import functions as F

BRONZE_TABLE = "workspace.bronze.customers_cdc_events"
SILVER_TABLE = "workspace.silver.customers_history"
GOLD_TABLE = "workspace.gold.customers_cdc_by_country"

bronze = spark.table(BRONZE_TABLE)
silver = spark.table(SILVER_TABLE)
gold = spark.table(GOLD_TABLE)

print("CDC pipeline validation started")

# COMMAND ----------

bronze_duplicates = (
    bronze
    .groupBy("topic", "partition", "offset")
    .count()
    .filter(F.col("count") > 1)
)

if bronze_duplicates.limit(1).count() > 0:
    raise RuntimeError("Bronze contains duplicate Kafka events")

invalid_bronze = bronze.filter(
    F.col("topic").isNull()
    | F.col("partition").isNull()
    | F.col("offset").isNull()
)

if invalid_bronze.limit(1).count() > 0:
    raise RuntimeError("Bronze contains null Kafka identifiers")

print(f"Bronze validation PASSED: {bronze.count()} events")

# COMMAND ----------

invalid_current = (
    silver
    .groupBy("customer_id")
    .agg(
        F.sum(
            F.when(F.col("is_current"), 1).otherwise(0)
        ).alias("current_versions")
    )
    .filter(F.col("current_versions") != 1)
)

if invalid_current.limit(1).count() > 0:
    raise RuntimeError("Invalid SCD2 current versions")

silver_duplicates = (
    silver
    .groupBy(
        "source_topic",
        "source_partition",
        "source_offset"
    )
    .count()
    .filter(F.col("count") > 1)
)

if silver_duplicates.limit(1).count() > 0:
    raise RuntimeError("Silver contains duplicate CDC events")

print(f"Silver validation PASSED: {silver.count()} versions")

# COMMAND ----------

bronze_changes = (
    bronze
    .filter(F.col("operation").isin("c", "u", "d", "r"))
    .select("topic", "partition", "offset")
)

silver_events = (
    silver
    .select(
        F.col("source_topic").alias("topic"),
        F.col("source_partition").alias("partition"),
        F.col("source_offset").alias("offset")
    )
)

missing_events = (
    bronze_changes
    .join(
        silver_events,
        ["topic", "partition", "offset"],
        "left_anti"
    )
)

unexpected_events = (
    silver_events
    .join(
        bronze_changes,
        ["topic", "partition", "offset"],
        "left_anti"
    )
)

if missing_events.limit(1).count() > 0:
    raise RuntimeError("Bronze contains events missing in Silver")

if unexpected_events.limit(1).count() > 0:
    raise RuntimeError("Silver contains events missing in Bronze")

print("Bronze-Silver reconciliation PASSED")

# COMMAND ----------

active_customers = silver.filter(
    (F.col("is_current") == True)
    & (F.col("is_deleted") == False)
)

expected_gold = (
    active_customers
    .groupBy("country")
    .agg(
        F.countDistinct("customer_id").alias("expected_count")
    )
)

actual_gold = gold.select(
    "country",
    F.col("total_customers").alias("actual_count")
)

differences = (
    expected_gold.alias("e")
    .join(
        actual_gold.alias("a"),
        F.col("e.country").eqNullSafe(F.col("a.country")),
        "full_outer"
    )
    .filter(
        F.col("e.expected_count").isNull()
        | F.col("a.actual_count").isNull()
        | ~F.col("e.expected_count").eqNullSafe(
            F.col("a.actual_count")
        )
    )
)

if differences.limit(1).count() > 0:
    raise RuntimeError("Silver-Gold reconciliation FAILED")

print("Silver-Gold reconciliation PASSED")
print("CDC PIPELINE VALIDATION PASSED")