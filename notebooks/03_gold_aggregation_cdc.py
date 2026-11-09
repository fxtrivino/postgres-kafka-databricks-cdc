# Databricks notebook source
from pyspark.sql import functions as F
from delta.tables import DeltaTable

SILVER_TABLE = "workspace.silver.customers_history"
GOLD_TABLE = "workspace.gold.customers_cdc_by_country"

df_active_customers = (
    spark.table(SILVER_TABLE)
    .filter(
        (F.col("is_current") == True) &
        (F.col("is_deleted") == False)
    )
)

print("Silver active customers loaded")

# COMMAND ----------

df_gold = (
    df_active_customers
    .groupBy("country")
    .agg(
        F.countDistinct("customer_id").alias("total_customers")
    )
    .withColumn(
        "updated_at",
        F.current_timestamp()
    )
)

print("Gold aggregation prepared")

# COMMAND ----------

# Validar que no existan clientes activos duplicados
duplicate_customers = (
    df_active_customers
    .groupBy("customer_id")
    .count()
    .filter(F.col("count") > 1)
)

if duplicate_customers.limit(1).count() > 0:
    raise RuntimeError(
        "Silver contains duplicate active customers"
    )

# Validar customer_id y country
invalid_customers = df_active_customers.filter(
    F.col("customer_id").isNull() |
    F.col("country").isNull()
)

if invalid_customers.limit(1).count() > 0:
    raise RuntimeError(
        "Silver contains active customers with null fields"
    )

silver_count = df_active_customers.count()

gold_count = (
    df_gold
    .agg(F.sum("total_customers").alias("total"))
    .first()["total"]
)

gold_count = gold_count or 0

print(f"Silver active customers: {silver_count}")
print(f"Gold aggregated customers: {gold_count}")

if silver_count != gold_count:
    raise RuntimeError(
        "Gold aggregation does not match Silver"
    )

print("Gold validation PASSED")

# COMMAND ----------

gold_table = DeltaTable.forName(
    spark,
    GOLD_TABLE
)

(
    gold_table.alias("target")
    .merge(
        df_gold.alias("source"),
        "target.country = source.country"
    )
    .whenMatchedUpdate(
        condition="""
            NOT (
                target.total_customers
                <=> source.total_customers
            )
        """,
        set={
            "total_customers": "source.total_customers",
            "updated_at": "source.updated_at"
        }
    )
    .whenNotMatchedInsertAll()
    .whenNotMatchedBySourceDelete()
    .execute()
)

print("Gold CDC MERGE completed")