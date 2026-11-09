# PostgreSQL CDC to Azure Databricks | Kafka, Debezium & Delta Lake

An end-to-end Change Data Capture (CDC) data engineering portfolio project. PostgreSQL changes flow through Debezium and Apache Kafka into a Python consumer that exports recoverable JSONL batches. Azure Databricks processes uploaded batches through Bronze, Silver (SCD Type 2), and Gold Delta tables, orchestrated by Lakeflow Jobs.

> **Scope:** Kafka-to-Databricks file transfer is **manual** in this lab. The four Databricks tasks are automated, but this is not a fully automated cloud ingestion pipeline.

## Architecture

```text
PostgreSQL 16 (logical replication)
   |  Debezium PostgreSQL connector 3.1
   v
Kafka 3.9.1 — postgres.public.customers
   |  Python confluent-kafka consumer
   |  manual offset commit after durable JSONL publication
   v
Local data/cdc_batches/*.jsonl
   |  manual upload to /Volumes/workspace/bronze/cdc_files/
   v
Azure Databricks / Delta Lake
   |-- Bronze: workspace.bronze.customers_cdc_events
   |-- Silver: workspace.silver.customers_history (SCD2)
   |-- Gold:   workspace.gold.customers_cdc_by_country
   `-- Validation: Bronze↔Silver and Silver↔Gold reconciliation

Lakeflow Jobs: bronze_cdc_ingestion → silver_cdc_transform
               → gold_cdc_aggregation → validate_cdc_pipeline
```

## Stack

PostgreSQL 16 · Debezium 3.1 · Apache Kafka 3.9.1 · Python 3.12 · confluent-kafka · Docker Compose · Azure Databricks · PySpark · Delta Lake · Unity Catalog · Lakeflow Jobs.

## Repository layout

```text
consumer/
  __init__.py
  kafka_consumer.py
  batch_writer.py
  batch_recovery.py
  requirements.txt
debezium/register-connector.json
postgres/init/01_create_customers.sql
notebooks/
  01_bronze_ingestion_cdc.py
  02_silver_transform_cdc.py
  03_gold_aggregation_cdc.py
  04_pipeline_validation_cdc.py
tests/test_batch_recovery.py
docker-compose.yml
```

## Run locally

Requirements: Docker with Compose, Python 3.12, and access to an Azure Databricks workspace for the second stage.

1. Start the local infrastructure from the repository root:

   ```bash
   docker compose up -d
   docker compose ps
   ```

2. Register the Debezium connector after Kafka Connect is ready:

   ```bash
   curl -f -X POST http://localhost:8083/connectors \
     -H 'Content-Type: application/json' \
     --data-binary @debezium/register-connector.json
   ```

   The connector expects a PostgreSQL publication named `customers_publication` because `publication.autocreate.mode` is `disabled`. Verify that the database initialization SQL creates the publication (or create it explicitly) before registering the connector. On a previously initialized Docker volume, changes to initialization SQL do not run automatically.

   Verify connector status:

   ```bash
   curl -s http://localhost:8083/connectors/postgres-customers-connector/status
   ```

3. Install Python dependencies and run the consumer **from the repository root**:

   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   pip install -r consumer/requirements.txt
   python -m consumer.kafka_consumer
   ```

   The consumer connects to `localhost:9092`, uses group `cdc-json-exporter-v1`, and exports batches to `data/cdc_batches/`. Flush is triggered at 100 events, after approximately 10 seconds, or on Ctrl+C. Do not start multiple consumers with the same output directory without reviewing filesystem and ownership assumptions.

4. Run local recovery tests:

   ```bash
   python -m pytest tests/test_batch_recovery.py
   ```

   Install `pytest` in the environment if it is not already present.

## Run the Databricks stage

1. Upload generated `.jsonl` files to the Unity Catalog volume `/Volumes/workspace/bronze/cdc_files/`.
2. Import the four files in `notebooks/` as Databricks notebooks, preserving their order.
3. Confirm that the Unity Catalog schemas and Delta tables referenced in the notebooks exist; the notebooks were developed against existing lab tables rather than a clean-workspace bootstrap script.
4. Configure the Lakeflow Job `customers_cdc_medallion_pipeline` with notebook tasks:
   - `bronze_cdc_ingestion`
   - `silver_cdc_transform` (depends on Bronze)
   - `gold_cdc_aggregation` (depends on Silver)
   - `validate_cdc_pipeline` (depends on Gold)
5. Run the job and inspect each task's logs and Delta history.

## Design choices

**Recoverable batch export.** The Python consumer disables automatic offset commits and stores. The writer uses deterministic filenames derived from Kafka partition/offset range, validates consecutive offsets, writes and fsyncs a temporary file, then publishes without overwriting an existing batch. The consumer commits offsets synchronously only after publication/recovery succeeds. A simulated post-export failure exercises replay and recovery behavior.

**Bronze idempotency.** Each event is identified by `(topic, partition, offset)` and ingested with Delta `MERGE`. Tombstone records remain in Bronze for technical traceability but do not generate SCD2 versions.

**Silver SCD Type 2.** New CDC events identify affected customers. Their full available Bronze history is reconstructed into versions with `valid_from`, `valid_to`, `is_current`, and `is_deleted`. The final deleted state can have both `is_current=true` and `is_deleted=true`.

**Gold aggregation.** Current non-deleted customers are grouped by country. The Delta `MERGE` updates changed totals and removes countries no longer present in the active snapshot.

**Validation.** The final notebook checks duplicate event keys, SCD2 current-version consistency, and reconciliation between Bronze/Silver and Silver/Gold.

## Demonstrated end-to-end result

A PostgreSQL UPDATE changed customer 6 (Pedro) from Argentina to Uruguay. Debezium emitted the change, Kafka delivered it, and the Python consumer exported offset 13. The Databricks job succeeded across all four tasks.

| Silver version | Country | Current | Kafka offset |
|---|---|---|---|
| 1 | Colombia | No | 9 |
| 2 | Argentina | No | 10 |
| 3 | Uruguay | Yes | 13 |

Final Gold totals:

| Country | Active customers |
|---|---:|
| Peru | 3 |
| Uruguay | 1 |

A subsequent Lakeflow Job execution also succeeded and preserved the same Gold totals. For a strict no-op check on all layers, inspect Delta `DESCRIBE HISTORY` metrics for each table.

## Limitations and next steps

- Manual JSONL upload is required between the local Kafka consumer and Databricks.
- Bronze currently rereads the available JSONL files and relies on Delta `MERGE` for deduplication.
- Kafka is configured as a single-node, plaintext local development broker; PostgreSQL uses example development credentials. **Do not reuse these settings for production.**
- The current SCD2 event ordering assumes the single-partition configuration used in the lab; multi-partition ordering requires a more explicit per-key sequencing strategy.
- Clean-workspace provisioning of Unity Catalog objects and the Lakeflow Job is not yet scripted.
- Future work: automated object-storage transfer, incremental file discovery, deployment-as-code, monitoring, and more integration tests.

## Security note

The repository's Docker and connector configurations contain the sample PostgreSQL password `postgres`. It is **not a production secret**, but change it for any shared or deployed environment. Check all configuration files and Git history for real secrets before publishing.
