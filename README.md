# CDC de PostgreSQL a Azure Databricks | Kafka, Debezium y Delta Lake

Proyecto práctico de portafolio de Data Engineering que implementa **Change Data Capture (CDC)** de extremo a extremo. Los cambios realizados en PostgreSQL se capturan mediante Debezium, se publican en Apache Kafka y se consumen con Python para generar lotes JSONL recuperables. Posteriormente, Azure Databricks procesa los archivos cargados en tablas Delta de las capas **Bronze, Silver (SCD Type 2) y Gold**, mediante tareas orquestadas con **Lakeflow Jobs**.

> **Alcance:** la transferencia de archivos desde el consumidor de Kafka hasta Databricks es **manual** en este proyecto. Las cuatro tareas de Databricks están automatizadas, pero la ingesta completa hacia la nube todavía no lo está.

## Arquitectura

```text
PostgreSQL 16 (replicación lógica)
   |  Conector Debezium PostgreSQL 3.1
   v
Kafka 3.9.1 — postgres.public.customers
   |  Consumidor Python con confluent-kafka
   |  Confirmación manual de offsets tras publicar el JSONL de forma durable
   v
Archivos locales data/cdc_batches/*.jsonl
   |  Carga manual a /Volumes/workspace/bronze/cdc_files/
   v
Azure Databricks / Delta Lake
   |-- Bronze: workspace.bronze.customers_cdc_events
   |-- Silver: workspace.silver.customers_history (SCD2)
   |-- Gold:   workspace.gold.customers_cdc_by_country
   `-- Validación: conciliación Bronze↔Silver y Silver↔Gold

Lakeflow Jobs: bronze_cdc_ingestion → silver_cdc_transform
               → gold_cdc_aggregation → validate_cdc_pipeline
```

## Tecnologías

PostgreSQL 16 · Debezium 3.1 · Apache Kafka 3.9.1 · Python 3.12 · confluent-kafka · Docker Compose · Azure Databricks · PySpark · Delta Lake · Unity Catalog · Lakeflow Jobs.

## Estructura del repositorio

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

## Ejecución local

**Requisitos:** Docker con Compose, Python 3.12 y acceso a un workspace de Azure Databricks para la segunda etapa.

1. **Iniciar la infraestructura local** desde la raíz del repositorio:

   ```bash
   docker compose up -d
   docker compose ps
   ```

2. **Registrar el conector de Debezium** cuando Kafka Connect esté disponible:

   ```bash
   curl -f -X POST http://localhost:8083/connectors \
     -H 'Content-Type: application/json' \
     --data-binary @debezium/register-connector.json
   ```

   El conector espera que exista una publicación de PostgreSQL llamada `customers_publication`, porque `publication.autocreate.mode` está configurado como `disabled`. Comprueba que el SQL de inicialización crea esa publicación o créala explícitamente antes de registrar el conector. Si el volumen de Docker ya estaba inicializado, los cambios posteriores en el SQL de inicialización **no se ejecutan automáticamente**.

   Verificar el estado del conector:

   ```bash
   curl -s http://localhost:8083/connectors/postgres-customers-connector/status
   ```

3. **Instalar las dependencias y ejecutar el consumidor Python** desde la raíz del repositorio:

   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   pip install -r consumer/requirements.txt
   python -m consumer.kafka_consumer
   ```

   El consumidor se conecta a `localhost:9092`, utiliza el grupo `cdc-json-exporter-v1` y exporta lotes a `data/cdc_batches/`. La escritura del lote se activa al acumular **100 eventos**, después de aproximadamente **10 segundos** o al presionar **Ctrl+C**. No ejecutes varios consumidores sobre el mismo directorio de salida sin revisar previamente las condiciones de concurrencia y propiedad de archivos.

4. **Ejecutar las pruebas locales de recuperación:**

   ```bash
   python -m pytest tests/test_batch_recovery.py
   ```

   Si `pytest` no está instalado en el entorno, instálalo antes de ejecutar las pruebas.

## Ejecución en Azure Databricks

1. Cargar los archivos `.jsonl` generados en el volumen de Unity Catalog `/Volumes/workspace/bronze/cdc_files/`.
2. Importar los cuatro archivos de `notebooks/` como notebooks de Databricks, respetando su orden.
3. Comprobar que existen los esquemas de Unity Catalog y las tablas Delta referenciadas en los notebooks. Estos notebooks se desarrollaron sobre tablas ya existentes en el proyecto; **todavía no hay un script que prepare un workspace vacío desde cero**.
4. Configurar el Lakeflow Job `customers_cdc_medallion_pipeline` con las siguientes tareas y dependencias:
   - `bronze_cdc_ingestion`
   - `silver_cdc_transform` (depende de Bronze)
   - `gold_cdc_aggregation` (depende de Silver)
   - `validate_cdc_pipeline` (depende de Gold)
5. Ejecutar el job y revisar los logs de cada tarea y el historial de las tablas Delta.

## Decisiones de diseño

### Exportación de lotes recuperables

El consumidor Python deshabilita la confirmación y el almacenamiento automáticos de offsets. El componente de escritura genera nombres de archivo determinísticos a partir de la partición de Kafka y del rango de offsets; valida que los offsets sean consecutivos, escribe un archivo temporal, ejecuta `fsync` y publica el lote sin sobrescribir archivos existentes.

El consumidor confirma los offsets de manera síncrona **solo después** de que la publicación o recuperación del lote se haya completado correctamente. Una falla simulada posterior a la exportación permite probar el reprocesamiento y la recuperación.

### Idempotencia en Bronze

Cada evento se identifica mediante `(topic, partition, offset)` y se incorpora con un `MERGE` de Delta Lake. Los registros *tombstone* permanecen en Bronze para conservar la trazabilidad técnica, pero no generan versiones SCD2.

### Historial SCD Type 2 en Silver

Los nuevos eventos CDC permiten identificar los clientes afectados. Para ellos se reconstruye todo el historial disponible en Bronze y se generan versiones con `valid_from`, `valid_to`, `is_current` e `is_deleted`.

Un cliente eliminado puede tener una última versión con **`is_current=true` e `is_deleted=true`**: es su estado más reciente, pero no representa un cliente activo.

### Agregación en Gold

Los clientes de su versión actual que no estén eliminados se agrupan por país. Un `MERGE` de Delta actualiza los totales modificados y elimina los países que dejan de estar presentes en el conjunto de clientes activos.

### Validación del pipeline

El último notebook comprueba claves de evento duplicadas, consistencia de las versiones actuales SCD2 y conciliación de datos entre Bronze/Silver y Silver/Gold.

## Resultado demostrado de extremo a extremo

Una operación `UPDATE` en PostgreSQL cambió el país del **cliente 6 (Pedro)** de Argentina a Uruguay. Debezium capturó el cambio, Kafka lo entregó y el consumidor Python exportó el evento correspondiente al **offset 13**. El Lakeflow Job terminó correctamente sus cuatro tareas.

| Versión en Silver | País | ¿Actual? | Offset de Kafka |
|---|---|---|---|
| 1 | Colombia | No | 9 |
| 2 | Argentina | No | 10 |
| 3 | Uruguay | Sí | 13 |

**Totales finales en Gold:**

| País | Clientes activos |
|---|---:|
| Perú | 3 |
| Uruguay | 1 |

Una ejecución posterior de Lakeflow Jobs también finalizó correctamente y mantuvo los mismos totales en Gold. Para verificar estrictamente que **ninguna capa realizó escrituras innecesarias**, es necesario inspeccionar las métricas de `DESCRIBE HISTORY` de cada tabla Delta.
