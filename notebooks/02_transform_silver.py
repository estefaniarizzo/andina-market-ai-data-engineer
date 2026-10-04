# Databricks notebook source
# MAGIC %md
# MAGIC # Nivel 2 — Transformación, calidad e historial en Silver
# MAGIC
# MAGIC Procesa solo las filas nuevas de Bronze (`_ingested_at` > último valor en `control.silver_progress`).
# MAGIC
# MAGIC 1. **Tipado y normalización** según el contrato de `src/andina_pipeline/config.py`
# MAGIC    (`try_to_timestamp` acepta fechas con o sin milisegundos; un valor imposible de convertir es un error de calidad, no una caída).
# MAGIC 2. **Deduplicación determinista**: última versión por llave ordenando por `updated_at`, `_ingested_at` y `_source_file`.
# MAGIC 3. **Calidad como código** (`quality.py`): reglas `error` → `control.quarantine_records`; reglas `warn` → se conservan con `_dq_warnings`.
# MAGIC    Si los errores superan `max_error_rate` el lote no se escribe y la tarea falla.
# MAGIC 4. **SCD1** con `MERGE` en `silver.<tabla>` (estado actual; nunca retrocede ante datos tardíos).
# MAGIC 5. **SCD2** en `silver.<tabla>_history` para clientes (segmento, email, ciudad...), productos (precio, estado),
# MAGIC    pedidos (estado) y pagos (estado, monto, fecha).
# MAGIC 6. **Checks de tabla** (unicidad, integridad referencial, totales, cobros duplicados, una versión vigente por llave)
# MAGIC    guardados en `control.data_quality_results`; los críticos detienen el pipeline.

# COMMAND ----------

import os
import sys
from uuid import uuid4

sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "..", "src")))

from andina_pipeline import pipeline  # noqa: E402
from andina_pipeline.config import LakehouseConfig  # noqa: E402

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema_prefix", "")
dbutils.widgets.text("max_error_rate", "0.05")
dbutils.widgets.dropdown("full_refresh", "false", ["false", "true"])

cfg = LakehouseConfig(catalog=dbutils.widgets.get("catalog"), schema_prefix=dbutils.widgets.get("schema_prefix"))
MAX_ERROR_RATE = float(dbutils.widgets.get("max_error_rate"))
FULL_REFRESH = dbutils.widgets.get("full_refresh") == "true"
RUN_ID = dbutils.jobs.taskValues.get(taskKey="bronze_ingest", key="run_id", default=str(uuid4()),
                                     debugValue=str(uuid4()))
pipeline.ensure_schemas(spark, cfg)

if FULL_REFRESH:
    pipeline.reset(spark, cfg, ["silver"])
    print("Silver reiniciado: se reprocesa todo Bronze")

# COMMAND ----------

result = pipeline.run_silver(spark, cfg, RUN_ID, MAX_ERROR_RATE)

display(spark.createDataFrame(
    [(s["entity"], s["rows_processed"], s["rows_quarantined"], s["history_rows"]) for s in result["entities"]],
    "entity string, rows_processed bigint, rows_quarantined bigint, new_history_rows bigint",
))

# COMMAND ----------

display(spark.sql(f"""
    SELECT table_name, check_name, check_type, severity, failed_rows, total_rows, check_status
    FROM {cfg.table('control', 'data_quality_results')}
    WHERE quality_run_id = '{RUN_ID}' AND failed_rows > 0
    ORDER BY severity, table_name, check_name
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Evidencia de historial (SCD2)

# COMMAND ----------

display(spark.sql(f"""
    SELECT customer_id, segment, valid_from, valid_to, is_current
    FROM {cfg.table('silver', 'customers_history')}
    WHERE customer_id IN (SELECT customer_id FROM {cfg.table('silver', 'customers_history')}
                          GROUP BY customer_id HAVING COUNT(*) > 1 LIMIT 5)
    ORDER BY customer_id, valid_from
"""))

display(spark.sql(f"""
    SELECT payment_id, payment_status, payment_date, valid_from, valid_to, is_current
    FROM {cfg.table('silver', 'payments_history')}
    WHERE payment_id IN (SELECT payment_id FROM {cfg.table('silver', 'payments_history')}
                         GROUP BY payment_id HAVING COUNT(*) > 1 LIMIT 5)
    ORDER BY payment_id, valid_from
"""))
