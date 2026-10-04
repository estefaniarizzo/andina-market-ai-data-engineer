# Databricks notebook source
# MAGIC %md
# MAGIC # Nivel 1 — Ingesta incremental a Bronze (Auto Loader)
# MAGIC
# MAGIC Ingesta los CSV que el exportador incremental (`sql/04_export_source_to_csv.py`) deja en el
# MAGIC Volume `landing/<tabla>/` y los agrega a tablas Delta Bronze.
# MAGIC
# MAGIC | Decisión | Cómo |
# MAGIC |---|---|
# MAGIC | Solo archivos nuevos | Auto Loader (`cloudFiles`) con checkpoint por tabla en el Volume `checkpoints` |
# MAGIC | Ejecución tipo batch programado | `trigger(availableNow=True)`: procesa lo pendiente y termina (serverless) |
# MAGIC | Fidelidad a la fuente | Todo como `string` (`inferColumnTypes=false`); el tipado ocurre en Silver |
# MAGIC | Cambios de esquema | `schemaEvolutionMode=addNewColumns` + `mergeSchema`; valores que no encajan van a `_rescued_data` |
# MAGIC | Historial | Solo `append`: cada versión de un registro (p. ej. un pago `pending` → `approved`) queda guardada |
# MAGIC | Trazabilidad | `_source_system`, `_source_table`, `_source_file`, `_source_file_modified_at`, `_ingestion_run_id`, `_ingested_at` |
# MAGIC | Control | `control.ingestion_watermarks`: filas leídas y mayor `updated_at` disponible en Bronze por tabla |
# MAGIC
# MAGIC Los parámetros llegan desde el Job del Bundle (`databricks.yml`); al ejecutarlo a mano se usan los valores por defecto.

# COMMAND ----------

import os
import sys
from uuid import uuid4

sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "..", "src")))

from andina_pipeline import pipeline  # noqa: E402
from andina_pipeline.config import LakehouseConfig  # noqa: E402

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema_prefix", "")
dbutils.widgets.dropdown("full_refresh", "false", ["false", "true"])

cfg = LakehouseConfig(catalog=dbutils.widgets.get("catalog"), schema_prefix=dbutils.widgets.get("schema_prefix"))
FULL_REFRESH = dbutils.widgets.get("full_refresh") == "true"
RUN_ID = str(uuid4())
dbutils.jobs.taskValues.set(key="run_id", value=RUN_ID)

pipeline.ensure_schemas(spark, cfg)
spark.sql(f"CREATE VOLUME IF NOT EXISTS {cfg.bronze}.{cfg.landing_volume}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {cfg.control}.{cfg.checkpoint_volume}")
print(f"Landing: {cfg.landing_path}\nCheckpoints: {cfg.checkpoint_path}\nRun ID: {RUN_ID}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Full refresh (opcional)
# MAGIC Borra tablas Bronze y checkpoints para reconstruir todo desde los archivos del Volume.
# MAGIC Solo se usa en el primer despliegue o tras un cambio incompatible; lo normal es `false`.

# COMMAND ----------

if FULL_REFRESH:
    pipeline.reset(spark, cfg, ["bronze"], remove_path=lambda path: dbutils.fs.rm(path, True))
    print("Bronze reiniciado")

# COMMAND ----------

results = pipeline.run_bronze(spark, cfg, RUN_ID, use_autoloader=True)

display(spark.createDataFrame(
    [(r["entity"], r["target"], r["rows_read"], r["watermark"]) for r in results],
    "entity string, target string, rows_read bigint, watermark timestamp",
))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Evidencia: historial de un pago en Bronze
# MAGIC Un pago que cambió de estado aparece una vez por cada exportación que lo incluyó.

# COMMAND ----------

display(spark.sql(f"""
    SELECT payment_id, payment_status, payment_date, updated_at, _source_file, _ingested_at
    FROM {cfg.table('bronze', 'payments')}
    WHERE payment_id IN (
        SELECT payment_id FROM {cfg.table('bronze', 'payments')}
        GROUP BY payment_id HAVING COUNT(DISTINCT payment_status) > 1 LIMIT 5)
    ORDER BY payment_id, updated_at
"""))
