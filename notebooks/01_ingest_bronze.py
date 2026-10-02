# Databricks notebook source
# MAGIC %md
# MAGIC # Nivel 1 — Ingesta a Bronze
# MAGIC
# MAGIC Carga archivos CSV exportados desde Azure SQL Database, almacenados en un
# MAGIC Unity Catalog Volume, a tablas Delta Bronze. Agrega metadatos de trazabilidad
# MAGIC y registra el resultado de la ejecución en una tabla de control.

# COMMAND ----------

from datetime import datetime, timezone
from uuid import uuid4

from pyspark.sql import functions as F
from pyspark.sql import Row
from pyspark.sql.types import (
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


CATALOG = "workspace"
BRONZE_SCHEMA = "bronze"
CONTROL_SCHEMA = "control"
LANDING_VOLUME = "landing"

LANDING_PATH = f"/Volumes/{CATALOG}/{BRONZE_SCHEMA}/{LANDING_VOLUME}"
INGESTION_RUN_ID = str(uuid4())
INGESTED_AT = datetime.now(timezone.utc).replace(tzinfo=None)

SOURCE_TABLES = {
    "customers": "dbo.Customers",
    "products": "dbo.Products",
    "orders": "dbo.Orders",
    "order_items": "dbo.OrderItems",
    "payments": "dbo.Payments",
    "support_tickets": "dbo.SupportTickets",
}

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{CONTROL_SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Carga CSV a Delta Bronze
# MAGIC
# MAGIC Se mantiene el contenido fuente como texto (`inferSchema=false`) para preservar
# MAGIC la fidelidad de Bronze. El tipado y la calidad se aplican en Silver.

# COMMAND ----------

for target_table, source_table in SOURCE_TABLES.items():
    csv_path = f"{LANDING_PATH}/{target_table}.csv"

    raw_df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .csv(csv_path)
    )

    bronze_df = (
        raw_df
        .withColumn("_source_system", F.lit("azure_sql"))
        .withColumn("_source_table", F.lit(source_table))
        .withColumn("_source_file", F.lit(csv_path))
        .withColumn("_ingestion_run_id", F.lit(INGESTION_RUN_ID))
        .withColumn("_ingested_at", F.lit(INGESTED_AT).cast("timestamp"))
    )

    (
        bronze_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{CATALOG}.{BRONZE_SCHEMA}.{target_table}")
    )

    row_count = spark.table(
        f"{CATALOG}.{BRONZE_SCHEMA}.{target_table}"
    ).count()

    print(
        f"{target_table}: {row_count} rows -> "
        f"{CATALOG}.{BRONZE_SCHEMA}.{target_table}"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Registro de control de ingesta
# MAGIC
# MAGIC Registra una fila por tabla fuente. En la demo, `last_successful_watermark`
# MAGIC se deja nulo porque la carga es completa; en producción se actualizaría
# MAGIC con el mayor `updated_at` confirmado después de una carga incremental exitosa.

# COMMAND ----------

watermark_schema = StructType([
    StructField("source_table", StringType(), False),
    StructField("last_successful_watermark", TimestampType(), True),
    StructField("last_run_id", StringType(), False),
    StructField("rows_read", LongType(), False),
    StructField("run_status", StringType(), False),
    StructField("updated_at", TimestampType(), False),
])

watermark_rows = [
    Row(
        source_table=source_table,
        last_successful_watermark=None,
        last_run_id=INGESTION_RUN_ID,
        rows_read=spark.table(
            f"{CATALOG}.{BRONZE_SCHEMA}.{target_table}"
        ).count(),
        run_status="success",
        updated_at=INGESTED_AT,
    )
    for target_table, source_table in SOURCE_TABLES.items()
]

watermarks_df = spark.createDataFrame(
    watermark_rows,
    schema=watermark_schema,
)

(
    watermarks_df.write
    .format("delta")
    .mode("append")
    .saveAsTable(f"{CATALOG}.{CONTROL_SCHEMA}.ingestion_watermarks")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validación de Bronze

# COMMAND ----------

for table_name in SOURCE_TABLES:
    table_path = f"{CATALOG}.{BRONZE_SCHEMA}.{table_name}"

    validation = (
        spark.table(table_path)
        .agg(
            F.count("*").alias("row_count"),
            F.countDistinct("_ingestion_run_id").alias("ingestion_runs"),
            F.countDistinct("_source_system").alias("source_systems"),
        )
        .collect()[0]
    )

    print(
        f"{table_path}: "
        f"rows={validation['row_count']}, "
        f"ingestion_runs={validation['ingestion_runs']}, "
        f"source_systems={validation['source_systems']}"
    )

print(f"\nBronze ingestion completed. Run ID: {INGESTION_RUN_ID}")