"""Bronze: incremental, append-only ingestion of the landing files with Auto Loader.

- Auto Loader (`cloudFiles`) discovers only new files; the checkpoint guarantees
  each file is ingested exactly once even if the job is re-run.
- Every column is kept as string (`inferColumnTypes=false`): Bronze is a faithful
  copy of the source and typing happens in Silver.
- Schema evolution: `addNewColumns` + Delta `mergeSchema`. A new source column
  stops the stream once, is added to the schema location and the job retry
  continues. Values that do not fit the schema go to `_rescued_data`.
- Append-only: each export (full or incremental) adds rows, so every version of
  a record (e.g. a payment going pending -> approved) is preserved.

Locally (plain Apache Spark, no Auto Loader) the same code uses the built-in
streaming file source with the explicit contract schema and its own checkpoint.
"""

from datetime import datetime, timezone

from pyspark.sql import Row
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType, TimestampType

from .config import EntitySpec, LakehouseConfig

SOURCE_SYSTEM = "azure_sql"
METADATA_COLUMNS = [
    "_rescued_data",
    "_source_system",
    "_source_table",
    "_source_file",
    "_source_file_modified_at",
    "_ingestion_run_id",
    "_ingested_at",
]

WATERMARK_SCHEMA = StructType([
    StructField("source_table", StringType(), False),
    StructField("last_successful_watermark", TimestampType(), True),
    StructField("last_run_id", StringType(), False),
    StructField("rows_read", LongType(), False),
    StructField("run_status", StringType(), False),
    StructField("updated_at", TimestampType(), False),
])


def checkpoint_dir(cfg: LakehouseConfig, entity: EntitySpec) -> str:
    return f"{cfg.checkpoint_path}/bronze/{entity.name}"


def read_landing(spark, entity: EntitySpec, cfg: LakehouseConfig, use_autoloader: bool = True):
    path = f"{cfg.landing_path}/{entity.name}"
    csv_options = {"header": "true", "multiLine": "true", "escape": '"'}

    if use_autoloader:
        return (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("cloudFiles.schemaLocation", f"{checkpoint_dir(cfg, entity)}/schema")
            .option("cloudFiles.inferColumnTypes", "false")
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            .options(**csv_options)
            .load(path)
        )

    schema = StructType([StructField(name, StringType()) for name in entity.column_names])
    return (
        spark.readStream.format("csv")
        .schema(schema)
        .options(**csv_options)
        .load(path)
        .withColumn("_rescued_data", F.lit(None).cast("string"))
    )


def with_ingestion_metadata(df, entity: EntitySpec, run_id: str):
    return (
        df.withColumn("_source_system", F.lit(SOURCE_SYSTEM))
        .withColumn("_source_table", F.lit(entity.source_table))
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_source_file_modified_at", F.col("_metadata.file_modification_time"))
        .withColumn("_ingestion_run_id", F.lit(run_id))
        .withColumn("_ingested_at", F.current_timestamp())
    )


def ingest_entity(spark, entity: EntitySpec, cfg: LakehouseConfig, run_id: str, use_autoloader: bool = True) -> dict:
    """Ingest all new landing files of one entity into its Bronze Delta table."""
    target = cfg.table("bronze", entity.name)
    query = (
        with_ingestion_metadata(read_landing(spark, entity, cfg, use_autoloader), entity, run_id)
        .writeStream.format("delta")
        .option("checkpointLocation", f"{checkpoint_dir(cfg, entity)}/checkpoint")
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable(target)
    )
    query.awaitTermination()

    table = spark.table(target)
    rows_read = table.filter(F.col("_ingestion_run_id") == run_id).count()
    watermark = table.select(F.max(F.expr("try_to_timestamp(updated_at)")).alias("w")).first()["w"]
    return {"entity": entity.name, "source_table": entity.source_table, "rows_read": rows_read,
            "watermark": watermark, "target": target}


def record_watermarks(spark, cfg: LakehouseConfig, run_id: str, results) -> None:
    """Append one row per table to control.ingestion_watermarks (max updated_at landed in Bronze)."""
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    rows = [
        Row(source_table=r["source_table"], last_successful_watermark=r["watermark"], last_run_id=run_id,
            rows_read=r["rows_read"], run_status="success", updated_at=now)
        for r in results
    ]
    (spark.createDataFrame(rows, WATERMARK_SCHEMA).write.format("delta").mode("append")
     .saveAsTable(cfg.table("control", "ingestion_watermarks")))


def contract_check(spark, entity: EntitySpec, cfg: LakehouseConfig) -> dict:
    """Compare the Bronze schema with the data contract (schema drift detection)."""
    columns = set(spark.table(cfg.table("bronze", entity.name)).columns) - set(METADATA_COLUMNS)
    expected = set(entity.column_names)
    return {"entity": entity.name, "missing_columns": sorted(expected - columns),
            "new_columns": sorted(columns - expected)}
