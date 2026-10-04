"""Silver: typing, normalization, deterministic dedup, quality gate, MERGE and SCD2.

Incremental processing: each run reads only Bronze rows with `_ingested_at`
greater than the last processed value stored in control.silver_progress.
All writes are idempotent, so re-running a failed batch is safe:
- current-state tables use MERGE and only overwrite when the incoming
  `updated_at` is newer or equal (late or re-exported rows never go backwards);
- history tables (SCD2) ignore versions not newer than the current one.
"""

from datetime import datetime, timezone

from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType, TimestampType

from . import quality
from .config import PARENT_BY_KEY, EntitySpec, LakehouseConfig

LINEAGE_COLUMNS = ["_source_file", "_ingestion_run_id", "_ingested_at"]
PROGRESS_SCHEMA = StructType([
    StructField("entity", StringType(), False),
    StructField("last_bronze_ingested_at", TimestampType(), False),
    StructField("silver_run_id", StringType(), False),
    StructField("rows_processed", LongType(), False),
    StructField("rows_quarantined", LongType(), False),
    StructField("processed_at", TimestampType(), False),
])


def utc_now():
    return datetime.now(timezone.utc).replace(tzinfo=None)


def table_exists(spark, name: str) -> bool:
    return spark.catalog.tableExists(name)


# ---------------------------------------------------------------------------
# Typing and normalization
# ---------------------------------------------------------------------------

def cast_column(column):
    raw = F.trim(F.col(column.name))
    if column.dtype == "string":
        normalizers = {"lower": F.lower, "upper": F.upper, "initcap": F.initcap}
        return normalizers.get(column.normalize, lambda c: c)(raw)
    if column.dtype == "boolean":
        lowered = F.lower(raw)
        return (F.when(lowered.isin("true", "1"), F.lit(True))
                .when(lowered.isin("false", "0"), F.lit(False)))
    if column.dtype == "timestamp":
        # Accepts "yyyy-MM-dd HH:mm:ss" with or without fractional seconds; invalid -> NULL.
        return F.expr(f"try_to_timestamp(trim(`{column.name}`))")
    if column.dtype == "date":
        return F.to_date(F.expr(f"try_to_timestamp(trim(`{column.name}`))"))
    return F.expr(f"try_cast(trim(`{column.name}`) AS {column.dtype})")


def typed(df, entity: EntitySpec):
    """Cast Bronze strings to the contract types; keep the raw values in `_raw` for rules and quarantine."""
    out = df.withColumn("_raw", F.struct(*[F.col(name) for name in entity.column_names]))
    for column in entity.columns:
        out = out.withColumn(column.name, cast_column(column))
    return out


# ---------------------------------------------------------------------------
# Deterministic deduplication
# ---------------------------------------------------------------------------

def _version_order():
    return [F.col("updated_at").desc_nulls_last(), F.col("_ingested_at").desc_nulls_last(),
            F.col("_source_file").desc_nulls_last()]


def distinct_versions(df, key: str):
    """One row per (key, updated_at): removes exact re-exports caused by the lookback window."""
    window = Window.partitionBy(key, "updated_at").orderBy(*_version_order()[1:])
    return df.withColumn("_rn", F.row_number().over(window)).filter("_rn = 1").drop("_rn")


def latest_per_key(df, key: str):
    """Latest version per key ordered by updated_at, then ingestion time and file (deterministic)."""
    window = Window.partitionBy(key).orderBy(*_version_order())
    return df.withColumn("_rn", F.row_number().over(window)).filter("_rn = 1").drop("_rn")


# ---------------------------------------------------------------------------
# Writes
# ---------------------------------------------------------------------------

def merge_latest(spark, target: str, df, key: str) -> None:
    """SCD1 upsert: insert new keys, update only when the incoming version is newer or equal."""
    if not table_exists(spark, target):
        df.write.format("delta").saveAsTable(target)
        return
    df.createOrReplaceTempView("_silver_upserts")
    spark.sql(f"""
        MERGE INTO {target} AS t
        USING _silver_upserts AS s
        ON t.{key} = s.{key}
        WHEN MATCHED AND s.updated_at >= t.updated_at THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)


def apply_scd2(spark, target: str, versions, key: str, tracked, run_id: str) -> int:
    """SCD Type 2 with a single atomic MERGE (closes the current row and inserts new versions).

    `versions` may contain several versions per key (e.g. a backlog of files);
    consecutive versions without changes in the tracked columns are collapsed.
    Returns the number of new history rows.
    """
    row_hash = F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("<null>")) for c in tracked]), 256)
    candidates = versions.select(key, *tracked, "updated_at").withColumn("_row_hash", row_hash)

    exists = table_exists(spark, target)
    if exists:
        current = (spark.table(target).filter("is_current")
                   .select(key, F.col("valid_from").alias("_cur_from"), F.col("_row_hash").alias("_cur_hash")))
    else:
        current = spark.createDataFrame([], f"{key} bigint, _cur_from timestamp, _cur_hash string")

    by_key = Window.partitionBy(key).orderBy("updated_at")
    changes = (
        candidates.join(current, key, "left")
        .filter(F.col("_cur_from").isNull() | (F.col("updated_at") > F.col("_cur_from")))
        .withColumn("_prev_hash", F.coalesce(F.lag("_row_hash").over(by_key), F.col("_cur_hash")))
        .filter(F.col("_prev_hash").isNull() | (F.col("_row_hash") != F.col("_prev_hash")))
    )
    new_rows = (
        changes
        .withColumn("valid_from", F.col("updated_at"))
        .withColumn("valid_to", F.lead("updated_at").over(by_key))
        .withColumn("is_current", F.col("valid_to").isNull())
        .withColumn("_silver_run_id", F.lit(run_id))
        .withColumn("_loaded_at", F.current_timestamp())
        .select(key, *tracked, "valid_from", "valid_to", "is_current", "_row_hash", "_silver_run_id", "_loaded_at")
    )

    if not exists:
        new_rows.write.format("delta").saveAsTable(target)
        return spark.table(target).count()

    closes = (changes.filter(F.col("_cur_from").isNotNull()).groupBy(key)
              .agg(F.min("updated_at").alias("valid_from"))
              .withColumn("_merge_key", F.col(key)))
    staged = new_rows.withColumn("_merge_key", F.lit(None).cast("bigint")).unionByName(closes, allowMissingColumns=True)
    insert_columns = [key, *tracked, "valid_from", "valid_to", "is_current", "_row_hash", "_silver_run_id", "_loaded_at"]

    new_count = new_rows.count()  # before the MERGE: afterwards these rows are no longer "new"
    staged.createOrReplaceTempView("_scd2_staged")
    spark.sql(f"""
        MERGE INTO {target} AS t
        USING _scd2_staged AS s
        ON t.{key} = s._merge_key AND t.is_current = true
        WHEN MATCHED THEN UPDATE SET valid_to = s.valid_from, is_current = false
        WHEN NOT MATCHED THEN INSERT ({", ".join(insert_columns)})
            VALUES ({", ".join("s." + c for c in insert_columns)})
    """)
    return new_count


def write_quarantine(spark, cfg: LakehouseConfig, entity: EntitySpec, rejected, run_id: str) -> None:
    (rejected.select(
        F.lit(entity.name).alias("entity"),
        F.col(entity.key).cast("string").alias("record_key"),
        F.col("_dq_errors").alias("errors"),
        F.to_json("_raw").alias("raw_record"),
        "_source_file",
        "_ingestion_run_id",
        F.lit(run_id).alias("silver_run_id"),
        F.current_timestamp().alias("quarantined_at"),
    ).write.format("delta").mode("append").saveAsTable(cfg.table("control", "quarantine_records")))


# ---------------------------------------------------------------------------
# Incremental driver
# ---------------------------------------------------------------------------

def last_processed(spark, cfg: LakehouseConfig, entity: EntitySpec):
    progress = cfg.table("control", "silver_progress")
    if not table_exists(spark, progress):
        return None
    return (spark.table(progress).filter(F.col("entity") == entity.name)
            .agg(F.max("last_bronze_ingested_at").alias("w")).first()["w"])


def process_entity(spark, entity: EntitySpec, cfg: LakehouseConfig, run_id: str, max_error_rate: float = 0.05) -> dict:
    bronze = spark.table(cfg.table("bronze", entity.name))
    since = last_processed(spark, cfg, entity)
    if since is not None:
        bronze = bronze.filter(F.col("_ingested_at") > F.lit(since))
    upper = bronze.agg(F.max("_ingested_at").alias("u")).first()["u"]
    stats = {"entity": entity.name, "rows_processed": 0, "rows_quarantined": 0, "rows_merged": 0,
             "history_rows": 0, "rule_failures": {}}
    if upper is None:
        return stats
    bronze = bronze.filter(F.col("_ingested_at") <= F.lit(upper))

    rules = quality.rules_for(entity)
    checked = quality.evaluate_rules(distinct_versions(typed(bronze, entity), entity.key), rules)
    checked = quality.add_referential_checks(spark, checked, entity, lambda fk: cfg.table("silver", fk_entity(fk)))

    counts = checked.agg(F.count("*").alias("total"),
                         F.sum((F.size("_dq_errors") > 0).cast("int")).alias("errors")).first()
    total, errors = counts["total"], counts["errors"] or 0
    quality.enforce_error_rate(entity.name, total, errors, max_error_rate)

    failures = (checked.select(F.explode(F.concat("_dq_errors", "_dq_warnings")).alias("rule"))
                .groupBy("rule").count().collect())
    stats["rule_failures"] = {row["rule"]: row["count"] for row in failures}

    if errors:
        write_quarantine(spark, cfg, entity, checked.filter(F.size("_dq_errors") > 0), run_id)
    valid = checked.filter(F.size("_dq_errors") == 0)

    current = (latest_per_key(valid, entity.key)
               .select(*entity.column_names, *LINEAGE_COLUMNS, "_dq_warnings")
               .withColumn("_silver_run_id", F.lit(run_id))
               .withColumn("_silver_updated_at", F.current_timestamp()))
    merge_latest(spark, cfg.table("silver", entity.name), current, entity.key)

    if entity.history_columns:
        stats["history_rows"] = apply_scd2(spark, cfg.table("silver", f"{entity.name}_history"), valid,
                                           entity.key, list(entity.history_columns), run_id)

    (spark.createDataFrame([(entity.name, upper, run_id, int(total), int(errors), utc_now())], PROGRESS_SCHEMA)
     .write.format("delta").mode("append").saveAsTable(cfg.table("control", "silver_progress")))
    stats.update(rows_processed=total, rows_quarantined=errors, rows_merged=total - errors)
    return stats


def fk_entity(fk: str) -> str:
    return PARENT_BY_KEY[fk].name


def write_quality_results(spark, cfg: LakehouseConfig, run_id: str, results) -> None:
    checked_at = utc_now()
    rows = [{**r, "quality_run_id": run_id, "checked_at": checked_at} for r in results]
    schema = ("quality_run_id string, table_name string, check_name string, check_type string, severity string, "
              "failed_rows bigint, total_rows bigint, check_status string, checked_at timestamp")
    ordered = [tuple(r[c.split()[0]] for c in schema.split(", ")) for r in rows]
    (spark.createDataFrame(ordered, schema).write.format("delta").mode("append").option("mergeSchema", "true")
     .saveAsTable(cfg.table("control", "data_quality_results")))


def row_rule_results(entity_stats, entities) -> list:
    """Turn per-batch rule failure counts into data_quality_results rows (one per rule)."""
    results = []
    for stats in entity_stats:
        entity = next(e for e in entities if e.name == stats["entity"])
        rules = {r.name: r for r in quality.rules_for(entity)}
        for name in list(rules) + [f"orphan_{fk}" for fk in entity.parents]:
            severity = rules[name].severity if name in rules else "error"
            failed = stats["rule_failures"].get(name, 0)
            results.append({"table_name": entity.name, "check_name": name, "check_type": "row_rule",
                            "severity": severity, "failed_rows": failed, "total_rows": stats["rows_processed"],
                            "check_status": "passed" if failed == 0 else
                            ("quarantined" if severity == "error" else "warning")})
    return results
