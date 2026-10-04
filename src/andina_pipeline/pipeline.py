"""Orchestration helpers shared by the Databricks notebooks and the local runner."""

from typing import Callable, Optional

from . import bronze, quality, silver
from .config import ENTITIES, LAYERS, LakehouseConfig


def ensure_schemas(spark, cfg: LakehouseConfig) -> None:
    for layer in LAYERS:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.schema(layer)}")


def reset(spark, cfg: LakehouseConfig, layers, remove_path: Optional[Callable[[str], None]] = None) -> None:
    """Full refresh: drop tables and checkpoints of the given layers (Bronze is rebuilt from the landing files)."""
    for layer in layers:
        for entity in ENTITIES:
            spark.sql(f"DROP TABLE IF EXISTS {cfg.table(layer, entity.name)}")
            spark.sql(f"DROP TABLE IF EXISTS {cfg.table(layer, entity.name + '_history')}")
        if layer == "bronze":
            spark.sql(f"DROP TABLE IF EXISTS {cfg.table('control', 'ingestion_watermarks')}")
            if remove_path:
                remove_path(f"{cfg.checkpoint_path}/bronze")
        if layer == "silver":
            spark.sql(f"DROP TABLE IF EXISTS {cfg.table('control', 'silver_progress')}")
            spark.sql(f"DROP TABLE IF EXISTS {cfg.table('control', 'quarantine_records')}")


def run_bronze(spark, cfg: LakehouseConfig, run_id: str, use_autoloader: bool = True) -> list:
    results = [bronze.ingest_entity(spark, entity, cfg, run_id, use_autoloader) for entity in ENTITIES]
    bronze.record_watermarks(spark, cfg, run_id, results)
    for entity in ENTITIES:
        drift = bronze.contract_check(spark, entity, cfg)
        if drift["missing_columns"]:
            raise quality.DataQualityError(f"Bronze {entity.name}: faltan columnas del contrato {drift['missing_columns']}")
        if drift["new_columns"]:
            print(f"AVISO {entity.name}: columnas nuevas en la fuente {drift['new_columns']} (agregadas por schema evolution)")
    return results


def run_silver(spark, cfg: LakehouseConfig, run_id: str, max_error_rate: float = 0.05) -> dict:
    entity_stats = [silver.process_entity(spark, entity, cfg, run_id, max_error_rate) for entity in ENTITIES]
    checks = quality.table_checks(spark, lambda name: cfg.table("silver", name))
    results = silver.row_rule_results(entity_stats, ENTITIES) + checks
    silver.write_quality_results(spark, cfg, run_id, results)
    quality.raise_on_critical(checks)
    return {"entities": entity_stats, "checks": results}
