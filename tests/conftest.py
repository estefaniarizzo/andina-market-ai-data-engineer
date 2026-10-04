import sys
import uuid
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "sql"))


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    from andina_pipeline.spark_session import local_spark

    session = local_spark(str(tmp_path_factory.mktemp("warehouse")), "andina-tests")
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture
def cfg(spark, tmp_path):
    """Isolated schemas per test (prefix) in the local spark_catalog."""
    from andina_pipeline import pipeline
    from andina_pipeline.config import LakehouseConfig

    config = LakehouseConfig(catalog="spark_catalog", schema_prefix=f"t{uuid.uuid4().hex[:8]}_",
                             landing_root=str(tmp_path / "landing"), checkpoint_root=str(tmp_path / "checkpoints"))
    pipeline.ensure_schemas(spark, config)
    return config


def bronze_df(spark, entity, rows, ingested_at="2026-01-01 00:00:00", source_file="file.csv"):
    """Build a Bronze-like DataFrame (all strings + lineage columns) for an entity."""
    from pyspark.sql import functions as F

    columns = entity.column_names
    data = [tuple(row.get(c) for c in columns) for row in rows]
    df = spark.createDataFrame(data, ", ".join(f"{c} string" for c in columns))
    return (df.withColumn("_source_file", F.lit(source_file))
            .withColumn("_ingestion_run_id", F.lit("test-run"))
            .withColumn("_ingested_at", F.to_timestamp(F.lit(ingested_at))))
