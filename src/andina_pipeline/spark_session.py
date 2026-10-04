"""Local SparkSession with Delta Lake, used by tests and scripts/run_local_pipeline.py."""

import os


def local_spark(warehouse_dir: str = "spark-warehouse", app_name: str = "andina-local"):
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.master("local[2]").appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", warehouse_dir)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.extraJavaOptions", f"-Dderby.system.home={warehouse_dir}")
        .enableHiveSupport()
    )
    jars = os.getenv("DELTA_JARS")
    if jars:
        return builder.config("spark.jars", jars).getOrCreate()
    from delta import configure_spark_with_delta_pip

    return configure_spark_with_delta_pip(builder).getOrCreate()
