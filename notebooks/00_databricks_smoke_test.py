# Databricks notebook source
# MAGIC %md
# MAGIC # Smoke test de Databricks
# MAGIC
# MAGIC Verifica que el workspace, Unity Catalog y Spark estén disponibles
# MAGIC antes de ejecutar la ingesta y las transformaciones.

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "workspace"

print(f"Spark version: {spark.version}")
print(f"Current catalog: {spark.sql('SELECT current_catalog()').first()[0]}")

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW SCHEMAS IN workspace;

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.bronze")
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.silver")
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.gold")
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.control")

print("Schemas verified: bronze, silver, gold, control")

# COMMAND ----------

display(
    spark.sql("""
        SELECT
            current_catalog() AS current_catalog,
            current_schema() AS current_schema,
            current_timestamp() AS validated_at
    """)
)