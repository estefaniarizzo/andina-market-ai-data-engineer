# Databricks notebook source
# MAGIC %md
# MAGIC # Extensión opcional (Nivel 3) — Publicación de Gold
# MAGIC
# MAGIC Modelo dimensional mínimo de ventas a partir de Silver (`dim_customers`, `dim_products`, `dim_date`,
# MAGIC `fact_sales`, `agg_daily_sales`). El alcance principal del proyecto son los Niveles 1 y 2.
# MAGIC
# MAGIC Los ingresos (`agg_daily_sales` y el KPI) excluyen pedidos cancelados y filas con baja lógica.
# MAGIC `fact_sales` conserva los cancelados con `is_revenue = false` para poder analizarlos.

# COMMAND ----------

import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "..", "src")))

from andina_pipeline import gold, pipeline  # noqa: E402
from andina_pipeline.config import LakehouseConfig  # noqa: E402

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema_prefix", "")
cfg = LakehouseConfig(catalog=dbutils.widgets.get("catalog"), schema_prefix=dbutils.widgets.get("schema_prefix"))
pipeline.ensure_schemas(spark, cfg)

# COMMAND ----------

counts = gold.publish(spark, cfg)
for table_name, rows in counts.items():
    print(f"{cfg.table('gold', table_name)}: {rows} rows")

# COMMAND ----------

display(spark.sql(f"""
    SELECT SUM(revenue) AS total_revenue,
           COUNT(DISTINCT order_id) AS total_orders,
           SUM(quantity) AS total_items_sold,
           ROUND(SUM(revenue) / COUNT(DISTINCT order_id), 2) AS average_order_value
    FROM {cfg.table('gold', 'fact_sales')}
    WHERE is_revenue
"""))
