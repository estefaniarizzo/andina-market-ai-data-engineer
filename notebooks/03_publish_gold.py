# Databricks notebook source
# MAGIC %md
# MAGIC # Extensión opcional — Publicación de Gold
# MAGIC
# MAGIC Construye un modelo dimensional mínimo de ventas a partir de Silver.
# MAGIC Esta extensión facilita consultas analíticas, pero el alcance principal
# MAGIC del proyecto corresponde a los Niveles 1 y 2.

# COMMAND ----------

from datetime import datetime, timezone
from uuid import uuid4

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType


CATALOG = "workspace"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"

SILVER = f"{CATALOG}.{SILVER_SCHEMA}"
GOLD = f"{CATALOG}.{GOLD_SCHEMA}"

GOLD_RUN_ID = str(uuid4())
GOLD_BUILT_AT = datetime.now(timezone.utc).replace(tzinfo=None)

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {GOLD}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dimensión de clientes

# COMMAND ----------

dim_customers = (
    spark.table(f"{SILVER}.customers")
    .select(
        F.col("customer_id").alias("customer_key"),
        "customer_id",
        "first_name",
        "last_name",
        "email",
        "phone",
        "city",
        "country",
        "segment",
        "signup_date",
        "is_deleted",
    )
)

(
    dim_customers.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{GOLD}.dim_customers")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dimensión de productos

# COMMAND ----------

dim_products = (
    spark.table(f"{SILVER}.products")
    .select(
        F.col("product_id").alias("product_key"),
        "product_id",
        "sku",
        "product_name",
        "category",
        "unit_price",
        "product_status",
        "is_deleted",
    )
)

(
    dim_products.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{GOLD}.dim_products")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dimensión de fechas

# COMMAND ----------

date_range = (
    spark.table(f"{SILVER}.orders")
    .select(F.to_date("order_date").alias("date_day"))
    .distinct()
)

dim_date = (
    date_range
    .withColumn(
        "date_key",
        F.date_format("date_day", "yyyyMMdd").cast(IntegerType()),
    )
    .withColumn("year", F.year("date_day"))
    .withColumn("month", F.month("date_day"))
    .withColumn("month_name", F.date_format("date_day", "MMMM"))
    .withColumn("day", F.dayofmonth("date_day"))
    .withColumn("day_of_week", F.date_format("date_day", "EEEE"))
    .withColumn("quarter", F.quarter("date_day"))
)

(
    dim_date.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{GOLD}.dim_date")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Hecho de ventas
# MAGIC
# MAGIC El grano de `fact_sales` es una fila por línea de pedido (`order_item`).

# COMMAND ----------

order_items_df = spark.table(f"{SILVER}.order_items").alias("oi")

orders_df = (
    spark.table(f"{SILVER}.orders")
    .select(
        "order_id",
        "customer_id",
        "order_date",
        "sales_channel",
        "order_status",
        "is_deleted",
    )
    .alias("o")
)

fact_sales = (
    order_items_df
    .join(
        orders_df,
        on=F.col("oi.order_id") == F.col("o.order_id"),
        how="inner",
    )
    .withColumn(
        "date_key",
        F.date_format(
            F.to_date(F.col("o.order_date")),
            "yyyyMMdd",
        ).cast(IntegerType()),
    )
    .withColumn("revenue", F.col("oi.line_total"))
    .select(
        F.col("oi.order_item_id").alias("sales_item_key"),
        F.col("oi.order_item_id").alias("order_item_id"),
        F.col("oi.order_id").alias("order_id"),
        F.col("o.customer_id").alias("customer_id"),
        F.col("oi.product_id").alias("product_id"),
        F.col("date_key"),
        F.col("o.sales_channel").alias("sales_channel"),
        F.col("o.order_status").alias("order_status"),
        F.col("oi.quantity").alias("quantity"),
        F.col("oi.unit_price").alias("unit_price"),
        F.col("revenue"),
        F.col("oi.is_deleted").alias("is_deleted"),
    )
)

(
    fact_sales.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{GOLD}.fact_sales")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Agregado diario de ventas

# COMMAND ----------

dim_products_df = spark.table(f"{GOLD}.dim_products").alias("dp")

agg_daily_sales = (
    fact_sales.alias("fs")
    .join(
        dim_products_df,
        on=F.col("fs.product_id") == F.col("dp.product_key"),
        how="inner",
    )
    .groupBy(
        F.col("fs.date_key").alias("date_key"),
        F.col("dp.category").alias("category"),
        F.col("fs.sales_channel").alias("sales_channel"),
    )
    .agg(
        F.countDistinct("fs.order_id").alias("total_orders"),
        F.sum("fs.quantity").alias("total_items_sold"),
        F.sum("fs.revenue").alias("total_revenue"),
    )
)

(
    agg_daily_sales.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{GOLD}.agg_daily_sales")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validación y vista previa

# COMMAND ----------

gold_tables = [
    "dim_customers",
    "dim_products",
    "dim_date",
    "fact_sales",
    "agg_daily_sales",
]

for table_name in gold_tables:
    table_path = f"{GOLD}.{table_name}"
    print(f"{table_path}: {spark.table(table_path).count()} rows")

display(
    spark.sql(f"""
        SELECT
            SUM(revenue) AS total_revenue,
            COUNT(DISTINCT order_id) AS total_orders,
            SUM(quantity) AS total_items_sold,
            ROUND(
                SUM(revenue) / COUNT(DISTINCT order_id),
                2
            ) AS average_order_value
        FROM {GOLD}.fact_sales
        WHERE is_deleted = false
          AND order_status NOT IN ('cancelled', 'canceled')
    """)
)

print(f"Gold publication completed. Run ID: {GOLD_RUN_ID}")