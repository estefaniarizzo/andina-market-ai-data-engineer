# Databricks notebook source
# MAGIC %md
# MAGIC # Nivel 2 — Transformación y calidad en Silver
# MAGIC
# MAGIC Convierte tablas Bronze crudas a tablas Silver tipadas, normalizadas,
# MAGIC deduplicadas y validadas. Los resultados de calidad se guardan en
# MAGIC `workspace.control.data_quality_results`.

# COMMAND ----------

from datetime import datetime, timezone
from uuid import uuid4

from pyspark.sql import Row
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DecimalType,
    IntegerType,
    LongType,
)


CATALOG = "workspace"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"
CONTROL_SCHEMA = "control"

BRONZE = f"{CATALOG}.{BRONZE_SCHEMA}"
SILVER = f"{CATALOG}.{SILVER_SCHEMA}"
CONTROL = f"{CATALOG}.{CONTROL_SCHEMA}"

TRANSFORMATION_RUN_ID = str(uuid4())
TRANSFORMED_AT = datetime.now(timezone.utc).replace(tzinfo=None)
DECIMAL_18_2 = DecimalType(18, 2)

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SILVER}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CONTROL}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Funciones auxiliares

# COMMAND ----------

def to_boolean(column_name):
    """Convierte valores CSV true/false o 1/0 a boolean."""
    return (
        F.when(F.lower(F.col(column_name)).isin("true", "1"), F.lit(True))
        .when(F.lower(F.col(column_name)).isin("false", "0"), F.lit(False))
        .otherwise(F.lit(None).cast(BooleanType()))
    )


def add_silver_metadata(df):
    """Agrega trazabilidad de la transformación sin alterar campos de origen."""
    return (
        df
        .withColumn("_transformation_run_id", F.lit(TRANSFORMATION_RUN_ID))
        .withColumn("_transformed_at", F.lit(TRANSFORMED_AT).cast("timestamp"))
    )


def write_silver(df, table_name):
    """Escribe un snapshot idempotente en formato Delta."""
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{SILVER}.{table_name}")
    )
    row_count = spark.table(f"{SILVER}.{table_name}").count()
    print(f"{table_name}: {row_count} rows -> {SILVER}.{table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transformación de entidades

# COMMAND ----------

customers_silver = (
    spark.table(f"{BRONZE}.customers")
    .withColumn("customer_id", F.col("customer_id").cast(LongType()))
    .withColumn("first_name", F.trim(F.col("first_name")))
    .withColumn("last_name", F.trim(F.col("last_name")))
    .withColumn("email", F.lower(F.trim(F.col("email"))))
    .withColumn("phone", F.trim(F.col("phone")))
    .withColumn("city", F.trim(F.col("city")))
    .withColumn("country", F.trim(F.col("country")))
    .withColumn("segment", F.trim(F.col("segment")))
    .withColumn("signup_date", F.to_date("signup_date", "yyyy-MM-dd"))
    .withColumn("created_at", F.to_timestamp("created_at", "yyyy-MM-dd HH:mm:ss"))
    .withColumn("updated_at", F.to_timestamp("updated_at", "yyyy-MM-dd HH:mm:ss"))
    .withColumn("is_deleted", to_boolean("is_deleted"))
    .dropDuplicates(["customer_id"])
)

write_silver(add_silver_metadata(customers_silver), "customers")

# COMMAND ----------

products_silver = (
    spark.table(f"{BRONZE}.products")
    .withColumn("product_id", F.col("product_id").cast(LongType()))
    .withColumn("sku", F.trim(F.col("sku")))
    .withColumn("product_name", F.trim(F.col("product_name")))
    .withColumn("category", F.trim(F.col("category")))
    .withColumn("unit_price", F.col("unit_price").cast(DECIMAL_18_2))
    .withColumn("product_description", F.trim(F.col("product_description")))
    .withColumn("product_status", F.lower(F.trim(F.col("product_status"))))
    .withColumn("created_at", F.to_timestamp("created_at", "yyyy-MM-dd HH:mm:ss"))
    .withColumn("updated_at", F.to_timestamp("updated_at", "yyyy-MM-dd HH:mm:ss"))
    .withColumn("is_deleted", to_boolean("is_deleted"))
    .dropDuplicates(["product_id"])
)

write_silver(add_silver_metadata(products_silver), "products")

# COMMAND ----------

orders_silver = (
    spark.table(f"{BRONZE}.orders")
    .withColumn("order_id", F.col("order_id").cast(LongType()))
    .withColumn("customer_id", F.col("customer_id").cast(LongType()))
    .withColumn("order_date", F.to_timestamp("order_date", "yyyy-MM-dd HH:mm:ss"))
    .withColumn("sales_channel", F.lower(F.trim(F.col("sales_channel"))))
    .withColumn("order_status", F.lower(F.trim(F.col("order_status"))))
    .withColumn("order_total", F.col("order_total").cast(DECIMAL_18_2))
    .withColumn("created_at", F.to_timestamp("created_at", "yyyy-MM-dd HH:mm:ss"))
    .withColumn("updated_at", F.to_timestamp("updated_at", "yyyy-MM-dd HH:mm:ss"))
    .withColumn("is_deleted", to_boolean("is_deleted"))
    .dropDuplicates(["order_id"])
)

write_silver(add_silver_metadata(orders_silver), "orders")

# COMMAND ----------

order_items_silver = (
    spark.table(f"{BRONZE}.order_items")
    .withColumn("order_item_id", F.col("order_item_id").cast(LongType()))
    .withColumn("order_id", F.col("order_id").cast(LongType()))
    .withColumn("product_id", F.col("product_id").cast(LongType()))
    .withColumn("quantity", F.col("quantity").cast(IntegerType()))
    .withColumn("unit_price", F.col("unit_price").cast(DECIMAL_18_2))
    .withColumn("line_total", F.col("line_total").cast(DECIMAL_18_2))
    .withColumn("created_at", F.to_timestamp("created_at", "yyyy-MM-dd HH:mm:ss"))
    .withColumn("updated_at", F.to_timestamp("updated_at", "yyyy-MM-dd HH:mm:ss"))
    .withColumn("is_deleted", to_boolean("is_deleted"))
    .dropDuplicates(["order_item_id"])
)

write_silver(add_silver_metadata(order_items_silver), "order_items")

# COMMAND ----------

payments_silver = (
    spark.table(f"{BRONZE}.payments")
    .withColumn("payment_id", F.col("payment_id").cast(LongType()))
    .withColumn("order_id", F.col("order_id").cast(LongType()))
    .withColumn("payment_method", F.lower(F.trim(F.col("payment_method"))))
    .withColumn("payment_status", F.lower(F.trim(F.col("payment_status"))))
    .withColumn("amount", F.col("amount").cast(DECIMAL_18_2))
    .withColumn("payment_date", F.to_timestamp("payment_date", "yyyy-MM-dd HH:mm:ss"))
    .withColumn("created_at", F.to_timestamp("created_at", "yyyy-MM-dd HH:mm:ss"))
    .withColumn("updated_at", F.to_timestamp("updated_at", "yyyy-MM-dd HH:mm:ss"))
    .withColumn("is_deleted", to_boolean("is_deleted"))
    .dropDuplicates(["payment_id"])
)

write_silver(add_silver_metadata(payments_silver), "payments")

# COMMAND ----------

support_tickets_silver = (
    spark.table(f"{BRONZE}.support_tickets")
    .withColumn("ticket_id", F.col("ticket_id").cast(LongType()))
    .withColumn("customer_id", F.col("customer_id").cast(LongType()))
    .withColumn("subject", F.trim(F.col("subject")))
    .withColumn("ticket_body", F.trim(F.col("ticket_body")))
    .withColumn("ticket_status", F.lower(F.trim(F.col("ticket_status"))))
    .withColumn("priority", F.lower(F.trim(F.col("priority"))))
    .withColumn("created_at", F.to_timestamp("created_at", "yyyy-MM-dd HH:mm:ss"))
    .withColumn("updated_at", F.to_timestamp("updated_at", "yyyy-MM-dd HH:mm:ss"))
    .withColumn("is_deleted", to_boolean("is_deleted"))
    .dropDuplicates(["ticket_id"])
)

write_silver(add_silver_metadata(support_tickets_silver), "support_tickets")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validación de nulos y unicidad de llaves

# COMMAND ----------

table_checks = {
    "customers": {
        "primary_key": "customer_id",
        "required_columns": ["customer_id", "email", "created_at", "updated_at"],
    },
    "products": {
        "primary_key": "product_id",
        "required_columns": ["product_id", "sku", "unit_price", "created_at", "updated_at"],
    },
    "orders": {
        "primary_key": "order_id",
        "required_columns": [
            "order_id",
            "customer_id",
            "order_date",
            "order_total",
            "created_at",
            "updated_at",
        ],
    },
    "order_items": {
        "primary_key": "order_item_id",
        "required_columns": [
            "order_item_id",
            "order_id",
            "product_id",
            "quantity",
            "unit_price",
            "line_total",
        ],
    },
    "payments": {
        "primary_key": "payment_id",
        "required_columns": [
            "payment_id",
            "order_id",
            "amount",
            "payment_date",
            "created_at",
            "updated_at",
        ],
    },
    "support_tickets": {
        "primary_key": "ticket_id",
        "required_columns": ["ticket_id", "customer_id", "created_at", "updated_at"],
    },
}

quality_results = []
QUALITY_RUN_ID = str(uuid4())
QUALITY_CHECKED_AT = datetime.now(timezone.utc).replace(tzinfo=None)

for table_name, checks in table_checks.items():
    df = spark.table(f"{SILVER}.{table_name}")
    total_rows = df.count()

    for column_name in checks["required_columns"]:
        failed_rows = df.filter(F.col(column_name).isNull()).count()

        quality_results.append(
            {
                "quality_run_id": QUALITY_RUN_ID,
                "table_name": table_name,
                "check_name": f"null_{column_name}",
                "check_type": "null_check",
                "failed_rows": failed_rows,
                "total_rows": total_rows,
                "check_status": "passed" if failed_rows == 0 else "warning",
                "checked_at": QUALITY_CHECKED_AT,
            }
        )

    primary_key = checks["primary_key"]
    duplicate_rows = total_rows - df.dropDuplicates([primary_key]).count()

    quality_results.append(
        {
            "quality_run_id": QUALITY_RUN_ID,
            "table_name": table_name,
            "check_name": "duplicate_primary_key",
            "check_type": "uniqueness_check",
            "failed_rows": duplicate_rows,
            "total_rows": total_rows,
            "check_status": "passed" if duplicate_rows == 0 else "failed",
            "checked_at": QUALITY_CHECKED_AT,
        }
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validación de integridad referencial

# COMMAND ----------

customers_df = spark.table(f"{SILVER}.customers").filter(
    ~F.coalesce(F.col("is_deleted"), F.lit(False))
)
products_df = spark.table(f"{SILVER}.products")
orders_df = spark.table(f"{SILVER}.orders").filter(
    ~F.coalesce(F.col("is_deleted"), F.lit(False))
)
order_items_df = spark.table(f"{SILVER}.order_items").filter(
    ~F.coalesce(F.col("is_deleted"), F.lit(False))
)
payments_df = spark.table(f"{SILVER}.payments").filter(
    ~F.coalesce(F.col("is_deleted"), F.lit(False))
)
tickets_df = spark.table(f"{SILVER}.support_tickets").filter(
    ~F.coalesce(F.col("is_deleted"), F.lit(False))
)

orphan_orders_customer = (
    orders_df
    .join(customers_df.select("customer_id"), on="customer_id", how="left_anti")
    .count()
)

orphan_order_items_order = (
    order_items_df
    .join(orders_df.select("order_id"), on="order_id", how="left_anti")
    .count()
)

orphan_order_items_product = (
    order_items_df
    .join(products_df.select("product_id"), on="product_id", how="left_anti")
    .count()
)

orphan_payments_order = (
    payments_df
    .join(orders_df.select("order_id"), on="order_id", how="left_anti")
    .count()
)

orphan_tickets_customer = (
    tickets_df
    .join(customers_df.select("customer_id"), on="customer_id", how="left_anti")
    .count()
)

referential_results = [
    {
        "quality_run_id": QUALITY_RUN_ID,
        "table_name": "orders",
        "check_name": "customer_id_references_customers",
        "check_type": "referential_integrity",
        "failed_rows": orphan_orders_customer,
        "total_rows": orders_df.count(),
        "check_status": "passed" if orphan_orders_customer == 0 else "failed",
        "checked_at": QUALITY_CHECKED_AT,
    },
    {
        "quality_run_id": QUALITY_RUN_ID,
        "table_name": "order_items",
        "check_name": "order_id_references_orders",
        "check_type": "referential_integrity",
        "failed_rows": orphan_order_items_order,
        "total_rows": order_items_df.count(),
        "check_status": "passed" if orphan_order_items_order == 0 else "failed",
        "checked_at": QUALITY_CHECKED_AT,
    },
    {
        "quality_run_id": QUALITY_RUN_ID,
        "table_name": "order_items",
        "check_name": "product_id_references_products",
        "check_type": "referential_integrity",
        "failed_rows": orphan_order_items_product,
        "total_rows": order_items_df.count(),
        "check_status": "passed" if orphan_order_items_product == 0 else "failed",
        "checked_at": QUALITY_CHECKED_AT,
    },
    {
        "quality_run_id": QUALITY_RUN_ID,
        "table_name": "payments",
        "check_name": "order_id_references_orders",
        "check_type": "referential_integrity",
        "failed_rows": orphan_payments_order,
        "total_rows": payments_df.count(),
        "check_status": "passed" if orphan_payments_order == 0 else "failed",
        "checked_at": QUALITY_CHECKED_AT,
    },
    {
        "quality_run_id": QUALITY_RUN_ID,
        "table_name": "support_tickets",
        "check_name": "customer_id_references_customers",
        "check_type": "referential_integrity",
        "failed_rows": orphan_tickets_customer,
        "total_rows": tickets_df.count(),
        "check_status": "passed" if orphan_tickets_customer == 0 else "failed",
        "checked_at": QUALITY_CHECKED_AT,
    },
]

quality_results.extend(referential_results)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Persistencia de resultados de calidad

# COMMAND ----------

quality_results_df = spark.createDataFrame(quality_results)

(
    quality_results_df.write
    .format("delta")
    .mode("append")
    .saveAsTable(f"{CONTROL}.data_quality_results")
)

display(
    spark.sql(f"""
        SELECT
            table_name,
            check_name,
            check_type,
            failed_rows,
            total_rows,
            check_status,
            checked_at
        FROM {CONTROL}.data_quality_results
        WHERE quality_run_id = '{QUALITY_RUN_ID}'
        ORDER BY table_name, check_name
    """)
)

print(f"Silver transformation completed. Run ID: {TRANSFORMATION_RUN_ID}")