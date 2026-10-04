"""Bronze incremental file ingestion (checkpoint), export serialization and the optional Gold fix."""

import importlib
from datetime import datetime
from decimal import Decimal

from pyspark.sql import functions as F

from andina_pipeline import bronze, gold
from andina_pipeline.config import ENTITIES_BY_NAME

PRODUCTS = ENTITIES_BY_NAME["products"]
HEADER = ",".join(PRODUCTS.column_names)


def _landing_file(cfg, name, rows):
    folder = f"{cfg.landing_path}/products"
    import os
    os.makedirs(folder, exist_ok=True)
    with open(f"{folder}/{name}", "w", encoding="utf-8") as handle:
        handle.write(HEADER + "\n" + "\n".join(rows) + "\n")


def test_bronze_reads_only_new_files_and_keeps_every_version(spark, cfg):
    _landing_file(cfg, "products_1.csv", [
        '1,TEC-001,Audífonos,Tecnología,100.00,"Con ""cancelación"", y coma",active,2026-01-01 00:00:00.000,'
        '2026-01-01 00:00:00.000,false'])
    first = bronze.ingest_entity(spark, PRODUCTS, cfg, "run-1", use_autoloader=False)
    _landing_file(cfg, "products_2.csv", [
        '1,TEC-001,Audífonos,Tecnología,110.00,desc,active,2026-01-01 00:00:00.000,2026-02-01 00:00:00.000,false'])
    second = bronze.ingest_entity(spark, PRODUCTS, cfg, "run-2", use_autoloader=False)
    third = bronze.ingest_entity(spark, PRODUCTS, cfg, "run-3", use_autoloader=False)

    assert (first["rows_read"], second["rows_read"], third["rows_read"]) == (1, 1, 0)
    assert second["watermark"] == datetime(2026, 2, 1)
    table = spark.table(cfg.table("bronze", "products"))
    assert [r["unit_price"] for r in table.orderBy("updated_at").collect()] == ["100.00", "110.00"]
    assert table.filter(F.col("product_description").contains('"cancelación", y coma')).count() == 1
    assert set(bronze.METADATA_COLUMNS) <= set(table.columns)
    assert table.filter(F.col("_source_file").endswith("products_2.csv")).count() == 1
    assert bronze.contract_check(spark, PRODUCTS, cfg) == {"entity": "products", "missing_columns": [],
                                                           "new_columns": []}


def test_export_serializes_milliseconds_and_types():
    export = importlib.import_module("04_export_source_to_csv")
    assert export.serialize_csv_value(datetime(2026, 1, 1, 10, 0, 0, 123456)) == "2026-01-01 10:00:00.123"
    assert export.serialize_csv_value(Decimal("10.50")) == "10.50"
    assert export.serialize_csv_value(True) == "true"
    assert export.serialize_csv_value(None) == ""


def test_gold_daily_sales_excludes_cancelled_and_deleted(spark):
    items = spark.createDataFrame(
        [(1, 10, 100, 1, Decimal("50.00"), Decimal("50.00"), False),
         (2, 11, 100, 2, Decimal("50.00"), Decimal("100.00"), False),
         (3, 12, 100, 1, Decimal("50.00"), Decimal("50.00"), True)],
        "order_item_id int, order_id int, product_id int, quantity int, unit_price decimal(18,2), "
        "line_total decimal(18,2), is_deleted boolean")
    orders = spark.createDataFrame(
        [(10, 1, datetime(2026, 1, 1, 9), "web", "delivered", False),
         (11, 1, datetime(2026, 1, 1, 10), "web", "cancelled", False),
         (12, 1, datetime(2026, 1, 1, 11), "web", "paid", False)],
        "order_id int, customer_id int, order_date timestamp, sales_channel string, order_status string, "
        "is_deleted boolean")
    products = spark.createDataFrame([(100, "Tecnología")], "product_key int, category string")

    fact = gold.build_fact_sales(items, orders)
    assert fact.count() == 3 and fact.filter("is_revenue").count() == 1
    agg = gold.build_agg_daily_sales(fact, products).first()
    assert (agg["total_orders"], agg["total_items_sold"], agg["total_revenue"]) == (1, 1, Decimal("50.00"))
