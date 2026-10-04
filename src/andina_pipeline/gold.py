"""Optional Gold layer (level 3): minimal star schema for sales analytics.

Revenue only counts orders that are not cancelled and rows that are not
soft-deleted; cancelled orders stay in fact_sales with their status so they can
still be analysed, but `is_revenue` = false.
"""

from pyspark.sql import functions as F

from .config import LakehouseConfig


def _overwrite(df, table):
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table)


def build_fact_sales(order_items, orders):
    o = orders.select("order_id", "customer_id", "order_date", "sales_channel", "order_status",
                      F.col("is_deleted").alias("order_is_deleted"))
    return (
        order_items.join(o, "order_id")
        .select(
            F.col("order_item_id").alias("sales_item_key"),
            "order_item_id", "order_id", "customer_id", "product_id",
            F.date_format(F.to_date("order_date"), "yyyyMMdd").cast("int").alias("date_key"),
            "sales_channel", "order_status", "quantity", "unit_price",
            F.col("line_total").alias("revenue"),
            (F.col("is_deleted") | F.col("order_is_deleted")).alias("is_deleted"),
        )
        .withColumn("is_revenue", (F.col("order_status") != "cancelled") & ~F.col("is_deleted"))
    )


def build_agg_daily_sales(fact_sales, dim_products):
    return (
        fact_sales.filter("is_revenue")
        .join(dim_products.select(F.col("product_key").alias("product_id"), "category"), "product_id")
        .groupBy("date_key", "category", "sales_channel")
        .agg(F.countDistinct("order_id").alias("total_orders"),
             F.sum("quantity").alias("total_items_sold"),
             F.sum("revenue").alias("total_revenue"))
    )


def publish(spark, cfg: LakehouseConfig) -> dict:
    s = lambda name: spark.table(cfg.table("silver", name))  # noqa: E731
    g = lambda name: cfg.table("gold", name)  # noqa: E731

    dim_customers = s("customers").select(
        F.col("customer_id").alias("customer_key"), "customer_id", "first_name", "last_name", "email", "phone",
        "city", "country", "segment", "signup_date", "is_deleted")
    dim_products = s("products").select(
        F.col("product_id").alias("product_key"), "product_id", "sku", "product_name", "category", "unit_price",
        "product_status", "is_deleted")
    dim_date = (s("orders").select(F.to_date("order_date").alias("date_day")).distinct()
                .withColumn("date_key", F.date_format("date_day", "yyyyMMdd").cast("int"))
                .withColumn("year", F.year("date_day")).withColumn("quarter", F.quarter("date_day"))
                .withColumn("month", F.month("date_day")).withColumn("month_name", F.date_format("date_day", "MMMM"))
                .withColumn("day", F.dayofmonth("date_day")).withColumn("day_of_week", F.date_format("date_day", "EEEE")))
    fact_sales = build_fact_sales(s("order_items"), s("orders"))
    agg_daily_sales = build_agg_daily_sales(fact_sales, dim_products)

    tables = {"dim_customers": dim_customers, "dim_products": dim_products, "dim_date": dim_date,
              "fact_sales": fact_sales, "agg_daily_sales": agg_daily_sales}
    for name, df in tables.items():
        _overwrite(df, g(name))
    return {name: spark.table(g(name)).count() for name in tables}
