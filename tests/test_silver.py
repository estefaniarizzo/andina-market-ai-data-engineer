"""Spark + Delta tests of the real Silver code (typing, dedup, quality gate, MERGE, SCD2)."""

import pytest
from pyspark.sql import functions as F

from andina_pipeline import quality, silver
from andina_pipeline.config import ENTITIES_BY_NAME
from conftest import bronze_df

CUSTOMERS = ENTITIES_BY_NAME["customers"]
ORDERS = ENTITIES_BY_NAME["orders"]
PAYMENTS = ENTITIES_BY_NAME["payments"]


def customer(cid, segment="Regular", updated_at="2026-01-01 10:00:00.000", email=None, phone="300"):
    return {"customer_id": str(cid), "first_name": " Ana ", "last_name": "Ruiz",
            "email": email or f"  ANA{cid}@Example.com ", "phone": phone, "city": "Bogotá", "country": "co",
            "segment": segment, "signup_date": "2025-01-01", "created_at": "2025-01-01 09:00:00",
            "updated_at": updated_at, "is_deleted": "false"}


def order(oid, cid=1, total="100.00", status="paid", updated_at="2026-01-02 10:00:00.123"):
    return {"order_id": str(oid), "customer_id": str(cid), "order_date": "2026-01-02 09:00:00",
            "sales_channel": "WEB", "order_status": status, "order_total": total,
            "created_at": "2026-01-02 09:00:00", "updated_at": updated_at, "is_deleted": "false"}


def checked(spark, entity, rows):
    return quality.evaluate_rules(silver.typed(bronze_df(spark, entity, rows), entity), quality.rules_for(entity))


def test_timestamps_with_and_without_milliseconds(spark):
    rows = checked(spark, ORDERS, [order(1, updated_at="2026-01-02 10:00:00.123"),
                                   order(2, updated_at="2026-01-02 10:00:00"),
                                   order(3, updated_at="not-a-date")]).orderBy("order_id").collect()
    assert rows[0]["updated_at"].microsecond == 123000
    assert rows[1]["updated_at"].second == 0
    assert rows[2]["updated_at"] is None
    assert {"updated_at_parseable", "updated_at_not_null"} <= set(rows[2]["_dq_errors"])
    assert rows[0]["_dq_errors"] == [] and rows[0]["sales_channel"] == "web"


def test_normalization_and_email_warning(spark):
    rows = checked(spark, CUSTOMERS, [customer(1), customer(2, email="sin-dominio@")]).orderBy("customer_id").collect()
    assert rows[0]["email"] == "ana1@example.com" and rows[0]["country"] == "CO" and rows[0]["first_name"] == "Ana"
    assert rows[0]["_dq_warnings"] == []
    assert rows[1]["_dq_warnings"] == ["email_format"] and rows[1]["_dq_errors"] == []


def test_payment_date_only_required_for_settled_payments(spark):
    base = {"order_id": "1", "payment_method": "pse", "amount": "10.00", "created_at": "2026-01-01 00:00:00",
            "updated_at": "2026-01-01 00:00:00", "is_deleted": "false"}
    rows = [{**base, "payment_id": "1", "payment_status": "pending", "payment_date": None},
            {**base, "payment_id": "2", "payment_status": "rejected", "payment_date": None},
            {**base, "payment_id": "3", "payment_status": "approved", "payment_date": None},
            {**base, "payment_id": "4", "payment_status": "approved", "payment_date": "2026-01-01 00:01:00.5"}]
    errors = {r["payment_id"]: r["_dq_errors"] for r in checked(spark, PAYMENTS, rows).collect()}
    assert errors == {1: [], 2: [], 3: ["payment_date_required_when_settled"], 4: []}


def test_latest_per_key_is_deterministic(spark):
    df = silver.typed(bronze_df(spark, CUSTOMERS, [customer(1, "Premium", "2026-02-01 00:00:00"),
                                                   customer(1, "Regular", "2026-01-01 00:00:00")]), CUSTOMERS)
    tie_old = silver.typed(bronze_df(spark, CUSTOMERS, [customer(2, "Regular")], "2026-01-01 00:00:00"), CUSTOMERS)
    tie_new = silver.typed(bronze_df(spark, CUSTOMERS, [customer(2, "Business")], "2026-01-05 00:00:00"), CUSTOMERS)
    latest = {r["customer_id"]: r["segment"]
              for r in silver.latest_per_key(df.unionByName(tie_old).unionByName(tie_new), "customer_id").collect()}
    assert latest == {1: "Premium", 2: "Business"}


def test_merge_latest_never_goes_backwards(spark, cfg):
    target = cfg.table("silver", "customers")
    new = silver.typed(bronze_df(spark, CUSTOMERS, [customer(1, "Premium", "2026-02-01 00:00:00")]), CUSTOMERS)
    old = silver.typed(bronze_df(spark, CUSTOMERS, [customer(1, "Regular", "2026-01-01 00:00:00"),
                                                    customer(2)]), CUSTOMERS)
    silver.merge_latest(spark, target, new.drop("_raw"), "customer_id")
    silver.merge_latest(spark, target, old.drop("_raw"), "customer_id")
    result = {r["customer_id"]: r["segment"] for r in spark.table(target).collect()}
    assert result == {1: "Premium", 2: "Regular"}


def test_scd2_tracks_changes_and_is_idempotent(spark, cfg):
    target = cfg.table("silver", "customers_history")
    tracked = list(CUSTOMERS.history_columns)

    def apply(rows):
        df = silver.typed(bronze_df(spark, CUSTOMERS, rows), CUSTOMERS)
        return silver.apply_scd2(spark, target, df, "customer_id", tracked, "run")

    apply([customer(1, "Regular", "2026-01-01 00:00:00")])
    assert apply([customer(1, "Premium", "2026-02-01 00:00:00")]) == 1
    assert apply([customer(1, "Premium", "2026-02-01 00:00:00")]) == 0  # replay
    assert apply([customer(1, "Premium", "2026-03-01 00:00:00", phone="999")]) == 0  # untracked change

    rows = spark.table(target).orderBy("valid_from").collect()
    assert [(r["segment"], r["is_current"]) for r in rows] == [("Regular", False), ("Premium", True)]
    assert rows[0]["valid_to"] == rows[1]["valid_from"]


def test_scd2_backlog_with_several_versions_in_one_batch(spark, cfg):
    target = cfg.table("silver", "customers_history")
    df = silver.typed(bronze_df(spark, CUSTOMERS, [
        customer(1, "Regular", "2026-01-01 00:00:00"), customer(1, "Premium", "2026-02-01 00:00:00"),
        customer(1, "Premium", "2026-03-01 00:00:00"), customer(1, "Business", "2026-04-01 00:00:00")]), CUSTOMERS)
    silver.apply_scd2(spark, target, df, "customer_id", list(CUSTOMERS.history_columns), "run")
    rows = spark.table(target).orderBy("valid_from").collect()
    assert [r["segment"] for r in rows] == ["Regular", "Premium", "Business"]
    assert [r["is_current"] for r in rows] == [False, False, True]


def _write_bronze(spark, cfg, entity, rows, ingested_at):
    bronze_df(spark, entity, rows, ingested_at).write.format("delta").mode("append") \
        .saveAsTable(cfg.table("bronze", entity.name))


def test_process_entity_quarantine_gate_and_incremental(spark, cfg):
    _write_bronze(spark, cfg, CUSTOMERS, [customer(1), customer(2)], "2026-01-01 00:00:00")
    silver.process_entity(spark, CUSTOMERS, cfg, "run-1")

    _write_bronze(spark, cfg, ORDERS, [order(10), order(11), order(12, cid=999), order(13, total="-1")],
                  "2026-01-01 00:00:00")
    with pytest.raises(quality.DataQualityError):
        silver.process_entity(spark, ORDERS, cfg, "run-1", max_error_rate=0.25)
    assert not spark.catalog.tableExists(cfg.table("silver", "orders"))  # nothing written

    stats = silver.process_entity(spark, ORDERS, cfg, "run-1", max_error_rate=0.5)
    assert stats["rows_quarantined"] == 2 and stats["rows_merged"] == 2
    assert stats["rule_failures"] == {"orphan_customer_id": 1, "order_total_non_negative": 1}
    quarantined = {r["record_key"]: r["errors"] for r in spark.table(cfg.table("control", "quarantine_records")).collect()}
    assert quarantined == {"12": ["orphan_customer_id"], "13": ["order_total_non_negative"]}

    assert silver.process_entity(spark, ORDERS, cfg, "run-2")["rows_processed"] == 0  # nothing new

    _write_bronze(spark, cfg, ORDERS, [order(10, status="shipped", updated_at="2026-01-03 00:00:00")],
                  "2026-01-02 00:00:00")
    stats = silver.process_entity(spark, ORDERS, cfg, "run-3")
    assert stats["rows_processed"] == 1 and stats["history_rows"] == 1
    current = spark.table(cfg.table("silver", "orders")).filter("order_id = 10").first()
    assert current["order_status"] == "shipped"
    history = spark.table(cfg.table("silver", "orders_history")).filter("order_id = 10").orderBy("valid_from")
    assert [r["order_status"] for r in history.collect()] == ["paid", "shipped"]
    assert spark.table(cfg.table("silver", "orders")).select(F.col("_dq_warnings")).first()[0] == []
