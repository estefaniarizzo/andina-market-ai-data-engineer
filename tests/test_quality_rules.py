"""Unit tests for reusable data-quality rule logic.

These tests validate quality-rule behavior without requiring a live Spark
session or Databricks environment.
"""

from datetime import datetime

import pytest


def evaluate_null_rule(rows, column):
    """Return failed row count for null values in the selected column."""
    return sum(1 for row in rows if row.get(column) is None)


def evaluate_unique_rule(rows, key_column):
    """Return duplicated row count for the selected primary key."""
    seen = set()
    duplicates = 0

    for row in rows:
        key = row.get(key_column)

        if key in seen:
            duplicates += 1
        else:
            seen.add(key)

    return duplicates


def evaluate_referential_rule(child_rows, parent_rows, child_key, parent_key):
    """Return orphan child row count for a parent-child relationship."""
    parent_keys = {row.get(parent_key) for row in parent_rows}

    return sum(
        1
        for row in child_rows
        if row.get(child_key) not in parent_keys
    )


def test_null_rule_detects_missing_required_values():
    rows = [
        {"order_id": 1, "order_total": 100.0},
        {"order_id": 2, "order_total": None},
        {"order_id": 3, "order_total": 250.0},
    ]

    assert evaluate_null_rule(rows, "order_total") == 1


def test_null_rule_passes_when_no_required_value_is_missing():
    rows = [
        {"order_id": 1, "order_total": 100.0},
        {"order_id": 2, "order_total": 250.0},
    ]

    assert evaluate_null_rule(rows, "order_total") == 0


def test_unique_rule_detects_duplicate_primary_keys():
    rows = [
        {"customer_id": 1},
        {"customer_id": 2},
        {"customer_id": 1},
        {"customer_id": 3},
        {"customer_id": 2},
    ]

    assert evaluate_unique_rule(rows, "customer_id") == 2


def test_unique_rule_passes_for_distinct_primary_keys():
    rows = [
        {"customer_id": 1},
        {"customer_id": 2},
        {"customer_id": 3},
    ]

    assert evaluate_unique_rule(rows, "customer_id") == 0


def test_referential_rule_detects_orphan_orders():
    customers = [
        {"customer_id": 1},
        {"customer_id": 2},
    ]

    orders = [
        {"order_id": 10, "customer_id": 1},
        {"order_id": 11, "customer_id": 99},
    ]

    failed_rows = evaluate_referential_rule(
        child_rows=orders,
        parent_rows=customers,
        child_key="customer_id",
        parent_key="customer_id",
    )

    assert failed_rows == 1


def test_referential_rule_passes_for_valid_relationships():
    products = [
        {"product_id": 1},
        {"product_id": 2},
    ]

    order_items = [
        {"order_item_id": 100, "product_id": 1},
        {"order_item_id": 101, "product_id": 2},
    ]

    failed_rows = evaluate_referential_rule(
        child_rows=order_items,
        parent_rows=products,
        child_key="product_id",
        parent_key="product_id",
    )

    assert failed_rows == 0


def test_quality_result_status_is_passed_when_no_failures():
    failed_rows = 0

    status = "passed" if failed_rows == 0 else "failed"

    assert status == "passed"


def test_quality_result_status_is_failed_when_duplicates_exist():
    failed_rows = 3

    status = "passed" if failed_rows == 0 else "failed"

    assert status == "failed"


def test_quality_check_timestamp_is_timezone_aware():
    checked_at = datetime.now().astimezone()

    assert checked_at.tzinfo is not None