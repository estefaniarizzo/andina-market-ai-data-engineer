"""Pure-Python tests of the data contract and the declared quality rules (no Spark needed)."""

from andina_pipeline.config import ENTITIES, ENTITIES_BY_NAME, PARENT_BY_KEY, LakehouseConfig
from andina_pipeline.quality import BUSINESS_RULES, DataQualityError, enforce_error_rate, rules_for

import pytest


def test_every_entity_has_key_and_watermark_rules():
    for entity in ENTITIES:
        names = {rule.name for rule in rules_for(entity)}
        assert f"{entity.key}_not_null" in names
        assert "updated_at_not_null" in names
        assert "updated_at" in entity.column_names


def test_typed_columns_get_parse_rules():
    names = {rule.name for rule in rules_for(ENTITIES_BY_NAME["payments"])}
    assert {"amount_parseable", "payment_date_parseable", "updated_at_parseable"} <= names
    assert "payment_method_parseable" not in names  # strings are never "unparseable"


def test_business_rules_reference_known_entities_and_severities():
    for rule in BUSINESS_RULES:
        assert rule.entity in ENTITIES_BY_NAME
        assert rule.severity in {"error", "warn"}


def test_payment_date_is_conditional_not_required():
    rule = next(r for r in BUSINESS_RULES if r.name == "payment_date_required_when_settled")
    assert "NOT IN ('approved', 'refunded')" in rule.expression
    assert rule.severity == "error"


def test_parents_are_processed_before_children():
    order = [entity.name for entity in ENTITIES]
    for entity in ENTITIES:
        for fk in entity.parents:
            assert order.index(PARENT_BY_KEY[fk].name) < order.index(entity.name)


def test_environment_isolation_with_schema_prefix():
    dev = LakehouseConfig(schema_prefix="dev_")
    assert dev.table("silver", "customers") == "workspace.dev_silver.customers"
    assert dev.landing_path == "/Volumes/workspace/dev_bronze/landing"
    assert LakehouseConfig().bronze == "workspace.bronze"


def test_error_rate_gate():
    enforce_error_rate("orders", total=100, errors=5, max_error_rate=0.05)
    with pytest.raises(DataQualityError):
        enforce_error_rate("orders", total=100, errors=6, max_error_rate=0.05)
    enforce_error_rate("orders", total=0, errors=0, max_error_rate=0.05)
