"""Data quality as code: declarative rules evaluated in Silver.

Severity:
- error: the row is sent to quarantine (control.quarantine_records) and is not
  merged into Silver. If the share of error rows in a batch exceeds
  `max_error_rate`, the whole batch fails before writing (the pipeline stops).
- warn:  the row is kept, the failed rule names are stored in `_dq_warnings`
  and the counts are reported in control.data_quality_results.
- critical (table checks only): the Silver task fails after the write, e.g.
  duplicate primary keys or more than one current SCD2 version.
"""

from dataclasses import dataclass
from typing import List

from pyspark.sql import functions as F

from .config import ENTITIES, PARENT_BY_KEY, EntitySpec

COUNTRIES = ("CO", "MX", "CL", "PE", "AR")


class DataQualityError(Exception):
    """Raised when data quality is bad enough to stop the pipeline."""


@dataclass(frozen=True)
class RowRule:
    name: str
    entity: str
    expression: str  # Spark SQL boolean expression; TRUE means the row is valid
    severity: str = "error"
    description: str = ""


def _in(column, values):
    return f"{column} IN ({', '.join(repr(v) for v in values)})"


BUSINESS_RULES: List[RowRule] = [
    # customers
    RowRule("country_domain", "customers", _in("country", COUNTRIES), "error", "País fuera del alcance"),
    RowRule("segment_domain", "customers", _in("segment", ("Regular", "Premium", "Business"))),
    RowRule("email_format", "customers", "email RLIKE '^[^@ ]+@[^@ ]+[.][^@ ]+$'", "warn",
            "Email inválido: se conserva el cliente pero no sirve para contacto"),
    RowRule("created_before_updated", "customers", "created_at <= updated_at", "warn"),
    # products
    RowRule("sku_not_null", "products", "sku IS NOT NULL"),
    RowRule("unit_price_non_negative", "products", "unit_price >= 0"),
    RowRule("product_status_domain", "products", _in("product_status", ("active", "inactive", "discontinued"))),
    # orders
    RowRule("customer_id_not_null", "orders", "customer_id IS NOT NULL"),
    RowRule("order_date_not_null", "orders", "order_date IS NOT NULL"),
    RowRule("order_status_domain", "orders",
            _in("order_status", ("created", "paid", "shipped", "delivered", "cancelled"))),
    RowRule("sales_channel_domain", "orders", _in("sales_channel", ("web", "app", "store"))),
    RowRule("order_total_non_negative", "orders", "order_total >= 0"),
    RowRule("order_date_not_in_future", "orders",
            "order_date IS NULL OR order_date <= current_timestamp() + INTERVAL 1 HOUR", "warn"),
    # order_items
    RowRule("order_id_not_null", "order_items", "order_id IS NOT NULL"),
    RowRule("product_id_not_null", "order_items", "product_id IS NOT NULL"),
    RowRule("quantity_positive", "order_items", "quantity > 0"),
    RowRule("unit_price_non_negative", "order_items", "unit_price >= 0"),
    RowRule("line_total_consistent", "order_items", "abs(line_total - quantity * unit_price) < 0.01", "warn"),
    # payments
    RowRule("order_id_not_null", "payments", "order_id IS NOT NULL"),
    RowRule("payment_status_domain", "payments", _in("payment_status", ("pending", "approved", "rejected", "refunded"))),
    RowRule("payment_method_domain", "payments",
            _in("payment_method", ("credit_card", "debit_card", "pse", "cash", "wallet"))),
    RowRule("amount_non_negative", "payments", "amount >= 0"),
    RowRule("payment_date_required_when_settled", "payments",
            "payment_status NOT IN ('approved', 'refunded') OR payment_date IS NOT NULL", "error",
            "Pending/rejected pueden no tener fecha; approved/refunded sí deben tenerla"),
    # support_tickets
    RowRule("customer_id_not_null", "support_tickets", "customer_id IS NOT NULL"),
    RowRule("ticket_status_domain", "support_tickets", _in("ticket_status", ("open", "in_progress", "resolved", "closed"))),
    RowRule("priority_domain", "support_tickets", _in("priority", ("low", "medium", "high", "urgent"))),
    RowRule("ticket_body_not_blank", "support_tickets", "ticket_body IS NOT NULL AND length(ticket_body) > 0", "warn"),
]


def rules_for(entity: EntitySpec) -> List[RowRule]:
    """Generic contract rules (key, watermark, parseable types) plus business rules."""
    rules = [
        RowRule(f"{entity.key}_not_null", entity.name, f"{entity.key} IS NOT NULL"),
        RowRule("updated_at_not_null", entity.name, "updated_at IS NOT NULL",
                description="Sin updated_at no se puede ordenar versiones ni avanzar el watermark"),
    ]
    for column in entity.columns:
        if column.dtype != "string":
            rules.append(RowRule(f"{column.name}_parseable", entity.name,
                                 f"_raw.{column.name} IS NULL OR {column.name} IS NOT NULL",
                                 description=f"El valor fuente no se puede convertir a {column.dtype}"))
    return rules + [rule for rule in BUSINESS_RULES if rule.entity == entity.name]


def _failed(rules: List[RowRule]):
    checks = [F.when(~F.coalesce(F.expr(rule.expression), F.lit(False)), F.lit(rule.name)) for rule in rules]
    if not checks:
        return F.array().cast("array<string>")
    return F.filter(F.array(*checks), lambda name: name.isNotNull())


def evaluate_rules(df, rules: List[RowRule]):
    """Add `_dq_errors` and `_dq_warnings` (arrays with the names of the failed rules)."""
    return (
        df.withColumn("_dq_errors", _failed([r for r in rules if r.severity == "error"]))
        .withColumn("_dq_warnings", _failed([r for r in rules if r.severity == "warn"]))
    )


def add_referential_checks(spark, df, entity: EntitySpec, silver_table_name):
    """Flag rows whose foreign key does not exist in the parent Silver table."""
    for fk in entity.parents:
        parent_table = silver_table_name(fk)
        parent_keys = spark.table(parent_table).select(F.col(fk).alias("_parent_key")).distinct()
        df = (
            df.join(parent_keys, df[fk] == parent_keys["_parent_key"], "left")
            .withColumn("_dq_errors", F.when(
                F.col(fk).isNotNull() & F.col("_parent_key").isNull(),
                F.concat(F.col("_dq_errors"), F.array(F.lit(f"orphan_{fk}"))),
            ).otherwise(F.col("_dq_errors")))
            .drop("_parent_key")
        )
    return df


def enforce_error_rate(entity_name: str, total: int, errors: int, max_error_rate: float) -> None:
    if total and errors / total > max_error_rate:
        raise DataQualityError(
            f"{entity_name}: {errors}/{total} filas con errores ({errors / total:.1%}) superan el umbral "
            f"de {max_error_rate:.1%}. No se escribió el lote; revisar control.quarantine_records."
        )


# ---------------------------------------------------------------------------
# Table-level checks over the resulting Silver tables
# ---------------------------------------------------------------------------

def _result(table, check, check_type, severity, failed, total):
    return {"table_name": table, "check_name": check, "check_type": check_type, "severity": severity,
            "failed_rows": int(failed), "total_rows": int(total),
            "check_status": "passed" if failed == 0 else ("failed" if severity in ("error", "critical") else "warning")}


def table_checks(spark, silver_table_name) -> list:
    t = silver_table_name
    results = []

    for entity in ENTITIES:
        df = spark.table(t(entity.name))
        total = df.count()
        duplicates = total - df.select(entity.key).distinct().count()
        results.append(_result(entity.name, f"unique_{entity.key}", "uniqueness", "critical", duplicates, total))
        for fk in entity.parents:
            orphans = df.join(spark.table(t(PARENT_BY_KEY[fk].name)).select(fk), fk, "left_anti").count()
            results.append(_result(entity.name, f"{fk}_references_parent", "referential_integrity",
                                   "error", orphans, total))
        if entity.history_columns:
            history = spark.table(t(f"{entity.name}_history"))
            bad_current = (history.groupBy(entity.key)
                           .agg(F.sum(F.col("is_current").cast("int")).alias("n"))
                           .filter("n <> 1").count())
            results.append(_result(f"{entity.name}_history", "one_current_version_per_key", "scd2",
                                   "critical", bad_current, history.select(entity.key).distinct().count()))

    orders = spark.table(t("orders")).filter("NOT is_deleted")
    items = spark.table(t("order_items")).filter("NOT is_deleted")
    payments = spark.table(t("payments")).filter("NOT is_deleted")
    total_orders = orders.count()

    line_totals = items.groupBy("order_id").agg(F.sum("line_total").alias("lines_total"))
    mismatched = (orders.join(line_totals, "order_id", "left")
                  .filter(F.abs(F.col("order_total") - F.coalesce("lines_total", F.lit(0))) > 0.01).count())
    results.append(_result("orders", "order_total_matches_lines", "business", "warn", mismatched, total_orders))

    approved = (payments.filter("payment_status = 'approved'").groupBy("order_id")
                .agg(F.sum("amount").alias("approved_amount"), F.count("*").alias("approved_payments")))
    overcharged = (orders.join(approved, "order_id")
                   .filter((F.col("approved_amount") > F.col("order_total") + 0.01) | (F.col("approved_payments") > 1))
                   .count())
    results.append(_result("payments", "no_duplicate_or_excess_charges", "business", "warn",
                           overcharged, total_orders))

    unpaid_fulfilled = (orders.filter(F.col("order_status").isin("paid", "shipped", "delivered"))
                        .join(approved, "order_id", "left_anti").count())
    results.append(_result("orders", "fulfilled_orders_have_approved_payment", "business", "warn",
                           unpaid_fulfilled, total_orders))
    return results


def raise_on_critical(results) -> None:
    failed = [r for r in results if r["severity"] == "critical" and r["failed_rows"] > 0]
    if failed:
        details = ", ".join(f"{r['table_name']}.{r['check_name']}={r['failed_rows']}" for r in failed)
        raise DataQualityError(f"Checks críticos fallidos en Silver: {details}")
