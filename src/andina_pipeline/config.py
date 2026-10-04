"""Configuration and data contracts for the Azure SQL -> Databricks lakehouse.

No secrets live here. Environment differences (dev/staging/prod) are expressed
with `catalog` and `schema_prefix`, which the Databricks Bundle passes as job
parameters (e.g. `workspace.dev_bronze` in dev, `workspace.bronze` in prod).
"""

from dataclasses import dataclass
from typing import Optional, Tuple

LAYERS = ("bronze", "silver", "gold", "control")


@dataclass(frozen=True)
class LakehouseConfig:
    """Unity Catalog names and storage paths for one environment."""

    catalog: str = "workspace"
    schema_prefix: str = ""
    landing_volume: str = "landing"
    checkpoint_volume: str = "checkpoints"
    landing_root: Optional[str] = None  # override for local runs
    checkpoint_root: Optional[str] = None  # override for local runs

    def schema(self, layer: str) -> str:
        if layer not in LAYERS:
            raise ValueError(f"Capa desconocida: {layer}")
        return f"{self.catalog}.{self.schema_prefix}{layer}"

    def table(self, layer: str, name: str) -> str:
        return f"{self.schema(layer)}.{name}"

    @property
    def bronze(self) -> str:
        return self.schema("bronze")

    @property
    def silver(self) -> str:
        return self.schema("silver")

    @property
    def gold(self) -> str:
        return self.schema("gold")

    @property
    def control(self) -> str:
        return self.schema("control")

    @property
    def landing_path(self) -> str:
        """Unity Catalog Volume where the Azure SQL exporter drops CSV files."""
        return self.landing_root or f"/Volumes/{self.catalog}/{self.schema_prefix}bronze/{self.landing_volume}"

    @property
    def checkpoint_path(self) -> str:
        """Volume holding Auto Loader schema locations and streaming checkpoints."""
        return self.checkpoint_root or f"/Volumes/{self.catalog}/{self.schema_prefix}control/{self.checkpoint_volume}"


@dataclass(frozen=True)
class Column:
    name: str
    dtype: str  # string | bigint | int | decimal(18,2) | timestamp | date | boolean
    normalize: Optional[str] = None  # lower | upper | initcap (strings are always trimmed)


@dataclass(frozen=True)
class EntitySpec:
    """Data contract of one source table: columns, key, parents and SCD2 columns."""

    name: str
    source_table: str
    key: str
    columns: Tuple[Column, ...]
    parents: Tuple[str, ...] = ()  # foreign-key columns; the parent entity has the same key name
    history_columns: Tuple[str, ...] = ()  # tracked as SCD Type 2 in silver.<name>_history

    @property
    def column_names(self):
        return [column.name for column in self.columns]


def _audit_columns():
    return (
        Column("created_at", "timestamp"),
        Column("updated_at", "timestamp"),
        Column("is_deleted", "boolean"),
    )


MONEY = "decimal(18,2)"

# Ordered so parents are processed before children (referential integrity checks).
ENTITIES: Tuple[EntitySpec, ...] = (
    EntitySpec(
        name="customers",
        source_table="dbo.Customers",
        key="customer_id",
        columns=(
            Column("customer_id", "bigint"),
            Column("first_name", "string"),
            Column("last_name", "string"),
            Column("email", "string", "lower"),
            Column("phone", "string"),
            Column("city", "string"),
            Column("country", "string", "upper"),
            Column("segment", "string", "initcap"),
            Column("signup_date", "date"),
        ) + _audit_columns(),
        history_columns=("segment", "email", "city", "country", "is_deleted"),
    ),
    EntitySpec(
        name="products",
        source_table="dbo.Products",
        key="product_id",
        columns=(
            Column("product_id", "bigint"),
            Column("sku", "string", "upper"),
            Column("product_name", "string"),
            Column("category", "string"),
            Column("unit_price", MONEY),
            Column("product_description", "string"),
            Column("product_status", "string", "lower"),
        ) + _audit_columns(),
        history_columns=("unit_price", "product_status", "is_deleted"),
    ),
    EntitySpec(
        name="orders",
        source_table="dbo.Orders",
        key="order_id",
        columns=(
            Column("order_id", "bigint"),
            Column("customer_id", "bigint"),
            Column("order_date", "timestamp"),
            Column("sales_channel", "string", "lower"),
            Column("order_status", "string", "lower"),
            Column("order_total", MONEY),
        ) + _audit_columns(),
        parents=("customer_id",),
        history_columns=("order_status", "order_total", "is_deleted"),
    ),
    EntitySpec(
        name="order_items",
        source_table="dbo.OrderItems",
        key="order_item_id",
        columns=(
            Column("order_item_id", "bigint"),
            Column("order_id", "bigint"),
            Column("product_id", "bigint"),
            Column("quantity", "int"),
            Column("unit_price", MONEY),
            Column("line_total", MONEY),
        ) + _audit_columns(),
        parents=("order_id", "product_id"),
    ),
    EntitySpec(
        name="payments",
        source_table="dbo.Payments",
        key="payment_id",
        columns=(
            Column("payment_id", "bigint"),
            Column("order_id", "bigint"),
            Column("payment_method", "string", "lower"),
            Column("payment_status", "string", "lower"),
            Column("amount", MONEY),
            Column("payment_date", "timestamp"),
        ) + _audit_columns(),
        parents=("order_id",),
        history_columns=("payment_status", "amount", "payment_date", "is_deleted"),
    ),
    EntitySpec(
        name="support_tickets",
        source_table="dbo.SupportTickets",
        key="ticket_id",
        columns=(
            Column("ticket_id", "bigint"),
            Column("customer_id", "bigint"),
            Column("subject", "string"),
            Column("ticket_body", "string"),
            Column("ticket_status", "string", "lower"),
            Column("priority", "string", "lower"),
        ) + _audit_columns(),
        parents=("customer_id",),
    ),
)

ENTITIES_BY_NAME = {entity.name: entity for entity in ENTITIES}
PARENT_BY_KEY = {entity.key: entity for entity in ENTITIES}

GOLD_TABLES = ["dim_customers", "dim_products", "dim_date", "fact_sales", "agg_daily_sales"]
