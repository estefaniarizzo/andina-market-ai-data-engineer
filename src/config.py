"""Centralized configuration for the Azure SQL to Databricks Lakehouse project.

This module contains environment-independent constants used by the Databricks
notebooks and validation utilities. It intentionally avoids secrets and
environment-specific credentials.
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class LakehouseConfig:
    """Configuration for the Unity Catalog lakehouse layers."""

    catalog: str = "workspace"
    bronze_schema: str = "bronze"
    silver_schema: str = "silver"
    gold_schema: str = "gold"
    control_schema: str = "control"

    landing_volume: str = "landing"

    @property
    def bronze(self) -> str:
        """Fully qualified Bronze schema."""
        return f"{self.catalog}.{self.bronze_schema}"

    @property
    def silver(self) -> str:
        """Fully qualified Silver schema."""
        return f"{self.catalog}.{self.silver_schema}"

    @property
    def gold(self) -> str:
        """Fully qualified Gold schema."""
        return f"{self.catalog}.{self.gold_schema}"

    @property
    def control(self) -> str:
        """Fully qualified control schema."""
        return f"{self.catalog}.{self.control_schema}"

    @property
    def landing_path(self) -> str:
        """Unity Catalog Volume path containing source CSV exports."""
        return (
            f"/Volumes/{self.catalog}/{self.bronze_schema}/{self.landing_volume}"
        )


@dataclass(frozen=True)
class SourceConfig:
    """Mapping between Bronze target tables and Azure SQL source tables."""

    source_system: str = "azure_sql"

    tables: dict[str, str] = None

    def __post_init__(self):
        if self.tables is None:
            object.__setattr__(
                self,
                "tables",
                {
                    "customers": "dbo.Customers",
                    "products": "dbo.Products",
                    "orders": "dbo.Orders",
                    "order_items": "dbo.OrderItems",
                    "payments": "dbo.Payments",
                    "support_tickets": "dbo.SupportTickets",
                },
            )


LAKEHOUSE = LakehouseConfig()
SOURCE = SourceConfig()

BRONZE_TABLES = [
    "customers",
    "products",
    "orders",
    "order_items",
    "payments",
    "support_tickets",
]

GOLD_TABLES = [
    "dim_customers",
    "dim_products",
    "dim_date",
    "fact_sales",
    "agg_daily_sales",
]