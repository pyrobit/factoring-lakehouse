"""Invoice-factoring lakehouse: Databricks (Spark/Delta) medallion pipeline feeding Snowflake."""

__version__ = "0.1.0"

ENTITIES = ("clients", "debtors", "invoices", "payments")
