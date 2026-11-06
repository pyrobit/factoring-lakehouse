# Databricks notebook source
# MAGIC %md
# MAGIC ## Delta table maintenance
# MAGIC `OPTIMIZE` + Z-ORDER on the main access paths, then `VACUUM` (7-day retention keeps time travel
# MAGIC for audits and lets late readers finish). Runs after each daily load.

# COMMAND ----------
dbutils.widgets.text("lakehouse_root", "")  # noqa: F821
root = dbutils.widgets.get("lakehouse_root")  # noqa: F821

TABLES = {
    "silver/invoices": ["client_id", "debtor_id"],
    "silver/payments": ["invoice_id"],
    "silver/debtors": ["debtor_id"],
    "gold/portfolio_snapshot": ["client_id", "debtor_id"],
    "gold/debtor_features": ["debtor_id"],
}

# COMMAND ----------
for table, zorder in TABLES.items():
    path = f"{root}/{table}"
    spark.sql(f"OPTIMIZE delta.`{path}` ZORDER BY ({', '.join(zorder)})")  # noqa: F821
    spark.sql(f"VACUUM delta.`{path}` RETAIN 168 HOURS")  # noqa: F821
    print(f"maintained {path}")
