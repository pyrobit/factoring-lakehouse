# Databricks notebook source
"""Delta Live Tables variant of the bronze/silver pipeline.

Same contracts and rules as the wheel-based job, expressed declaratively:
  * Auto Loader streaming tables for bronze
  * ``@dlt.expect_all_or_drop`` + a quarantine table built from the inverted rules
  * ``dlt.apply_changes`` for the invoices upsert (SCD1, sequenced by updated_at) and the debtors history
    (SCD2), which handles out-of-order CDC natively

Kept as an alternative deployment so teams can choose DLT (managed and declarative) or plain jobs
(portable and unit-testable). See docs/adr/0004-dlt-and-jobs.md.
"""

import dlt
from pyspark.sql import functions as F

from factoring_lakehouse.contracts import CONTRACTS, derive_rules, spark_schema

LANDING = spark.conf.get("factoring.landing_root")  # noqa: F821  (spark is injected by DLT)


def _sql_expectations(entity: str) -> dict[str, str]:
    """Translate contract rules into DLT SQL expectations (FK and unique are enforced by apply_changes)."""
    out = {}
    for r in derive_rules(CONTRACTS[entity]):
        c = r.column
        if r.kind == "not_null":
            out[r.name] = f"{c} IS NOT NULL"
        elif r.kind == "accepted_values":
            vals = ", ".join(f"'{v}'" for v in r.params["values"])
            out[r.name] = f"{c} IS NULL OR {c} IN ({vals})"
        elif r.kind == "range":
            ops = {"ge": ">=", "gt": ">", "le": "<=", "lt": "<"}
            cond = " AND ".join(f"{c} {ops[k]} {v}" for k, v in r.params.items())
            out[r.name] = f"{c} IS NULL OR ({cond})"
        elif r.kind == "length":
            out[r.name] = (f"{c} IS NULL OR length({c}) BETWEEN {r.params.get('min_length', 0)} "
                           f"AND {r.params.get('max_length', 1 << 30)}")
    return out


def _bronze(entity: str):
    @dlt.table(name=f"bronze_{entity}", comment=f"Raw {entity} from the landing zone (Auto Loader)",
               table_properties={"quality": "bronze", "delta.enableChangeDataFeed": "true"})
    def _t():
        return (
            spark.readStream.format("cloudFiles")  # noqa: F821
            .option("cloudFiles.format", "json")
            .option("cloudFiles.schemaEvolutionMode", "rescue")
            .schema(spark_schema(CONTRACTS[entity]))
            .load(f"{LANDING}/*/{entity}")
            .withColumn("_ingested_at", F.current_timestamp())
            .withColumn("_source_file", F.col("_metadata.file_path"))
        )


def _validated(entity: str):
    rules = _sql_expectations(entity)

    @dlt.view(name=f"{entity}_validated")
    @dlt.expect_all_or_drop(rules)
    def _v():
        return dlt.read_stream(f"bronze_{entity}")

    @dlt.table(name=f"quarantine_{entity}", comment=f"{entity} rows that failed at least one expectation",
               table_properties={"quality": "quarantine"})
    def _q():
        failed_any = " OR ".join(f"NOT ({cond})" for cond in rules.values())
        return dlt.read_stream(f"bronze_{entity}").where(failed_any)


for _entity in CONTRACTS:
    _bronze(_entity)
    _validated(_entity)

dlt.create_streaming_table("silver_invoices", table_properties={"quality": "silver"})
dlt.apply_changes(target="silver_invoices", source="invoices_validated", keys=["invoice_id"],
                  sequence_by=F.col("updated_at"), stored_as_scd_type=1)

dlt.create_streaming_table("silver_debtors", table_properties={"quality": "silver"})
dlt.apply_changes(target="silver_debtors", source="debtors_validated", keys=["debtor_id"],
                  sequence_by=F.col("updated_at"), stored_as_scd_type=2,
                  track_history_except_column_list=["updated_at", "_ingested_at", "_source_file"])

dlt.create_streaming_table("silver_clients", table_properties={"quality": "silver"})
dlt.apply_changes(target="silver_clients", source="clients_validated", keys=["client_id"],
                  sequence_by=F.col("updated_at"), stored_as_scd_type=1)


@dlt.table(name="silver_payments", table_properties={"quality": "silver"})
@dlt.expect_or_drop("invoice_exists", "invoice_id IS NOT NULL")
def silver_payments():
    return dlt.read_stream("payments_validated").withWatermark("event_ts", "2 days") \
        .dropDuplicates(["payment_id"])
