"""Pipeline entrypoint. The same code runs locally and as Databricks job tasks.

    python -m factoring_lakehouse.pipeline --batch day1 --as-of 2026-10-31            # all layers
    python -m factoring_lakehouse.pipeline --batch day1 --as-of 2026-10-31 --layer silver
    python -m factoring_lakehouse.pipeline --report --as-of 2026-11-01
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from . import ENTITIES, bronze, gold, silver
from .config import PipelineConfig, get_logger, load_config
from .snowflake_io.loader import build_manifest, manifest_path
from .spark_session import get_spark

log = get_logger(__name__)

# Tables handed off to Snowflake: (layer, table, partitioned_by_as_of)
EXPORTS = [
    ("silver", "clients", False),
    ("silver", "debtors", False),
    ("silver", "invoices", False),
    ("silver", "payments", False),
    ("gold", "portfolio_snapshot", True),
    ("gold", "debtor_features", True),
]


def export_for_snowflake(spark: SparkSession, cfg: PipelineConfig, as_of: dt.date) -> dict[str, str]:
    """Write Parquet files to the export location that the Snowflake external stage points at.

    Layout: ``<exports>/<table>/as_of_date=YYYY-MM-DD/*.parquet``. Silver tables are exported as full
    snapshots (small dimensions/facts); gold only exports the as-of partition. Snowflake then loads
    them with ``COPY INTO`` / Snowpipe, keyed by the as-of folder, which makes reloads idempotent.
    """
    written = {}
    for layer, table, partitioned in EXPORTS:
        df = spark.read.format("delta").load(cfg.paths.table(layer, table))
        if partitioned:
            df = df.where(F.col("as_of_date") == F.lit(as_of)).drop("as_of_date")
        target = f"{cfg.paths.exports}/{layer}_{table}/as_of_date={as_of.isoformat()}"
        df.coalesce(1).write.mode("overwrite").parquet(target)
        written[f"{layer}_{table}"] = target
    manifest = build_manifest(spark, cfg, as_of)
    _publish_manifest(cfg, as_of, manifest)
    log.info(f"step=export as_of={as_of} tables={list(written)}")
    return written


def _publish_manifest(cfg: PipelineConfig, as_of: dt.date, manifest: dict) -> None:
    """Write manifest.json next to the exports (the reconciliation source for Snowflake)."""
    path = manifest_path(cfg, as_of)
    if "://" in path:  # pragma: no cover - on Databricks, write through dbutils / the cloud SDK
        from pyspark.dbutils import DBUtils  # type: ignore[import-not-found]

        DBUtils(SparkSession.getActiveSession()).fs.put(path, json.dumps(manifest), overwrite=True)
        return
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    Path(path).write_text(json.dumps(manifest, indent=2))


def report(spark: SparkSession, cfg: PipelineConfig, as_of: dt.date) -> None:
    snap = spark.read.format("delta").load(cfg.paths.table("gold", "portfolio_snapshot")) \
        .where(F.col("as_of_date") == F.lit(as_of))
    print(f"\n=== Portfolio aging as of {as_of} ===")
    (snap.where("is_open").groupBy("aging_bucket")
     .agg(F.count("*").alias("invoices"), F.sum("outstanding_amount").alias("outstanding"))
     .orderBy(F.expr("array_position(array('CURRENT','1-30','31-60','61-90','90+'), aging_bucket)"))
     .show(truncate=False))

    print(f"=== Debtor concentration breaches (> {cfg.concentration_limit:.0%} of a client's open book) ===")
    by_pair = snap.where("is_open").groupBy("client_id", "debtor_id").agg(F.sum("outstanding_amount").alias("exp"))
    by_client = by_pair.groupBy("client_id").agg(F.sum("exp").alias("client_exp"))
    (by_pair.join(by_client, "client_id")
     .withColumn("share", F.round(F.col("exp") / F.col("client_exp"), 3))
     .where(F.col("share") > cfg.concentration_limit)
     .orderBy(F.desc("share")).show(10, truncate=False))

    print("=== Debtor features (ML) — riskiest 10 by avg days late ===")
    (spark.read.format("delta").load(cfg.paths.table("gold", "debtor_features"))
     .where(F.col("as_of_date") == F.lit(as_of))
     .select("debtor_id", "risk_rating", "open_exposure", "credit_utilization", "avg_days_late_90d",
             "pct_paid_late_90d", "dilution_rate_90d", "max_days_past_due")
     .orderBy(F.desc_nulls_last("avg_days_late_90d")).show(10, truncate=False))

    print("=== Data-quality metrics ===")
    (spark.read.format("delta").load(cfg.paths.table("ops", "dq_metrics"))
     .orderBy("batch_id", "entity").drop("measured_at").show(truncate=False))


def run(spark: SparkSession, cfg: PipelineConfig, batch_id: str, as_of: dt.date, layer: str = "all",
        export: bool = True) -> dict:
    summary: dict = {"batch": batch_id, "as_of": as_of.isoformat()}
    if layer in ("all", "bronze"):
        summary["bronze"] = {e: bronze.ingest_batch(spark, cfg, e, batch_id) for e in ENTITIES}
    if layer in ("all", "silver"):
        summary["silver"] = {s.entity: {"input": s.input_rows, "dedup": s.deduplicated, "rejected": s.rejected,
                                        **s.extra} for s in silver.run(spark, cfg, batch_id)}
    if layer in ("all", "gold"):
        summary["gold"] = gold.run(spark, cfg, as_of)
    if export and layer in ("all", "export"):
        summary["exports"] = export_for_snowflake(spark, cfg, as_of)
    return summary


def main() -> None:
    p = argparse.ArgumentParser(description="Factoring lakehouse pipeline")
    p.add_argument("--batch", help="landing batch id, e.g. day1")
    p.add_argument("--as-of", required=True, type=dt.date.fromisoformat)
    p.add_argument("--layer", default="all", choices=["all", "bronze", "silver", "gold", "export"])
    p.add_argument("--no-export", action="store_true")
    p.add_argument("--report", action="store_true")
    p.add_argument("--config")
    args = p.parse_args()

    cfg = load_config(args.config)
    spark = get_spark()
    if args.report:
        report(spark, cfg, args.as_of)
        return
    if not args.batch and args.layer in ("all", "bronze", "silver"):
        p.error("--batch is required for bronze/silver")
    summary = run(spark, cfg, args.batch, args.as_of, args.layer, export=not args.no_export)
    print(json.dumps(summary, indent=2, default=str))


if __name__ == "__main__":
    main()
