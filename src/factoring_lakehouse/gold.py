"""Gold layer: business-ready, point-in-time tables partitioned by ``as_of_date``.

* ``portfolio_snapshot``: one row per invoice per as-of date. Outstanding balance, aging bucket,
  advance/reserve, dilution, and the debtor's risk rating **as it was on that date** (SCD2 lookup).
  This feeds the Snowflake/dbt BI marts.
* ``debtor_features``: one row per debtor per as-of date, for the late-payment / credit-risk model.
  Only facts known on or before ``as_of_date`` are used (no leakage from the future).

Both are written with dynamic partition overwrite, so recomputing a date is idempotent.
"""

from __future__ import annotations

import datetime as dt

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .config import PipelineConfig, get_logger

log = get_logger(__name__)

OPEN_STATUSES = ("OPEN", "FUNDED", "DISPUTED")


def _as_of_bounds(as_of: dt.date) -> tuple[F.Column, F.Column]:
    """(as_of_date literal, exclusive end-of-day timestamp)."""
    return F.lit(as_of).cast("date"), F.lit(dt.datetime.combine(as_of + dt.timedelta(days=1), dt.time())).cast(
        "timestamp"
    )


def _nonzero(name: str) -> F.Column:
    """NULL instead of 0, so ratios never divide by zero."""
    return F.when(F.col(name) != 0, F.col(name))


def aging_bucket(dpd: F.Column) -> F.Column:
    return (
        F.when(dpd.isNull(), F.lit("CLOSED"))
        .when(dpd <= 0, F.lit("CURRENT"))
        .when(dpd <= 30, F.lit("1-30"))
        .when(dpd <= 60, F.lit("31-60"))
        .when(dpd <= 90, F.lit("61-90"))
        .otherwise(F.lit("90+"))
    )


def build_portfolio_snapshot(
    invoices: DataFrame, payments: DataFrame, debtors_scd2: DataFrame, clients: DataFrame, as_of: dt.date
) -> DataFrame:
    as_of_date, eod = _as_of_bounds(as_of)

    collected = (
        payments.where(F.col("paid_at") < eod)
        .groupBy("invoice_id")
        .agg(F.sum("amount").alias("collected_amount"), F.max("paid_at").alias("last_paid_at"))
    )
    # Point-in-time debtor attributes from the SCD2 history.
    debtor_pit = debtors_scd2.where(
        (F.col("valid_from") < eod) & (F.col("valid_to").isNull() | (F.col("valid_to") >= eod))
    ).select("debtor_id", "debtor_name", F.col("risk_rating").alias("debtor_risk_rating"),
             F.col("credit_limit").alias("debtor_credit_limit"), F.col("country").alias("debtor_country"))

    inv = invoices.where(F.col("invoice_date") <= as_of_date).join(collected, "invoice_id", "left")
    collected_amt = F.coalesce(F.col("collected_amount"), F.lit(0)).cast("decimal(18,2)")
    is_open = F.col("status").isin(*OPEN_STATUSES)
    dpd = F.when(is_open, F.datediff(as_of_date, F.col("due_date")))

    return (
        inv.join(debtor_pit, "debtor_id", "left")
        .join(clients.select("client_id", "client_name", "advance_rate", "fee_rate"), "client_id", "left")
        .select(
            as_of_date.alias("as_of_date"),
            "invoice_id", "client_id", "client_name", "debtor_id", "debtor_name",
            "debtor_risk_rating", "debtor_credit_limit", "debtor_country",
            "invoice_date", "due_date", "status", "amount",
            collected_amt.alias("collected_amount"),
            F.when(is_open, F.greatest(F.col("amount") - collected_amt, F.lit(0)))
            .otherwise(F.lit(0)).cast("decimal(18,2)").alias("outstanding_amount"),
            F.coalesce(F.col("advance_amount"), F.lit(0)).cast("decimal(18,2)").alias("advance_amount"),
            F.when(F.col("advance_amount").isNotNull(), F.col("amount") - F.col("advance_amount"))
            .otherwise(F.lit(0)).cast("decimal(18,2)").alias("reserve_amount"),
            # Dilution: value lost to short pays or cancellations.
            F.when(F.col("status") == "PAID", F.greatest(F.col("amount") - collected_amt, F.lit(0)))
            .when(F.col("status") == "CANCELLED", F.col("amount"))
            .otherwise(F.lit(0)).cast("decimal(18,2)").alias("dilution_amount"),
            (F.col("amount") * F.col("fee_rate")).cast("decimal(18,2)").alias("expected_fee"),
            dpd.alias("days_past_due"),
            aging_bucket(dpd).alias("aging_bucket"),
            is_open.alias("is_open"),
            "last_paid_at",
        )
    )


def build_debtor_features(
    snapshot: DataFrame, payments: DataFrame, invoices: DataFrame, as_of: dt.date,
    lookback_days: int = 90, late_tolerance_days: int = 0,
) -> DataFrame:
    as_of_date, eod = _as_of_bounds(as_of)
    window_start = F.date_sub(as_of_date, lookback_days)

    pay = (
        payments.where(F.col("paid_at") < eod)
        .join(invoices.select("invoice_id", "debtor_id", "invoice_date", "due_date"), "invoice_id")
        .where(F.to_date("paid_at") > window_start)
        .withColumn("days_to_pay", F.datediff(F.to_date("paid_at"), F.col("invoice_date")))
        .withColumn("days_late", F.datediff(F.to_date("paid_at"), F.col("due_date")))
        .groupBy("debtor_id")
        .agg(
            F.count("*").alias("payments_90d"),
            F.round(F.avg("days_to_pay"), 2).alias("avg_days_to_pay_90d"),
            F.round(F.avg("days_late"), 2).alias("avg_days_late_90d"),
            F.round(F.avg((F.col("days_late") > late_tolerance_days).cast("int")), 4).alias("pct_paid_late_90d"),
        )
    )

    recent = snapshot.where(F.col("invoice_date") > window_start)
    activity = recent.groupBy("debtor_id").agg(
        F.count("*").alias("invoices_90d"),
        F.sum("amount").alias("billed_amount_90d"),
        F.sum("dilution_amount").alias("dilution_amount_90d"),
        F.sum((F.col("status") == "DISPUTED").cast("int")).alias("disputes_90d"),
    )
    exposure = snapshot.groupBy("debtor_id").agg(
        F.first("debtor_risk_rating", ignorenulls=True).alias("risk_rating"),
        F.first("debtor_credit_limit", ignorenulls=True).alias("credit_limit"),
        F.sum("outstanding_amount").alias("open_exposure"),
        F.sum(F.when(F.col("days_past_due") > 30, F.col("outstanding_amount")).otherwise(0)).alias("exposure_30plus"),
        F.max("days_past_due").alias("max_days_past_due"),
        F.countDistinct(F.when(F.col("is_open"), F.col("client_id"))).alias("open_clients"),
    )

    return (
        exposure.join(activity, "debtor_id", "left")
        .join(pay, "debtor_id", "left")
        .select(
            as_of_date.alias("as_of_date"),
            "debtor_id", "risk_rating", "credit_limit",
            F.col("open_exposure").cast("decimal(18,2)").alias("open_exposure"),
            F.round(F.col("open_exposure") / _nonzero("credit_limit"), 4).alias("credit_utilization"),
            F.round(F.col("exposure_30plus") / _nonzero("open_exposure"), 4).alias("pct_exposure_30plus"),
            F.coalesce("max_days_past_due", F.lit(0)).alias("max_days_past_due"),
            "open_clients",
            F.coalesce("invoices_90d", F.lit(0)).alias("invoices_90d"),
            F.coalesce("billed_amount_90d", F.lit(0)).cast("decimal(18,2)").alias("billed_amount_90d"),
            F.round(F.col("dilution_amount_90d") / _nonzero("billed_amount_90d"), 4)
            .alias("dilution_rate_90d"),
            F.coalesce("disputes_90d", F.lit(0)).alias("disputes_90d"),
            F.coalesce("payments_90d", F.lit(0)).alias("payments_90d"),
            "avg_days_to_pay_90d", "avg_days_late_90d", "pct_paid_late_90d",
        )
    )


def _write_partition(df: DataFrame, path: str) -> None:
    df.write.format("delta").mode("overwrite").option("partitionOverwriteMode", "dynamic") \
        .partitionBy("as_of_date").save(path)


def run(spark: SparkSession, cfg: PipelineConfig, as_of: dt.date) -> dict[str, int]:
    from .silver import read_silver

    invoices = read_silver(spark, cfg, "invoices")
    payments = read_silver(spark, cfg, "payments")
    debtors = read_silver(spark, cfg, "debtors", current_only=False)
    clients = read_silver(spark, cfg, "clients")

    snapshot = build_portfolio_snapshot(invoices, payments, debtors, clients, as_of).cache()
    _write_partition(snapshot, cfg.paths.table("gold", "portfolio_snapshot"))
    features = build_debtor_features(snapshot, payments, invoices, as_of, cfg.feature_lookback_days,
                                     cfg.late_tolerance_days)
    _write_partition(features, cfg.paths.table("gold", "debtor_features"))

    counts = {"portfolio_snapshot": snapshot.count(), "debtor_features": features.count()}
    log.info(f"layer=gold as_of={as_of} rows={counts}")
    return counts
