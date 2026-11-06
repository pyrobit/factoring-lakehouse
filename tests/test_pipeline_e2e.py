"""End-to-end: generate data, run day1 + day2 through every layer, and assert business guarantees.

One module-scoped run keeps the suite fast; each test checks one property.
"""

from __future__ import annotations

import datetime as dt
import json

import pytest
from pyspark.sql import functions as F

from factoring_lakehouse import datagen, pipeline
from factoring_lakehouse.config import load_config

DAY1, DAY2 = dt.date(2026, 10, 31), dt.date(2026, 11, 1)


@pytest.fixture(scope="module")
def run(spark, tmp_path_factory):
    root = tmp_path_factory.mktemp("e2e")
    cfg = load_config(landing=str(root / "landing"), lakehouse=str(root / "lakehouse"),
                      exports=str(root / "exports"), checkpoints=str(root / "chk"))
    manifest = datagen.write(cfg.paths.landing, seed=42, n_clients=10, n_debtors=30, n_invoices=250)
    s1 = pipeline.run(spark, cfg, "day1", DAY1)
    s2 = pipeline.run(spark, cfg, "day2", DAY2)
    return {"cfg": cfg, "manifest": manifest, "day1": s1, "day2": s2}


def _load(spark, cfg, layer, table):
    return spark.read.format("delta").load(cfg.paths.table(layer, table))


def test_quarantine_matches_planted_defects_exactly(spark, run):
    cfg, expected = run["cfg"], run["manifest"]["day1"]["rejected"]
    for entity, key in [("clients", "client_id"), ("invoices", "invoice_id"), ("payments", "payment_id")]:
        q = _load(spark, cfg, "quarantine", entity).where("_batch_id = 'day1'")
        assert sorted(r[key] for r in q.select(key).collect()) == expected[entity], entity
        assert q.where(F.size("_dq_failures") == 0).count() == 0  # every row carries a reason


def test_duplicates_removed_before_merge(run):
    m = run["manifest"]["day1"]
    assert run["day1"]["silver"]["invoices"]["dedup"] == len(m["duplicates"]["invoices"])
    assert run["day1"]["silver"]["payments"]["dedup"] == len(m["duplicates"]["payments"])


def test_silver_keys_are_unique(spark, run):
    cfg = run["cfg"]
    for table, key in [("invoices", "invoice_id"), ("payments", "payment_id"), ("clients", "client_id")]:
        df = _load(spark, cfg, "silver", table)
        assert df.count() == df.select(key).distinct().count(), table


def test_scd2_history_for_rating_changes(spark, run):
    cfg, m = run["cfg"], run["manifest"]["day2"]
    debtors = _load(spark, cfg, "silver", "debtors")
    for debtor_id, (old, new) in m["rating_changes"].items():
        versions = {(r["risk_rating"], r["is_current"]) for r in debtors.where(F.col("debtor_id") == debtor_id)
                    .collect()}
        assert versions == {(old, False), (new, True)}
    for debtor_id in m["replayed_unchanged_debtors"]:
        assert debtors.where(F.col("debtor_id") == debtor_id).count() == 1
    assert debtors.groupBy("debtor_id").agg(F.sum(F.col("is_current").cast("int")).alias("c")) \
        .where("c != 1").count() == 0


def test_stale_late_arriving_updates_are_ignored(spark, run):
    cfg, stale = run["cfg"], run["manifest"]["day2"]["stale_updates"]
    assert run["day2"]["silver"]["invoices"]["stale_skipped"] == len(stale)
    stale_ts = dt.datetime(2026, 7, 1)  # updated_at stamped on the stale CDC events by datagen
    rows = _load(spark, cfg, "silver", "invoices").where(F.col("invoice_id").isin(stale)).collect()
    assert len(rows) == len(stale)
    assert all(r["updated_at"] > stale_ts for r in rows), [(r["invoice_id"], r["updated_at"]) for r in rows]
    assert all(r["_batch_id"] == "day1" for r in rows)  # silver still holds the day-1 version


def test_settled_invoices_are_paid_and_closed(spark, run):
    cfg, settled = run["cfg"], run["manifest"]["day2"]["settled_invoices"]
    snap = _load(spark, cfg, "gold", "portfolio_snapshot").where(F.col("as_of_date") == F.lit(DAY2))
    rows = snap.where(F.col("invoice_id").isin(settled)).select("status", "outstanding_amount").distinct().collect()
    assert {(r["status"], float(r["outstanding_amount"])) for r in rows} == {("PAID", 0.0)}


def test_gold_partitions_are_independent(spark, run):
    snap = _load(spark, run["cfg"], "gold", "portfolio_snapshot")
    dates = {r["as_of_date"] for r in snap.select("as_of_date").distinct().collect()}
    assert dates == {DAY1, DAY2}


def test_rerun_of_day2_is_idempotent(spark, run):
    cfg = run["cfg"]

    def fingerprint():
        return {
            t: _load(spark, cfg, layer, t).count()
            for layer, t in [("silver", "invoices"), ("silver", "debtors"), ("silver", "payments"),
                             ("gold", "portfolio_snapshot"), ("gold", "debtor_features"), ("quarantine", "invoices")]
        }

    before = fingerprint()
    pipeline.run(spark, cfg, "day2", DAY2)
    assert fingerprint() == before


def test_exports_written_for_snowflake(run):
    exports = run["day2"]["exports"]
    assert set(exports) == {"silver_clients", "silver_debtors", "silver_invoices", "silver_payments",
                            "gold_portfolio_snapshot", "gold_debtor_features"}
    assert all(p.endswith("as_of_date=2026-11-01") for p in exports.values())
    json.dumps(run["day2"], default=str)  # summary is serialisable for Airflow XCom


def test_export_manifest_matches_silver(spark, run):
    from factoring_lakehouse.snowflake_io.loader import read_manifest

    cfg = run["cfg"]
    manifest = read_manifest(cfg, DAY2)
    assert manifest["INVOICES"][0] == _load(spark, cfg, "silver", "invoices").count()
    assert manifest["PORTFOLIO_SNAPSHOT"][0] == run["day2"]["gold"]["portfolio_snapshot"]
