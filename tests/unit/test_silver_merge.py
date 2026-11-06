import datetime as dt

from factoring_lakehouse.silver import latest_per_key, merge_insert_only, merge_scd2, merge_upsert

T0 = dt.datetime(2026, 10, 1)
T1 = dt.datetime(2026, 10, 2)
T2 = dt.datetime(2026, 10, 3)

DEBTOR_SCHEMA = "debtor_id string, risk_rating string, credit_limit int, updated_at timestamp"
INV_SCHEMA = "invoice_id string, status string, updated_at timestamp, _ingested_at timestamp"


def _debtors(spark, rows):
    return spark.createDataFrame(rows, DEBTOR_SCHEMA)


def _history(spark, path):
    return {(r["debtor_id"], r["risk_rating"], r["is_current"]) for r in
            spark.read.format("delta").load(path).collect()}


def test_latest_per_key_keeps_newest_and_passes_null_keys(spark):
    df = spark.createDataFrame(
        [("I1", "OPEN", T0, T0), ("I1", "PAID", T1, T0), (None, "OPEN", T0, T0), (None, "OPEN", T0, T0)],
        INV_SCHEMA,
    )
    out = latest_per_key(df, "invoice_id").collect()
    assert len(out) == 3
    assert [r["status"] for r in out if r["invoice_id"] == "I1"] == ["PAID"]


def test_scd2_change_closes_old_version_and_inserts_new(spark, tmp_path):
    path = str(tmp_path / "debtors")
    merge_scd2(spark, _debtors(spark, [("D1", "A", 100, T0), ("D2", "B", 50, T0)]), path, "debtor_id",
               ["risk_rating", "credit_limit"])
    stats = merge_scd2(spark, _debtors(spark, [("D1", "C", 100, T1), ("D3", "A", 10, T1)]), path, "debtor_id",
                       ["risk_rating", "credit_limit"])
    assert stats == {"inserted": 2, "closed": 1}
    assert _history(spark, path) == {("D1", "A", False), ("D1", "C", True), ("D2", "B", True), ("D3", "A", True)}
    closed = spark.read.format("delta").load(path).where("debtor_id = 'D1' AND NOT is_current").first()
    assert closed["valid_to"] == T1


def test_scd2_unchanged_replay_is_noop(spark, tmp_path):
    path = str(tmp_path / "debtors")
    rows = [("D1", "A", 100, T0)]
    merge_scd2(spark, _debtors(spark, rows), path, "debtor_id", ["risk_rating", "credit_limit"])
    stats = merge_scd2(spark, _debtors(spark, [("D1", "A", 100, T1)]), path, "debtor_id",
                       ["risk_rating", "credit_limit"])
    assert stats["inserted"] == 0 and stats["closed"] == 0
    assert spark.read.format("delta").load(path).count() == 1


def test_scd2_out_of_order_version_is_ignored(spark, tmp_path):
    path = str(tmp_path / "debtors")
    merge_scd2(spark, _debtors(spark, [("D1", "B", 100, T1)]), path, "debtor_id", ["risk_rating", "credit_limit"])
    merge_scd2(spark, _debtors(spark, [("D1", "D", 100, T0)]), path, "debtor_id", ["risk_rating", "credit_limit"])
    assert _history(spark, path) == {("D1", "B", True)}


def test_upsert_guard_ignores_stale_updates(spark, tmp_path):
    path = str(tmp_path / "invoices")
    merge_upsert(spark, spark.createDataFrame([("I1", "PAID", T2, T2)], INV_SCHEMA), path, "invoice_id",
                 "updated_at")
    stats = merge_upsert(
        spark, spark.createDataFrame([("I1", "DISPUTED", T0, T2), ("I2", "OPEN", T1, T2)], INV_SCHEMA),
        path, "invoice_id", "updated_at",
    )
    assert stats == {"inserted": 1, "updated": 0, "stale_skipped": 1}
    state = {r["invoice_id"]: r["status"] for r in spark.read.format("delta").load(path).collect()}
    assert state == {"I1": "PAID", "I2": "OPEN"}


def test_insert_only_is_idempotent(spark, tmp_path):
    path = str(tmp_path / "payments")
    df = spark.createDataFrame([("P1", 10), ("P2", 20)], "payment_id string, amount int")
    merge_insert_only(spark, df, path, "payment_id")
    assert merge_insert_only(spark, df, path, "payment_id") == {"inserted": 0}
    assert spark.read.format("delta").load(path).count() == 2
