import datetime as dt
from decimal import Decimal

import pytest

from factoring_lakehouse import quality
from factoring_lakehouse.contracts import Payment as PaymentContract
from factoring_lakehouse.contracts import RuleSpec, spark_schema

TS = dt.datetime(2026, 10, 1, 12, 0)


def _payments(spark, rows):
    return spark.createDataFrame(rows, spark_schema(PaymentContract))


def _row(pid, inv="I1", amount="10.00", method="ACH", paid_at=TS):
    return (pid, inv, Decimal(amount), paid_at, method, TS)


def test_apply_rules_lists_every_failed_rule(spark):
    df = _payments(spark, [_row("P1"), _row("P2", amount="-5.00", method="CRYPTO"), _row("P3", paid_at=None)])
    out = {r["payment_id"]: set(r["_dq_failures"]) for r in quality.apply_rules(
        df, [RuleSpec("amount_range", "range", "amount", {"gt": 0}),
             RuleSpec("method_accepted", "accepted_values", "method", {"values": ["ACH", "WIRE"]}),
             RuleSpec("paid_at_not_null", "not_null", "paid_at", {})]).collect()}
    assert out == {"P1": set(), "P2": {"amount_range", "method_accepted"}, "P3": {"paid_at_not_null"}}


def test_unique_rule_flags_all_copies(spark):
    df = _payments(spark, [_row("P1"), _row("P1"), _row("P2")])
    flagged = quality.apply_rules(df, [RuleSpec("pid_unique", "unique", "payment_id", {})]) \
        .where("size(_dq_failures) > 0").count()
    assert flagged == 2


def test_foreign_key_rule_uses_reference_frame(spark):
    df = _payments(spark, [_row("P1", inv="I1"), _row("P2", inv="ORPHAN")])
    ref = spark.createDataFrame([("I1",)], "invoice_id string")
    rule = RuleSpec("fk", "foreign_key", "invoice_id", {"entity": "invoices", "column": "invoice_id"})
    rows = {r["payment_id"]: r["_dq_failures"] for r in quality.apply_rules(df, [rule], {"invoices": ref}).collect()}
    assert rows == {"P1": [], "P2": ["fk"]}


def test_split_and_threshold(spark, cfg):
    df = _payments(spark, [_row(f"P{i}") for i in range(10)] + [_row("BAD", method="CRYPTO")])
    result = quality.split(df, "payments")
    assert (result.total, result.rejected_count) == (11, 1)
    assert result.rule_failures == {"method_accepted": 1}
    assert result.valid.count() == 10
    quality.enforce_threshold(result, cfg)  # 9.1% is under the 10% payments limit: no exception


def test_threshold_breach_raises(spark, cfg):
    df = _payments(spark, [_row("P1"), _row("P2", method="CRYPTO")])
    result = quality.split(df, "payments")
    with pytest.raises(quality.DataQualityError, match="reject rate 50.00%"):
        quality.enforce_threshold(result, cfg)
