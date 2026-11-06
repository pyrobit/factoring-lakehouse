import datetime as dt
from decimal import Decimal

import pytest
from pyspark.sql import functions as F

from factoring_lakehouse.gold import aging_bucket, build_debtor_features, build_portfolio_snapshot

AS_OF = dt.date(2026, 10, 31)

INV = ("invoice_id string, client_id string, debtor_id string, invoice_date date, due_date date, "
       "amount decimal(18,2), status string, advance_amount decimal(18,2)")
PAY = "payment_id string, invoice_id string, amount decimal(18,2), paid_at timestamp"
DEB = ("debtor_id string, debtor_name string, risk_rating string, credit_limit decimal(18,2), country string, "
       "valid_from timestamp, valid_to timestamp")
CLI = "client_id string, client_name string, advance_rate double, fee_rate double"


def D(x):
    return Decimal(str(x)).quantize(Decimal("0.01"))


@pytest.fixture
def frames(spark):
    invoices = spark.createDataFrame([
        # open, 45 days past due, partially collected
        ("I1", "C1", "D1", dt.date(2026, 8, 1), dt.date(2026, 9, 16), D(1000), "FUNDED", D(800)),
        # paid with a short pay -> dilution 50
        ("I2", "C1", "D1", dt.date(2026, 9, 1), dt.date(2026, 10, 1), D(500), "PAID", D(400)),
        # cancelled -> dilution = full amount
        ("I3", "C1", "D2", dt.date(2026, 10, 1), dt.date(2026, 11, 1), D(300), "CANCELLED", None),
        # issued after as-of -> excluded (no future leakage)
        ("I4", "C1", "D1", dt.date(2026, 11, 2), dt.date(2026, 12, 2), D(999), "FUNDED", D(800)),
    ], INV)
    payments = spark.createDataFrame([
        ("P1", "I1", D(200), dt.datetime(2026, 10, 10)),
        ("P2", "I2", D(450), dt.datetime(2026, 10, 5)),
        ("P3", "I1", D(800), dt.datetime(2026, 11, 3)),  # after as-of -> ignored
    ], PAY)
    debtors = spark.createDataFrame([
        ("D1", "Acme", "A", D(10_000), "US", dt.datetime(2026, 1, 1), dt.datetime(2026, 11, 1)),
        ("D1", "Acme", "C", D(10_000), "US", dt.datetime(2026, 11, 1), None),  # downgrade after as-of
        ("D2", "Beta", "B", D(5_000), "MX", dt.datetime(2026, 1, 1), None),
    ], DEB)
    clients = spark.createDataFrame([("C1", "Client 1", 0.8, 0.02)], CLI)
    return invoices, payments, debtors, clients


@pytest.mark.parametrize("dpd,bucket", [(None, "CLOSED"), (-3, "CURRENT"), (0, "CURRENT"), (1, "1-30"),
                                        (30, "1-30"), (31, "31-60"), (60, "31-60"), (61, "61-90"),
                                        (90, "61-90"), (91, "90+")])
def test_aging_bucket_boundaries(spark, dpd, bucket):
    df = spark.createDataFrame([(dpd,)], "dpd int")
    assert df.select(aging_bucket(F.col("dpd")).alias("b")).first()["b"] == bucket


def test_portfolio_snapshot_balances_and_point_in_time(spark, frames):
    snap = {r["invoice_id"]: r for r in build_portfolio_snapshot(*frames, AS_OF).collect()}
    assert set(snap) == {"I1", "I2", "I3"}
    i1, i2, i3 = snap["I1"], snap["I2"], snap["I3"]
    assert (i1["outstanding_amount"], i1["days_past_due"], i1["aging_bucket"]) == (D(800), 45, "31-60")
    assert i1["reserve_amount"] == D(200)
    assert i1["debtor_risk_rating"] == "A"  # rating as of 2026-10-31, not today's "C"
    assert (i2["outstanding_amount"], i2["dilution_amount"], i2["aging_bucket"]) == (D(0), D(50), "CLOSED")
    assert (i3["outstanding_amount"], i3["dilution_amount"]) == (D(0), D(300))


def test_debtor_features_have_no_future_leakage(spark, frames):
    invoices, payments, debtors, clients = frames
    snap = build_portfolio_snapshot(invoices, payments, debtors, clients, AS_OF)
    feats = {r["debtor_id"]: r for r in build_debtor_features(snap, payments, invoices, AS_OF).collect()}
    d1 = feats["D1"]
    assert d1["payments_90d"] == 2            # P3 (after as-of) excluded
    assert d1["open_exposure"] == D(800)
    # P1: paid 10-10 vs due 09-16 -> 24 days late; P2: paid 10-05 vs due 10-01 -> 4 days late.
    assert d1["avg_days_late_90d"] == pytest.approx(14.0)
    assert d1["pct_paid_late_90d"] == pytest.approx(1.0)
    assert d1["risk_rating"] == "A"
    assert feats["D2"]["dilution_rate_90d"] == pytest.approx(1.0)
