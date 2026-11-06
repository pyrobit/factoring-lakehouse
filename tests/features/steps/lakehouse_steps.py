"""Step definitions. Scenarios drive the real pipeline code: landing JSONL -> bronze -> silver -> gold."""

from __future__ import annotations

import dataclasses
import datetime as dt
import json
from decimal import Decimal
from pathlib import Path

from behave import given, then, when
from pyspark.sql import functions as F

from factoring_lakehouse import bronze, gold, silver
from factoring_lakehouse.quality import DataQualityError

INVOICE_DEFAULTS = {
    "invoice_number": None, "invoice_date": "2026-09-01", "due_date": "2026-10-01", "amount": "100.00",
    "currency": "USD", "status": "FUNDED", "advance_amount": None, "funded_at": None,
    "updated_at": "2026-10-30T00:00:00Z",
}


def _write_landing(context, batch: str, entity: str, rows: list[dict]) -> None:
    target = Path(context.cfg.paths.landing) / batch / entity
    target.mkdir(parents=True, exist_ok=True)
    with open(target / "part-00000.jsonl", "w") as fh:
        for row in rows:
            fh.write(json.dumps(row) + "\n")


def _load(context, batch: str) -> None:
    """Bronze + silver for every entity present in the landing batch, in dependency order."""
    context.error = None
    for entity in ("clients", "debtors", "invoices", "payments"):
        if not (Path(context.cfg.paths.landing) / batch / entity).exists():
            continue
        bronze.ingest_batch(context.spark, context.cfg, entity, batch)
        try:
            context.last_stats = silver.LOADERS[entity](context.spark, context.cfg, batch)
        except DataQualityError as exc:
            context.error = exc
            return


def _blank_to_none(value: str):
    return None if value is None or value.strip() == "" else value.strip()


def _silver(context, entity: str):
    return context.spark.read.format("delta").load(context.cfg.paths.table("silver", entity))


# ------------------------------------------------------------------------------------- config
@given("the invoice reject-rate limit is {pct:d}%")
def step_reject_limit(context, pct):
    limits = {**context.cfg.max_reject_rate, "invoices": pct / 100}
    context.cfg = dataclasses.replace(context.cfg, max_reject_rate=limits)


# ------------------------------------------------------------------------------------- masters
@given('the client "{client_id}" exists')
def step_client(context, client_id):
    _write_landing(context, f"seed_{client_id}", "clients", [{
        "client_id": client_id, "client_name": f"{client_id} LLC", "industry": "Trucking", "advance_rate": 0.8,
        "reserve_rate": 0.2, "fee_rate": 0.02, "credit_limit": 500000, "updated_at": "2026-01-01T00:00:00Z",
    }])
    _load(context, f"seed_{client_id}")


def _debtor_row(debtor_id, rating, ts):
    return {"debtor_id": debtor_id, "debtor_name": f"{debtor_id} Corp", "tax_id": "12-3456789",
            "industry": "Wholesale", "country": "US", "credit_limit": 100000, "risk_rating": rating,
            "updated_at": ts}


@given('the debtor "{debtor_id}" exists with risk rating "{rating}"')
def step_debtor(context, debtor_id, rating):
    _write_landing(context, f"seed_{debtor_id}", "debtors", [_debtor_row(debtor_id, rating, "2026-01-01T00:00:00Z")])
    _load(context, f"seed_{debtor_id}")


@when('the debtor "{debtor_id}" is updated to risk rating "{rating}" at "{ts}"')
def step_debtor_update(context, debtor_id, rating, ts):
    _write_landing(context, f"upd_{debtor_id}", "debtors", [_debtor_row(debtor_id, rating, ts)])
    _load(context, f"upd_{debtor_id}")


@then('debtor "{debtor_id}" has {n:d} versions')
def step_debtor_versions(context, debtor_id, n):
    assert _silver(context, "debtors").where(F.col("debtor_id") == debtor_id).count() == n


@then('the current rating of debtor "{debtor_id}" is "{rating}"')
def step_current_rating(context, debtor_id, rating):
    row = _silver(context, "debtors").where((F.col("debtor_id") == debtor_id) & F.col("is_current")).first()
    assert row["risk_rating"] == rating, row


@then('the closed version of debtor "{debtor_id}" ends at "{ts}"')
def step_closed_version(context, debtor_id, ts):
    row = _silver(context, "debtors").where((F.col("debtor_id") == debtor_id) & ~F.col("is_current")).first()
    expected = dt.datetime.fromisoformat(ts.replace("Z", "+00:00")).replace(tzinfo=None)
    assert row["valid_to"] == expected, row["valid_to"]


# ------------------------------------------------------------------------------------- invoices
@given('a landing batch "{batch}" with invoices:')
def step_landing_invoices(context, batch):
    rows = []
    for r in context.table:
        row = {**INVOICE_DEFAULTS, **{h: _blank_to_none(r[h]) for h in context.table.headings}}
        row["invoice_number"] = row["invoice_number"] or row["invoice_id"]
        row["amount"] = float(row["amount"]) if row["amount"] is not None else None
        rows.append(row)
    _write_landing(context, batch, "invoices", rows)


@given('the pipeline loads batch "{batch}" into silver')
@when('the pipeline loads batch "{batch}" into silver')
def step_load(context, batch):
    _load(context, batch)


@then('silver invoices contain only "{invoice_id}"')
def step_only_invoice(context, invoice_id):
    ids = [r["invoice_id"] for r in _silver(context, "invoices").select("invoice_id").collect()]
    assert ids == [invoice_id], ids


@then("quarantined invoices have these failures:")
def step_quarantine_reasons(context):
    q = context.spark.read.format("delta").load(context.cfg.paths.table("quarantine", "invoices"))
    actual = {r["invoice_id"]: set(r["_dq_failures"]) for r in q.collect()}
    expected = {r["invoice_id"]: {f.strip() for f in r["failures"].split(",")} for r in context.table}
    assert actual == expected, actual


@then('the load fails with a data-quality error mentioning "{entity}"')
def step_dq_error(context, entity):
    assert context.error is not None, "expected a DataQualityError"
    assert entity in str(context.error)


@then("the rejected invoices are still written to quarantine")
def step_quarantine_written(context):
    q = context.spark.read.format("delta").load(context.cfg.paths.table("quarantine", "invoices"))
    assert q.count() == 2


@then('invoice "{invoice_id}" has status "{status}" in silver')
def step_invoice_status(context, invoice_id, status):
    row = _silver(context, "invoices").where(F.col("invoice_id") == invoice_id).first()
    assert row["status"] == status, row


@then("{n:d} stale update was skipped")
def step_stale(context, n):
    assert context.last_stats.extra["stale_skipped"] == n, context.last_stats


@then("silver has {n:d} invoices")
def step_invoice_count(context, n):
    assert _silver(context, "invoices").count() == n


# ------------------------------------------------------------------------------------- gold / aging
INV_SCHEMA = ("invoice_id string, client_id string, debtor_id string, invoice_date date, due_date date, "
              "amount decimal(18,2), status string, advance_amount decimal(18,2)")


def _frames(context, invoice_rows, payment_rows):
    spark = context.spark
    debtors = spark.createDataFrame(
        [("D1", "D1 Corp", "A", Decimal("100000"), "US", dt.datetime(2026, 1, 1), None)],
        "debtor_id string, debtor_name string, risk_rating string, credit_limit decimal(18,2), country string, "
        "valid_from timestamp, valid_to timestamp")
    clients = spark.createDataFrame([("C1", "C1 LLC", 0.8, 0.02)],
                                    "client_id string, client_name string, advance_rate double, fee_rate double")
    invoices = spark.createDataFrame(invoice_rows, INV_SCHEMA)
    payments = spark.createDataFrame(payment_rows,
                                     "payment_id string, invoice_id string, amount decimal(18,2), paid_at timestamp")
    return invoices, payments, debtors, clients


@given('an open invoice of {amount} due on "{due}"')
def step_open_invoice(context, amount, due):
    context.frames = _frames(context, [("I1", "C1", "D1", dt.date(2026, 5, 1), dt.date.fromisoformat(due),
                                        Decimal(amount), "FUNDED", Decimal(amount) * Decimal("0.8"))], [])


@given("a paid invoice of {amount} with payments totalling {paid}")
def step_paid_invoice(context, amount, paid):
    context.frames = _frames(
        context,
        [("I1", "C1", "D1", dt.date(2026, 9, 1), dt.date(2026, 10, 1), Decimal(amount), "PAID", Decimal("800"))],
        [("P1", "I1", Decimal(paid), dt.datetime(2026, 10, 3))],
    )


@when('the portfolio snapshot is built as of "{as_of}"')
def step_build_snapshot(context, as_of):
    context.snapshot = gold.build_portfolio_snapshot(*context.frames, dt.date.fromisoformat(as_of)).first()


@then('the invoice is in aging bucket "{bucket}" with {dpd:d} days past due')
def step_bucket(context, bucket, dpd):
    assert (context.snapshot["aging_bucket"], context.snapshot["days_past_due"]) == (bucket, dpd), context.snapshot


@then("the invoice has outstanding {outstanding} and dilution {dilution}")
def step_outstanding(context, outstanding, dilution):
    assert context.snapshot["outstanding_amount"] == Decimal(outstanding)
    assert context.snapshot["dilution_amount"] == Decimal(dilution)
