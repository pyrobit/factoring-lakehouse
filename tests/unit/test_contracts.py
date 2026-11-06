from decimal import Decimal

import pytest
from pydantic import ValidationError
from pyspark.sql import types as T

from factoring_lakehouse.contracts import CONTRACTS, Invoice, Payment, derive_rules, spark_schema


def test_every_entity_has_contract():
    assert set(CONTRACTS) == {"clients", "debtors", "invoices", "payments"}


def test_spark_schema_types_are_explicit_and_nullable():
    schema = spark_schema(Invoice)
    by_name = {f.name: f for f in schema.fields}
    assert by_name["amount"].dataType == T.DecimalType(18, 2)
    assert by_name["invoice_date"].dataType == T.DateType()
    assert by_name["funded_at"].dataType == T.TimestampType()
    assert by_name["status"].dataType == T.StringType()
    # Nullable at read time so bad rows reach the quality layer instead of being dropped.
    assert all(f.nullable for f in schema.fields)


def test_optional_fields_do_not_get_not_null_rule():
    names = {r.name for r in derive_rules(Invoice)}
    assert "advance_amount_not_null" not in names
    assert "funded_at_not_null" not in names
    assert "advance_amount_range" in names  # constraint inside Optional[Annotated[...]] is kept


def test_rules_derived_from_contract_metadata():
    rules = {r.name: r for r in derive_rules(Invoice)}
    assert rules["amount_range"].params == {"gt": 0.0}
    assert rules["status_accepted"].params["values"] == ["OPEN", "FUNDED", "PAID", "DISPUTED", "CANCELLED"]
    assert rules["invoice_id_unique"].kind == "unique"
    assert rules["debtor_id_fk_debtors"].params == {"entity": "debtors", "column": "debtor_id"}


def test_pydantic_contract_rejects_bad_payment():
    with pytest.raises(ValidationError):
        Payment.model_validate({"payment_id": "P1", "invoice_id": "I1", "amount": Decimal("-1"),
                                "paid_at": "2026-10-01T00:00:00Z", "method": "CRYPTO",
                                "event_ts": "2026-10-01T00:00:00Z"})
