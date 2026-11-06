import datetime as dt

import pytest

from factoring_lakehouse.snowflake_io.loader import (
    TABLES,
    ReconResult,
    copy_sql,
    merge_sql,
    plan,
    reconcile,
    reconcile_sql,
)

AS_OF = dt.date(2026, 11, 1)
SPECS = {s.target: s for s in TABLES}


def test_copy_is_scoped_to_as_of_folder():
    sql = copy_sql(SPECS["INVOICES"], "RAW.LAKEHOUSE_EXPORT_STAGE", AS_OF)
    assert "@RAW.LAKEHOUSE_EXPORT_STAGE/silver_invoices/as_of_date=2026-11-01/" in sql
    assert "MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE" in sql
    assert "UPDATE" not in sql  # silver tables carry no as-of column


def test_copy_stamps_as_of_on_snapshot_tables():
    assert "SET AS_OF_DATE = '2026-11-01'::DATE" in copy_sql(SPECS["PORTFOLIO_SNAPSHOT"], "RAW.S", AS_OF)


def test_merge_upserts_silver_on_business_key():
    sql = merge_sql(SPECS["INVOICES"], ["invoice_id", "status", "amount"], AS_OF)
    assert "ON t.INVOICE_ID = s.INVOICE_ID" in sql
    assert "t.INVOICE_ID = s.INVOICE_ID," not in sql.split("UPDATE SET")[1]  # key is never updated
    assert "WHEN NOT MATCHED THEN INSERT (INVOICE_ID, STATUS, AMOUNT)" in sql


def test_snapshot_tables_replace_partition():
    sql = merge_sql(SPECS["DEBTOR_FEATURES"], ["as_of_date", "debtor_id"], AS_OF)
    assert sql.startswith("DELETE FROM RAW.DEBTOR_FEATURES WHERE AS_OF_DATE = '2026-11-01'::DATE;")


def test_unsafe_identifier_rejected():
    with pytest.raises(ValueError):
        merge_sql(SPECS["INVOICES"], ["status; DROP TABLE x"], AS_OF)


def test_reconcile_sql_filters_snapshot_partition():
    assert "WHERE AS_OF_DATE" in reconcile_sql(SPECS["PORTFOLIO_SNAPSHOT"], AS_OF)
    assert "WHERE" not in reconcile_sql(SPECS["CLIENTS"], AS_OF)


def test_reconciliation_detects_mismatches():
    results = {r.table: r for r in reconcile(
        {"INVOICES": (100, 5000.0), "PAYMENTS": (10, 99.99), "CLIENTS": (3, 1.0)},
        {"INVOICES": (100, 5000.004), "PAYMENTS": (9, 99.99)},
    )}
    assert results["INVOICES"].ok
    assert not results["PAYMENTS"].ok
    assert not results["CLIENTS"].ok and results["CLIENTS"].target_rows == 0
    assert ReconResult("X", 1, 1, 10.0, 10.02).ok is False


def test_dry_run_plan_is_transactional(cfg):
    stmts = plan(cfg, AS_OF)
    assert stmts[3] == "BEGIN;" and "COMMIT;" in stmts
    assert stmts.index("COMMIT;") < len(stmts) - len(TABLES)  # reconciliation runs after commit
