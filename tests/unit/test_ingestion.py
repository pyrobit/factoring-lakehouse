import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from factoring_lakehouse.ingestion.mongo_events import build_filter, normalize, partition_valid
from factoring_lakehouse.ingestion.postgres_incremental import build_incremental_query, write_jsonl
from factoring_lakehouse.ingestion.watermark import EPOCH, WatermarkStore

UTC = timezone.utc


def test_incremental_query_is_parameterised_with_lookback():
    since, until = datetime(2026, 10, 31, 10, tzinfo=UTC), datetime(2026, 10, 31, 23, tzinfo=UTC)
    q = build_incremental_query("invoices", since, until, lookback=timedelta(minutes=10))
    assert "%(since)s" in q.sql and "%(until)s" in q.sql and "ORDER BY updated_at" in q.sql
    assert q.params == {"since": since - timedelta(minutes=10), "until": until}


def test_incremental_query_rejects_unknown_table():
    with pytest.raises(ValueError, match="allow-listed"):
        build_incremental_query("users; DROP TABLE x", EPOCH, EPOCH)


def test_watermark_store_roundtrip_and_monotonic(tmp_path):
    store = WatermarkStore(tmp_path / "wm.json")
    assert store.get("postgres.invoices") == EPOCH
    t1 = datetime(2026, 10, 31, tzinfo=UTC)
    store.set("postgres.invoices", t1)
    assert WatermarkStore(tmp_path / "wm.json").get("postgres.invoices") == t1
    with pytest.raises(ValueError, match="backwards"):
        store.set("postgres.invoices", t1 - timedelta(days=1))


def test_write_jsonl_returns_max_watermark(tmp_path):
    rows = [{"id": 1, "amount": Decimal("1.50"), "updated_at": datetime(2026, 10, 1, tzinfo=UTC)},
            {"id": 2, "amount": Decimal("2.00"), "updated_at": datetime(2026, 10, 3, tzinfo=UTC)}]
    target = tmp_path / "b" / "invoices" / "part-00000.jsonl"
    count, max_ts = write_jsonl(rows, target)
    assert (count, max_ts) == (2, datetime(2026, 10, 3, tzinfo=UTC))
    assert json.loads(target.read_text().splitlines()[0])["amount"] == 1.5
    assert not list(target.parent.glob("*.inprogress"))


def test_mongo_filter_and_normalisation():
    since = datetime(2026, 10, 31, tzinfo=UTC)
    f = build_filter(since, since + timedelta(days=1), lookback=timedelta(minutes=5))
    assert f["event_ts"]["$gt"] == since - timedelta(minutes=5)
    doc = {"_id": "abc", "invoice": {"id": "INV-1"}, "amount": Decimal("10.00"), "method": "ach",
           "paid_at": since, "event_ts": since}
    assert normalize(doc)["method"] == "ACH" and normalize(doc)["invoice_id"] == "INV-1"


def test_mongo_partition_valid_rejects_contract_violations():
    ts = datetime(2026, 10, 31, tzinfo=UTC)
    good = {"payment_id": "P1", "invoice_id": "I1", "amount": Decimal("5"), "method": "WIRE", "paid_at": ts,
            "event_ts": ts}
    bad = good | {"payment_id": "P2", "method": "CRYPTO"}
    valid, rejected = partition_valid([good, bad])
    assert [v["payment_id"] for v in valid] == ["P1"]
    assert rejected[0]["payment_id"] == "P2" and rejected[0]["_errors"]
