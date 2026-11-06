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


def test_naive_mongo_datetimes_become_utc_and_watermark_compares():
    naive = datetime(2026, 11, 1, 14, 5)  # pymongo returns naive UTC datetimes
    doc = {"payment_id": "P1", "invoice_id": "I1", "amount": Decimal("1"), "method": "ACH",
           "paid_at": naive, "event_ts": naive}
    row = normalize(doc)
    assert row["event_ts"].tzinfo is not None
    assert max(row["event_ts"], EPOCH) == row["event_ts"]  # no naive/aware TypeError


def test_jsonl_timestamps_use_one_canonical_utc_format(tmp_path):
    aware = datetime(2026, 11, 1, 11, 0, 0, 123456, tzinfo=timezone(timedelta(hours=-3)))
    target = tmp_path / "x.jsonl"
    write_jsonl([{"updated_at": aware}], target)
    assert json.loads(target.read_text())["updated_at"] == "2026-11-01T14:00:00.123456Z"


def test_bronze_timestamp_format_parses_every_source_variant(spark):
    from factoring_lakehouse.bronze import TIMESTAMP_FORMAT

    df = spark.createDataFrame(
        [("2026-11-01T14:00:00Z",), ("2026-11-01T14:00:00.123456Z",), ("2026-11-01T14:00:00+00:00",),
         ("2026-11-01T11:00:00.5-03:00",)], "s string")
    parsed = [r["t"] for r in df.selectExpr(f"to_timestamp(s, \"{TIMESTAMP_FORMAT}\") AS t").collect()]
    assert all(p is not None for p in parsed), parsed
    assert parsed[0] == datetime(2026, 11, 1, 14, 0) and parsed[3] == datetime(2026, 11, 1, 14, 0, 0, 500000)


class _NamedCursor:
    """Mimics a psycopg2 server-side cursor: `description` is None until the first fetch."""

    def __init__(self, rows):
        self._rows, self.description, self.itersize = rows, None, None

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, params):
        self.executed = (sql, params)

    def __iter__(self):
        for row in self._rows:
            self.description = [("invoice_id",), ("updated_at",)]
            yield row


class _Conn:
    def __init__(self, rows):
        self.cur = _NamedCursor(rows)

    def cursor(self, name=None):
        assert name, "extraction must use a named (server-side) cursor"
        return self.cur


def test_extract_table_with_server_side_cursor_advances_watermark(tmp_path):
    from factoring_lakehouse.ingestion.postgres_incremental import extract_table

    t1, t2 = datetime(2026, 11, 1, 10, tzinfo=UTC), datetime(2026, 11, 1, 12, tzinfo=UTC)
    store = WatermarkStore(tmp_path / "wm.json")
    n = extract_table(_Conn([("INV-1", t1), ("INV-2", t2)]), "invoices", str(tmp_path / "landing"), "b1",
                      store, until=datetime(2026, 11, 2, tzinfo=UTC))
    assert n == 2
    assert store.get("postgres.invoices") == t2
    lines = (tmp_path / "landing" / "b1" / "invoices" / "part-00000.jsonl").read_text().splitlines()
    assert json.loads(lines[1]) == {"invoice_id": "INV-2", "updated_at": "2026-11-01T12:00:00.000000Z"}


def test_extract_table_with_no_rows_keeps_watermark(tmp_path):
    from factoring_lakehouse.ingestion.postgres_incremental import extract_table

    store = WatermarkStore(tmp_path / "wm.json")
    assert extract_table(_Conn([]), "clients", str(tmp_path), "b1", store, until=EPOCH) == 0
    assert store.get("postgres.clients") == EPOCH
