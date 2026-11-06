"""Incremental extraction from the operational Postgres DB into the landing zone (JSONL).

Strategy: high-watermark on ``updated_at`` with a small **lookback overlap** to catch rows committed
late by long transactions (commit order differs from ``updated_at`` order). Rows re-read through
the overlap are deduplicated downstream by silver's ``latest_per_key`` + MERGE.

A server-side (named) cursor streams rows in chunks, so memory stays flat on large tables.
"""

from __future__ import annotations

import argparse
import json
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

from ..config import get_logger
from .watermark import WatermarkStore

log = get_logger(__name__)

ALLOWED_TABLES = {
    "clients": ["client_id", "client_name", "industry", "advance_rate", "reserve_rate", "fee_rate",
                "credit_limit", "updated_at"],
    "debtors": ["debtor_id", "debtor_name", "tax_id", "industry", "country", "credit_limit", "risk_rating",
                "updated_at"],
    "invoices": ["invoice_id", "client_id", "debtor_id", "invoice_number", "invoice_date", "due_date", "amount",
                 "currency", "status", "advance_amount", "funded_at", "updated_at"],
}


@dataclass(frozen=True)
class IncrementalQuery:
    sql: str
    params: dict


def build_incremental_query(table: str, since: datetime, until: datetime,
                            lookback: timedelta = timedelta(minutes=10)) -> IncrementalQuery:
    """Parameterised, allow-listed SQL. Never interpolate user input into identifiers."""
    if table not in ALLOWED_TABLES:
        raise ValueError(f"Table {table!r} is not allow-listed for extraction")
    columns = ", ".join(ALLOWED_TABLES[table])
    sql = (
        f"SELECT {columns} FROM factoring.{table} "
        "WHERE updated_at > %(since)s AND updated_at <= %(until)s "
        "ORDER BY updated_at"
    )
    return IncrementalQuery(sql, {"since": since - lookback, "until": until})


def to_utc(ts: datetime) -> datetime:
    """Naive datetimes (e.g. from pymongo) are UTC by convention; make them tz-aware."""
    return ts.replace(tzinfo=timezone.utc) if ts.tzinfo is None else ts.astimezone(timezone.utc)


def _json_default(o):
    if isinstance(o, Decimal):
        return float(o)
    if isinstance(o, datetime):
        # One canonical wire format for every source: UTC, microseconds, 'Z' suffix.
        return to_utc(o).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    if hasattr(o, "isoformat"):
        return o.isoformat()
    raise TypeError(type(o))


def write_jsonl(rows: Iterable[dict], target: Path) -> tuple[int, datetime | None]:
    """Write rows; return (count, max updated_at seen)."""
    target.parent.mkdir(parents=True, exist_ok=True)
    count, max_ts = 0, None
    tmp = target.with_suffix(".inprogress")
    with open(tmp, "w") as fh:
        for row in rows:
            fh.write(json.dumps(row, default=_json_default) + "\n")
            count += 1
            ts = row.get("updated_at")
            ts = to_utc(ts) if isinstance(ts, datetime) else ts
            if ts is not None and (max_ts is None or ts > max_ts):
                max_ts = ts
    tmp.replace(target)  # readers (Auto Loader) never see a half-written file
    return count, max_ts


def _stream_rows(conn, query: IncrementalQuery, chunk_size: int) -> Iterator[dict]:
    with conn.cursor(name="factoring_extract") as cur:  # server-side cursor
        cur.itersize = chunk_size
        cur.execute(query.sql, query.params)
        cols: list[str] | None = None
        for record in cur:
            # Named (server-side) cursors in psycopg2 only populate `description` after the first
            # fetch, so read the column names lazily.
            if cols is None:
                cols = [d[0] for d in cur.description]
            yield dict(zip(cols, record, strict=True))


def extract_table(conn, table: str, landing: str, batch_id: str, store: WatermarkStore,
                  until: datetime, chunk_size: int = 10_000) -> int:
    since = store.get(f"postgres.{table}")
    query = build_incremental_query(table, since, until)
    target = Path(landing) / batch_id / table / "part-00000.jsonl"
    count, max_ts = write_jsonl(_stream_rows(conn, query, chunk_size), target)
    if max_ts is not None:
        store.set(f"postgres.{table}", max(max_ts, since))
    log.info(f"source=postgres table={table} batch={batch_id} rows={count} since={since} until={until}")
    return count


def main() -> None:  # pragma: no cover - needs a live database
    import psycopg

    p = argparse.ArgumentParser()
    p.add_argument("--dsn", required=True)
    p.add_argument("--landing", default="data/landing")
    p.add_argument("--batch", required=True)
    p.add_argument("--until", type=datetime.fromisoformat, required=True)
    p.add_argument("--state", default="data/state/watermarks.json")
    args = p.parse_args()
    store = WatermarkStore(args.state)
    with psycopg.connect(args.dsn) as conn:
        for table in ALLOWED_TABLES:
            extract_table(conn, table, args.landing, args.batch, store, args.until)


if __name__ == "__main__":  # pragma: no cover
    main()
