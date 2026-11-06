"""Payment events from MongoDB (``factoring.payment_events``) into the landing zone.

Events are append-only documents written by the payments service. We extract by ``event_ts``
watermark. BSON types (ObjectId, Decimal128, datetime) are normalised to the ``Payment`` contract,
and documents that fail the contract are written to a ``_rejected`` file instead of being dropped.
"""

from __future__ import annotations

import argparse
from collections.abc import Iterable
from datetime import datetime, timedelta
from pathlib import Path

from pydantic import ValidationError

from ..config import get_logger
from ..contracts import Payment
from .postgres_incremental import write_jsonl
from .watermark import WatermarkStore

log = get_logger(__name__)


def build_filter(since: datetime, until: datetime, lookback: timedelta = timedelta(minutes=5)) -> dict:
    return {"event_ts": {"$gt": since - lookback, "$lte": until}, "type": "payment.received"}


def normalize(doc: dict) -> dict:
    """Map a Mongo payment document onto the flat ``Payment`` contract shape."""
    amount = doc.get("amount")
    if hasattr(amount, "to_decimal"):  # bson.Decimal128
        amount = amount.to_decimal()
    return {
        "payment_id": str(doc.get("payment_id") or doc.get("_id")),
        "invoice_id": doc.get("invoice", {}).get("id") if isinstance(doc.get("invoice"), dict)
        else doc.get("invoice_id"),
        "amount": amount,
        "paid_at": doc.get("paid_at"),
        "method": (doc.get("method") or "").upper() or None,
        "event_ts": doc.get("event_ts"),
    }


def partition_valid(docs: Iterable[dict]) -> tuple[list[dict], list[dict]]:
    valid, rejected = [], []
    for doc in docs:
        row = normalize(doc)
        try:
            Payment.model_validate(row)
            valid.append(row)
        except ValidationError as exc:
            rejected.append({**row, "_errors": [e["msg"] for e in exc.errors()]})
    return valid, rejected


def extract_payments(collection, landing: str, batch_id: str, store: WatermarkStore, until: datetime) -> int:
    since = store.get("mongo.payment_events")
    cursor = collection.find(build_filter(since, until)).sort("event_ts", 1).batch_size(5_000)
    valid, rejected = partition_valid(cursor)
    base = Path(landing) / batch_id / "payments"
    count, _ = write_jsonl(valid, base / "part-00000.jsonl")
    if rejected:
        write_jsonl(rejected, base.parent / "_rejected" / "payments.jsonl")
    if valid:
        store.set("mongo.payment_events", max(max(r["event_ts"] for r in valid), since))
    log.info(f"source=mongo collection=payment_events rows={count} rejected={len(rejected)}")
    return count


def main() -> None:  # pragma: no cover - needs a live database
    from pymongo import MongoClient

    p = argparse.ArgumentParser()
    p.add_argument("--uri", required=True)
    p.add_argument("--landing", default="data/landing")
    p.add_argument("--batch", required=True)
    p.add_argument("--until", type=datetime.fromisoformat, required=True)
    p.add_argument("--state", default="data/state/watermarks.json")
    args = p.parse_args()
    coll = MongoClient(args.uri)["factoring"]["payment_events"]
    extract_payments(coll, args.landing, args.batch, WatermarkStore(args.state), args.until)


if __name__ == "__main__":  # pragma: no cover
    main()
