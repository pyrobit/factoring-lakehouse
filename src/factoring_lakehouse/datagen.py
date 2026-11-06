"""Deterministic synthetic data for an invoice-factoring business, with planted defects.

Two batches land as JSONL in ``<out>/<batch>/<entity>/part-00000.jsonl``:

* ``day1``: initial load as of 2026-10-31.
* ``day2``: incremental CDC as of 2026-11-01. Debtor rating changes (SCD2), invoices settled,
  new invoices and payments, **stale late-arriving updates** that must be ignored, and replays of
  unchanged debtor rows that must not create new SCD2 versions.

A ``manifest.json`` records exactly which defects were planted, so tests can assert that the
quality layer quarantines precisely those rows.
"""

from __future__ import annotations

import argparse
import json
import random
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path

DAY1_CUTOFF = datetime(2026, 10, 31, 23, 0, tzinfo=timezone.utc)
DAY2_CUTOFF = datetime(2026, 11, 1, 23, 0, tzinfo=timezone.utc)

INDUSTRIES = ["Trucking", "Staffing", "Manufacturing", "Wholesale", "Healthcare", "Construction", "Oilfield"]
COUNTRIES = ["US", "US", "US", "MX", "CA", "CO", "BR"]
# Mean days a debtor pays *after* the due date, by risk rating.
LATENESS = {"A": -2, "B": 4, "C": 15, "D": 38}


def _money(x: float) -> Decimal:
    return Decimal(str(x)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def _iso(ts: datetime) -> str:
    return ts.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _json_default(o):
    if isinstance(o, Decimal):
        return float(o)
    if isinstance(o, datetime):
        return _iso(o)
    if isinstance(o, date):
        return o.isoformat()
    raise TypeError(type(o))


@dataclass
class Batch:
    clients: list[dict] = field(default_factory=list)
    debtors: list[dict] = field(default_factory=list)
    invoices: list[dict] = field(default_factory=list)
    payments: list[dict] = field(default_factory=list)


@dataclass
class Generator:
    seed: int = 42
    n_clients: int = 20
    n_debtors: int = 60
    n_invoices: int = 800

    def __post_init__(self) -> None:
        self.rng = random.Random(self.seed)
        self.manifest: dict = {"day1": {}, "day2": {}}

    # ---------------------------------------------------------------- masters
    def _clients(self) -> list[dict]:
        out = []
        for i in range(1, self.n_clients + 1):
            adv = self.rng.choice([0.75, 0.80, 0.85, 0.90])
            out.append(
                {
                    "client_id": f"C{i:04d}",
                    "client_name": f"Client {i} LLC",
                    "industry": self.rng.choice(INDUSTRIES),
                    "advance_rate": adv,
                    "reserve_rate": round(1 - adv, 2),
                    "fee_rate": self.rng.choice([0.015, 0.02, 0.025, 0.03]),
                    "credit_limit": _money(self.rng.choice([250_000, 500_000, 1_000_000])),
                    "updated_at": DAY1_CUTOFF - timedelta(days=self.rng.randint(5, 200)),
                }
            )
        return out

    def _debtors(self) -> list[dict]:
        out = []
        for i in range(1, self.n_debtors + 1):
            out.append(
                {
                    "debtor_id": f"D{i:04d}",
                    "debtor_name": f"Debtor {i} Corp",
                    "tax_id": f"{self.rng.randint(10, 99)}-{self.rng.randint(1_000_000, 9_999_999)}",
                    "industry": self.rng.choice(INDUSTRIES),
                    "country": self.rng.choice(COUNTRIES),
                    "credit_limit": _money(self.rng.choice([50_000, 100_000, 250_000, 500_000])),
                    "risk_rating": self.rng.choices("ABCD", weights=[35, 35, 20, 10])[0],
                    "updated_at": DAY1_CUTOFF - timedelta(days=self.rng.randint(5, 200)),
                }
            )
        return out

    # ---------------------------------------------------------------- invoices + payments
    def _invoice(self, n: int, client: dict, debtor: dict, invoice_date: date, cutoff: datetime) -> dict:
        amount = _money(self.rng.uniform(1_500, 60_000))
        due = invoice_date + timedelta(days=self.rng.choice([30, 45, 60]))
        funded_at = datetime.combine(
            invoice_date + timedelta(days=self.rng.randint(0, 3)), datetime.min.time(), timezone.utc
        ) + timedelta(hours=self.rng.randint(9, 17))
        roll = self.rng.random()
        status = "DISPUTED" if roll < 0.03 else "CANCELLED" if roll < 0.05 else "FUNDED"
        funded = status != "CANCELLED" and funded_at <= cutoff
        return {
            "invoice_id": f"INV-{n:06d}",
            "client_id": client["client_id"],
            "debtor_id": debtor["debtor_id"],
            "invoice_number": f"{client['client_id']}-{n:06d}",
            "invoice_date": invoice_date,
            "due_date": due,
            "amount": amount,
            "currency": "USD",
            "status": status if funded or status == "CANCELLED" else "OPEN",
            "advance_amount": _money(float(amount) * client["advance_rate"]) if funded else None,
            "funded_at": funded_at if funded else None,
            "updated_at": min(funded_at + timedelta(hours=1), cutoff),
        }

    def _maybe_pay(self, inv: dict, debtor: dict, cutoff: datetime, seq: list[int]) -> list[dict]:
        """Pay an invoice if the debtor's simulated pay date falls before the cutoff."""
        if inv["status"] not in ("FUNDED",):
            return []
        delay = int(self.rng.gauss(LATENESS[debtor["risk_rating"]], 6))
        paid_at = datetime.combine(
            inv["due_date"] + timedelta(days=delay), datetime.min.time(), timezone.utc
        ) + timedelta(hours=self.rng.randint(8, 18))
        if paid_at > cutoff or paid_at < inv["funded_at"]:
            return []
        short_pay = self.rng.random() < 0.08  # dilution: debtor pays less than face value
        amount = _money(float(inv["amount"]) * (self.rng.uniform(0.85, 0.98) if short_pay else 1.0))
        seq[0] += 1
        payment = {
            "payment_id": f"PAY-{seq[0]:07d}",
            "invoice_id": inv["invoice_id"],
            "amount": amount,
            "paid_at": paid_at,
            "method": self.rng.choice(["ACH", "ACH", "WIRE", "CHECK"]),
            "event_ts": paid_at + timedelta(minutes=self.rng.randint(1, 600)),
        }
        inv["status"] = "PAID"
        inv["updated_at"] = max(inv["updated_at"], payment["event_ts"])
        return [payment]

    # ---------------------------------------------------------------- batches
    def build(self) -> dict[str, Batch]:
        clients, debtors = self._clients(), self._debtors()
        debtor_by_id = {d["debtor_id"]: d for d in debtors}
        pay_seq = [0]

        # ---------------- day 1 (clean data first)
        d1 = Batch(clients=[dict(c) for c in clients], debtors=[dict(d) for d in debtors])
        start = date(2026, 6, 1)
        for n in range(1, self.n_invoices + 1):
            client = self.rng.choice(clients)
            # Skewed debtor choice so some clients get concentration breaches.
            debtor = debtors[min(int(self.rng.paretovariate(1.2)) - 1, len(debtors) - 1)] \
                if self.rng.random() < 0.35 else self.rng.choice(debtors)
            inv = self._invoice(n, client, debtor, start + timedelta(days=self.rng.randint(0, 150)), DAY1_CUTOFF)
            d1.payments += self._maybe_pay(inv, debtor, DAY1_CUTOFF, pay_seq)
            d1.invoices.append(inv)

        # ---------------- day 1 planted defects (on rows that have no payments)
        unpaid = [i for i in d1.invoices if i["status"] in ("FUNDED", "OPEN")]
        picks = self.rng.sample(unpaid, 9)
        neg, null_debtor, orphan_debtor = picks[0:2], picks[2:5], picks[5:7]
        bad_currency = picks[7:9]
        for inv in neg:
            inv["amount"] = -inv["amount"]
        for inv in null_debtor:
            inv["debtor_id"] = None
        for inv in orphan_debtor:
            inv["debtor_id"] = "D9999"
        for inv in bad_currency:
            inv["currency"] = "usd"
        healthy = [i for i in d1.invoices if i not in picks]
        dup_invoices = self.rng.sample(healthy, 5)
        d1.invoices += [dict(i) for i in dup_invoices]  # exact replays (at-least-once delivery)

        # A newly onboarded client with a missing name: no invoices reference it, so the
        # rejection does not cascade into FK failures downstream.
        bad_client = dict(clients[0]) | {"client_id": f"C{self.n_clients + 1:04d}", "client_name": None}
        d1.clients.append(bad_client)
        dup_payments = self.rng.sample(d1.payments, 4)
        d1.payments += [dict(p) for p in dup_payments]
        seq_orphan = []
        for k in range(2):
            pay_seq[0] += 1
            seq_orphan.append(
                {
                    "payment_id": f"PAY-{pay_seq[0]:07d}",
                    "invoice_id": f"INV-ORPHAN-{k}",
                    "amount": _money(1000),
                    "paid_at": DAY1_CUTOFF - timedelta(days=1),
                    "method": "ACH",
                    "event_ts": DAY1_CUTOFF - timedelta(hours=20),
                }
            )
        d1.payments += seq_orphan
        bad_method = self.rng.choice([p for p in d1.payments if p not in dup_payments and p not in seq_orphan])
        bad_method["method"] = "CRYPTO"

        self.manifest["day1"] = {
            "rows": {e: len(getattr(d1, e)) for e in ("clients", "debtors", "invoices", "payments")},
            "rejected": {
                "clients": [bad_client["client_id"]],
                "debtors": [],
                "invoices": sorted(i["invoice_id"] for i in picks),
                "payments": sorted([p["payment_id"] for p in seq_orphan] + [bad_method["payment_id"]]),
            },
            "duplicates": {
                "invoices": sorted(i["invoice_id"] for i in dup_invoices),
                "payments": sorted(p["payment_id"] for p in dup_payments),
            },
        }

        # ---------------- day 2 (CDC)
        d2 = Batch()
        changed = self.rng.sample(debtors, 5)
        rating_changes = {}
        for d in changed:
            new = dict(d)
            new["risk_rating"] = {"A": "B", "B": "C", "C": "D", "D": "C"}[d["risk_rating"]]
            new["updated_at"] = DAY2_CUTOFF - timedelta(hours=6)
            rating_changes[d["debtor_id"]] = (d["risk_rating"], new["risk_rating"])
            debtor_by_id[d["debtor_id"]] = new
            d2.debtors.append(new)
        replayed = self.rng.sample([d for d in debtors if d not in changed], 3)
        d2.debtors += [dict(d) for d in replayed]  # unchanged: must NOT create SCD2 versions
        new_debtor = self._debtors()[0] | {"debtor_id": f"D{self.n_debtors + 1:04d}",
                                           "updated_at": DAY2_CUTOFF - timedelta(hours=5)}
        debtor_by_id[new_debtor["debtor_id"]] = new_debtor
        d2.debtors.append(new_debtor)

        # Settle open invoices whose debtor pays during day 2.
        bad_ids = set(self.manifest["day1"]["rejected"]["invoices"])
        open_invs = [dict(i) for i in d1.invoices if i["status"] == "FUNDED" and i["invoice_id"] not in bad_ids]
        seen = set()
        settled = []
        for inv in open_invs:
            if inv["invoice_id"] in seen or len(settled) >= 40:
                continue
            seen.add(inv["invoice_id"])
            pay_ts = DAY2_CUTOFF - timedelta(hours=self.rng.randint(2, 20))
            pay_seq[0] += 1
            d2.payments.append(
                {
                    "payment_id": f"PAY-{pay_seq[0]:07d}",
                    "invoice_id": inv["invoice_id"],
                    "amount": inv["amount"],
                    "paid_at": pay_ts,
                    "method": "ACH",
                    "event_ts": pay_ts + timedelta(minutes=5),
                }
            )
            inv["status"] = "PAID"
            inv["updated_at"] = pay_ts + timedelta(minutes=10)
            settled.append(inv)
        d2.invoices += settled

        # Stale late-arriving updates for invoices NOT otherwise touched today: their updated_at is
        # older than what silver already holds, so the MERGE guard must ignore them.
        settled_ids = {i["invoice_id"] for i in settled}
        untouched = sorted(
            {i["invoice_id"]: i for i in d1.invoices
             if i["invoice_id"] not in settled_ids and i["invoice_id"] not in bad_ids}.values(),
            key=lambda i: i["invoice_id"],
        )
        stale_targets = self.rng.sample(untouched, 2)
        stale = []
        for inv in stale_targets:
            s = dict(inv)
            s["status"] = "DISPUTED"
            s["updated_at"] = datetime(2026, 7, 1, tzinfo=timezone.utc)
            stale.append(s)
        d2.invoices += stale

        for n in range(self.n_invoices + 1, self.n_invoices + 101):
            client = self.rng.choice(clients)
            debtor = debtor_by_id[self.rng.choice(sorted(debtor_by_id))]
            inv = self._invoice(n, client, debtor, date(2026, 10, 25) + timedelta(days=self.rng.randint(0, 7)),
                                DAY2_CUTOFF)
            d2.invoices.append(inv)

        self.manifest["day2"] = {
            "rows": {e: len(getattr(d2, e)) for e in ("clients", "debtors", "invoices", "payments")},
            "rating_changes": rating_changes,
            "replayed_unchanged_debtors": sorted(d["debtor_id"] for d in replayed),
            "new_debtor": new_debtor["debtor_id"],
            "settled_invoices": sorted(i["invoice_id"] for i in settled),
            "stale_updates": sorted(i["invoice_id"] for i in stale),
            "new_invoices": 100,
        }
        return {"day1": d1, "day2": d2}


def write(out: str | Path, seed: int = 42, **sizes: int) -> dict:
    """Write both batches + manifest. ``sizes`` overrides n_clients / n_debtors / n_invoices."""
    out = Path(out)
    gen = Generator(seed=seed, **sizes)
    batches = gen.build()
    for batch_name, batch in batches.items():
        for entity in ("clients", "debtors", "invoices", "payments"):
            rows = getattr(batch, entity)
            target = out / batch_name / entity
            target.mkdir(parents=True, exist_ok=True)
            with open(target / "part-00000.jsonl", "w") as fh:
                for row in rows:
                    fh.write(json.dumps(row, default=_json_default) + "\n")
    (out / "manifest.json").write_text(json.dumps(gen.manifest, indent=2, default=_json_default))
    return gen.manifest


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--out", default="data/landing")
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()
    manifest = write(args.out, args.seed)
    print(json.dumps({b: m["rows"] for b, m in manifest.items()}, indent=2))


if __name__ == "__main__":
    main()
